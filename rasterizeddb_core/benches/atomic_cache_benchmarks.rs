use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0},
    hint::black_box,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use rasterizeddb_core::cache::atomic_cache::AtomicGenericCache;

// Test data generation
fn generate_test_keys(count: usize, pattern: &str) -> Vec<u64> {
    match pattern {
        "sequential" => (0..count as u64).collect(),
        "random" => {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            (0..count)
                .map(|i| {
                    let mut hasher = DefaultHasher::new();
                    (i * 17 + 42).hash(&mut hasher);
                    hasher.finish()
                })
                .collect()
        },
        "clustered" => {
            // Keys that will likely hash to similar buckets
            (0..count as u64)
                .map(|i| i * 1024) // Every 1024th number
                .collect()
        },
        _ => (0..count as u64).collect(),
    }
}

fn generate_test_values(count: usize) -> Vec<u64> {
    (0..count as u64).map(|i| i * 2 + 1).collect()
}

// Throughput-focused insertion benchmarks
fn bench_insert_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_throughput");
    
    for size in [1000, 10000, 100000, 1000000].iter() {
        // Set throughput measurement
        group.throughput(Throughput::Elements(*size as u64));
        
        let cache = AtomicGenericCache::<u64, u64>::new(0, *size * 2);
        let keys = generate_test_keys(*size, "sequential");
        let values = generate_test_values(*size);
        
        // Prefetch data
        unsafe {
            _mm_prefetch::<_MM_HINT_T0>(keys.as_ptr() as *const i8);
            _mm_prefetch::<_MM_HINT_T0>(values.as_ptr() as *const i8);
        }

        group.bench_with_input(
            BenchmarkId::new("sequential", size), 
            size, 
            |b, _| {
                b.iter(|| {
                    for (key, value) in keys.iter().zip(values.iter()) {
                        let _ = cache.insert(black_box(*key), black_box(*value));
                    }
                    cache.clear();
                })
            }
        );

        // Random pattern
        let random_keys = generate_test_keys(*size, "random");
        group.bench_with_input(
            BenchmarkId::new("random", size), 
            size, 
            |b, _| {
                b.iter(|| {
                    for (key, value) in random_keys.iter().zip(values.iter()) {
                        let _ = cache.insert(black_box(*key), black_box(*value));
                    }
                    cache.clear();
                })
            }
        );
    }
    group.finish();
}

// Get throughput benchmarks
fn bench_get_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_throughput");
    
    for size in [1000, 10000, 100000, 1000000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        let cache = AtomicGenericCache::<u64, u64>::new(0, *size);
        let keys = generate_test_keys(*size, "sequential");
        let values = generate_test_values(*size);
        
        // Pre-populate cache
        for (key, value) in keys.iter().zip(values.iter()) {
            let _ = cache.insert(*key, *value);
        }

        unsafe {
            _mm_prefetch::<_MM_HINT_T0>(keys.as_ptr() as *const i8);
        }

        group.bench_with_input(
            BenchmarkId::new("100_percent_hit", size), 
            size, 
            |b, _| {
                b.iter(|| {
                    for key in &keys {
                        black_box(cache.get(black_box(key)));
                    }
                })
            }
        );

        // 50% hit rate test
        let mut mixed_keys = keys[0..*size/2].to_vec();
        mixed_keys.extend(*size as u64 + 1..*size as u64 + 1 + *size as u64 / 2);
        
        group.bench_with_input(
            BenchmarkId::new("50_percent_hit", size), 
            size, 
            |b, _| {
                b.iter(|| {
                    for key in &mixed_keys {
                        black_box(cache.get(black_box(key)));
                    }
                })
            }
        );
    }
    group.finish();
}

// Concurrent throughput benchmarks
fn bench_concurrent_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_throughput");
    group.measurement_time(Duration::from_secs(10));
    
    for thread_count in [1, 2, 4, 8, 16].iter() {
        let ops_per_thread = 50000;
        let total_ops = thread_count * ops_per_thread;
        
        group.throughput(Throughput::Elements(total_ops as u64));
        
        let cache = Arc::new(AtomicGenericCache::<u64, u64>::new(0, total_ops));
        
        group.bench_with_input(
            BenchmarkId::new("threads", thread_count), 
            thread_count, 
            |b, &threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..threads).map(|thread_id| {
                        let cache_clone = Arc::clone(&cache);
                        thread::spawn(move || {
                            let start_key = thread_id as u64 * ops_per_thread as u64;
                            
                            for i in 0..ops_per_thread {
                                let key = start_key + i as u64;
                                let op = i % 10;
                                
                                if op < 7 {
                                    let _ = cache_clone.insert(black_box(key), black_box(key * 2));
                                } else if op < 9 {
                                    black_box(cache_clone.get(black_box(&key)));
                                } else {
                                    black_box(cache_clone.remove(black_box(&key)));
                                }
                            }
                        })
                    }).collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    
                    cache.clear();
                })
            }
        );
    }
    group.finish();
}

// Memory throughput under pressure
fn bench_memory_pressure_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pressure_throughput");
    
    for (cache_size, insert_count) in [(1000, 10000), (5000, 50000), (10000, 100000)].iter() {
        group.throughput(Throughput::Elements(*insert_count as u64));
        
        let cache = AtomicGenericCache::<u64, u64>::new(0, *cache_size);
        let keys = generate_test_keys(*insert_count, "sequential");
        let values = generate_test_values(*insert_count);

        group.bench_with_input(
            BenchmarkId::new("eviction_pressure", format!("{}_{}", cache_size, insert_count)), 
            &(cache_size, insert_count), 
            |b, _| {
                b.iter(|| {
                    for (key, value) in keys.iter().zip(values.iter()) {
                        let _ = cache.insert(black_box(*key), black_box(*value));
                    }
                    cache.clear();
                })
            }
        );
    }
    group.finish();
}

// Bulk operations throughput
fn bench_bulk_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_throughput");
    
    for size in [1000, 10000, 100000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        let cache = AtomicGenericCache::<u64, u64>::new(0, *size);
        let keys = generate_test_keys(*size, "random");
        let values = generate_test_values(*size);
        
        // Pre-populate for get operations
        for (key, value) in keys.iter().zip(values.iter()) {
            let _ = cache.insert(*key, *value);
        }

        group.bench_with_input(
            BenchmarkId::new("get_all_by_keys", size), 
            size, 
            |b, _| {
                b.iter(|| {
                    black_box(cache.get_all_by_keys(black_box(&keys)));
                })
            }
        );

        group.bench_with_input(
            BenchmarkId::new("remove_all", size), 
            size, 
            |b, _| {
                b.iter(|| {
                    // Re-populate before each iteration
                    for (key, value) in keys.iter().zip(values.iter()) {
                        let _ = cache.insert(*key, *value);
                    }
                    black_box(cache.remove_all(black_box(&keys)));
                })
            }
        );
    }
    group.finish();
}

// Bytes throughput (for cache data transfer)
fn bench_data_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("data_throughput");
    
    for size in [1000, 10000, 100000].iter() {
        // Each operation transfers key (8 bytes) + value (8 bytes) = 16 bytes
        let bytes_per_op = 16u64;
        group.throughput(Throughput::Bytes(*size as u64 * bytes_per_op));
        
        let cache = AtomicGenericCache::<u64, u64>::new(0, *size);
        let keys = generate_test_keys(*size, "sequential");
        let values = generate_test_values(*size);

        group.bench_with_input(
            BenchmarkId::new("insert_data_throughput", size), 
            size, 
            |b, _| {
                b.iter(|| {
                    for (key, value) in keys.iter().zip(values.iter()) {
                        let _ = cache.insert(black_box(*key), black_box(*value));
                    }
                    cache.clear();
                })
            }
        );

        // Pre-populate for read throughput
        for (key, value) in keys.iter().zip(values.iter()) {
            let _ = cache.insert(*key, *value);
        }

        group.bench_with_input(
            BenchmarkId::new("get_data_throughput", size), 
            size, 
            |b, _| {
                b.iter(|| {
                    for key in &keys {
                        black_box(cache.get(black_box(key)));
                    }
                })
            }
        );
    }
    group.finish();
}

// Custom throughput measurement with detailed metrics
fn bench_detailed_throughput_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("detailed_throughput_analysis");
    group.measurement_time(Duration::from_secs(5));
    
    let cache = AtomicGenericCache::<u64, u64>::new(0, 100000);
    let test_size = 50000;
    let keys = generate_test_keys(test_size, "random");
    let values = generate_test_values(test_size);
    
    // Pre-populate half the cache
    for (key, value) in keys.iter().take(test_size / 2).zip(values.iter()) {
        let _ = cache.insert(*key, *value);
    }

    group.throughput(Throughput::Elements(test_size as u64));
    
    group.bench_function("realistic_workload", |b| {
        b.iter_custom(|iters| {
            let mut total_time = Duration::new(0, 0);
            
            for _ in 0..iters {
                let start = Instant::now();
                
                // Simulate realistic access pattern
                for i in 0..test_size {
                    let key = keys[i];
                    let value = values[i];
                    
                    match i % 100 {
                        0..=69 => {
                            // 70% reads
                            black_box(cache.get(black_box(&key)));
                        },
                        70..=89 => {
                            // 20% writes
                            let _ = cache.insert(black_box(key), black_box(value));
                        },
                        90..=94 => {
                            // 5% updates (overwrite existing)
                            let existing_key = keys[i % (test_size / 2)];
                            let _ = cache.insert(black_box(existing_key), black_box(value));
                        },
                        _ => {
                            // 5% removes
                            black_box(cache.remove(black_box(&key)));
                        }
                    }
                }
                
                total_time += start.elapsed();
            }
            
            total_time
        });
    });
    
    group.finish();
}

// Scalability throughput analysis
fn bench_scalability_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalability_throughput");
    
    // Test how throughput scales with cache size
    for cache_size in [100, 1000, 10000, 100000, 1000000].iter() {
        let ops_count = cache_size / 2; // Half capacity operations
        group.throughput(Throughput::Elements(ops_count as u64));
        
        let cache = AtomicGenericCache::<u64, u64>::new(0, *cache_size);
        let keys = generate_test_keys(ops_count, "random");
        let values = generate_test_values(ops_count);

        group.bench_with_input(
            BenchmarkId::new("cache_size_scaling", cache_size), 
            cache_size, 
            |b, _| {
                b.iter(|| {
                    // Insert + get pattern
                    for (key, value) in keys.iter().zip(values.iter()) {
                        let _ = cache.insert(black_box(*key), black_box(*value));
                        black_box(cache.get(black_box(key)));
                    }
                    cache.clear();
                })
            }
        );
    }
    group.finish();
}

// High-frequency operations throughput
fn bench_high_frequency_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("high_frequency_throughput");
    group.measurement_time(Duration::from_secs(3));
    
    let cache = Arc::new(AtomicGenericCache::<u64, u64>::new(0, 10000));
    
    // Test with very high frequency operations on a small key set
    let hot_keys: Vec<u64> = (0..100).collect(); // 100 hot keys
    
    for thread_count in [1, 2, 4, 8].iter() {
        let ops_per_thread = 100000;
        let total_ops = thread_count * ops_per_thread;
        
        group.throughput(Throughput::Elements(total_ops as u64));
        
        group.bench_with_input(
            BenchmarkId::new("hot_keys_contention", thread_count), 
            thread_count, 
            |b, &threads| {
                b.iter(|| {
                    let handles: Vec<_> = (0..threads).map(|thread_id| {
                        let cache_clone = Arc::clone(&cache);
                        let hot_keys_clone = hot_keys.clone();
                        
                        thread::spawn(move || {
                            for i in 0..ops_per_thread {
                                let key_idx = (thread_id * ops_per_thread + i) % hot_keys_clone.len();
                                let key = hot_keys_clone[key_idx];
                                
                                if i % 3 == 0 {
                                    let _ = cache_clone.insert(black_box(key), black_box(key * 2));
                                } else {
                                    black_box(cache_clone.get(black_box(&key)));
                                }
                            }
                        })
                    }).collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                })
            }
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_get_throughput,
    bench_insert_throughput,
    bench_concurrent_throughput,
    bench_memory_pressure_throughput,
    bench_bulk_throughput,
    bench_data_throughput,
    bench_detailed_throughput_analysis,
    bench_scalability_throughput,
    bench_high_frequency_throughput
);

criterion_main!(benches);