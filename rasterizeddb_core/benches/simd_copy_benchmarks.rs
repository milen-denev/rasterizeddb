use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use rasterizeddb_core::instructions::{
    simd_memcpy, simd_memcpy_aggressive, simd_memcpy_avx2_multi,
};

fn rust_std_copy(dst: &mut [u8], src: &[u8]) {
    dst.copy_from_slice(src);
}

fn criterion_benchmark_simd_memcpy(c: &mut Criterion) {
    let size = 4096; // Benchmark with 4KB buffer
    let src: Vec<u8> = vec![55u8; size];
    let mut dst = vec![0u8; size];

    c.bench_function("rust_std_copy", |b| {
        b.iter(|| {
            rust_std_copy(black_box(&mut dst), black_box(&src));
        })
    });

    c.bench_function("simd_memcpy", |b| {
        b.iter(|| unsafe {
            simd_memcpy(black_box(&mut dst), black_box(&src));
        })
    });

    c.bench_function("simd_memcpy_avx2_multi", |b| {
        b.iter(|| unsafe {
            simd_memcpy_avx2_multi(black_box(&mut dst), black_box(&src));
        })
    });

    c.bench_function("simd_memcpy_aggressive", |b| {
        b.iter(|| unsafe {
            simd_memcpy_aggressive(black_box(&mut dst), black_box(&src));
        })
    });
}

criterion_group!(benches, criterion_benchmark_simd_memcpy);
criterion_main!(benches);
