use std::{
    arch::x86_64::{_mm_prefetch, _MM_HINT_T0},
    hint::black_box,
};

use byteorder::{LittleEndian, ReadBytesExt};
use criterion::{criterion_group, criterion_main, Criterion};
use rasterizeddb_core::simds::endianess::{read_u128, read_u64};


fn get_read_u64_raw(ptr: *const u8) -> u64 {
    let u64 = unsafe { read_u64(ptr) };
    u64
}

fn get_read_u64_crate(slice: &[u8; 8]) -> u64 {
    let u64 = slice.as_slice().read_u64::<LittleEndian>().unwrap();
    u64
}

fn get_read_u128_raw(ptr: *const u8) -> u128 {
    let u128 = unsafe { read_u128(ptr) };
    u128
}

fn get_read_u128_crate(slice: &[u8; 16]) -> u128 {
    let u128 = slice.as_slice().read_u128::<LittleEndian>().unwrap();
    u128
}

fn criterion_benchmark_buffers(c: &mut Criterion) {
    let data_8: [u8; 8] = [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]; // 0x123456789ABCDEF0
    let data_16: [u8; 16] = [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0];

    let _8_ptr = data_8.as_ptr();
    let _16_ptr = data_16.as_ptr();

    unsafe {
        _mm_prefetch::<_MM_HINT_T0>(_8_ptr as *const i8);
        _mm_prefetch::<_MM_HINT_T0>(_16_ptr as *const i8);
    }

    c.bench_function("get_read_u64_raw",
        |b| b.iter(||
        {
            get_read_u64_raw(black_box(_8_ptr));
        })
    );

    c.bench_function("get_read_u64_crate",
        |b| b.iter(|| get_read_u64_crate(black_box(&data_8)))
    );

    c.bench_function("get_read_u128_raw",
        |b| b.iter(||
        {
            get_read_u128_raw(black_box(_16_ptr));
        })
    );

    c.bench_function("get_read_u128_crate",
        |b| b.iter(|| get_read_u128_crate(black_box(&data_16)))
    );
}

criterion_group!(benches, criterion_benchmark_buffers);
criterion_main!(benches);
