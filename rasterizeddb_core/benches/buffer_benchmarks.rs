use std::{arch::x86_64::{_mm_prefetch, _MM_HINT_T0}, hint::black_box, io::Cursor};

use byteorder::{LittleEndian, ReadBytesExt};
use criterion::{criterion_group, criterion_main, Criterion};
use rasterizeddb_core::simds::little_endian::{read_big_endian_u64, slice_to_u32_avx2};

// fn get_read_u32_raw(buffer: Vec<u8>) -> u32 {
//     let u32 = &buffer[0..4];
//     let u32 = unsafe { slice_to_u32_avx2(u32) };
//     u32
// }

// fn get_read_u32_cursor(mut buffer: Cursor<Vec<u8>>) -> u32 {
//     let u32 = buffer.read_u32::<LittleEndian>().unwrap();
//     u32
// }

fn get_read_u64_raw(ptr: *const u8) -> u64 {
    let u64 = unsafe { read_big_endian_u64(ptr) };
    u64
}

fn get_read_u64_crate(slice: &[u8; 8]) -> u64 {
    let u64 = slice.as_slice().read_u64::<LittleEndian>().unwrap();
    u64
}

fn criterion_benchmark_buffers(c: &mut Criterion) { 
    // c.bench_function("get_read_u32_raw", 
    //     |b| b.iter(|| get_read_u32_raw(black_box(vec![200, 50, 30, 0])))
    // );

    // c.bench_function("get_read_u32_cursor", 
    //     |b| b.iter(|| get_read_u32_cursor(black_box(Cursor::new(vec![200, 50, 30, 0]))))
    // );

    let data: [u8; 8] = [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]; // 0x123456789ABCDEF0

    let be_ptr = data.as_ptr();

    unsafe { 
        _mm_prefetch::<_MM_HINT_T0>(be_ptr as *const i8);
    }

    c.bench_function("get_read_u64_raw", 
        |b| b.iter(|| 
        {
            get_read_u64_raw(black_box(be_ptr));
        })
    );

    c.bench_function("get_read_u64_crate", 
        |b| b.iter(|| get_read_u64_crate(black_box(&data)))
    );
}

criterion_group!(benches, criterion_benchmark_buffers);
criterion_main!(benches);