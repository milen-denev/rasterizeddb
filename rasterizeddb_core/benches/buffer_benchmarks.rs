use std::{hint::black_box, io::Cursor};

use byteorder::{LittleEndian, ReadBytesExt};
use criterion::{criterion_group, criterion_main, Criterion};
use rasterizeddb_core::simds::little_endian::slice_to_u32_avx2;

fn get_read_u32_raw(buffer: Vec<u8>) -> u32 {
    let u32 = &buffer[0..4];
    let u32 = unsafe { slice_to_u32_avx2(u32) };
    u32
}

fn get_read_u32_cursor(mut buffer: Cursor<Vec<u8>>) -> u32 {
    let u32 = buffer.read_u32::<LittleEndian>().unwrap();
    u32
}

fn criterion_benchmark_buffers(c: &mut Criterion) { 
    c.bench_function("get_read_u32_raw", |b| b.iter(|| get_read_u32_raw(black_box(vec![200, 50, 30, 0]))));
    c.bench_function("get_read_u32_cursor", |b| b.iter(|| get_read_u32_cursor(black_box(Cursor::new(vec![200, 50, 30, 0])))));
}

criterion_group!(benches, criterion_benchmark_buffers);
criterion_main!(benches);