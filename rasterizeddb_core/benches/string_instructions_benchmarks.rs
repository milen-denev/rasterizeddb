use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use rasterizeddb_core::core::{processor::transformer::ComparerOperation, row::common::simd_compare_strings};

use std::{hint::black_box, str};

// Helper functions for standard library comparisons
#[inline(always)]
fn std_equals(s1: &[u8], s2: &[u8]) -> bool {
    s1 == s2 // Direct byte slice comparison is equivalent to str eq for valid UTF-8
}

#[inline(always)]
fn std_contains(text: &[u8], pattern: &[u8]) -> bool {
    let text_str = str::from_utf8(text).unwrap();
    let pattern_str = str::from_utf8(pattern).unwrap();
    text_str.contains(pattern_str)
}

#[inline(always)]
fn std_starts_with(text: &[u8], prefix: &[u8]) -> bool {
    let text_str = str::from_utf8(text).unwrap();
    let prefix_str = str::from_utf8(prefix).unwrap();
    text_str.starts_with(prefix_str)
}

#[inline(always)]
fn std_ends_with(text: &[u8], suffix: &[u8]) -> bool {
    let text_str = str::from_utf8(text).unwrap();
    let suffix_str = str::from_utf8(suffix).unwrap();
    text_str.ends_with(suffix_str)
}

fn criterion_benchmark_string_ops(c: &mut Criterion) {
    // Data for Equals
    let s_eq_32_a = "0123456789abcdef0123456789abcdef".as_bytes(); // len 32
    let s_eq_32_b = "0123456789abcdef0123456789abcdef".as_bytes(); // len 32
    let s_eq_32_c_diff = "0123456789abcdef0123456789abcDef".as_bytes(); // len 32, diff at end

    let s_eq_35_a = "0123456789abcdef0123456789abcdeXXX".as_bytes(); // len 35
    let s_eq_35_c_diff = "0123456789abcdef0123456789abcdeXXY".as_bytes(); // len 35, diff in remainder

    let s_short_other_len = "short".as_bytes(); // len 5
    let s_empty = "".as_bytes(); // len 0

    // Data for Contains, StartsWith, EndsWith
    // text_main length: 26 (abc) + 1 (_) + 26 (ABC) + 1 (_) + 10 (0-9) + 1 (_) + 26 (abc) = 91
    let text_main = "abcdefghijklmnopqrstuvwxyz_ABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789_abcdefghijklmnopqrstuvwxyz".as_bytes();
    let pattern_mid = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".as_bytes(); // len 26
    let pattern_start = "abcdefghijklmnopqrstuvwxyz".as_bytes(); // len 26
    let pattern_end = "ghijklmnopqrstuvwxyz".as_bytes(); // len 20, (matches end of text_main)
    let pattern_not_found = "not_in_the_text_at_all_12345".as_bytes(); // len 29

    let mut group = c.benchmark_group("string_operations");

    // Equals
    for (id_suffix, s1, s2) in [
        ("len32_match", s_eq_32_a, s_eq_32_b),
        ("len35_mismatch_content", s_eq_35_a, s_eq_35_c_diff),
        ("mismatch_len", s_eq_32_a, s_short_other_len),
    ] {
        group.bench_with_input(
            BenchmarkId::new("simd_equals", id_suffix),
            &(s1, s2),
            |b, (s1, s2)| {
                b.iter(|| {
                    simd_compare_strings(black_box(s1), black_box(s2), &ComparerOperation::Equals)
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("std_equals", id_suffix),
            &(s1, s2),
            |b, (s1, s2)| b.iter(|| std_equals(black_box(s1), black_box(s2))),
        );
    }

    // NotEquals
    for (id_suffix, s1, s2, expected_simd, expected_std) in [
        (
            "len32_mismatch_content",
            s_eq_32_a,
            s_eq_32_c_diff,
            true,
            true,
        ),
        ("len32_match", s_eq_32_a, s_eq_32_b, false, false),
        ("mismatch_len", s_eq_32_a, s_short_other_len, true, true),
    ] {
        group.bench_with_input(
            BenchmarkId::new("simd_not_equals", id_suffix),
            &(s1, s2),
            |b, (s1, s2)| {
                b.iter(|| {
                    let res = simd_compare_strings(
                        black_box(s1),
                        black_box(s2),
                        &ComparerOperation::NotEquals,
                    );
                    assert_eq!(res, expected_simd);
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("std_not_equals", id_suffix),
            &(s1, s2),
            |b, (s1, s2)| {
                b.iter(|| {
                    let res = !std_equals(black_box(s1), black_box(s2));
                    assert_eq!(res, expected_std);
                })
            },
        );
    }

    // Contains
    for (id_suffix, text, pattern, expected_simd, expected_std) in [
        ("match_mid", text_main, pattern_mid, true, true),
        ("mismatch", text_main, pattern_not_found, false, false),
        // SIMD contains returns false for empty pattern, std::str::contains returns true
        ("empty_pattern", text_main, s_empty, false, true),
    ] {
        group.bench_with_input(
            BenchmarkId::new("simd_contains", id_suffix),
            &(text, pattern),
            |b, (text, pattern)| {
                b.iter(|| {
                    let res = simd_compare_strings(
                        black_box(text),
                        black_box(pattern),
                        &ComparerOperation::Contains,
                    );
                    assert_eq!(res, expected_simd);
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("std_contains", id_suffix),
            &(text, pattern),
            |b, (text, pattern)| {
                b.iter(|| {
                    let res = std_contains(black_box(text), black_box(pattern));
                    assert_eq!(res, expected_std);
                })
            },
        );
    }

    // StartsWith
    for (id_suffix, text, pattern, expected_simd, expected_std) in [
        ("match", text_main, pattern_start, true, true),
        ("mismatch", text_main, pattern_mid, false, false),
        ("empty_pattern", text_main, s_empty, true, true), // SIMD starts_with returns true for empty pattern
    ] {
        group.bench_with_input(
            BenchmarkId::new("simd_starts_with", id_suffix),
            &(text, pattern),
            |b, (text, pattern)| {
                b.iter(|| {
                    let res = simd_compare_strings(
                        black_box(text),
                        black_box(pattern),
                        &ComparerOperation::StartsWith,
                    );
                    assert_eq!(res, expected_simd);
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("std_starts_with", id_suffix),
            &(text, pattern),
            |b, (text, pattern)| {
                b.iter(|| {
                    let res = std_starts_with(black_box(text), black_box(pattern));
                    assert_eq!(res, expected_std);
                })
            },
        );
    }

    // EndsWith
    for (id_suffix, text, pattern, expected_simd, expected_std) in [
        ("match", text_main, pattern_end, true, true),
        ("mismatch", text_main, pattern_mid, false, false),
        ("empty_pattern", text_main, s_empty, true, true), // SIMD ends_with returns true for empty pattern
    ] {
        group.bench_with_input(
            BenchmarkId::new("simd_ends_with", id_suffix),
            &(text, pattern),
            |b, (text, pattern)| {
                b.iter(|| {
                    let res = simd_compare_strings(
                        black_box(text),
                        black_box(pattern),
                        &ComparerOperation::EndsWith,
                    );
                    assert_eq!(res, expected_simd);
                })
            },
        );
        group.bench_with_input(
            BenchmarkId::new("std_ends_with", id_suffix),
            &(text, pattern),
            |b, (text, pattern)| {
                b.iter(|| {
                    let res = std_ends_with(black_box(text), black_box(pattern));
                    assert_eq!(res, expected_std);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, criterion_benchmark_string_ops);
criterion_main!(benches);
