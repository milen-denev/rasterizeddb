use std::arch::x86_64::*;

/// Converts a slice of 4 `u8` to a `u32` using little-endian ordering.
/// This function requires AVX2 support.
#[target_feature(enable = "avx2")]
pub unsafe fn slice_to_u32_avx2(slice: &[u8]) -> u32 {
    assert!(slice.len() >= 4, "Slice must have at least 4 bytes");

    // Load the first 4 bytes into an AVX2 register
    let input = _mm_loadu_si32(slice.as_ptr() as *const _);

    // Convert the 4 bytes into a u32 using little-endian ordering
    let result = _mm_cvtsi128_si32(input) as u32;

    result
}