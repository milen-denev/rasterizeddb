use std::arch::{asm, x86_64::*};

/// Converts a slice of 4 `u8` to a `u32` using little-endian ordering.
/// This function requires SSE2 support.
#[target_feature(enable = "sse2")]
pub unsafe fn slice_to_u32_avx2(slice: &[u8]) -> u32 {
    assert!(slice.len() >= 4, "Slice must have at least 4 bytes");

    // Load the first 4 bytes into an AVX2 register
    let input = _mm_loadu_si32(slice.as_ptr() as *const _);

    // Convert the 4 bytes into a u32 using little-endian ordering
    let result = _mm_cvtsi128_si32(input) as u32;

    result
}

#[inline(always)]
pub unsafe fn read_big_endian_u64(ptr: *const u8) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        {
            let val: u64;
            asm!(
                "mov {0}, qword ptr [{1}]",
                out(reg) val,
                in(reg) ptr,
                options(nostack, pure, readonly)
            );
            return val.to_be();
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        // Portable fallback for non-x86_64 CPUs
        core::ptr::read_unaligned(ptr as *const u64).to_be()
    }
}