use std::ptr;
#[cfg(target_arch = "x86_64")]
use std::arch::{asm, x86_64::*};

#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub unsafe fn compare_raw_vecs(
    vec_a: *mut u8,
    vec_b: *mut u8,
    vec_a_len: usize,
    vec_b_len: usize,
) -> bool {
    let mut result: u8;

    if vec_a_len != vec_b_len {
        return false;
    }

    unsafe {
        asm!(
            "repe cmpsb",
            inout("rsi") vec_a => _,
            inout("rdi") vec_b => _,
            inout("rcx") vec_a_len => _,
            out("al") result
        );
    }

    result == 0
}

/// Hints that the cache line at `p` will be read soon and should be kept in
/// L1: `prefetcht0` on x86_64, `prfm pldl1keep` on aarch64, nothing
/// elsewhere. A hint only, so any address is safe, mapped or not.
#[inline(always)]
pub fn prefetch_read_l1(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch never faults.
    unsafe {
        _mm_prefetch::<_MM_HINT_T0>(p as *const i8)
    };
    #[cfg(target_arch = "aarch64")]
    // SAFETY: prfm never faults and touches no register but its operand.
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, preserves_flags, readonly))
    };
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = p;
}

#[inline(always)]
#[track_caller]
pub fn copy_vec_to_ptr(vec: &[u8], dst: *mut u8) {
    unsafe {
        #[cfg(debug_assertions)]
        {
            // Debug assertions to ensure safety
            debug_assert!(dst.is_null() == false, "Destination pointer is null");
            debug_assert!(vec.len() > 0, "Source vector is empty");

            // Pointer variables
            let const_dst = dst as *const u8;
            let vec_ptr = vec.as_ptr();
            let vec_len = vec.len();
            let vec_ptr_end = vec_ptr.add(vec_len);

            let dst_end = dst.add(vec_len) as *const u8;

            // Check overlapping
            debug_assert!(dst_end.is_null() == false, "Destination pointer is null");
            debug_assert!(vec_ptr_end.is_null() == false, "Source vector is null");
            debug_assert!(
                const_dst < vec_ptr || const_dst > vec_ptr_end,
                "Memory regions overlap"
            );
        }

        ptr::copy_nonoverlapping(vec.as_ptr(), dst, vec.len());
    }
}

#[inline(always)]
pub fn copy_ptr_to_vec(src: *const u8, vec: &mut Vec<u8>, len: usize) {
    unsafe {
        vec[..len].copy_from_slice(std::slice::from_raw_parts(src, len));
    }
}

#[inline(always)]
pub fn vec_from_ptr_safe(ptr: *mut u8, len: usize) -> Vec<u8> {
    let mut vec = vec![0_u8; len];
    copy_ptr_to_vec(ptr, &mut vec, len);
    vec
}

#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
pub unsafe fn ref_slice_mut(ptr: *mut u8, len: usize) -> &'static mut [u8] {
    std::slice::from_raw_parts_mut(ptr, len)
}

#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
pub unsafe fn ref_slice(ptr: *mut u8, len: usize) -> &'static [u8] {
    std::slice::from_raw_parts(ptr, len)
}

#[inline(always)]
#[allow(unsafe_op_in_unsafe_fn)]
pub unsafe fn copy_ptr_to_ptr(heap_data_1: *mut u8, heap_data_2: *mut u8, len: usize) {
    if heap_data_1.is_null() || heap_data_2.is_null() {
        panic!("Cannot copy from/to null pointer.");
    }
    unsafe {
        std::ptr::copy_nonoverlapping(heap_data_1, heap_data_2, len);
    }
}

#[cfg(target_arch = "x86_64")]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(dead_code)]
pub unsafe fn simd_memcpy(dst: &mut [u8], src: &[u8]) {
    let len = dst.len();

    let mut i = 0;

    // Copy 32 bytes at a time (AVX2 width)
    let chunk_size = 32;
    while i + chunk_size <= len {
        let src_ptr = src.as_ptr().add(i) as *const __m256i;
        let dst_ptr = dst.as_mut_ptr().add(i) as *mut __m256i;

        let val = _mm256_loadu_si256(src_ptr);
        _mm256_storeu_si256(dst_ptr, val);

        i += chunk_size;
    }

    // Copy any remaining bytes
    if i < len {
        ptr::copy_nonoverlapping(src.as_ptr().add(i), dst.as_mut_ptr().add(i), len - i);
    }
}

/// SIMD-accelerated memcpy using AVX-512 if available, falling back to multiple AVX2 stores.
///
/// # Safety
/// - dst and src must have at least `len` bytes.
/// - Slices must not overlap.
/// - This function does NOT check for CPU support. Check at runtime with is_x86_feature_detected!().
// #[allow(unsafe_op_in_unsafe_fn)]
// pub unsafe fn simd_memcpy_avx512(dst: &mut [u8], src: &[u8]) {
//     assert_eq!(dst.len(), src.len());
//     let len = dst.len();
//     let mut i = 0;

//     if is_x86_feature_detected!("avx512f") {
//         // Use AVX-512: 64 bytes per iteration
//         let chunk_size = 64;
//         while i + chunk_size <= len {
//             let src_ptr = src.as_ptr().add(i) as *const __m512i;
//             let dst_ptr = dst.as_mut_ptr().add(i) as *mut __m512i;

//             let val = _mm512_loadu_si512(src_ptr);
//             _mm512_storeu_si512(dst_ptr, val);

//             i += chunk_size;
//         }
//     } else if is_x86_feature_detected!("avx2") {
//         // Use two AVX2 stores per loop: 2 * 32 bytes = 64 bytes
//         let chunk_size = 64;
//         while i + chunk_size <= len {
//             let src_ptr0 = src.as_ptr().add(i) as *const __m256i;
//             let dst_ptr0 = dst.as_mut_ptr().add(i) as *mut __m256i;
//             let src_ptr1 = src.as_ptr().add(i + 32) as *const __m256i;
//             let dst_ptr1 = dst.as_mut_ptr().add(i + 32) as *mut __m256i;

//             let val0 = _mm256_loadu_si256(src_ptr0);
//             let val1 = _mm256_loadu_si256(src_ptr1);
//             _mm256_storeu_si256(dst_ptr0, val0);
//             _mm256_storeu_si256(dst_ptr1, val1);

//             i += chunk_size;
//         }
//     }

//     // Copy any remaining bytes with fallback
//     if i < len {
//         ptr::copy_nonoverlapping(src.as_ptr().add(i), dst.as_mut_ptr().add(i), len - i);
//     }
// }

#[cfg(target_arch = "x86_64")]
/// SIMD-accelerated memcpy using multiple AVX2 stores per iteration (128 bytes at a time).
///
/// # Safety
/// - dst and src must have at least `len` bytes.
/// - Slices must not overlap.
/// - This does NOT check for AVX2 support. Use is_x86_feature_detected!("avx2") before calling.
#[allow(unsafe_op_in_unsafe_fn)]
pub unsafe fn simd_memcpy_avx2_multi(dst: &mut [u8], src: &[u8]) {
    debug_assert_eq!(dst.len(), src.len());
    let len = dst.len();
    let mut i = 0;

    // Copy 128 bytes at a time (4 x 32 bytes AVX2 registers)
    let chunk_size = 128;

    while i + chunk_size <= len {
        let src_ptr0 = src.as_ptr().add(i) as *const __m256i;
        let dst_ptr0 = dst.as_mut_ptr().add(i) as *mut __m256i;
        let src_ptr1 = src.as_ptr().add(i + 32) as *const __m256i;
        let dst_ptr1 = dst.as_mut_ptr().add(i + 32) as *mut __m256i;
        let src_ptr2 = src.as_ptr().add(i + 64) as *const __m256i;
        let dst_ptr2 = dst.as_mut_ptr().add(i + 64) as *mut __m256i;
        let src_ptr3 = src.as_ptr().add(i + 96) as *const __m256i;
        let dst_ptr3 = dst.as_mut_ptr().add(i + 96) as *mut __m256i;

        let val0 = _mm256_loadu_si256(src_ptr0);
        let val1 = _mm256_loadu_si256(src_ptr1);
        let val2 = _mm256_loadu_si256(src_ptr2);
        let val3 = _mm256_loadu_si256(src_ptr3);

        _mm256_storeu_si256(dst_ptr0, val0);
        _mm256_storeu_si256(dst_ptr1, val1);
        _mm256_storeu_si256(dst_ptr2, val2);
        _mm256_storeu_si256(dst_ptr3, val3);

        i += chunk_size;
    }

    // Copy any remaining bytes
    if i < len {
        ptr::copy_nonoverlapping(src.as_ptr().add(i), dst.as_mut_ptr().add(i), len - i);
    }
}

#[cfg(target_arch = "x86_64")]
/// SIMD-accelerated memcpy with loop unrolling and prefetching.
/// This may outperform std::slice::copy_from_slice for large buffers on AVX2 CPUs.
///
/// # Safety
/// - dst and src must have at least `len` bytes.
/// - Slices must not overlap.
/// - This does NOT check for AVX2 support. Use is_x86_feature_detected!("avx2") before calling.
#[allow(unsafe_op_in_unsafe_fn)]
#[inline(always)]
pub unsafe fn simd_memcpy_aggressive(dst: &mut [u8], src: &[u8]) {
    let len = dst.len();
    let mut i = 0;

    // Unroll four AVX2 (32-byte) stores per iteration (128 bytes at a time)
    let chunk_size = 128;

    while i + chunk_size <= len {
        // Prefetch next chunk
        _mm_prefetch(src.as_ptr().add(i + chunk_size) as *const i8, _MM_HINT_T0);

        let src_ptr0 = src.as_ptr().add(i) as *const __m256i;
        let dst_ptr0 = dst.as_mut_ptr().add(i) as *mut __m256i;
        let src_ptr1 = src.as_ptr().add(i + 32) as *const __m256i;
        let dst_ptr1 = dst.as_mut_ptr().add(i + 32) as *mut __m256i;
        let src_ptr2 = src.as_ptr().add(i + 64) as *const __m256i;
        let dst_ptr2 = dst.as_mut_ptr().add(i + 64) as *mut __m256i;
        let src_ptr3 = src.as_ptr().add(i + 96) as *const __m256i;
        let dst_ptr3 = dst.as_mut_ptr().add(i + 96) as *mut __m256i;

        let val0 = _mm256_loadu_si256(src_ptr0);
        let val1 = _mm256_loadu_si256(src_ptr1);
        let val2 = _mm256_loadu_si256(src_ptr2);
        let val3 = _mm256_loadu_si256(src_ptr3);

        _mm256_storeu_si256(dst_ptr0, val0);
        _mm256_storeu_si256(dst_ptr1, val1);
        _mm256_storeu_si256(dst_ptr2, val2);
        _mm256_storeu_si256(dst_ptr3, val3);

        i += chunk_size;
    }

    // Copy any remaining bytes
    if i < len {
        ptr::copy_nonoverlapping(src.as_ptr().add(i), dst.as_mut_ptr().add(i), len - i);
    }
}

/// `compare_raw_vecs` off x86_64 (no `repe cmpsb`): the same byte equality.
#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
pub unsafe fn compare_raw_vecs(vec_a: *mut u8, vec_b: *mut u8, vec_a_len: usize, vec_b_len: usize) -> bool {
    vec_a_len == vec_b_len
        && unsafe { std::slice::from_raw_parts(vec_a, vec_a_len) == std::slice::from_raw_parts(vec_b, vec_b_len) }
}

/// The AVX2 copies off x86_64: `copy_from_slice`, which is `memcpy`.
#[cfg(not(target_arch = "x86_64"))]
#[allow(dead_code)]
pub unsafe fn simd_memcpy(dst: &mut [u8], src: &[u8]) {
    dst.copy_from_slice(&src[..dst.len()]);
}

#[cfg(not(target_arch = "x86_64"))]
pub unsafe fn simd_memcpy_avx2_multi(dst: &mut [u8], src: &[u8]) {
    dst.copy_from_slice(&src[..dst.len()]);
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
pub unsafe fn simd_memcpy_aggressive(dst: &mut [u8], src: &[u8]) {
    dst.copy_from_slice(&src[..dst.len()]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_memcpy() {
        let src: Vec<u8> = (0..100).collect();
        let mut dst = vec![0u8; 100];
        unsafe { simd_memcpy(&mut dst, &src) }
        assert_eq!(src, dst);
    }

    // #[test]
    // fn test_simd_memcpy_avx512() {
    //     let src: Vec<u8> = (0..256).collect();
    //     let mut dst = vec![0u8; 256];
    //     unsafe { simd_memcpy_avx512(&mut dst, &src) }
    //     assert_eq!(src, dst);
    // }

    #[test]
    fn test_simd_memcpy_avx2_multi() {
        let src: Vec<u8> = (0..255).collect();
        let mut dst = vec![0u8; 255];
        unsafe { simd_memcpy_avx2_multi(&mut dst, &src) }
        assert_eq!(src, dst);
    }

    #[test]
    fn test_simd_memcpy_aggressive() {
        let src: Vec<u8> = (0..255).collect();
        let mut dst = vec![0u8; 255];
        unsafe { simd_memcpy_aggressive(&mut dst, &src) }
        assert_eq!(src, dst);
    }
}
