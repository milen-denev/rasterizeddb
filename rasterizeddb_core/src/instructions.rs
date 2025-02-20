use std::{
    arch::{asm, x86_64::*},
    mem::ManuallyDrop,
    ptr,
};

#[inline]
pub(crate) fn compare_vecs_eq(vec1: &[u8], vec2: &[u8]) -> bool {
    // If lengths differ, the vectors can't be equal.
    if vec1.len() != vec2.len() {
        return false;
    }

    return vec1.eq(vec2);
}

#[inline]
pub(crate) fn compare_vecs_ne(vec1: &[u8], vec2: &[u8]) -> bool {
    // If lengths differ, the vectors can't be equal.
    if vec1.len() != vec2.len() {
        return true;
    }

    return vec1.ne(vec2);
}

/// Check if `haystack` contains `needle`
#[inline(always)]
pub(crate) fn contains_subsequence(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }

    // Get the lengths
    let needle_len = needle.len();
    let haystack_len = haystack.len();

    unsafe {
        // If x86_64 is used
        #[cfg(target_arch = "x86_64")]
        {
            let needle_first_byte = _mm_set1_epi8(needle[0] as i8);
            let mut i = 0;

            while i <= haystack_len - needle_len {
                // Load 16 bytes from haystack using SSE2
                let chunk = _mm_loadu_si128(haystack[i..].as_ptr() as *const __m128i);

                // Compare with first byte of needle
                let cmp_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(chunk, needle_first_byte));

                // Process matching positions
                for offset in 0..16 {
                    if cmp_mask & (1 << offset) != 0 {
                        // Check for full needle match
                        if haystack[i + offset..].starts_with(needle) {
                            return true;
                        }
                    }
                }

                i += 16;
            }

            return false;
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        // Fallback: Naive search if SIMD is unavailable
        haystack.windows(needle_len).any(|window| window == needle)
    }
}

#[inline]
pub(crate) fn compare_vecs_starts_with(vec1: &[u8], vec2: &[u8]) -> bool {
    return vec1.starts_with(vec2);
}

#[inline]
pub(crate) fn compare_vecs_ends_with(vec1: &[u8], vec2: &[u8]) -> bool {
    return vec1.ends_with(vec2);
}

#[inline(always)]
pub unsafe fn compare_raw_vecs(
    vec_a: *mut u8,
    vec_b: *mut u8,
    vec_a_len: u32,
    vec_b_len: u32,
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

#[allow(unused)]
pub fn copy_vec_to_ptr(vec: &[u8], dst: *mut u8) {
    unsafe {
        ptr::copy_nonoverlapping(vec.as_ptr(), dst, vec.len());
    }
}

pub fn copy_ptr_to_vec(src: *const u8, vec: &mut Vec<u8>, len: usize) {
    unsafe {
        vec[..len].copy_from_slice(std::slice::from_raw_parts(src, len));
    }
}

pub fn vec_from_ptr_safe(ptr: *mut u8, len: usize) -> Vec<u8> {
    let mut vec = vec![0_u8; len];
    copy_ptr_to_vec(ptr, &mut vec, len);
    vec
}

pub unsafe fn ref_vec(ptr: *mut u8, len: usize) -> ManuallyDrop<Vec<u8>> {
    unsafe {
        let vec = Vec::from_raw_parts(ptr, len, len);
        let manual = ManuallyDrop::new(vec);
        manual
    }
}
