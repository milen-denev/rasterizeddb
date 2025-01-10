use std::arch::x86_64::*;

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
#[inline]
pub(crate) fn contains_subsequence(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || needle.len() > haystack.len() {
        return false;
    }

    // Get the lengths
    let needle_len = needle.len();
    let haystack_len = haystack.len();

    unsafe {
        // Use SIMD if available
        if is_x86_feature_detected!("sse2") {
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

    // Fallback: Naive search if SIMD is unavailable
    haystack.windows(needle_len).any(|window| window == needle)
}

#[inline]
pub(crate) fn compare_vecs_starts_with(vec1: &[u8], vec2: &[u8]) -> bool {
    return vec1.starts_with(vec2); 
}

#[inline]
pub(crate) fn compare_vecs_ends_with(vec1: &[u8], vec2: &[u8]) -> bool {
    return vec1.ends_with(vec2); 
}