use std::arch::x86_64::*;

use super::transformer::ComparerOperation;

// Helper trait to convert number types to/from little-endian byte slices
pub trait FromLeBytes: Sized {
    fn from_le_bytes(bytes: &[u8]) -> Self;
    fn to_le_bytes(&self) -> Vec<u8>;
}

macro_rules! impl_from_le_bytes {
    ($($type:ty),*) => {
        $(
            impl FromLeBytes for $type {

                #[inline(always)]
                fn from_le_bytes(bytes: &[u8]) -> Self {
                    <$type>::from_le_bytes(bytes.try_into().expect("Invalid byte slice"))
                }

                #[inline(always)]
                fn to_le_bytes(&self) -> Vec<u8> {
                    (*self).to_le_bytes().to_vec() // Explicitly call the standard method
                }
            }
        )*
    };
}

// Implement the trait for all required types
impl_from_le_bytes!(i8, u8, i16, u16, i32, u32, i64, u64, i128, u128, f32, f64);


// Custom trait for converting from f64
pub trait FromF64: Sized {
    fn from_f64(value: f64) -> Self;
}

macro_rules! impl_from_f64 {
    ($($type:ty),*) => {
        $(
            impl FromF64 for $type {

                #[inline(always)]
                fn from_f64(value: f64) -> Self {
                    value.round() as $type
                }
            }
        )*
    };
}

// Implement the FromF64 trait for all numeric types
impl_from_f64!(i8, u8, i16, u16, i32, u32, i64, u64, i128, u128);

impl FromF64 for f32 {
    fn from_f64(value: f64) -> Self {
        value as f32
    }
}

impl FromF64 for f64 {
    fn from_f64(value: f64) -> Self {
        value
    }
}

// Custom trait for converting to f64
pub trait IntoF64 {
    fn into_f64(self) -> f64;
}

macro_rules! impl_into_f64 {
    ($($type:ty),*) => {
        $(
            impl IntoF64 for $type {
                
                #[inline(always)]
                fn into_f64(self) -> f64 {
                    self as f64
                }
            }
        )*
    };
}

// Implement the IntoF64 trait for all numeric types
impl_into_f64!(i8, u8, i16, u16, i32, u32, f32);

impl IntoF64 for i64 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

impl IntoF64 for u64 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

impl IntoF64 for i128 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

impl IntoF64 for u128 {
    fn into_f64(self) -> f64 {
        self as f64
    }
}

impl IntoF64 for f64 {
    fn into_f64(self) -> f64 {
        self
    }
}

/// Perform SIMD-based string comparisons on two slices of bytes (`&[u8]`).
/// The operation is determined by the `ComparerOperation` enum.
///
/// # Arguments
/// * `input1` - The first slice of bytes representing a string.
/// * `input2` - The second slice of bytes representing a string.
/// * `operation` - The comparison operation to perform.
///
/// # Returns
/// A `bool` indicating the result of the comparison.
///
/// # Panic
/// This function will panic if the input lengths are incompatible for the operation.
///
/// # Safety
/// This function uses unsafe Rust for SIMD intrinsics.
pub fn simd_compare_strings(input1: &[u8], input2: &[u8], operation: &ComparerOperation) -> bool {
    match operation {
        ComparerOperation::Equals => unsafe { simd_equals(input1, input2) },
        ComparerOperation::NotEquals => !unsafe { simd_equals(input1, input2) },
        ComparerOperation::Contains => unsafe { simd_contains(input1, input2) },
        ComparerOperation::StartsWith => unsafe { simd_starts_with(input1, input2) },
        ComparerOperation::EndsWith => unsafe { simd_ends_with(input1, input2) },
        _ => panic!("Unsupported operation for string types: {:?}", operation),
    }
}

/// SIMD-based implementation of string equality (`==`) for `&[u8]`.
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn simd_equals(input1: &[u8], input2: &[u8]) -> bool {
    if input1.len() != input2.len() {
        return false;
    }

    let len = input1.len();
    let mut i = 0;

    while i + 32 <= len {
        let chunk1 = _mm256_loadu_si256(input1.as_ptr().add(i) as *const __m256i);
        let chunk2 = _mm256_loadu_si256(input2.as_ptr().add(i) as *const __m256i);

        let cmp = _mm256_cmpeq_epi8(chunk1, chunk2);
        if _mm256_movemask_epi8(cmp) != -1 {
            return false;
        }

        i += 32;
    }

    // Handle remaining bytes
    for j in i..len {
        if input1[j] != input2[j] {
            return false;
        }
    }

    true
}

/// SIMD-based implementation to check if `input1` contains `input2`.
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn simd_contains(input1: &[u8], input2: &[u8]) -> bool {
    let target_len = input2.len();
    if target_len == 0 || target_len > input1.len() {
        return false;
    }

    for i in 0..=(input1.len() - target_len) {
        if simd_equals(&input1[i..i + target_len], input2) {
            return true;
        }
    }

    false
}

/// SIMD-based implementation to check if `input1` starts with `input2`.
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn simd_starts_with(input1: &[u8], input2: &[u8]) -> bool {
    let prefix_len = input2.len();
    if prefix_len > input1.len() {
        return false;
    }

    simd_equals(&input1[..prefix_len], input2)
}

/// SIMD-based implementation to check if `input1` ends with `input2`.
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn simd_ends_with(input1: &[u8], input2: &[u8]) -> bool {
    let suffix_len = input2.len();
    if suffix_len > input1.len() {
        return false;
    }

    simd_equals(&input1[input1.len() - suffix_len..], input2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::row_v2::transformer::ComparerOperation;

    #[test]
    fn test_simd_compare_strings_equals() {
        let input1 = "hello".as_bytes();
        let input2 = "hello".as_bytes();
        let input3 = "world".as_bytes();

        assert!(simd_compare_strings(input1, input2, &ComparerOperation::Equals));
        assert!(!simd_compare_strings(input1, input3, &ComparerOperation::Equals));
    }

    #[test]
    fn test_simd_compare_strings_not_equals() {
        let input1 = "hello".as_bytes();
        let input2 = "world".as_bytes();

        assert!(simd_compare_strings(input1, input2, &ComparerOperation::NotEquals));
        assert!(!simd_compare_strings(input1, input1, &ComparerOperation::NotEquals));
    }

    #[test]
    fn test_simd_compare_strings_contains() {
        let input1 = "hello world".as_bytes();
        let input2 = "world".as_bytes();
        let input3 = "rust".as_bytes();

        assert!(simd_compare_strings(input1, input2, &ComparerOperation::Contains));
        assert!(!simd_compare_strings(input1, input3, &ComparerOperation::Contains));
    }

    #[test]
    fn test_simd_compare_strings_starts_with() {
        let input1 = "hello world".as_bytes();
        let input2 = "hello".as_bytes();
        let input3 = "world".as_bytes();

        assert!(simd_compare_strings(input1, input2, &ComparerOperation::StartsWith));
        assert!(!simd_compare_strings(input1, input3, &ComparerOperation::StartsWith));
    }

    #[test]
    fn test_simd_compare_strings_ends_with() {
        let input1 = "hello world".as_bytes();
        let input2 = "world".as_bytes();
        let input3 = "hello".as_bytes();

        assert!(simd_compare_strings(input1, input2, &ComparerOperation::EndsWith));
        assert!(!simd_compare_strings(input1, input3, &ComparerOperation::EndsWith));
    }

    #[test]
    #[should_panic(expected = "Unsupported operation for string types")]
    fn test_simd_compare_strings_unsupported_operation() {
        let input1 = "hello".as_bytes();
        let input2 = "world".as_bytes();

        simd_compare_strings(input1, input2, &ComparerOperation::Greater);
    }
}