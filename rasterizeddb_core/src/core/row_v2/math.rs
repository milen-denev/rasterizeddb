use crate::{core::{db_type::DbType, row_v2::transformer::MathOperation}, memory_pool::{MemoryBlock, MEMORY_POOL}};

/// Function to perform a mathematical operation on two byte slices 
/// each representing a numeric type. The numeric type is determined by the `db_type` parameter.
/// The operation is specified by the `operation` parameter.
/// The function returns a `MemoryBlock` containing the result of the operation.
/// The function uses a generic approach to handle different numeric types.
/// `&[u8]` must be in little-endian format.
#[inline(always)]
pub fn perform_math_operation(input1: &[u8], input2: &[u8], db_type: DbType, operation: MathOperation) -> MemoryBlock {
    assert_eq!(input1.len(), input2.len(), "Input slices must have the same length");

    match db_type {
        DbType::I8 => generic_operation::<i8>(input1, input2, operation),
        DbType::U8 => generic_operation::<u8>(input1, input2, operation),
        DbType::I16 => generic_operation::<i16>(input1, input2, operation),
        DbType::U16 => generic_operation::<u16>(input1, input2, operation),
        DbType::I32 => generic_operation::<i32>(input1, input2, operation),
        DbType::U32 => generic_operation::<u32>(input1, input2, operation),
        DbType::I64 => generic_operation::<i64>(input1, input2, operation),
        DbType::U64 => generic_operation::<u64>(input1, input2, operation),
        DbType::I128 => generic_operation::<i128>(input1, input2, operation),
        DbType::U128 => generic_operation::<u128>(input1, input2, operation),
        DbType::F32 => generic_operation::<f32>(input1, input2, operation),
        DbType::F64 => generic_operation::<f64>(input1, input2, operation),
        _ => panic!("Unsupported data type for math operation: {:?}", db_type),
    }
}

#[inline(always)]
fn generic_operation<T>(input1: &[u8], input2: &[u8], operation: MathOperation) -> MemoryBlock
where
    T: Copy + Default + std::ops::Add<Output = T>
        + std::ops::Sub<Output = T>
        + std::ops::Mul<Output = T>
        + std::ops::Div<Output = T>
        + PartialEq
        + FromLeBytes
        + FromF64
        + IntoF64,
{
    let size = std::mem::size_of::<T>();
    
    assert!(
        input1.len() % size == 0,
        "Input slices must be divisible by the size of the target type"
    );

    let block = MEMORY_POOL.acquire(input1.len());
    let mut block_wrapper = unsafe { block.into_wrapper() };
    let result = block_wrapper.as_vec_mut();

    let num1 = T::from_le_bytes(input1);
    let num2 = T::from_le_bytes(input2);

    let res = match operation {
        MathOperation::Add => num1 + num2,
        MathOperation::Subtract => num1 - num2,
        MathOperation::Multiply => num1 * num2,
        MathOperation::Divide => {
            assert!(num2 != T::default(), "Division by zero is not allowed");
            num1 / num2
        },
        MathOperation::Exponent => {
            let base: f64 = num1.into_f64();
            let exp: f64 = num2.into_f64();
            T::from_f64(base.powf(exp))
        }
        MathOperation::Root => {
            let value: f64 = num1.into_f64();
            let root: f64 = num2.into_f64();
            assert!(root != 0.0, "Root by zero is not allowed");
            T::from_f64(value.powf(1.0 / root))
        }
    };

    result.copy_from_slice(&res.to_le_bytes());

    block
}

// Helper trait to convert number types to/from little-endian byte slices
trait FromLeBytes: Sized {
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
trait FromF64: Sized {
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
trait IntoF64 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::row_v2::transformer::MathOperation;
    
    // Helper function to convert a value to little-endian bytes
    fn to_le_bytes<T>(value: T) -> Vec<u8>
    where
        T: Copy + Default + FromLeBytes,
    {
        FromLeBytes::to_le_bytes(&value)
    }
    
    // Helper function to extract value from memory block
    fn from_memory_block<T>(block: MemoryBlock) -> T
    where
        T: Copy + Default + FromLeBytes,
    {
        let block_wrapper = unsafe { block.into_wrapper() };
        let bytes = block_wrapper.as_vec();
        T::from_le_bytes(bytes)
    }

    #[test]
    fn test_add_operation() {
        // Integer addition
        let result = perform_math_operation(&to_le_bytes(5i32), &to_le_bytes(3i32), DbType::I32, MathOperation::Add);
        assert_eq!(from_memory_block::<i32>(result), 8);

        // Unsigned integer addition
        let result = perform_math_operation(&to_le_bytes(5u64), &to_le_bytes(3u64), DbType::U64, MathOperation::Add);
        assert_eq!(from_memory_block::<u64>(result), 8);

        // Floating point addition
        let result = perform_math_operation(&to_le_bytes(5.5f32), &to_le_bytes(3.2f32), DbType::F32, MathOperation::Add);
        assert!((from_memory_block::<f32>(result) - 8.7).abs() < 0.001);

        let result = perform_math_operation(&to_le_bytes(5.5f64), &to_le_bytes(3.2f64), DbType::F64, MathOperation::Add);
        assert!((from_memory_block::<f64>(result) - 8.7).abs() < 0.001);
    }

    #[test]
    fn test_subtract_operation() {
        // Integer subtraction
        let result = perform_math_operation(&to_le_bytes(5i32), &to_le_bytes(3i32), DbType::I32, MathOperation::Subtract);
        assert_eq!(from_memory_block::<i32>(result), 2);

        // Unsigned integer subtraction
        let result = perform_math_operation(&to_le_bytes(5u64), &to_le_bytes(3u64), DbType::U64, MathOperation::Subtract);
        assert_eq!(from_memory_block::<u64>(result), 2);

        // Floating point subtraction
        let result = perform_math_operation(&to_le_bytes(5.5f32), &to_le_bytes(3.2f32), DbType::F32, MathOperation::Subtract);
        assert!((from_memory_block::<f32>(result) - 2.3).abs() < 0.001);

        let result = perform_math_operation(&to_le_bytes(5.5f64), &to_le_bytes(3.2f64), DbType::F64, MathOperation::Subtract);
        assert!((from_memory_block::<f64>(result) - 2.3).abs() < 0.001);
    }

    #[test]
    fn test_multiply_operation() {
        // Integer multiplication
        let result = perform_math_operation(&to_le_bytes(5i32), &to_le_bytes(3i32), DbType::I32, MathOperation::Multiply);
        assert_eq!(from_memory_block::<i32>(result), 15);

        // Unsigned integer multiplication
        let result = perform_math_operation(&to_le_bytes(5u64), &to_le_bytes(3u64), DbType::U64, MathOperation::Multiply);
        assert_eq!(from_memory_block::<u64>(result), 15);

        // Floating point multiplication
        let result = perform_math_operation(&to_le_bytes(5.5f32), &to_le_bytes(3.0f32), DbType::F32, MathOperation::Multiply);
        assert!((from_memory_block::<f32>(result) - 16.5).abs() < 0.001);

        let result = perform_math_operation(&to_le_bytes(5.5f64), &to_le_bytes(3.0f64), DbType::F64, MathOperation::Multiply);
        assert!((from_memory_block::<f64>(result) - 16.5).abs() < 0.001);
    }

    #[test]
    fn test_divide_operation() {
        // Integer division
        let result = perform_math_operation(&to_le_bytes(15i32), &to_le_bytes(3i32), DbType::I32, MathOperation::Divide);
        assert_eq!(from_memory_block::<i32>(result), 5);

        // Unsigned integer division
        let result = perform_math_operation(&to_le_bytes(15u64), &to_le_bytes(3u64), DbType::U64, MathOperation::Divide);
        assert_eq!(from_memory_block::<u64>(result), 5);

        // Floating point division
        let result = perform_math_operation(&to_le_bytes(16.5f32), &to_le_bytes(3.0f32), DbType::F32, MathOperation::Divide);
        assert!((from_memory_block::<f32>(result) - 5.5).abs() < 0.001);

        let result = perform_math_operation(&to_le_bytes(16.5f64), &to_le_bytes(3.0f64), DbType::F64, MathOperation::Divide);
        assert!((from_memory_block::<f64>(result) - 5.5).abs() < 0.001);
    }

    #[test]
    fn test_exponent_operation() {
        // Integer exponentiation
        let result = perform_math_operation(&to_le_bytes(2i32), &to_le_bytes(3i32), DbType::I32, MathOperation::Exponent);
        assert_eq!(from_memory_block::<i32>(result), 8);

        // Unsigned integer exponentiation
        let result = perform_math_operation(&to_le_bytes(2u64), &to_le_bytes(3u64), DbType::U64, MathOperation::Exponent);
        assert_eq!(from_memory_block::<u64>(result), 8);

        // Floating point exponentiation
        let result = perform_math_operation(&to_le_bytes(2.0f32), &to_le_bytes(1.5f32), DbType::F32, MathOperation::Exponent);
        assert!((from_memory_block::<f32>(result) - 2.83).abs() < 0.01);

        let result = perform_math_operation(&to_le_bytes(2.0f64), &to_le_bytes(1.5f64), DbType::F64, MathOperation::Exponent);
        assert!((from_memory_block::<f64>(result) - 2.83).abs() < 0.01);
    }

    #[test]
    fn test_root_operation() {
        // Integer root
        let result = perform_math_operation(&to_le_bytes(8i32), &to_le_bytes(3i32), DbType::I32, MathOperation::Root);
        assert_eq!(from_memory_block::<i32>(result), 2);

        // Unsigned integer root
        let result = perform_math_operation(&to_le_bytes(8u64), &to_le_bytes(3u64), DbType::U64, MathOperation::Root);
        assert_eq!(from_memory_block::<u64>(result), 2);

        // Floating point root
        let result = perform_math_operation(&to_le_bytes(9.0f32), &to_le_bytes(2.0f32), DbType::F32, MathOperation::Root);
        assert!((from_memory_block::<f32>(result) - 3.0).abs() < 0.001);

        let result = perform_math_operation(&to_le_bytes(9.0f64), &to_le_bytes(2.0f64), DbType::F64, MathOperation::Root);
        assert!((from_memory_block::<f64>(result) - 3.0).abs() < 0.001);
    }

    #[test]
    fn test_all_numeric_types() {
        // Test i8
        let result = perform_math_operation(&to_le_bytes(5i8), &to_le_bytes(3i8), DbType::I8, MathOperation::Add);
        assert_eq!(from_memory_block::<i8>(result), 8);

        // Test u8
        let result = perform_math_operation(&to_le_bytes(5u8), &to_le_bytes(3u8), DbType::U8, MathOperation::Add);
        assert_eq!(from_memory_block::<u8>(result), 8);

        // Test i16
        let result = perform_math_operation(&to_le_bytes(5i16), &to_le_bytes(3i16), DbType::I16, MathOperation::Add);
        assert_eq!(from_memory_block::<i16>(result), 8);

        // Test u16
        let result = perform_math_operation(&to_le_bytes(5u16), &to_le_bytes(3u16), DbType::U16, MathOperation::Add);
        assert_eq!(from_memory_block::<u16>(result), 8);

        // Test i128
        let result = perform_math_operation(&to_le_bytes(5i128), &to_le_bytes(3i128), DbType::I128, MathOperation::Add);
        assert_eq!(from_memory_block::<i128>(result), 8);

        // Test u128
        let result = perform_math_operation(&to_le_bytes(5u128), &to_le_bytes(3u128), DbType::U128, MathOperation::Add);
        assert_eq!(from_memory_block::<u128>(result), 8);
    }

    #[test]
    #[should_panic(expected = "Division by zero is not allowed")]
    fn test_divide_by_zero() {
        perform_math_operation(&to_le_bytes(5i32), &to_le_bytes(0i32), DbType::I32, MathOperation::Divide);
    }

    #[test]
    #[should_panic(expected = "Root by zero is not allowed")]
    fn test_root_by_zero() {
        perform_math_operation(&to_le_bytes(5i32), &to_le_bytes(0i32), DbType::I32, MathOperation::Root);
    }

    #[test]
    #[should_panic(expected = "attempt to add with overflow")]
    fn test_edge_cases() {
        // Test with minimum and maximum values
        let _result = perform_math_operation(
            &to_le_bytes(i32::MAX), &to_le_bytes(1i32), 
            DbType::I32, MathOperation::Add
        );
    }

    #[test]
    #[should_panic(expected = "Input slices must have the same length")]
    fn test_unequal_length_inputs() {
        let input1 = to_le_bytes(5i32);
        let input2 = to_le_bytes(3i16);
        perform_math_operation(&input1, &input2, DbType::I32, MathOperation::Add);
    }

    #[test]
    #[should_panic(expected = "Unsupported data type for math operation")]
    fn test_unsupported_type() {
        perform_math_operation(&to_le_bytes(5i32), &to_le_bytes(3i32), DbType::STRING, MathOperation::Add);
    }
}