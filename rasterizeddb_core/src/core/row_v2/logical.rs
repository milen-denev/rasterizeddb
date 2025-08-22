use std::fmt::Debug;

use crate::core::db_type::DbType;

use super::{common::{simd_compare_strings, FromLeBytes, FromLeBytesUnsafe}, transformer::ComparerOperation};

#[inline(always)]
pub fn perform_comparison_operation(input1: &[u8], input2: &[u8], db_type: &DbType, operation: &ComparerOperation) -> bool {
    match db_type {
        DbType::I8 => generic_comparison::<i8>(input1, input2, operation),
        DbType::U8 => generic_comparison::<u8>(input1, input2, operation),
        DbType::I16 => generic_comparison::<i16>(input1, input2, operation),
        DbType::U16 => generic_comparison::<u16>(input1, input2, operation),
        DbType::I32 => generic_comparison::<i32>(input1, input2, operation),
        DbType::U32 => generic_comparison::<u32>(input1, input2, operation),
        DbType::I64 => generic_comparison::<i64>(input1, input2, operation),
        DbType::U64 => generic_comparison::<u64>(input1, input2, operation),
        DbType::I128 => generic_comparison::<i128>(input1, input2, operation),
        DbType::U128 => generic_comparison::<u128>(input1, input2, operation),
        DbType::F32 => generic_comparison::<f32>(input1, input2, operation),
        DbType::F64 => generic_comparison::<f64>(input1, input2, operation),
        DbType::STRING => string_comparison(input1, input2, operation),
        _ => panic!("Unsupported data type for comparison operation: {:?}", db_type),
    }
}

#[inline(always)]
fn generic_comparison<T>(input1: &[u8], input2: &[u8], operation: &ComparerOperation) -> bool
where
    T: Copy + Default + PartialEq + PartialOrd + FromLeBytes + FromLeBytesUnsafe + Debug,
{
    debug_assert_eq!(input1.len(), std::mem::size_of::<T>());
    debug_assert_eq!(input2.len(), std::mem::size_of::<T>());

    let num1 = unsafe { T::from_le_bytes_unchecked(input1) };
    let num2 = unsafe { T::from_le_bytes_unchecked(input2) };
    
    match operation {
        ComparerOperation::Equals => num1 == num2,
        ComparerOperation::NotEquals => num1 != num2,
        ComparerOperation::Greater => num1 > num2,
        ComparerOperation::GreaterOrEquals => num1 >= num2,
        ComparerOperation::Less => num1 < num2,
        ComparerOperation::LessOrEquals => num1 <= num2,
        ComparerOperation::Contains | ComparerOperation::StartsWith | ComparerOperation::EndsWith => {
            panic!("Unsupported operation for numeric types: {:?}", operation)
        }
    }
}

#[inline(always)]
fn string_comparison(input1: &[u8], input2: &[u8], operation: &ComparerOperation) -> bool {
    simd_compare_strings(input1, input2, operation)
    // let str1 = std::str::from_utf8(input1).expect("Invalid UTF-8 in input1");
    // let str2 = std::str::from_utf8(input2).expect("Invalid UTF-8 in input2");

    // match operation {
    //     ComparerOperation::Equals => str1 == str2,
    //     ComparerOperation::NotEquals => str1 != str2,
    //     ComparerOperation::Contains => str1.contains(str2),
    //     ComparerOperation::StartsWith => str1.starts_with(str2),
    //     ComparerOperation::EndsWith => str1.ends_with(str2),
    //     _ => panic!("Unsupported operation for string types: {:?}", operation),
    // }
}

#[cfg(test)]
mod tests {
    use crate::core::{db_type::DbType, row_v2::{logical::perform_comparison_operation, transformer::ComparerOperation}};
 
    #[test]
    fn test_numeric_comparisons() {
        let input1 = 10i32.to_le_bytes();
        let input2 = 5i32.to_le_bytes();

        assert!(perform_comparison_operation(&input1, &input1, &DbType::I32, &ComparerOperation::Equals));
        assert!(!perform_comparison_operation(&input1, &input2, &DbType::I32, &ComparerOperation::Equals));
        assert!(perform_comparison_operation(&input2, &input1, &DbType::I32, &ComparerOperation::Less));
        assert!(perform_comparison_operation(&input1, &input2, &DbType::I32, &ComparerOperation::Greater));
        assert!(perform_comparison_operation(&input1, &input1, &DbType::I32, &ComparerOperation::GreaterOrEquals));
        assert!(perform_comparison_operation(&input2, &input1, &DbType::I32, &ComparerOperation::LessOrEquals));
    }

    #[test]
    fn test_string_comparisons() {
        let input1 = "hello".as_bytes();
        let input2 = "world".as_bytes();
        let input3 = "hello".as_bytes();

        assert!(perform_comparison_operation(input1, input3, &DbType::STRING, &ComparerOperation::Equals));
        assert!(!perform_comparison_operation(input1, input2, &DbType::STRING, &ComparerOperation::Equals));
        assert!(perform_comparison_operation(input1, input2, &DbType::STRING, &ComparerOperation::NotEquals));
    }

    #[test]
    #[should_panic(expected = "Unsupported data type for comparison operation: DATETIME")]
    fn test_unsupported_db_type() {
        let input1 = [0u8; 4];
        let input2 = [0u8; 4];
        perform_comparison_operation(&input1, &input2, &DbType::DATETIME, &ComparerOperation::Equals);
    }

    #[test]
    #[should_panic(expected = "Unsupported operation for numeric types")]
    fn test_unsupported_operation_numeric() {
        let input1 = 5i32.to_le_bytes();
        let input2 = 10i32.to_le_bytes();
        perform_comparison_operation(&input1, &input2, &DbType::I32, &ComparerOperation::Contains);
    }

    #[test]
    #[should_panic(expected = "Unsupported operation for string types")]
    fn test_unsupported_operation_string() {
        let input1 = "hello".as_bytes();
        let input2 = "world".as_bytes();
        perform_comparison_operation(input1, input2, &DbType::STRING, &ComparerOperation::Greater);
    }
}