use itertools::Either;

use crate::{core::db_type::DbType, memory_pool::MemoryBlock};

use super::{logical::perform_comparison_operation, math::perform_math_operation};

pub struct ColumnTransformer {
    pub column_type: DbType,
    pub column_1: MemoryBlock,
    pub column_2: MemoryBlock,
    pub transformer_type: ColumnTransformerType,
    pub next: Option<Next>
}

pub enum ColumnTransformerType {
    MathOperation(MathOperation),
    ComparerOperation(ComparerOperation),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComparerOperation {
    Equals,
    NotEquals,
    Contains,
    StartsWith,
    EndsWith,
    Greater,
    GreaterOrEquals,
    Less,
    LessOrEquals
}

#[derive(Debug, Clone, PartialEq)]
pub enum Next {
    And,
    Or
}

#[derive(Debug, PartialEq, Clone)]
pub enum MathOperation {
    Add,
    Subtract,
    Multiply,
    Divide,
    Exponent,
    Root
}

impl ColumnTransformer {
    pub fn new(
        column_type: DbType,
        column_1: MemoryBlock,
        column_2: MemoryBlock,
        transformer_type: ColumnTransformerType,
        next: Option<Next>
    ) -> Self {
        Self {
            column_type,
            column_1,
            column_2,
            transformer_type,
            next
        }
    }

    pub fn transform_single(&self) -> Either<MemoryBlock, bool> {  
        let wrapper_1 = unsafe { self.column_1.into_wrapper() };
        let wrapper_2 = unsafe { self.column_2.into_wrapper() };

        let input1 =  wrapper_1.as_slice();
        let input2 = wrapper_2.as_slice();

        match self.transformer_type {
            ColumnTransformerType::MathOperation(ref operation) => {
                if self.next.is_some() {
                    panic!("Next operation is not supported for MathOperation");
                }

                Either::Left(perform_math_operation(input1, input2, &self.column_type, operation))
            },
            ColumnTransformerType::ComparerOperation(ref operation) => {
                Either::Right(
                    perform_comparison_operation(
                        input1,
                        input2,
                        &self.column_type,
                        operation
                    )
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::MEMORY_POOL;

    fn create_memory_block_from_i32(value: i32) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let mut wrapper = unsafe { data.into_wrapper() };
        let vec = wrapper.as_vec_mut();
        vec.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_string(value: &str) -> MemoryBlock {
        let bytes = value.as_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let mut wrapper = unsafe { data.into_wrapper() };
        let vec = wrapper.as_vec_mut();
        vec.copy_from_slice(bytes);
        data
    }

    #[test]
    fn test_transform_single_math_add() {
        let i32_data_1 = create_memory_block_from_i32(10);
        let i32_data_2 = create_memory_block_from_i32(20);

        let transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1,
            i32_data_2,
            ColumnTransformerType::MathOperation(MathOperation::Add),
            None,
        );

        if let Either::Left(result) = transformer.transform_single() {
            let wrapper = unsafe { result.into_wrapper() };
            let result_slice = wrapper.as_slice();
            let result_value = i32::from_le_bytes(result_slice.try_into().unwrap());
            assert_eq!(result_value, 30);
        } else {
            panic!("Expected MemoryBlock result");
        }
    }

    #[test]
    fn test_transform_single_comparer_equals() {
        let i32_data_1 = create_memory_block_from_i32(42);
        let i32_data_2 = create_memory_block_from_i32(42);

        let transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1,
            i32_data_2,
            ColumnTransformerType::ComparerOperation(ComparerOperation::Equals),
            None,
        );

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    fn test_transform_single_comparer_not_equals() {
        let i32_data_1 = create_memory_block_from_i32(42);
        let i32_data_2 = create_memory_block_from_i32(43);

        let transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1,
            i32_data_2,
            ColumnTransformerType::ComparerOperation(ComparerOperation::NotEquals),
            None,
        );

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    #[should_panic(expected = "Next operation is not supported for MathOperation")]
    fn test_transform_single_math_with_next_panic() {
        let i32_data_1 = create_memory_block_from_i32(10);
        let i32_data_2 = create_memory_block_from_i32(20);

        let transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1,
            i32_data_2,
            ColumnTransformerType::MathOperation(MathOperation::Add),
            Some(Next::And),
        );

        transformer.transform_single();
    }

    #[test]
    fn test_transform_single_comparer_contains() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("world");

        let transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1,
            string_data_2,
            ColumnTransformerType::ComparerOperation(ComparerOperation::Contains),
            None,
        );

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    fn test_transform_single_comparer_starts_with() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("Hello");

        let transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1,
            string_data_2,
            ColumnTransformerType::ComparerOperation(ComparerOperation::StartsWith),
            None,
        );

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    fn test_transform_single_comparer_ends_with() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("world!");

        let transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1,
            string_data_2,
            ColumnTransformerType::ComparerOperation(ComparerOperation::EndsWith),
            None,
        );

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    fn test_transform_single_comparer_not_contains() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("planet");

        let transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1,
            string_data_2,
            ColumnTransformerType::ComparerOperation(ComparerOperation::Contains),
            None,
        );

        if let Either::Right(result) = transformer.transform_single() {
            assert!(!result);
        } else {
            panic!("Expected bool result");
        }
    }
}