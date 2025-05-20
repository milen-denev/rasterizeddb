use std::collections::VecDeque;

use itertools::Either;

use crate::{core::db_type::DbType, memory_pool::MemoryBlock};

use super::{logical::perform_comparison_operation, math::perform_math_operation};

pub struct ColumnTransformer {
    pub column_type: DbType,
    pub column_1: MemoryBlock,
    pub column_2: Option<MemoryBlock>,
    pub transformer_type: ColumnTransformerType,
    pub next: Option<Next>,

    pub result_store_index: Option<usize>,
    pub result_index_1: Option<usize>,
    pub result_index_2: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
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
        transformer_type: ColumnTransformerType,
        next: Option<Next>
    ) -> Self {
        Self {
            column_type,
            column_1,
            column_2: None,
            transformer_type,
            next,
            result_store_index: None,
            result_index_1: None,
            result_index_2: None,
        }
    }

    pub fn transform_single(&self) -> Either<MemoryBlock, (bool, Option<Next>)> {  
        let input1 = self.column_1.into_slice();
        let input2 = self.column_2.as_ref().unwrap().into_slice();

        match self.transformer_type {
            ColumnTransformerType::MathOperation(ref operation) => {
                if self.next.is_some() {
                    panic!("Next operation is not supported for MathOperation");
                }

                Either::Left(perform_math_operation(input1, input2, &self.column_type, operation))
            },
            ColumnTransformerType::ComparerOperation(ref operation) => {
                Either::Right(
                    (perform_comparison_operation(
                        input1,
                        input2,
                        &self.column_type,
                        operation
                    ), self.next.clone())
                )
            }
        }
    }

    pub fn setup_column_2(&mut self, column_2: MemoryBlock) {
        self.column_2 = Some(column_2);
    }
}

/// An enum to represent either a direct memory block or an index to an intermediate result
pub enum ComparisonOperand {
    Direct(MemoryBlock),
    Intermediate(usize),
}

impl From<MemoryBlock> for ComparisonOperand {
    fn from(mem: MemoryBlock) -> Self {
        ComparisonOperand::Direct(mem)
    }
}

impl From<usize> for ComparisonOperand {
    fn from(idx: usize) -> Self {
        ComparisonOperand::Intermediate(idx)
    }
}

pub struct TransformerProcessor {
    transformers: VecDeque<ColumnTransformer>,
    intermediate_results: Vec<MemoryBlock>,
}

impl TransformerProcessor {
    pub fn new() -> Self {
        Self {
            transformers: VecDeque::new(),
            intermediate_results: Vec::new(),
        }
    }    
    
    /// Add a math operation transformer
    pub fn add_math_operation(
        &mut self,
        column_type: DbType,
        column_1: MemoryBlock, 
        column_2: MemoryBlock,
        operation: MathOperation,
    ) -> usize {
        let result_index = self.intermediate_results.len();
        
        let mut transformer = ColumnTransformer::new(
            column_type,
            column_1,
            ColumnTransformerType::MathOperation(operation),
            None,
        );
        
        // Store the result index in the transformer
        transformer.result_store_index = Some(result_index);
        
        transformer.setup_column_2(column_2);
        self.transformers.push_back(transformer);
        
        // Reserve space for the result
        self.intermediate_results.push(MemoryBlock::default());
        
        result_index
    }
    
    /// Add a comparison operation with a logical connector
    /// 
    /// This unified method can compare:
    /// - Two direct memory blocks (direct values)
    /// - Two intermediate results from previous operations
    /// - A memory block with an intermediate result
    pub fn add_comparison(
        &mut self,
        column_type: DbType,
        left: impl Into<ComparisonOperand>,
        right: impl Into<ComparisonOperand>,
        operation: ComparerOperation,
        next: Option<Next>,
    ) {
        let left_operand = left.into();
        let right_operand = right.into();
        
        // Create a transformer with placeholder memory blocks
        let placeholder = MemoryBlock::default();
        let mut transformer = ColumnTransformer::new(
            column_type,
            placeholder.clone(),
            ColumnTransformerType::ComparerOperation(operation),
            next,
        );
        
        transformer.setup_column_2(placeholder);
        
        // Set up the correct operands based on type
        match (left_operand, right_operand) {
            (ComparisonOperand::Direct(mem1), ComparisonOperand::Direct(mem2)) => {
                // Direct comparison of two memory blocks
                transformer.column_1 = mem1;
                transformer.setup_column_2(mem2);
            },
            (ComparisonOperand::Intermediate(idx1), ComparisonOperand::Intermediate(idx2)) => {
                // Comparison of two intermediate results
                transformer.result_index_1 = Some(idx1);
                transformer.result_index_2 = Some(idx2);
            },
            (ComparisonOperand::Direct(mem1), ComparisonOperand::Intermediate(idx2)) => {
                // Direct memory block compared with intermediate result
                transformer.column_1 = mem1;
                transformer.result_index_2 = Some(idx2);
            },
            (ComparisonOperand::Intermediate(idx1), ComparisonOperand::Direct(mem2)) => {
                // Intermediate result compared with direct memory block
                transformer.result_index_1 = Some(idx1);
                transformer.setup_column_2(mem2);
            },
        }
        
        self.transformers.push_back(transformer);
    }

    /// Execute all transformations and return final result
    pub fn execute(&mut self) -> bool {
        let mut comparison_results: Vec<(bool, Option<Next>)> = Vec::new();
        
        // Process all transformers
        while let Some(mut transformer) = self.transformers.pop_front() {
            // If this transformer uses intermediate results, update its memory blocks
            if let Some(idx1) = transformer.result_index_1 {
                transformer.column_1 = self.intermediate_results[idx1].clone();
            }
            
            if let Some(idx2) = transformer.result_index_2 {
                transformer.setup_column_2(self.intermediate_results[idx2].clone());
            }
            
            // Process the transformation
            match transformer.transform_single() {
                Either::Left(result) => {
                    // Store math operation result
                    if let Some(idx) = transformer.result_store_index {
                        self.intermediate_results[idx] = result;
                    }
                },
                Either::Right(comparison_result) => {
                    // Store comparison result
                    comparison_results.push(comparison_result);
                }
            }
        }
        
        // Evaluate the final boolean result
        self.evaluate_comparison_results(comparison_results)
    }
    
    /// Evaluate the logical combination of comparison results
    fn evaluate_comparison_results(&self, results: Vec<(bool, Option<Next>)>) -> bool {
        if results.is_empty() {
            return false;
        }
        
        let mut final_result = results[0].0;
        
        for i in 0..results.len()-1 {
            let (current_result, next_op) = &results[i];
            let (next_result, _) = results[i+1];
            
            match next_op {
                Some(Next::And) => {
                    final_result = *current_result && next_result;
                },
                Some(Next::Or) => {
                    final_result = *current_result || next_result;
                },
                None => {
                    // If no connector, this is the last result
                    final_result = *current_result;
                    break;
                }
            }
        }
        
        final_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::MEMORY_POOL;

    fn create_memory_block_from_i32(value: i32) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_string(value: &str) -> MemoryBlock {
        let bytes = value.as_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(bytes);
        data
    }

    #[test]
    fn test_transform_single_math_add() {
        let i32_data_1 = create_memory_block_from_i32(10);
        let i32_data_2 = create_memory_block_from_i32(20);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1,
            ColumnTransformerType::MathOperation(MathOperation::Add),
            None,
        );
        
        transformer.setup_column_2(i32_data_2);

        if let Either::Left(result) = transformer.transform_single() {
            let result_slice = result.into_slice();
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

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1,
            ColumnTransformerType::ComparerOperation(ComparerOperation::Equals),
            None,
        );

        transformer.setup_column_2(i32_data_2);

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result.0);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    fn test_transform_single_comparer_not_equals() {
        let i32_data_1 = create_memory_block_from_i32(42);
        let i32_data_2 = create_memory_block_from_i32(43);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1,
            ColumnTransformerType::ComparerOperation(ComparerOperation::NotEquals),
            None,
        );

        transformer.setup_column_2(i32_data_2);

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result.0);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    #[should_panic(expected = "Next operation is not supported for MathOperation")]
    fn test_transform_single_math_with_next_panic() {
        let i32_data_1 = create_memory_block_from_i32(10);
        let i32_data_2 = create_memory_block_from_i32(20);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1,
            ColumnTransformerType::MathOperation(MathOperation::Add),
            Some(Next::And),
        );

        transformer.setup_column_2(i32_data_2);

        transformer.transform_single();
    }

    #[test]
    fn test_transform_single_comparer_contains() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("world");

        let mut transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1,
            ColumnTransformerType::ComparerOperation(ComparerOperation::Contains),
            None,
        );

        transformer.setup_column_2(string_data_2);

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result.0);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    fn test_transform_single_comparer_starts_with() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("Hello");

        let mut transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1,
            ColumnTransformerType::ComparerOperation(ComparerOperation::StartsWith),
            None,
        );

        transformer.setup_column_2(string_data_2);

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result.0);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    fn test_transform_single_comparer_ends_with() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("world!");

        let mut transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1,
            ColumnTransformerType::ComparerOperation(ComparerOperation::EndsWith),
            None,
        );

        transformer.setup_column_2(string_data_2);

        if let Either::Right(result) = transformer.transform_single() {
            assert!(result.0);
        } else {
            panic!("Expected bool result");
        }
    }

    #[test]
    fn test_transform_single_comparer_not_contains() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("planet");

        let mut transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1,
            ColumnTransformerType::ComparerOperation(ComparerOperation::Contains),
            None,
        );

        transformer.setup_column_2(string_data_2);

        if let Either::Right(result) = transformer.transform_single() {
            assert!(!result.0);
        } else {
            panic!("Expected bool result");
        }
    }    
    
    #[test]
    fn test_complex_query_where_clause() {
        // WHERE Column_1 + 5 = 0 + Column_2 AND Column_3 = 1
        let mut multi_transformer = TransformerProcessor::new();

        // Create memory blocks for our column values
        let column_1 = create_memory_block_from_i32(10);
        let column_2 = create_memory_block_from_i32(15);
        let column_3 = create_memory_block_from_i32(1);

        let const_5 = create_memory_block_from_i32(5);
        let const_0 = create_memory_block_from_i32(0);
        let const_1 = create_memory_block_from_i32(1);
        
        // Process "Column_1 + 5"
        let result_1_idx = multi_transformer.add_math_operation(
            DbType::I32,
            column_1,
            const_5,
            MathOperation::Add
        );
        
        // Process "0 + Column_2"
        let result_2_idx = multi_transformer.add_math_operation(
            DbType::I32,
            const_0,
            column_2,
            MathOperation::Add
        );
        
        // Compare the results with the Equals operation and connect with AND
        multi_transformer.add_comparison(
            DbType::I32,
            result_1_idx,
            result_2_idx,
            ComparerOperation::Equals,
            Some(Next::And)
        );
        
        // Process "Column_3 = 1"
        multi_transformer.add_comparison(
            DbType::I32,
            column_3,
            const_1,
            ComparerOperation::Equals,
            None
        );
        
        // Execute and verify the result
        assert!(multi_transformer.execute());
    }

    #[test]
    fn test_complex_query_where_clause_2() { 
        // Let's try another case where the condition should fail
        let mut multi_transformer = TransformerProcessor::new();
        
        // Use different values that will make the condition false
        let column_1 = create_memory_block_from_i32(10);
        let column_2 = create_memory_block_from_i32(20); // Now the sum won't match
        let column_3 = create_memory_block_from_i32(1);
        
        let const_5 = create_memory_block_from_i32(5);
        let const_0 = create_memory_block_from_i32(0);
        let const_1 = create_memory_block_from_i32(1);
        
        let result_1_idx = multi_transformer.add_math_operation(
            DbType::I32,
            column_1,
            const_5,
            MathOperation::Add
        );
        
        let result_2_idx = multi_transformer.add_math_operation(
            DbType::I32,
            const_0,
            column_2,
            MathOperation::Add
        );
        
        multi_transformer.add_comparison(
            DbType::I32,
            result_1_idx,
            result_2_idx,
            ComparerOperation::Equals,
            Some(Next::And)
        );
        
        multi_transformer.add_comparison(
            DbType::I32,
            column_3,
            const_1,
            ComparerOperation::Equals,
            None
        );
        
        // This should be false since 10 + 5 != 0 + 20
        assert!(!multi_transformer.execute());
    }

    #[test]
    fn test_complex_query_where_clause_3() { 
        let mut multi_transformer = TransformerProcessor::new();
        
        // Use different values that will make the condition false
        let column_1 = create_memory_block_from_string("test");
        
        let const_5 = create_memory_block_from_string("test");
        let const_0 = create_memory_block_from_string("not_test");

        multi_transformer.add_comparison(
            DbType::STRING,
            column_1.clone(),
            const_5,
            ComparerOperation::Equals,
            Some(Next::And)
        );

        multi_transformer.add_comparison(
            DbType::STRING,
            column_1,
            const_0,
            ComparerOperation::NotEquals,
            None
        );

        // The first condition is true but the second is false
        assert!(multi_transformer.execute());
    }

    #[test]
    fn test_complex_query_where_clause_4() { 
        let mut multi_transformer = TransformerProcessor::new();
        
        // Use different values that will make the condition false
        let column_1 = create_memory_block_from_string("test");
        
        let const_1 = create_memory_block_from_string("test");
        let const_2 = create_memory_block_from_string("not_test");
        let const_3 = create_memory_block_from_string("t");

        multi_transformer.add_comparison(
            DbType::STRING,
            column_1.clone(),
            const_1,
            ComparerOperation::Equals,
            Some(Next::And)
        );

        multi_transformer.add_comparison(
            DbType::STRING,
            column_1.clone(),
            const_2,
            ComparerOperation::Equals,
            Some(Next::Or)
        );

       multi_transformer.add_comparison(
            DbType::STRING,
            column_1,
            const_3,
            ComparerOperation::Contains,
            None
        );

        // The first condition is true but the second is false
        assert!(multi_transformer.execute());
    }
}