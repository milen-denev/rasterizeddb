use itertools::Either;
use smallvec::SmallVec;

use crate::{core::{db_type::DbType, helpers2::smallvec_extensions::SmallVecExtensions}, memory_pool::MemoryBlock};

use super::{logical::perform_comparison_operation, math::perform_math_operation};

#[derive(Debug)]
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
#[derive(Debug, Clone)]
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

#[derive(Debug)]
pub struct TransformerProcessor<'a> {
    pub transformers: &'a mut SmallVec<[ColumnTransformer; 36]>,
    pub intermediate_results: &'a mut  SmallVec<[MemoryBlock; 20]>,
}

impl<'a> TransformerProcessor<'a> {
    pub fn new(transformers: &'a mut SmallVec<[ColumnTransformer; 36]>, intermediate_results: &'a mut SmallVec<[MemoryBlock; 20]>) -> Self {
        transformers.clear();
        intermediate_results.clear();
        Self {
            transformers: transformers,
            intermediate_results: intermediate_results,
        }
    }    
    
    /// Add a math operation transformer
    /// Accepts either direct MemoryBlocks or intermediate result indices (usize) as operands.
    pub fn add_math_operation(
        &mut self,
        column_type: DbType,
        left_operand_in: impl Into<ComparisonOperand>,
        right_operand_in: impl Into<ComparisonOperand>,
        operation: MathOperation,
    ) -> usize {
        let result_store_idx = self.intermediate_results.len(); // Index for storing the output of THIS math operation

        let left_op = left_operand_in.into();
        let right_op = right_operand_in.into();

        let placeholder = MemoryBlock::default();
        // Initialize ColumnTransformer with placeholders for column_1 and column_2
        let mut transformer = ColumnTransformer::new(
            column_type,
            placeholder.clone(), 
            ColumnTransformerType::MathOperation(operation),
            None, // Math operations don't have a 'Next' logical connector
        );
        // setup_column_2 also needs to be called, even with a placeholder initially for column_2
        transformer.setup_column_2(placeholder); 

        // Configure column_1 or result_index_1 based on the left operand type
        match left_op {
            ComparisonOperand::Direct(mem) => {
                transformer.column_1 = mem;
            }
            ComparisonOperand::Intermediate(idx) => {
                transformer.result_index_1 = Some(idx);
                // transformer.column_1 remains the placeholder; it will be filled by execute()
            }
        }

        // Configure column_2 or result_index_2 based on the right operand type
        match right_op {
            ComparisonOperand::Direct(mem) => {
                transformer.setup_column_2(mem);
            }
            ComparisonOperand::Intermediate(idx) => {
                transformer.result_index_2 = Some(idx);
                // transformer.column_2 remains the placeholder (set via setup_column_2 above); it will be filled by execute()
            }
        }
        
        // Set the index where the result of this math operation will be stored
        transformer.result_store_index = Some(result_store_idx); 
        
        self.transformers.push_back(transformer);
        // Reserve space in intermediate_results for the output of this operation
        self.intermediate_results.push(MemoryBlock::default()); 
        
        result_store_idx // Return the index where the output will be stored
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
    pub fn execute(&mut self, comparison_results: &mut SmallVec<[(bool, Option<Next>); 20]>) -> bool {

        #[cfg(debug_assertions)]
        println!("Executing {} transformers and {} intermediate results", self.transformers.len(), self.intermediate_results.len());

        #[cfg(debug_assertions)]
        for transformer in self.transformers.iter() {
            println!("Transformer: {:?}", transformer);
        }

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

    /// Evaluate the logical combination of comparison results respecting AND/OR precedence
    fn evaluate_comparison_results(&self, results: &[(bool, Option<Next>)]) -> bool {
        if results.is_empty() {
            return false;
        }
        
        let mut current_and_result = true;
        
        for (result, next_op) in results.iter() {
            // Update current AND group result
            current_and_result &= *result;
            
            match next_op {
                Some(Next::And) => {
                    // Continue with current AND group
                    // Early termination: if current AND group is false, 
                    // we can skip ahead to the next OR
                    if !current_and_result {
                        // Fast-forward to next OR or end
                        continue;
                    }
                },
                Some(Next::Or) | None => {
                    // Complete current AND group
                    if current_and_result {
                        return true; // Early termination!
                    }
                    // Reset for next AND group
                    current_and_result = true;
                    
                    if next_op.is_none() {
                        break;
                    }
                }
            }
        }
        
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::MEMORY_POOL;
    
    fn create_memory_block_from_f64(value: f64) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u8(value: u8) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u128(value: u128) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

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
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // WHERE Column_1 + 5 = 0 + Column_2 AND Column_3 = 1
        let mut multi_transformer = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

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
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(multi_transformer.execute(&mut bool_buffer));
    }

    #[test]
    fn test_complex_query_where_clause_2() { 
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Let's try another case where the condition should fail
        let mut multi_transformer = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        
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
           let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        // This should be false since 10 + 5 != 0 + 20
        assert!(!multi_transformer.execute(&mut bool_buffer));
    }

    #[test]
    fn test_complex_query_where_clause_3() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new(); 

        let mut multi_transformer = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
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
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        assert!(multi_transformer.execute(&mut bool_buffer));
    }

    #[test]
    fn test_complex_query_where_clause_4() { 
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        let mut multi_transformer = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

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
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(multi_transformer.execute(&mut bool_buffer));
    }

    
    #[test]
    fn test_numerical_simple_and_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: (10 > 5) AND (20 = 20) -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_10 = create_memory_block_from_i32(10);
        let val_5 = create_memory_block_from_i32(5);
        let val_20 = create_memory_block_from_i32(20);

        processor.add_comparison(DbType::I32, val_10, val_5, ComparerOperation::Greater, Some(Next::And));
        processor.add_comparison(DbType::I32, val_20.clone(), val_20, ComparerOperation::Equals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_simple_and_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: (10 > 5) AND (20 = 10) -> false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_10 = create_memory_block_from_i32(10);
        let val_5 = create_memory_block_from_i32(5);
        let val_20 = create_memory_block_from_i32(20);
        let val_10_b = create_memory_block_from_i32(10);

        processor.add_comparison(DbType::I32, val_10, val_5, ComparerOperation::Greater, Some(Next::And));
        processor.add_comparison(DbType::I32, val_20, val_10_b, ComparerOperation::Equals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(!processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_simple_or_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: (10 < 5) OR (20 = 20) -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_10 = create_memory_block_from_i32(10);
        let val_5 = create_memory_block_from_i32(5);
        let val_20 = create_memory_block_from_i32(20);

        processor.add_comparison(DbType::I32, val_10, val_5, ComparerOperation::Less, Some(Next::Or));
        processor.add_comparison(DbType::I32, val_20.clone(), val_20, ComparerOperation::Equals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_simple_or_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: (10 < 5) OR (20 = 10) -> false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_10 = create_memory_block_from_i32(10);
        let val_5 = create_memory_block_from_i32(5);
        let val_20 = create_memory_block_from_i32(20);
        let val_10_b = create_memory_block_from_i32(10);

        processor.add_comparison(DbType::I32, val_10, val_5, ComparerOperation::Less, Some(Next::Or));
        processor.add_comparison(DbType::I32, val_20, val_10_b, ComparerOperation::Equals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(!processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_math_then_compare_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: (5 + 5) = 10 -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_5 = create_memory_block_from_i32(5);
        let val_10 = create_memory_block_from_i32(10);

        let sum_idx = processor.add_math_operation(DbType::I32, val_5.clone(), val_5, MathOperation::Add);
        processor.add_comparison(DbType::I32, sum_idx, val_10, ComparerOperation::Equals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_math_then_compare_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: (5 + 10) != 10 -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_5 = create_memory_block_from_i32(5);
        let val_10 = create_memory_block_from_i32(10);

        let sum_idx = processor.add_math_operation(DbType::I32, val_5, val_10.clone(), MathOperation::Add);
        processor.add_comparison(DbType::I32, sum_idx, val_10, ComparerOperation::NotEquals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_two_math_ops_and_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: (10 - 3) > (2 * 3) -> 7 > 6 -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_10 = create_memory_block_from_i32(10);
        let val_3 = create_memory_block_from_i32(3);
        let val_2 = create_memory_block_from_i32(2);

        let diff_idx = processor.add_math_operation(DbType::I32, val_10, val_3.clone(), MathOperation::Subtract);
        let prod_idx = processor.add_math_operation(DbType::I32, val_2, val_3, MathOperation::Multiply);
        processor.add_comparison(DbType::I32, diff_idx, prod_idx, ComparerOperation::Greater, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }
    
    #[test]
    fn test_numerical_greater_or_equals_true_equal() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: 10 >= 10 -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_10a = create_memory_block_from_i32(10);
        let val_10b = create_memory_block_from_i32(10);
        processor.add_comparison(DbType::I32, val_10a, val_10b, ComparerOperation::GreaterOrEquals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_greater_or_equals_true_greater() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: 15 >= 10 -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_15 = create_memory_block_from_i32(15);
        let val_10 = create_memory_block_from_i32(10);
        processor.add_comparison(DbType::I32, val_15, val_10, ComparerOperation::GreaterOrEquals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_greater_or_equals_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: 5 >= 10 -> false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_5 = create_memory_block_from_i32(5);
        let val_10 = create_memory_block_from_i32(10);
        processor.add_comparison(DbType::I32, val_5, val_10, ComparerOperation::GreaterOrEquals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(!processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_less_or_equals_true_equal() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: 10 <= 10 -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_10a = create_memory_block_from_i32(10);
        let val_10b = create_memory_block_from_i32(10);
        processor.add_comparison(DbType::I32, val_10a, val_10b, ComparerOperation::LessOrEquals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_less_or_equals_true_less() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: 5 <= 10 -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_5 = create_memory_block_from_i32(5);
        let val_10 = create_memory_block_from_i32(10);
        processor.add_comparison(DbType::I32, val_5, val_10, ComparerOperation::LessOrEquals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_less_or_equals_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: 15 <= 10 -> false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_15 = create_memory_block_from_i32(15);
        let val_10 = create_memory_block_from_i32(10);
        processor.add_comparison(DbType::I32, val_15, val_10, ComparerOperation::LessOrEquals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(!processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_complex_and_or_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ((5 + 5) = 10 AND 7 > 3) OR 2 < 1 -> (true AND true) OR false -> true OR false -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_5 = create_memory_block_from_i32(5);
        let val_10 = create_memory_block_from_i32(10);
        let val_7 = create_memory_block_from_i32(7);
        let val_3 = create_memory_block_from_i32(3);
        let val_2 = create_memory_block_from_i32(2);
        let val_1 = create_memory_block_from_i32(1);

        let sum_idx = processor.add_math_operation(DbType::I32, val_5.clone(), val_5, MathOperation::Add);
        processor.add_comparison(DbType::I32, sum_idx, val_10, ComparerOperation::Equals, Some(Next::And));
        processor.add_comparison(DbType::I32, val_7, val_3, ComparerOperation::Greater, Some(Next::Or));
        processor.add_comparison(DbType::I32, val_2, val_1, ComparerOperation::Less, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_numerical_complex_and_or_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ((5 + 5) = 7 AND 7 > 3) OR 2 < 1 -> (false AND true) OR false -> false OR false -> false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let val_5 = create_memory_block_from_i32(5);
        let val_7 = create_memory_block_from_i32(7);
        let val_3 = create_memory_block_from_i32(3);
        let val_2 = create_memory_block_from_i32(2);
        let val_1 = create_memory_block_from_i32(1);

        let sum_idx = processor.add_math_operation(DbType::I32, val_5.clone(), val_5, MathOperation::Add); // result is 10
        processor.add_comparison(DbType::I32, sum_idx, val_7.clone(), ComparerOperation::Equals, Some(Next::And)); // 10 = 7 is false
        processor.add_comparison(DbType::I32, val_7, val_3, ComparerOperation::Greater, Some(Next::Or));
        processor.add_comparison(DbType::I32, val_2, val_1, ComparerOperation::Less, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(!processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_string_simple_and_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ("hello" = "hello") AND ("world" CONTAINS "orl") -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let str_hello_a = create_memory_block_from_string("hello");
        let str_hello_b = create_memory_block_from_string("hello");
        let str_world = create_memory_block_from_string("world");
        let str_orl = create_memory_block_from_string("orl");

        processor.add_comparison(DbType::STRING, str_hello_a, str_hello_b, ComparerOperation::Equals, Some(Next::And));
        processor.add_comparison(DbType::STRING, str_world, str_orl, ComparerOperation::Contains, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_string_simple_and_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ("hello" = "hello") AND ("world" CONTAINS "xyz") -> false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let str_hello_a = create_memory_block_from_string("hello");
        let str_hello_b = create_memory_block_from_string("hello");
        let str_world = create_memory_block_from_string("world");
        let str_xyz = create_memory_block_from_string("xyz");

        processor.add_comparison(DbType::STRING, str_hello_a, str_hello_b, ComparerOperation::Equals, Some(Next::And));
        processor.add_comparison(DbType::STRING, str_world, str_xyz, ComparerOperation::Contains, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(!processor.execute(&mut bool_buffer));
    }
    
    #[test]
    fn test_string_simple_or_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ("apple" STARTSWITH "b") OR ("banana" ENDSWITH "ana") -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let str_apple = create_memory_block_from_string("apple");
        let str_b = create_memory_block_from_string("b");
        let str_banana = create_memory_block_from_string("banana");
        let str_ana = create_memory_block_from_string("ana");

        processor.add_comparison(DbType::STRING, str_apple, str_b, ComparerOperation::StartsWith, Some(Next::Or));
        processor.add_comparison(DbType::STRING, str_banana, str_ana, ComparerOperation::EndsWith, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_string_simple_or_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ("apple" STARTSWITH "b") OR ("banana" ENDSWITH "xyz") -> false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let str_apple = create_memory_block_from_string("apple");
        let str_b = create_memory_block_from_string("b");
        let str_banana = create_memory_block_from_string("banana");
        let str_xyz = create_memory_block_from_string("xyz");

        processor.add_comparison(DbType::STRING, str_apple, str_b, ComparerOperation::StartsWith, Some(Next::Or));
        processor.add_comparison(DbType::STRING, str_banana, str_xyz, ComparerOperation::EndsWith, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(!processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_string_not_equals_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: "cat" != "dog" -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let str_cat = create_memory_block_from_string("cat");
        let str_dog = create_memory_block_from_string("dog");
        processor.add_comparison(DbType::STRING, str_cat, str_dog, ComparerOperation::NotEquals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_string_not_equals_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: "cat" != "cat" -> false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let str_cat_a = create_memory_block_from_string("cat");
        let str_cat_b = create_memory_block_from_string("cat");
        processor.add_comparison(DbType::STRING, str_cat_a, str_cat_b, ComparerOperation::NotEquals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(!processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_string_complex_starts_ends_contains_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ("longstring" STARTSWITH "long") AND ("anotherstring" ENDSWITH "string") OR ("middle" CONTAINS "dd")
        // -> (true AND true) OR true -> true OR true -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let longstring = create_memory_block_from_string("longstring");
        let long_ = create_memory_block_from_string("long");
        let anotherstring = create_memory_block_from_string("anotherstring");
        let string_ = create_memory_block_from_string("string");
        let middle = create_memory_block_from_string("middle");
        let dd = create_memory_block_from_string("dd");

        processor.add_comparison(DbType::STRING, longstring, long_, ComparerOperation::StartsWith, Some(Next::And));
        processor.add_comparison(DbType::STRING, anotherstring, string_, ComparerOperation::EndsWith, Some(Next::Or));
        processor.add_comparison(DbType::STRING, middle, dd, ComparerOperation::Contains, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_string_complex_all_false_with_or() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ("abc" STARTSWITH "d") OR ("def" ENDSWITH "g") OR ("ghi" CONTAINS "j")
        // -> false OR false OR false -> false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let abc = create_memory_block_from_string("abc");
        let d = create_memory_block_from_string("d");
        let def = create_memory_block_from_string("def");
        let g = create_memory_block_from_string("g");
        let ghi = create_memory_block_from_string("ghi");
        let j = create_memory_block_from_string("j");

        processor.add_comparison(DbType::STRING, abc, d, ComparerOperation::StartsWith, Some(Next::Or));
        processor.add_comparison(DbType::STRING, def, g, ComparerOperation::EndsWith, Some(Next::Or));
        processor.add_comparison(DbType::STRING, ghi, j, ComparerOperation::Contains, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(!processor.execute(&mut bool_buffer));
    }
    
    #[test]
    fn test_string_mixed_operators_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ("test" = "test" AND "example" CONTAINS "amp") OR "false" = "true"
        // -> (true AND true) OR false -> true OR false -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let test_a = create_memory_block_from_string("test");
        let test_b = create_memory_block_from_string("test");
        let example = create_memory_block_from_string("example");
        let amp = create_memory_block_from_string("amp");
        let str_false = create_memory_block_from_string("false");
        let str_true = create_memory_block_from_string("true");

        processor.add_comparison(DbType::STRING, test_a, test_b, ComparerOperation::Equals, Some(Next::And));
        processor.add_comparison(DbType::STRING, example, amp, ComparerOperation::Contains, Some(Next::Or));
        processor.add_comparison(DbType::STRING, str_false, str_true, ComparerOperation::Equals, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    #[test]
    fn test_string_empty_string_comparisons() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Test: ("" = "") AND ("" CONTAINS "") OR ("a" STARTSWITH "")
        // -> (true AND true) OR true -> true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let empty_a = create_memory_block_from_string("");
        let empty_b = create_memory_block_from_string("");
        let str_a = create_memory_block_from_string("a");

        processor.add_comparison(DbType::STRING, empty_a.clone(), empty_b.clone(), ComparerOperation::Equals, Some(Next::And));
        processor.add_comparison(DbType::STRING, empty_a.clone(), empty_b.clone(), ComparerOperation::Contains, Some(Next::Or));
        processor.add_comparison(DbType::STRING, str_a, empty_a, ComparerOperation::StartsWith, None);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert!(processor.execute(&mut bool_buffer));
    }

    // --- Additional Complex Mixed Scenario Tests ---

    #[test]
    fn test_complex_mixed_scenario_1() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ((10 + 5) = 15 AND "apple" STARTSWITH "app") OR ((20 - 5) > 10 AND "banana" CONTAINS "nan")
        // B1: (10 + 5) = 15  => 15 = 15 => true
        // B2: "apple" STARTSWITH "app" => true
        // B3: (20 - 5) > 10  => 15 > 10 => true
        // B4: "banana" CONTAINS "nan" => true
        // Evaluation: (((B1 AND B2) OR B3) AND B4)
        // => (((true AND true) OR true) AND true) => ((true OR true) AND true) => (true AND true) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_10 = create_memory_block_from_i32(10);
        let num_5 = create_memory_block_from_i32(5);
        let num_15 = create_memory_block_from_i32(15);
        let num_20 = create_memory_block_from_i32(20);

        let str_apple = create_memory_block_from_string("apple");
        let str_app = create_memory_block_from_string("app");
        let str_banana = create_memory_block_from_string("banana");
        let str_nan = create_memory_block_from_string("nan");

        let sum_idx = processor.add_math_operation(DbType::I32, num_10.clone(), num_5.clone(), MathOperation::Add);
        processor.add_comparison(DbType::I32, sum_idx, num_15.clone(), ComparerOperation::Equals, Some(Next::And)); // B1

        processor.add_comparison(DbType::STRING, str_apple, str_app, ComparerOperation::StartsWith, Some(Next::Or)); // B2

        let diff_idx = processor.add_math_operation(DbType::I32, num_20, num_5.clone(), MathOperation::Subtract);
        processor.add_comparison(DbType::I32, diff_idx, num_10, ComparerOperation::Greater, Some(Next::And)); // B3

        processor.add_comparison(DbType::STRING, str_banana, str_nan, ComparerOperation::Contains, None); // B4
        
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true);
    }

    #[test]
    fn test_complex_mixed_scenario_2() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ((10 * 2) < 15 OR "test" ENDSWITH "est") AND ((100 / 4) = 25 OR "string" != "string")
        // B1: (10 * 2) < 15 => 20 < 15 => false
        // B2: "test" ENDSWITH "est" => true
        // B3: (100 / 4) = 25 => 25 = 25 => true
        // B4: "string" != "string" => false
        // Evaluation: (((B1 OR B2) AND B3) OR B4)
        // => (((false OR true) AND true) OR false) => ((true AND true) OR false) => (true OR false) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_10 = create_memory_block_from_i32(10);
        let num_2 = create_memory_block_from_i32(2);
        let num_15 = create_memory_block_from_i32(15);
        let num_100 = create_memory_block_from_i32(100);
        let num_4 = create_memory_block_from_i32(4);
        let num_25 = create_memory_block_from_i32(25);

        let str_test = create_memory_block_from_string("test");
        let str_est = create_memory_block_from_string("est");
        let str_string = create_memory_block_from_string("string");

        let mul_idx = processor.add_math_operation(DbType::I32, num_10, num_2, MathOperation::Multiply);
        processor.add_comparison(DbType::I32, mul_idx, num_15, ComparerOperation::Less, Some(Next::Or)); // B1

        processor.add_comparison(DbType::STRING, str_test, str_est, ComparerOperation::EndsWith, Some(Next::And)); // B2
        
        let div_idx = processor.add_math_operation(DbType::I32, num_100, num_4, MathOperation::Divide);
        processor.add_comparison(DbType::I32, div_idx, num_25, ComparerOperation::Equals, Some(Next::Or)); // B3

        processor.add_comparison(DbType::STRING, str_string.clone(), str_string, ComparerOperation::NotEquals, None); // B4

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true);
    }

    #[test]
    fn test_complex_mixed_scenario_5() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (3 * 7) <= 20 OR "openai" != "google" ) AND ( (50 / 2) > 24 OR "gpt" STARTSWITH "ai" )
        // B1: (3 * 7) <= 20 => 21 <= 20 => false
        // B2: "openai" != "google" => true
        // B3: (50 / 2) > 24 => 25 > 24 => true
        // B4: "gpt" STARTSWITH "ai" => false
        // Evaluation: (((B1 OR B2) AND B3) OR B4)
        // => (((false OR true) AND true) OR false) => ((true AND true) OR false) => (true OR false) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_3 = create_memory_block_from_i32(3);
        let num_7 = create_memory_block_from_i32(7);
        let num_20 = create_memory_block_from_i32(20);
        let num_50 = create_memory_block_from_i32(50);
        let num_2 = create_memory_block_from_i32(2);
        let num_24 = create_memory_block_from_i32(24);

        let str_openai = create_memory_block_from_string("openai");
        let str_google = create_memory_block_from_string("google");
        let str_gpt = create_memory_block_from_string("gpt");
        let str_ai = create_memory_block_from_string("ai");

        let mul_idx = processor.add_math_operation(DbType::I32, num_3, num_7, MathOperation::Multiply);
        processor.add_comparison(DbType::I32, mul_idx, num_20, ComparerOperation::LessOrEquals, Some(Next::Or)); // B1

        processor.add_comparison(DbType::STRING, str_openai, str_google, ComparerOperation::NotEquals, Some(Next::And)); // B2

        let div_idx = processor.add_math_operation(DbType::I32, num_50, num_2, MathOperation::Divide);
        processor.add_comparison(DbType::I32, div_idx, num_24, ComparerOperation::Greater, Some(Next::Or)); // B3

        processor.add_comparison(DbType::STRING, str_gpt, str_ai, ComparerOperation::StartsWith, None); // B4

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true);
    }
    
    #[test]
    fn test_complex_mixed_scenario_7() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (64 ROOT 2) = 8 AND "database" STARTSWITH "data" ) AND ( (7 * 3) > 20 OR "query" = "Query" )
        // B1: (64 ROOT 2) = 8 => 8 = 8 => true (integer square root)
        // B2: "database" STARTSWITH "data" => true
        // B3: (7 * 3) > 20 => 21 > 20 => true
        // B4: "query" = "Query" => false (case-sensitive)
        // Evaluation: (((B1 AND B2) AND B3) OR B4)
        // => (((true AND true) AND true) OR false) => ((true AND true) OR false) => (true OR false) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_64 = create_memory_block_from_i32(64);
        let num_2 = create_memory_block_from_i32(2); // Root index
        let num_8 = create_memory_block_from_i32(8);
        let num_7 = create_memory_block_from_i32(7);
        let num_3 = create_memory_block_from_i32(3);
        let num_20 = create_memory_block_from_i32(20);

        let str_database = create_memory_block_from_string("database");
        let str_data = create_memory_block_from_string("data");
        let str_query_lc = create_memory_block_from_string("query");
        let str_query_uc = create_memory_block_from_string("Query");

        let root_idx = processor.add_math_operation(DbType::I32, num_64, num_2, MathOperation::Root);
        processor.add_comparison(DbType::I32, root_idx, num_8, ComparerOperation::Equals, Some(Next::And)); // B1

        processor.add_comparison(DbType::STRING, str_database, str_data, ComparerOperation::StartsWith, Some(Next::And)); // B2

        let mul_idx = processor.add_math_operation(DbType::I32, num_7, num_3, MathOperation::Multiply);
        processor.add_comparison(DbType::I32, mul_idx, num_20, ComparerOperation::Greater, Some(Next::Or)); // B3

        processor.add_comparison(DbType::STRING, str_query_lc, str_query_uc, ComparerOperation::Equals, None); // B4

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true);
    }

    #[test]
    fn test_complex_mixed_scenario_10() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (4 ** 3) = 60 OR "string one" STARTSWITH "str" ) AND ( (50 / 5) != 10 OR "string two" CONTAINS "three" )
        // B1: (4 ** 3) = 60 => 64 = 60 => false
        // B2: "string one" STARTSWITH "str" => true
        // B3: (50 / 5) != 10 => 10 != 10 => false
        // B4: "string two" CONTAINS "three" => false
        // Evaluation: (((B1 OR B2) AND B3) OR B4)
        // => (((false OR true) AND false) OR false) => ((true AND false) OR false) => (false OR false) => false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_4 = create_memory_block_from_i32(4);
        let num_3_exp = create_memory_block_from_i32(3); // Exponent
        let num_60 = create_memory_block_from_i32(60);
        let num_50 = create_memory_block_from_i32(50);
        let num_5_div = create_memory_block_from_i32(5);
        let num_10 = create_memory_block_from_i32(10);

        let str_one = create_memory_block_from_string("string one");
        let str_str = create_memory_block_from_string("str");
        let str_two = create_memory_block_from_string("string two");
        let str_three = create_memory_block_from_string("three");

        let exp_idx = processor.add_math_operation(DbType::I32, num_4, num_3_exp, MathOperation::Exponent);
        processor.add_comparison(DbType::I32, exp_idx, num_60, ComparerOperation::Equals, Some(Next::Or)); // B1

        processor.add_comparison(DbType::STRING, str_one, str_str, ComparerOperation::StartsWith, Some(Next::And)); // B2

        let div_idx = processor.add_math_operation(DbType::I32, num_50, num_5_div, MathOperation::Divide);
        processor.add_comparison(DbType::I32, div_idx, num_10, ComparerOperation::NotEquals, Some(Next::Or)); // B3

        processor.add_comparison(DbType::STRING, str_two, str_three, ComparerOperation::Contains, None); // B4

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), false);
    }

    #[test]
    fn test_complex_mixed_scenario_12() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (7 * 0) = 0 OR "a" != "a" ) AND ( (100 / 10) >= 10 OR "b" STARTSWITH "c" ) AND ( (2+2) = 4 )
        // B1: (7 * 0) = 0 => 0 = 0 => true
        // B2: "a" != "a" => false
        // B3: (100 / 10) >= 10 => 10 >= 10 => true
        // B4: "b" STARTSWITH "c" => false
        // B5: (2+2) = 4 => 4 = 4 => true
        // Evaluation: ((((B1 OR B2) AND B3) OR B4) AND B5)
        // => ((((true OR false) AND true) OR false) AND true) => (((true AND true) OR false) AND true)
        // => ((true OR false) AND true) => (true AND true) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_7 = create_memory_block_from_i32(7);
        let num_0 = create_memory_block_from_i32(0);
        let num_100 = create_memory_block_from_i32(100);
        let num_10 = create_memory_block_from_i32(10);
        let num_2 = create_memory_block_from_i32(2);
        let num_4 = create_memory_block_from_i32(4);

        let str_a = create_memory_block_from_string("a");
        let str_b = create_memory_block_from_string("b");
        let str_c = create_memory_block_from_string("c");

        let mul_idx = processor.add_math_operation(DbType::I32, num_7, num_0.clone(), MathOperation::Multiply);
        processor.add_comparison(DbType::I32, mul_idx, num_0, ComparerOperation::Equals, Some(Next::Or)); // B1

        processor.add_comparison(DbType::STRING, str_a.clone(), str_a, ComparerOperation::NotEquals, Some(Next::And)); // B2

        let div_idx = processor.add_math_operation(DbType::I32, num_100, num_10.clone(), MathOperation::Divide);
        processor.add_comparison(DbType::I32, div_idx, num_10, ComparerOperation::GreaterOrEquals, Some(Next::Or)); // B3

        processor.add_comparison(DbType::STRING, str_b, str_c, ComparerOperation::StartsWith, Some(Next::And)); // B4
        
        let sum_idx = processor.add_math_operation(DbType::I32, num_2.clone(), num_2, MathOperation::Add);
        processor.add_comparison(DbType::I32, sum_idx, num_4, ComparerOperation::Equals, None); // B5

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true);
    }

    #[test]
    fn test_complex_mixed_scenario_13() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( "xyz" ENDSWITH "yz" AND (5-5) = 1 ) OR ( "abc" CONTAINS "d" AND (10*1) > 5 )
        // B1: "xyz" ENDSWITH "yz" => true
        // M1: 5-5 = 0
        // B2: M1 = 1 => 0 = 1 => false
        // B3: "abc" CONTAINS "d" => false
        // M2: 10*1 = 10
        // B4: M2 > 5 => 10 > 5 => true
        // Evaluation: (((B1 AND B2) OR B3) AND B4)
        // => (((true AND false) OR false) AND true) => ((false OR false) AND true) => (false AND true) => false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_5 = create_memory_block_from_i32(5);
        let num_1 = create_memory_block_from_i32(1);
        let num_10 = create_memory_block_from_i32(10);

        let str_xyz = create_memory_block_from_string("xyz");
        let str_yz = create_memory_block_from_string("yz");
        let str_abc = create_memory_block_from_string("abc");
        let str_d = create_memory_block_from_string("d");

        processor.add_comparison(DbType::STRING, str_xyz, str_yz, ComparerOperation::EndsWith, Some(Next::And)); // B1

        let diff_idx = processor.add_math_operation(DbType::I32, num_5.clone(), num_5.clone(), MathOperation::Subtract); // M1
        processor.add_comparison(DbType::I32, diff_idx, num_1.clone(), ComparerOperation::Equals, Some(Next::Or)); // B2

        processor.add_comparison(DbType::STRING, str_abc, str_d, ComparerOperation::Contains, Some(Next::And)); // B3

        let mul_idx = processor.add_math_operation(DbType::I32, num_10, num_1, MathOperation::Multiply); // M2
        processor.add_comparison(DbType::I32, mul_idx, num_5, ComparerOperation::Greater, None); // B4

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), false);
    }

    #[test]
    fn test_complex_mixed_scenario_15() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (2**5) != 32 AND "A" = "B" ) OR ( (10/2) > 5 AND "C" != "D" ) OR ( (1-1) = 0 )
        // B1: (2**5) != 32 => 32 != 32 => false
        // B2: "A" = "B" => false
        // B3: (10/2) > 5 => 5 > 5 => false
        // B4: "C" != "D" => true
        // B5: (1-1) = 0 => 0 = 0 => true
        // Evaluation: ((( (B1 AND B2) OR B3) AND B4) OR B5)
        // => ((( (false AND false) OR false) AND true) OR true) => (((false OR false) AND true) OR true)
        // => ((false AND true) OR true) => (false OR true) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_2 = create_memory_block_from_i32(2);
        let num_5_exp = create_memory_block_from_i32(5);
        let num_32 = create_memory_block_from_i32(32);
        let num_10 = create_memory_block_from_i32(10);
        let num_2_div = create_memory_block_from_i32(2);
        let num_5_comp = create_memory_block_from_i32(5);
        let num_1 = create_memory_block_from_i32(1);
        let num_0 = create_memory_block_from_i32(0);

        let str_a = create_memory_block_from_string("A");
        let str_b = create_memory_block_from_string("B");
        let str_c = create_memory_block_from_string("C");
        let str_d = create_memory_block_from_string("D");

        let exp_idx = processor.add_math_operation(DbType::I32, num_2.clone(), num_5_exp, MathOperation::Exponent);
        processor.add_comparison(DbType::I32, exp_idx, num_32, ComparerOperation::NotEquals, Some(Next::And)); // B1

        processor.add_comparison(DbType::STRING, str_a, str_b, ComparerOperation::Equals, Some(Next::Or)); // B2

        let div_idx = processor.add_math_operation(DbType::I32, num_10, num_2_div, MathOperation::Divide);
        processor.add_comparison(DbType::I32, div_idx, num_5_comp, ComparerOperation::Greater, Some(Next::And)); // B3

        processor.add_comparison(DbType::STRING, str_c, str_d, ComparerOperation::NotEquals, Some(Next::Or)); // B4

        let diff_idx = processor.add_math_operation(DbType::I32, num_1.clone(), num_1, MathOperation::Subtract);
        processor.add_comparison(DbType::I32, diff_idx, num_0, ComparerOperation::Equals, None); // B5

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true);
    }

    #[test]
    fn test_complex_mixed_scenario_16_all_false_chain() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (10 + 10) = 21 AND "true" = "false" ) OR ( (5 * 5) < 20 AND "pass" = "fail" ) OR ( (100-1) > 100 )
        // B1: (10+10)=21 => 20=21 => false
        // B2: "true"="false" => false
        // B3: (5*5)<20 => 25<20 => false
        // B4: "pass"="fail" => false
        // B5: (100-1)>100 => 99>100 => false
        // Evaluation: ((( (B1 AND B2) OR B3) AND B4) OR B5)
        // => ((( (false AND false) OR false) AND false) OR false) => (((false OR false) AND false) OR false)
        // => ((false AND false) OR false) => (false OR false) => false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_10 = create_memory_block_from_i32(10);
        let num_21 = create_memory_block_from_i32(21);
        let num_5 = create_memory_block_from_i32(5);
        let num_20 = create_memory_block_from_i32(20);
        let num_100 = create_memory_block_from_i32(100);
        let num_1 = create_memory_block_from_i32(1);

        let str_true = create_memory_block_from_string("true");
        let str_false = create_memory_block_from_string("false");
        let str_pass = create_memory_block_from_string("pass");
        let str_fail = create_memory_block_from_string("fail");

        let sum_idx = processor.add_math_operation(DbType::I32, num_10.clone(), num_10.clone(), MathOperation::Add);
        processor.add_comparison(DbType::I32, sum_idx, num_21, ComparerOperation::Equals, Some(Next::And)); // B1

        processor.add_comparison(DbType::STRING, str_true, str_false, ComparerOperation::Equals, Some(Next::Or)); // B2

        let mul_idx = processor.add_math_operation(DbType::I32, num_5.clone(), num_5.clone(), MathOperation::Multiply);
        processor.add_comparison(DbType::I32, mul_idx, num_20, ComparerOperation::Less, Some(Next::And)); // B3

        processor.add_comparison(DbType::STRING, str_pass, str_fail, ComparerOperation::Equals, Some(Next::Or)); // B4

        let diff_idx = processor.add_math_operation(DbType::I32, num_100.clone(), num_1, MathOperation::Subtract);
        processor.add_comparison(DbType::I32, diff_idx, num_100.clone(), ComparerOperation::Greater, None); // B5

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), false);
    }

    #[test]
    fn test_complex_mixed_scenario_18_alternating_true_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (10 > 5) AND "str1" = "str2" ) OR ( (20-10) = 10 AND "str3" != "str3" ) OR ( (3*3) < 10 )
        // B1: 10 > 5 => true
        // B2: "str1" = "str2" => false
        // B3: (20-10) = 10 => 10 = 10 => true
        // B4: "str3" != "str3" => false
        // B5: (3*3) < 10 => 9 < 10 => true
        // Evaluation: ((( (B1 AND B2) OR B3) AND B4) OR B5)
        // => ((( (true AND false) OR true) AND false) OR true) => (((false OR true) AND false) OR true)
        // => ((true AND false) OR true) => (false OR true) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_10 = create_memory_block_from_i32(10);
        let num_5 = create_memory_block_from_i32(5);
        let num_20 = create_memory_block_from_i32(20);
        let num_3 = create_memory_block_from_i32(3);

        let str1 = create_memory_block_from_string("str1");
        let str2 = create_memory_block_from_string("str2");
        let str3 = create_memory_block_from_string("str3");

        processor.add_comparison(DbType::I32, num_10.clone(), num_5.clone(), ComparerOperation::Greater, Some(Next::And)); // B1
        processor.add_comparison(DbType::STRING, str1, str2, ComparerOperation::Equals, Some(Next::Or)); // B2

        let diff_idx = processor.add_math_operation(DbType::I32, num_20, num_10.clone(), MathOperation::Subtract);
        processor.add_comparison(DbType::I32, diff_idx, num_10.clone(), ComparerOperation::Equals, Some(Next::And)); // B3
        processor.add_comparison(DbType::STRING, str3.clone(), str3, ComparerOperation::NotEquals, Some(Next::Or)); // B4

        let mul_idx = processor.add_math_operation(DbType::I32, num_3.clone(), num_3, MathOperation::Multiply);
        processor.add_comparison(DbType::I32, mul_idx, num_10, ComparerOperation::Less, None); // B5

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        assert_eq!(processor.execute(&mut bool_buffer), true);
    }

    #[test]
    fn test_complex_mixed_scenario_19_mostly_string_ops() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( "start" STARTSWITH "st" AND (1+1)=2 ) OR ( "middle" CONTAINS "ddl" AND (5>10) ) OR ( "end" ENDSWITH "D" )
        // B1: "start" STARTSWITH "st" => true
        // B2: (1+1)=2 => true
        // B3: "middle" CONTAINS "ddl" => true
        // B4: (5>10) => false
        // B5: "end" ENDSWITH "D" => false (case-sensitive)
        // Evaluation: (B1 AND B2) OR (B3 AND B4) OR B5
        // => (true AND true) OR (true AND false) OR false
        // => true OR false OR false
        // => true
        
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_1 = create_memory_block_from_i32(1);
        let num_2 = create_memory_block_from_i32(2);
        let num_5 = create_memory_block_from_i32(5);
        let num_10 = create_memory_block_from_i32(10);

        let str_start = create_memory_block_from_string("start");
        let str_st = create_memory_block_from_string("st");
        let str_middle = create_memory_block_from_string("middle");
        let str_ddl = create_memory_block_from_string("ddl");
        let str_end = create_memory_block_from_string("end");
        let str_cap_d = create_memory_block_from_string("D");

        processor.add_comparison(DbType::STRING, str_start, str_st, ComparerOperation::StartsWith, Some(Next::And)); // B1
        let sum_idx = processor.add_math_operation(DbType::I32, num_1.clone(), num_1, MathOperation::Add);
        processor.add_comparison(DbType::I32, sum_idx, num_2, ComparerOperation::Equals, Some(Next::Or)); // B2

        processor.add_comparison(DbType::STRING, str_middle, str_ddl, ComparerOperation::Contains, Some(Next::And)); // B3
        processor.add_comparison(DbType::I32, num_5, num_10, ComparerOperation::Greater, Some(Next::Or)); // B4

        processor.add_comparison(DbType::STRING, str_end, str_cap_d, ComparerOperation::EndsWith, None); // B5

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        assert_eq!(processor.execute(&mut bool_buffer), true); // Should be true, not false!
    }

    #[test]
    fn test_complex_mixed_scenario_20_final_true_from_last_or() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (100/0) ... this would panic, so use valid math ... (100/25)=3 AND "no" = "yes" ) OR ( (10-20) < 0 AND "false" != "true" ) OR ( (1*1)=1 )
        // B1: (100/25)=3 => 4=3 => false
        // B2: "no" = "yes" => false
        // B3: (10-20) < 0 => -10 < 0 => true
        // B4: "false" != "true" => true
        // B5: (1*1)=1 => 1=1 => true
        // Evaluation: ((( (B1 AND B2) OR B3) AND B4) OR B5)
        // => ((( (false AND false) OR true) AND true) OR true) => (((false OR true) AND true) OR true)
        // => ((true AND true) OR true) => (true OR true) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_100 = create_memory_block_from_i32(100);
        let num_25 = create_memory_block_from_i32(25);
        let num_3 = create_memory_block_from_i32(3);
        let num_10 = create_memory_block_from_i32(10);
        let num_20 = create_memory_block_from_i32(20);
        let num_0 = create_memory_block_from_i32(0);
        let num_1 = create_memory_block_from_i32(1);

        let str_no = create_memory_block_from_string("no");
        let str_yes = create_memory_block_from_string("yes");
        let str_false = create_memory_block_from_string("false");
        let str_true = create_memory_block_from_string("true");

        let div_idx = processor.add_math_operation(DbType::I32, num_100, num_25, MathOperation::Divide);
        processor.add_comparison(DbType::I32, div_idx, num_3, ComparerOperation::Equals, Some(Next::And)); // B1
        processor.add_comparison(DbType::STRING, str_no, str_yes, ComparerOperation::Equals, Some(Next::Or)); // B2

        let diff_idx = processor.add_math_operation(DbType::I32, num_10, num_20, MathOperation::Subtract);
        processor.add_comparison(DbType::I32, diff_idx, num_0, ComparerOperation::Less, Some(Next::And)); // B3
        processor.add_comparison(DbType::STRING, str_false, str_true, ComparerOperation::NotEquals, Some(Next::Or)); // B4

        let mul_idx = processor.add_math_operation(DbType::I32, num_1.clone(), num_1.clone(), MathOperation::Multiply);
        processor.add_comparison(DbType::I32, mul_idx, num_1.clone(), ComparerOperation::Equals, None); // B5

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true);
    }

    #[test]
    fn test_complex_mixed_scenario_17_heavy_nesting_math() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( ((100 / (2*5)) - 5) = 5 AND "complex_string_test_case" CONTAINS "test" ) OR ( ( (2**3)**2 ) < 60 AND "end" ENDSWITH "nd" )
        // M1: 2*5 = 10
        // M2: 100/M1 = 100/10 = 10
        // M3: M2-5 = 10-5 = 5
        // B1: M3 = 5 => 5 = 5 => true
        // B2: "complex_string_test_case" CONTAINS "test" => true
        // M4: 2**3 = 8
        // M5: M4**2 = 8**2 = 64
        // B3: M5 < 60 => 64 < 60 => false
        // B4: "end" ENDSWITH "nd" => true
        // Evaluation:
        // 1. B1 (true) AND B2 (true) => true
        // 2. (result of 1) (true) OR B3 (false) => true
        // 3. (result of 2) (true) AND B4 (true) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_100 = create_memory_block_from_i32(100);
        let num_2 = create_memory_block_from_i32(2);
        let num_5 = create_memory_block_from_i32(5);
        let num_3_exp = create_memory_block_from_i32(3);
        let num_2_exp = create_memory_block_from_i32(2);
        let num_60 = create_memory_block_from_i32(60);

        let str_complex = create_memory_block_from_string("complex_string_test_case");
        let str_test = create_memory_block_from_string("test");
        let str_end = create_memory_block_from_string("end");
        let str_nd = create_memory_block_from_string("nd");

        let m1_idx = processor.add_math_operation(DbType::I32, num_2.clone(), num_5.clone(), MathOperation::Multiply);
        let m2_idx = processor.add_math_operation(DbType::I32, num_100.clone(), m1_idx, MathOperation::Divide);
        let m3_idx = processor.add_math_operation(DbType::I32, m2_idx, num_5.clone(), MathOperation::Subtract);
        processor.add_comparison(DbType::I32, m3_idx, num_5, ComparerOperation::Equals, Some(Next::And)); // B1

        processor.add_comparison(DbType::STRING, str_complex, str_test, ComparerOperation::Contains, Some(Next::Or)); // B2

        let m4_idx = processor.add_math_operation(DbType::I32, num_2.clone(), num_3_exp, MathOperation::Exponent);
        let m5_idx = processor.add_math_operation(DbType::I32, m4_idx, num_2_exp, MathOperation::Exponent);
        processor.add_comparison(DbType::I32, m5_idx, num_60, ComparerOperation::Less, Some(Next::And)); // B3

        processor.add_comparison(DbType::STRING, str_end, str_nd, ComparerOperation::EndsWith, None); // B4

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true); // Corrected based on left-to-right
    }

    #[test]
    fn test_complex_mixed_scenario_14() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (9 ROOT 2) < 3 OR "hello world" STARTSWITH "hello" ) AND ( (4+4+4) = 12 OR "false" = "true" )
        // M1: 9 ROOT 2 = 3 (integer square root)
        // B1: M1 < 3 => 3 < 3 => false
        // B2: "hello world" STARTSWITH "hello" => true
        // M2: 4+4 = 8
        // M3: M2+4 = 8+4 = 12
        // B3: M3 = 12 => 12 = 12 => true
        // B4: "false" = "true" => false
        // Evaluation:
        // 1. B1 (false) OR B2 (true) => true
        // 2. (result of 1) (true) AND B3 (true) => true
        // 3. (result of 2) (true) OR B4 (false) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_9_val = create_memory_block_from_i32(9); // Renamed to avoid conflict
        let num_2_root = create_memory_block_from_i32(2);
        let num_3 = create_memory_block_from_i32(3);
        let num_4 = create_memory_block_from_i32(4);
        let num_12 = create_memory_block_from_i32(12);

        let str_helloworld = create_memory_block_from_string("hello world");
        let str_hello = create_memory_block_from_string("hello");
        let str_false = create_memory_block_from_string("false");
        let str_true = create_memory_block_from_string("true");

        let root_idx = processor.add_math_operation(DbType::I32, num_9_val, num_2_root, MathOperation::Root); // M1
        processor.add_comparison(DbType::I32, root_idx, num_3, ComparerOperation::Less, Some(Next::Or)); // B1

        processor.add_comparison(DbType::STRING, str_helloworld, str_hello, ComparerOperation::StartsWith, Some(Next::And)); // B2

        let sum1_idx = processor.add_math_operation(DbType::I32, num_4.clone(), num_4.clone(), MathOperation::Add); // M2
        let sum2_idx = processor.add_math_operation(DbType::I32, sum1_idx, num_4, MathOperation::Add); // M3
        processor.add_comparison(DbType::I32, sum2_idx, num_12, ComparerOperation::Equals, Some(Next::Or)); // B3

        processor.add_comparison(DbType::STRING, str_false, str_true, ComparerOperation::Equals, None); // B4

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true); // Corrected based on left-to-right
    }

    #[test]
    fn test_complex_mixed_scenario_11() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (1 + 2 + 3) = 6 AND "complex" CONTAINS "plex" ) OR ( (10 - 1) < 8 AND "test" = "testing" )
        // M1: 1 + 2 = 3
        // M2: M1 + 3 = 3 + 3 = 6
        // B1: M2 = 6 => 6 = 6 => true
        // B2: "complex" CONTAINS "plex" => true
        // M3: 10 - 1 = 9
        // B3: M3 < 8 => 9 < 8 => false
        // B4: "test" = "testing" => false
        // Evaluation: (B1 AND B2) OR (B3 AND B4) = (true AND true) OR (false AND false) = true OR false = true
        
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_1 = create_memory_block_from_i32(1);
        let num_2 = create_memory_block_from_i32(2);
        let num_3 = create_memory_block_from_i32(3);
        let num_6 = create_memory_block_from_i32(6);
        let num_10 = create_memory_block_from_i32(10);
        let num_8 = create_memory_block_from_i32(8);

        let str_complex = create_memory_block_from_string("complex");
        let str_plex = create_memory_block_from_string("plex");
        let str_test = create_memory_block_from_string("test");
        let str_testing = create_memory_block_from_string("testing");

        let sum1_idx = processor.add_math_operation(DbType::I32, num_1.clone(), num_2.clone(), MathOperation::Add); // M1
        let sum2_idx = processor.add_math_operation(DbType::I32, sum1_idx, num_3.clone(), MathOperation::Add); // M2
        processor.add_comparison(DbType::I32, sum2_idx, num_6, ComparerOperation::Equals, Some(Next::And)); // B1

        processor.add_comparison(DbType::STRING, str_complex, str_plex, ComparerOperation::Contains, Some(Next::Or)); // B2

        let diff_idx = processor.add_math_operation(DbType::I32, num_10, num_1, MathOperation::Subtract); // M3
        processor.add_comparison(DbType::I32, diff_idx, num_8, ComparerOperation::Less, Some(Next::And)); // B3

        processor.add_comparison(DbType::STRING, str_test, str_testing, ComparerOperation::Equals, None); // B4

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        assert_eq!(processor.execute(&mut bool_buffer), true); // Should be true, not false!
    }

    #[test]
    fn test_complex_mixed_scenario_9() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Logic: ( (20 - (3*5)) > 0 AND "example" ENDSWITH "ple" ) OR ( (100 ROOT 2) < 9 AND "another" = "another" )
        // M1: 3 * 5 = 15
        // M1_res: 20 - M1 = 20 - 15 = 5
        // B1: M1_res > 0 => 5 > 0 => true
        // B2: "example" ENDSWITH "ple" => true
        // M2: 100 ROOT 2 = 10
        // B3: M2 < 9 => 10 < 9 => false
        // B4: "another" = "another" => true
        // Evaluation:
        // 1. B1 (true) AND B2 (true) => true
        // 2. (result of 1) (true) OR B3 (false) => true
        // 3. (result of 2) (true) AND B4 (true) => true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let num_20_val = create_memory_block_from_i32(20); // Renamed
        let num_3_val = create_memory_block_from_i32(3);   // Renamed
        let num_5_val = create_memory_block_from_i32(5);   // Renamed
        let num_0_val = create_memory_block_from_i32(0);   // Renamed
        let num_100_val = create_memory_block_from_i32(100); // Renamed
        let num_2_root_val = create_memory_block_from_i32(2); // Renamed
        let num_9_val = create_memory_block_from_i32(9);     // Renamed

        let str_example = create_memory_block_from_string("example");
        let str_ple = create_memory_block_from_string("ple");
        let str_another = create_memory_block_from_string("another");

        let mul_res_idx = processor.add_math_operation(DbType::I32, num_3_val, num_5_val, MathOperation::Multiply); // M1
        let sub_res_idx = processor.add_math_operation(DbType::I32, num_20_val, mul_res_idx, MathOperation::Subtract); // M1_res
        processor.add_comparison(DbType::I32, sub_res_idx, num_0_val, ComparerOperation::Greater, Some(Next::And)); // B1

        processor.add_comparison(DbType::STRING, str_example, str_ple, ComparerOperation::EndsWith, Some(Next::Or)); // B2

        let root_res_idx = processor.add_math_operation(DbType::I32, num_100_val, num_2_root_val, MathOperation::Root); // M2
        processor.add_comparison(DbType::I32, root_res_idx, num_9_val, ComparerOperation::Less, Some(Next::And)); // B3

        processor.add_comparison(DbType::STRING, str_another.clone(), str_another, ComparerOperation::Equals, None); // B4

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true); // Corrected based on left-to-right
    }

    // --- Ultra Complex Scenarios (15 contributing operations) ---

    #[test]
    #[allow(non_snake_case)]
    fn test_ultra_complex_scenario_1_expected_true() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Operations: 5 math, 10 comparisons = 15 total contributing operations
        // Logic:
        // Op1: Math: (10 + 20) -> res_m1 (30)
        // Op2: Comp: res_m1 = 30 (B1: true) AND
        // Op3: Comp: "rust" STARTSWITH "ru" (B2: true) OR
        // Op4: Math: (5 * 5) -> res_m2 (25)
        // Op5: Comp: res_m2 > 20 (B3: true) AND
        // Op6: Comp: "processor" CONTAINS "cess" (B4: true) AND
        // Op7: Math: (100 / 4) -> res_m3 (25)
        // Op8: Comp: res_m3 = 25 (B5: true) OR
        // Op9: Comp: "test" != "testing" (B6: true) AND
        // Op10: Math: (2 ** 4) -> res_m4 (16)
        // Op11: Comp: res_m4 >= 16 (B7: true) AND
        // Op12: Comp: "example" ENDSWITH "ple" (B8: true) OR
        // Op13: Math: (81 ROOT 2) -> res_m5 (9)
        // Op14: Comp: res_m5 < 10 (B9: true) AND
        // Op15: Comp: "final" = "final" (B10: true)
        //
        // Evaluation (left-to-right):
        // 1. B1 (true) AND B2 (true) => true
        // 2. (prev: true) OR B3 (true) => true
        // 3. (prev: true) AND B4 (true) => true
        // 4. (prev: true) AND B5 (true) => true
        // 5. (prev: true) OR B6 (true) => true
        // 6. (prev: true) AND B7 (true) => true
        // 7. (prev: true) AND B8 (true) => true
        // 8. (prev: true) OR B9 (true) => true
        // 9. (prev: true) AND B10 (true) => true
        // Expected: true
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let uc1_num_10 = create_memory_block_from_i32(10);
        let uc1_num_20 = create_memory_block_from_i32(20);
        let uc1_num_30 = create_memory_block_from_i32(30);
        let uc1_str_rust = create_memory_block_from_string("rust");
        let uc1_str_ru = create_memory_block_from_string("ru");
        let uc1_num_5 = create_memory_block_from_i32(5);
        let uc1_str_processor = create_memory_block_from_string("processor");
        let uc1_str_cess = create_memory_block_from_string("cess");
        let uc1_num_100 = create_memory_block_from_i32(100);
        let uc1_num_4 = create_memory_block_from_i32(4);
        let uc1_num_25 = create_memory_block_from_i32(25);
        let uc1_str_test = create_memory_block_from_string("test");
        let uc1_str_testing = create_memory_block_from_string("testing");
        let uc1_num_2 = create_memory_block_from_i32(2);
        let uc1_num_16 = create_memory_block_from_i32(16);
        let uc1_str_example = create_memory_block_from_string("example");
        let uc1_str_ple = create_memory_block_from_string("ple");
        let uc1_num_81 = create_memory_block_from_i32(81);
        let uc1_num_sq_root = create_memory_block_from_i32(2); // for ROOT 2
        let uc1_str_final = create_memory_block_from_string("final");

        // Op1 & Op2
        let res_m1 = processor.add_math_operation(DbType::I32, uc1_num_10.clone(), uc1_num_20.clone(), MathOperation::Add);
        processor.add_comparison(DbType::I32, res_m1, uc1_num_30.clone(), ComparerOperation::Equals, Some(Next::And)); // B1
        // Op3
        processor.add_comparison(DbType::STRING, uc1_str_rust.clone(), uc1_str_ru.clone(), ComparerOperation::StartsWith, Some(Next::Or)); // B2
        // Op4 & Op5
        let res_m2 = processor.add_math_operation(DbType::I32, uc1_num_5.clone(), uc1_num_5.clone(), MathOperation::Multiply);
        processor.add_comparison(DbType::I32, res_m2, uc1_num_20.clone(), ComparerOperation::Greater, Some(Next::And)); // B3
        // Op6
        processor.add_comparison(DbType::STRING, uc1_str_processor.clone(), uc1_str_cess.clone(), ComparerOperation::Contains, Some(Next::And)); // B4
        // Op7 & Op8
        let res_m3 = processor.add_math_operation(DbType::I32, uc1_num_100.clone(), uc1_num_4.clone(), MathOperation::Divide);
        processor.add_comparison(DbType::I32, res_m3, uc1_num_25.clone(), ComparerOperation::Equals, Some(Next::Or)); // B5
        // Op9
        processor.add_comparison(DbType::STRING, uc1_str_test.clone(), uc1_str_testing.clone(), ComparerOperation::NotEquals, Some(Next::And)); // B6
        // Op10 & Op11
        let res_m4 = processor.add_math_operation(DbType::I32, uc1_num_2.clone(), uc1_num_4.clone(), MathOperation::Exponent);
        processor.add_comparison(DbType::I32, res_m4, uc1_num_16.clone(), ComparerOperation::GreaterOrEquals, Some(Next::And)); // B7
        // Op12
        processor.add_comparison(DbType::STRING, uc1_str_example.clone(), uc1_str_ple.clone(), ComparerOperation::EndsWith, Some(Next::Or)); // B8
        // Op13 & Op14
        let res_m5 = processor.add_math_operation(DbType::I32, uc1_num_81.clone(), uc1_num_sq_root.clone(), MathOperation::Root);
        processor.add_comparison(DbType::I32, res_m5, uc1_num_10.clone(), ComparerOperation::Less, Some(Next::And)); // B9
        // Op15
        processor.add_comparison(DbType::STRING, uc1_str_final.clone(), uc1_str_final.clone(), ComparerOperation::Equals, None); // B10
        
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), true);
    }

    #[test]
    #[allow(non_snake_case)]
    fn test_ultra_complex_scenario_2_expected_false() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        // Operations: 5 math, 10 comparisons = 15 total contributing operations
        // Logic:
        // Op1: Math: (7 - 3) -> res_mA (4)
        // Op2: Comp: res_mA = 5 (C1: false) OR
        // Op3: Comp: "alpha" CONTAINS "Z" (C2: false) AND
        // Op4: Math: (3 * 8) -> res_mB (24)
        // Op5: Comp: res_mB <= 20 (C3: false) OR
        // Op6: Comp: "omega" ENDSWITH "gaX" (C4: false) AND
        // Op7: Math: (50 / 2) -> res_mC (25)
        // Op8: Comp: res_mC != 25 (C5: false) OR
        // Op9: Comp: "chain" = "link" (C6: false) AND
        // Op10: Math: (3 ** 3) -> res_mD (27)
        // Op11: Comp: res_mD < 27 (C7: false) OR
        // Op12: Comp: "database" STARTSWITH "Data" (C8: false) AND
        // Op13: Math: (16 ROOT 4) -> res_mE (2)
        // Op14: Comp: res_mE > 2 (C9: false) OR
        // Op15: Comp: "true_str" = "false_str" (C10: false) // Renamed variables to avoid conflict
        //
        // Evaluation (left-to-right):
        // 1. C1 (false) OR C2 (false) => false
        // 2. (prev: false) AND C3 (false) => false
        // 3. (prev: false) OR C4 (false) => false
        // 4. (prev: false) AND C5 (false) => false
        // 5. (prev: false) OR C6 (false) => false
        // 6. (prev: false) AND C7 (false) => false
        // 7. (prev: false) OR C8 (false) => false
        // 8. (prev: false) AND C9 (false) => false
        // 9. (prev: false) OR C10 (false) => false
        // Expected: false
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);

        let uc2_num_7 = create_memory_block_from_i32(7);
        let uc2_num_3 = create_memory_block_from_i32(3);
        let uc2_num_5_comp = create_memory_block_from_i32(5); // For C1 comparison
        let uc2_str_alpha = create_memory_block_from_string("alpha");
        let uc2_str_z = create_memory_block_from_string("Z");
        let uc2_num_8 = create_memory_block_from_i32(8);
        let uc2_num_20_comp = create_memory_block_from_i32(20); // For C3 comparison
        let uc2_str_omega = create_memory_block_from_string("omega");
        let uc2_str_gax = create_memory_block_from_string("gaX");
        let uc2_num_50 = create_memory_block_from_i32(50);
        let uc2_num_2_div = create_memory_block_from_i32(2); // For division and C9 comparison
        let uc2_num_25_comp = create_memory_block_from_i32(25); // For C5 comparison
        let uc2_str_chain = create_memory_block_from_string("chain");
        let uc2_str_link = create_memory_block_from_string("link");
        let uc2_num_3_exp = create_memory_block_from_i32(3); // For exponent
        let uc2_num_27_comp = create_memory_block_from_i32(27); // For C7 comparison
        let uc2_str_database = create_memory_block_from_string("database");
        let uc2_str_cap_data = create_memory_block_from_string("Data");
        let uc2_num_16_root = create_memory_block_from_i32(16);
        let uc2_num_4_root_idx = create_memory_block_from_i32(4);
        let uc2_str_true = create_memory_block_from_string("true_str"); // Renamed
        let uc2_str_false = create_memory_block_from_string("false_str"); // Renamed

        // Op1 & Op2
        let res_mA = processor.add_math_operation(DbType::I32, uc2_num_7.clone(), uc2_num_3.clone(), MathOperation::Subtract);
        processor.add_comparison(DbType::I32, res_mA, uc2_num_5_comp.clone(), ComparerOperation::Equals, Some(Next::Or)); // C1
        // Op3
        processor.add_comparison(DbType::STRING, uc2_str_alpha.clone(), uc2_str_z.clone(), ComparerOperation::Contains, Some(Next::And)); // C2
        // Op4 & Op5
        let res_mB = processor.add_math_operation(DbType::I32, uc2_num_3.clone(), uc2_num_8.clone(), MathOperation::Multiply);
        processor.add_comparison(DbType::I32, res_mB, uc2_num_20_comp.clone(), ComparerOperation::LessOrEquals, Some(Next::Or)); // C3
        // Op6
        processor.add_comparison(DbType::STRING, uc2_str_omega.clone(), uc2_str_gax.clone(), ComparerOperation::EndsWith, Some(Next::And)); // C4
        // Op7 & Op8
        let res_mC = processor.add_math_operation(DbType::I32, uc2_num_50.clone(), uc2_num_2_div.clone(), MathOperation::Divide);
        processor.add_comparison(DbType::I32, res_mC, uc2_num_25_comp.clone(), ComparerOperation::NotEquals, Some(Next::Or)); // C5
        // Op9
        processor.add_comparison(DbType::STRING, uc2_str_chain.clone(), uc2_str_link.clone(), ComparerOperation::Equals, Some(Next::And)); // C6
        // Op10 & Op11
        let res_mD = processor.add_math_operation(DbType::I32, uc2_num_3.clone(), uc2_num_3_exp.clone(), MathOperation::Exponent);
        processor.add_comparison(DbType::I32, res_mD, uc2_num_27_comp.clone(), ComparerOperation::Less, Some(Next::Or)); // C7
        // Op12
        processor.add_comparison(DbType::STRING, uc2_str_database.clone(), uc2_str_cap_data.clone(), ComparerOperation::StartsWith, Some(Next::And)); // C8
        // Op13 & Op14
        let res_mE = processor.add_math_operation(DbType::I32, uc2_num_16_root.clone(), uc2_num_4_root_idx.clone(), MathOperation::Root);
        processor.add_comparison(DbType::I32, res_mE, uc2_num_2_div.clone(), ComparerOperation::Greater, Some(Next::Or)); // C9
        // Op15
        processor.add_comparison(DbType::STRING, uc2_str_true.clone(), uc2_str_false.clone(), ComparerOperation::Equals, None); // C10

        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
assert_eq!(processor.execute(&mut bool_buffer), false);
    }

    #[test]
    fn test_complex_business_query_with_or_groups() {
        let mut transformers = SmallVec::new();
        let mut intermediate_results = SmallVec::new();
        let mut processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        let mut bool_buffer: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();

        // Create test values
        // Group 1: id = 42 AND name CONTAINS 'John'
        let id_val = create_memory_block_from_u128(42);
        let id_42 = create_memory_block_from_u128(42);
        let name_val = create_memory_block_from_string("John Smith");
        let name_pattern = create_memory_block_from_string("John");
        
        // Group 2: age > 25 AND salary < 50000.0
        let age_val = create_memory_block_from_u8(30);
        let age_comp = create_memory_block_from_u8(25);
        let salary_val = create_memory_block_from_f64(40000.0);
        let salary_comp = create_memory_block_from_f64(50000.0);
        
        // Group 3: bank_balance >= 1000.43 AND net_assets > 800000
        let bank_balance_val = create_memory_block_from_f64(1500.75);
        let bank_balance_comp = create_memory_block_from_f64(1000.43);
        let net_assets_val = create_memory_block_from_f64(900000.0);
        let net_assets_comp = create_memory_block_from_f64(800000.0);

        // Group 4: department STARTSWITH 'Eng' AND email ENDSWITH 'example.com'
        let dept_val = create_memory_block_from_string("Engineering");
        let dept_prefix = create_memory_block_from_string("Eng");
        let email_val = create_memory_block_from_string("john.smith@example.com");
        let email_suffix = create_memory_block_from_string("example.com");

        // Build the query:
        // (id = 42 AND name CONTAINS 'John') OR
        // (age > 25 AND salary < 50000.0) OR
        // (bank_balance >= 1000.43 AND net_assets > 800000) OR
        // (department STARTSWITH 'Eng' AND email ENDSWITH 'example.com')

        // First group: id = 42 AND name CONTAINS 'John'
        processor.add_comparison(
            DbType::U128,
            id_val.clone(), 
            id_42.clone(),
            ComparerOperation::Equals,
            Some(Next::And)
        );
        
        processor.add_comparison(
            DbType::STRING,
            name_val.clone(),
            name_pattern.clone(),
            ComparerOperation::Contains,
            Some(Next::Or) // OR connecting to the next group
        );
        
        // Second group: age > 25 AND salary < 50000.0
        processor.add_comparison(
            DbType::U8,
            age_val.clone(),
            age_comp.clone(),
            ComparerOperation::Greater,
            Some(Next::And)
        );
        
        processor.add_comparison(
            DbType::F64,
            salary_val.clone(),
            salary_comp.clone(),
            ComparerOperation::Less,
            Some(Next::Or) // OR connecting to the next group
        );
        
        // Third group: bank_balance >= 1000.43 AND net_assets > 800000
        processor.add_comparison(
            DbType::F64,
            bank_balance_val.clone(),
            bank_balance_comp.clone(),
            ComparerOperation::GreaterOrEquals,
            Some(Next::And)
        );
        
        processor.add_comparison(
            DbType::F64,
            net_assets_val.clone(),
            net_assets_comp.clone(),
            ComparerOperation::Greater,
            Some(Next::Or) // OR connecting to the next group
        );
        
        // Fourth group: department STARTSWITH 'Eng' AND email ENDSWITH 'example.com'
        processor.add_comparison(
            DbType::STRING,
            dept_val.clone(),
            dept_prefix.clone(),
            ComparerOperation::StartsWith,
            Some(Next::And)
        );
        
        processor.add_comparison(
            DbType::STRING,
            email_val.clone(),
            email_suffix.clone(),
            ComparerOperation::EndsWith,
            None // End of the query
        );
        
        // Execute the query - expecting true since all conditions should evaluate to true
        assert!(processor.execute(&mut bool_buffer));
        
        // Test a failing case (when none of the conditions match)
        // Clear previous state
        transformers.clear();
        intermediate_results.clear();
        bool_buffer.clear();
        
        let mut failing_processor = TransformerProcessor::new(&mut transformers, &mut intermediate_results);
        
        // Create failing test values
        let id_val_fail = create_memory_block_from_u128(43); // Not 42
        let name_val_fail = create_memory_block_from_string("Smith"); // Doesn't contain "John"
        let age_val_fail = create_memory_block_from_u8(20); // Not > 25
        let salary_val_fail = create_memory_block_from_f64(60000.0); // Not < 50000
        let bank_balance_val_fail = create_memory_block_from_f64(900.0); // Not >= 1000.43
        let net_assets_val_fail = create_memory_block_from_f64(700000.0); // Not > 800000
        let dept_val_fail = create_memory_block_from_string("Finance"); // Doesn't start with "Eng"
        let email_val_fail = create_memory_block_from_string("john@othercompany.com"); // Doesn't end with "example.com"
        
        // First failing group
        failing_processor.add_comparison(
            DbType::U128,
            id_val_fail.clone(), 
            id_42.clone(),
            ComparerOperation::Equals,
            Some(Next::And)
        );
        
        failing_processor.add_comparison(
            DbType::STRING,
            name_val_fail.clone(),
            name_pattern.clone(),
            ComparerOperation::Contains,
            Some(Next::Or)
        );
        
        // Second failing group
        failing_processor.add_comparison(
            DbType::U8,
            age_val_fail.clone(),
            age_comp.clone(),
            ComparerOperation::Greater,
            Some(Next::And)
        );
        
        failing_processor.add_comparison(
            DbType::F64,
            salary_val_fail.clone(),
            salary_comp.clone(),
            ComparerOperation::Less,
            Some(Next::Or)
        );
        
        // Third failing group
        failing_processor.add_comparison(
            DbType::F64,
            bank_balance_val_fail.clone(),
            bank_balance_comp.clone(),
            ComparerOperation::GreaterOrEquals,
            Some(Next::And)
        );
        
        failing_processor.add_comparison(
            DbType::F64,
            net_assets_val_fail.clone(),
            net_assets_comp.clone(),
            ComparerOperation::Greater,
            Some(Next::Or)
        );
        
        // Fourth failing group
        failing_processor.add_comparison(
            DbType::STRING,
            dept_val_fail.clone(),
            dept_prefix.clone(),
            ComparerOperation::StartsWith,
            Some(Next::And)
        );
        
        failing_processor.add_comparison(
            DbType::STRING,
            email_val_fail.clone(),
            email_suffix.clone(),
            ComparerOperation::EndsWith,
            None
        );
        
        // This should fail because none of the condition groups are satisfied
        assert!(!failing_processor.execute(&mut bool_buffer));
    }
}