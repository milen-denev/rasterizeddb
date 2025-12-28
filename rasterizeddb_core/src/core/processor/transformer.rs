use ahash::{HashSet, HashSetExt};
use itertools::Either;
use smallvec::SmallVec;

use crate::{
    core::{db_type::DbType, helpers::smallvec_extensions::SmallVecExtensions, row::{logical::perform_comparison_operation, math::perform_math_operation}},
    memory_pool::MemoryBlock,
};

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

    pub column_1_index: Option<usize>,
    pub column_2_index: Option<usize>,
}

impl Clone for ColumnTransformer {
    fn clone(&self) -> Self {
        Self {
            column_type: self.column_type.clone(),
            column_1: self.column_1.clone(),
            column_2: self.column_2.clone(),
            transformer_type: self.transformer_type.clone(),
            next: self.next.clone(),
            result_store_index: self.result_store_index,
            result_index_1: self.result_index_1,
            result_index_2: self.result_index_2,
            column_1_index: self.column_1_index,
            column_2_index: self.column_2_index,
        }
    }
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
    LessOrEquals,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Next {
    And,
    Or,
}

#[derive(Debug, PartialEq, Clone)]
pub enum MathOperation {
    Add,
    Subtract,
    Multiply,
    Divide,
    Exponent,
    Root,
}

// A small trait to view "row columns" without cloning.
// Implemented for &[MemoryBlock] and for SmallVec of (name, MemoryBlock) rows used by QueryParser.
pub trait RowBlocks {
    fn mb_at(&self, idx: usize) -> &MemoryBlock;
    fn len(&self) -> usize;
}

impl RowBlocks for [MemoryBlock] {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        &self[idx]
    }
    #[inline]
    fn len(&self) -> usize {
        <[MemoryBlock]>::len(self)
    }
}

// Allow fixed-size arrays of MemoryBlock to be used directly as a row view in tests and callers.
impl<const N: usize> RowBlocks for [MemoryBlock; N] {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        &self[idx]
    }
    #[inline]
    fn len(&self) -> usize {
        N
    }
}

impl<'a> RowBlocks for SmallVec<[(std::borrow::Cow<'a, str>, MemoryBlock); 20]> {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        &self[idx].1
    }
    #[inline]
    fn len(&self) -> usize {
        SmallVec::len(self)
    }
}

impl ColumnTransformer {
    pub fn new(
        column_type: DbType,
        column_1: MemoryBlock,
        transformer_type: ColumnTransformerType,
        next: Option<Next>,
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
            column_1_index: None,
            column_2_index: None,
        }
    }

    // New: resolve inputs on the fly (no per-row cloning)
    #[inline]
    fn resolve_inputs<'a, R: RowBlocks + ?Sized>(
        &'a self,
        row: &'a R,
        inter: &'a [MemoryBlock],
    ) -> (&'a MemoryBlock, &'a MemoryBlock) {
        let left: &MemoryBlock = if let Some(r1) = self.result_index_1 {
            &inter[r1]
        } else if let Some(i1) = self.column_1_index {
            row.mb_at(i1)
        } else {
            &self.column_1
        };

        let right: &MemoryBlock = if let Some(r2) = self.result_index_2 {
            &inter[r2]
        } else if let Some(i2) = self.column_2_index {
            row.mb_at(i2)
        } else {
            self.column_2
                .as_ref()
                .expect("column_2 must be set before transform")
        };

        (left, right)
    }

    // New: transform using a row view
    #[inline]
    pub fn transform_on_row<R: RowBlocks + ?Sized>(
        &self,
        row: &R,
        inter: &[MemoryBlock],
    ) -> Either<MemoryBlock, (bool, Option<Next>)> {
        let (left_mb, right_mb) = self.resolve_inputs(row, inter);
        let input1 = left_mb.into_slice();
        let input2 = right_mb.into_slice();

        match self.transformer_type {
            ColumnTransformerType::MathOperation(ref operation) => {
                if self.next.is_some() {
                    panic!("Next operation is not supported for MathOperation");
                }
                Either::Left(perform_math_operation(
                    input1,
                    input2,
                    &self.column_type,
                    operation,
                ))
            }
            ColumnTransformerType::ComparerOperation(ref operation) => Either::Right((
                perform_comparison_operation(input1, input2, &self.column_type, operation),
                self.next.clone(),
            )),
        }
    }

    #[inline]
    pub fn setup_column_2(&mut self, column_2: MemoryBlock) {
        self.column_2 = Some(column_2);
    }

    #[inline]
    pub fn clear_column_2(&mut self) {
        self.column_2 = None;
    }

    #[inline]
    pub fn set_column_1_index(&mut self, index: usize) {
        self.column_1_index = Some(index);
    }

    #[inline]
    pub fn set_column_2_index(&mut self, index: usize) {
        self.column_2_index = Some(index);
    }

    // These remain for completeness, but are no longer used on the hot path.
    pub fn update_column_data_by_index(&mut self, new_columns: &[MemoryBlock]) {
        if let Some(idx1) = self.column_1_index {
            if self.result_index_1.is_none() {
                self.column_1 = new_columns
                    .get(idx1)
                    .unwrap_or_else(|| panic!("Column index {} out of bounds", idx1))
                    .clone();
            }
        }
        if let Some(idx2) = self.column_2_index {
            if self.result_index_2.is_none() {
                let mb = new_columns
                    .get(idx2)
                    .unwrap_or_else(|| panic!("Column index {} out of bounds", idx2))
                    .clone();
                self.column_2 = Some(mb);
            }
        }
    }

    pub fn get_column_dependencies(&self) -> SmallVec<[usize; 2]> {
        let mut deps = SmallVec::new();
        if let Some(i) = self.column_1_index {
            if self.result_index_1.is_none() {
                deps.push(i);
            }
        }
        if let Some(i) = self.column_2_index {
            if self.result_index_2.is_none() {
                deps.push(i);
            }
        }
        deps
    }

    pub fn has_direct_column_dependencies(&self) -> bool {
        (self.column_1_index.is_some() && self.result_index_1.is_none())
            || (self.column_2_index.is_some() && self.result_index_2.is_none())
    }

    pub fn get_used_column_indices(&self) -> Vec<usize> {
        let mut v = Vec::new();
        if let Some(i) = self.column_1_index {
            if self.result_index_1.is_none() {
                v.push(i);
            }
        }
        if let Some(i) = self.column_2_index {
            if self.result_index_2.is_none() {
                v.push(i);
            }
        }
        v
    }

    pub fn uses_column_index(&self, column_index: usize) -> bool {
        (self.column_1_index == Some(column_index) && self.result_index_1.is_none())
            || (self.column_2_index == Some(column_index) && self.result_index_2.is_none())
    }

    pub fn update_column_by_index(&mut self, column_index: usize, new_data: MemoryBlock) {
        if self.column_1_index == Some(column_index) && self.result_index_1.is_none() {
            self.column_1 = new_data.clone();
        }
        if self.column_2_index == Some(column_index) && self.result_index_2.is_none() {
            self.column_2 = Some(new_data);
        }
    }

    pub fn debug_info(&self) -> String {
        format!(
            "ColumnTransformer {{ type: {:?}, col1_idx: {:?}, col2_idx: {:?}, res_idx1: {:?}, res_idx2: {:?}, store_idx: {:?}, next: {:?} }}",
            self.transformer_type,
            self.column_1_index,
            self.column_2_index,
            self.result_index_1,
            self.result_index_2,
            self.result_store_index,
            self.next
        )
    }

    pub fn validate(&self) -> Result<(), String> {
        match self.transformer_type {
            ColumnTransformerType::MathOperation(_) => {
                if self.column_2.is_none() && self.result_index_2.is_none() {
                    return Err("Math operations require second operand".into());
                }
            }
            ColumnTransformerType::ComparerOperation(_) => {
                if self.column_2.is_none() && self.result_index_2.is_none() {
                    return Err("Comparison operations require second operand".into());
                }
            }
        }
        Ok(())
    }

    pub fn clear_column_indices(&mut self) {
        self.column_1_index = None;
        self.column_2_index = None;
    }
}

// TODO: Consider refactoring ComparisonOperand to remove DirectWithIndex and keep only Column(idx) for columns.
#[derive(Debug, Clone)]
pub enum ComparisonOperand {
    Direct(MemoryBlock),
    DirectWithIndex(MemoryBlock, usize),
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

impl ComparisonOperand {
    pub fn get_memory_block(&self) -> &MemoryBlock {
        match self {
            ComparisonOperand::Direct(mb) => mb,
            ComparisonOperand::DirectWithIndex(mb, _) => mb,
            ComparisonOperand::Intermediate(_) => {
                panic!("Intermediate has no direct memory block")
            }
        }
    }
    pub fn get_column_index(&self) -> Option<usize> {
        match self {
            ComparisonOperand::DirectWithIndex(_, i) => Some(*i),
            _ => None,
        }
    }
    pub fn is_column(&self) -> bool {
        matches!(self, ComparisonOperand::DirectWithIndex(_, _))
    }
    pub fn is_intermediate(&self) -> bool {
        matches!(self, ComparisonOperand::Intermediate(_))
    }
}

#[derive(Debug)]
pub struct TransformerProcessor<'a> {
    pub transformers: &'a mut SmallVec<[ColumnTransformer; 36]>,
    pub intermediate_results: &'a mut SmallVec<[MemoryBlock; 20]>,

    // New: cache maximum result index once (set when building plan)
    max_result_store_index: usize,
}

impl<'a> TransformerProcessor<'a> {
    pub fn new(
        transformers: &'a mut SmallVec<[ColumnTransformer; 36]>,
        intermediate_results: &'a mut SmallVec<[MemoryBlock; 20]>,
    ) -> Self {
        transformers.clear();
        intermediate_results.clear();
        Self {
            transformers,
            intermediate_results,
            max_result_store_index: 0,
        }
    }

    #[inline]
    fn update_max_result_index(&mut self, idx: usize) {
        if idx > self.max_result_store_index {
            self.max_result_store_index = idx;
        }
    }

    pub fn add_math_operation(
        &mut self,
        column_type: DbType,
        left_operand_in: impl Into<ComparisonOperand>,
        right_operand_in: impl Into<ComparisonOperand>,
        operation: MathOperation,
    ) -> usize {
        let result_store_idx = self.intermediate_results.len();

        let left_op = left_operand_in.into();
        let right_op = right_operand_in.into();

        let placeholder = MemoryBlock::default();
        let mut transformer = ColumnTransformer::new(
            column_type,
            placeholder.clone(),
            ColumnTransformerType::MathOperation(operation),
            None,
        );
        transformer.setup_column_2(placeholder);

        match left_op {
            ComparisonOperand::Direct(mem) => transformer.column_1 = mem,
            ComparisonOperand::DirectWithIndex(mem, index) => {
                transformer.column_1 = mem;
                transformer.set_column_1_index(index);
            }
            ComparisonOperand::Intermediate(idx) => {
                transformer.result_index_1 = Some(idx);
            }
        }
        match right_op {
            ComparisonOperand::Direct(mem) => transformer.setup_column_2(mem),
            ComparisonOperand::DirectWithIndex(mem, index) => {
                transformer.setup_column_2(mem);
                transformer.set_column_2_index(index);
            }
            ComparisonOperand::Intermediate(idx) => transformer.result_index_2 = Some(idx),
        }

        transformer.result_store_index = Some(result_store_idx);

        self.transformers.push_back(transformer);
        self.intermediate_results.push(MemoryBlock::default());
        self.update_max_result_index(result_store_idx);

        result_store_idx
    }

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

        let placeholder = MemoryBlock::default();
        let mut transformer = ColumnTransformer::new(
            column_type,
            placeholder.clone(),
            ColumnTransformerType::ComparerOperation(operation),
            next,
        );
        transformer.setup_column_2(placeholder);

        match (&left_operand, &right_operand) {
            (ComparisonOperand::Direct(m1), ComparisonOperand::Direct(m2)) => {
                transformer.column_1 = m1.clone();
                transformer.setup_column_2(m2.clone());
            }
            (ComparisonOperand::DirectWithIndex(m1, i1), ComparisonOperand::Direct(m2)) => {
                transformer.column_1 = m1.clone();
                transformer.setup_column_2(m2.clone());
                transformer.set_column_1_index(*i1);
            }
            (ComparisonOperand::Direct(m1), ComparisonOperand::DirectWithIndex(m2, i2)) => {
                transformer.column_1 = m1.clone();
                transformer.setup_column_2(m2.clone());
                transformer.set_column_2_index(*i2);
            }
            (
                ComparisonOperand::DirectWithIndex(m1, i1),
                ComparisonOperand::DirectWithIndex(m2, i2),
            ) => {
                transformer.column_1 = m1.clone();
                transformer.setup_column_2(m2.clone());
                transformer.set_column_1_index(*i1);
                transformer.set_column_2_index(*i2);
            }
            (ComparisonOperand::Intermediate(i1), ComparisonOperand::Intermediate(i2)) => {
                transformer.result_index_1 = Some(*i1);
                transformer.result_index_2 = Some(*i2);
            }
            (ComparisonOperand::Direct(m1), ComparisonOperand::Intermediate(i2)) => {
                transformer.column_1 = m1.clone();
                transformer.result_index_2 = Some(*i2);
            }
            (ComparisonOperand::DirectWithIndex(m1, i1), ComparisonOperand::Intermediate(i2)) => {
                transformer.column_1 = m1.clone();
                transformer.set_column_1_index(*i1);
                transformer.result_index_2 = Some(*i2);
            }
            (ComparisonOperand::Intermediate(i1), ComparisonOperand::Direct(m2)) => {
                transformer.result_index_1 = Some(*i1);
                transformer.setup_column_2(m2.clone());
            }
            (ComparisonOperand::Intermediate(i1), ComparisonOperand::DirectWithIndex(m2, i2)) => {
                transformer.result_index_1 = Some(*i1);
                transformer.setup_column_2(m2.clone());
                transformer.set_column_2_index(*i2);
            }
        }

        self.transformers.push_back(transformer);
    }

    // New: Ensure intermediate_results has enough capacity once (no per-row scans).
    #[inline]
    fn ensure_intermediate_capacity(&mut self) {
        if self.intermediate_results.len() <= self.max_result_store_index {
            self.intermediate_results.resize(
                self.max_result_store_index.saturating_add(1),
                MemoryBlock::default(),
            );
        }
    }

    pub fn replace_row_inputs(&mut self, _new_columns: &[MemoryBlock]) {
        self.ensure_intermediate_capacity();
    }

    // New: execute directly with a row view (no cloning of inputs)
    pub fn execute_row<R: RowBlocks + ?Sized>(
        &mut self,
        row: &R,
        comparison_results: &mut SmallVec<[(bool, Option<Next>); 20]>,
    ) -> bool {
        comparison_results.clear();
        self.ensure_intermediate_capacity();

        for t in self.transformers.iter() {
            match t.transform_on_row(row, &self.intermediate_results) {
                Either::Left(result) => {
                    if let Some(store_idx) = t.result_store_index {
                        if store_idx >= self.intermediate_results.len() {
                            self.intermediate_results
                                .resize(store_idx + 1, MemoryBlock::default());
                        }
                        self.intermediate_results[store_idx] = result;
                    }
                }
                Either::Right(comp) => {
                    comparison_results.push(comp);
                }
            }
        }

        self.evaluate_comparison_results(comparison_results)
    }

    fn evaluate_comparison_results(&self, results: &[(bool, Option<Next>)]) -> bool {
        if results.is_empty() {
            return false;
        }

        let mut current_and = true;

        for (value, next) in results.iter() {
            current_and &= *value;

            match next {
                Some(Next::And) => {}
                Some(Next::Or) => {
                    if current_and {
                        return true;
                    }
                    current_and = true; // reset for next group
                }
                None => {
                    if current_and {
                        return true;
                    }
                    break;
                }
            }
        }

        false
    }

    pub fn get_used_column_indices(&self) -> HashSet<usize> {
        let mut s = HashSet::new();
        for t in self.transformers.iter() {
            if let Some(i) = t.column_1_index {
                if t.result_index_1.is_none() {
                    s.insert(i);
                }
            }
            if let Some(i) = t.column_2_index {
                if t.result_index_2.is_none() {
                    s.insert(i);
                }
            }
        }
        s
    }

    pub fn has_column_dependencies(&self) -> bool {
        self.transformers
            .iter()
            .any(|t| t.has_direct_column_dependencies())
    }

    pub fn reset_intermediate_results(&mut self) {
        self.intermediate_results.clear();
        if self.max_result_store_index > 0 {
            self.intermediate_results
                .resize(self.max_result_store_index + 1, MemoryBlock::default());
        }
    }

    // Debug validation helpers (call in debug builds after building transformer list)
    pub fn debug_validate(&self) {
        #[cfg(debug_assertions)]
        {
            self.validate_intermediate_dependencies();
            self.validate_logic_links();
        }
    }

    #[cfg(debug_assertions)]
    fn validate_intermediate_dependencies(&self) {
        for (i, t) in self.transformers.iter().enumerate() {
            if let Some(r1) = t.result_index_1 {
                assert!(
                    r1 < self.intermediate_results.len()
                        || self
                            .transformers
                            .iter()
                            .any(|tt| tt.result_store_index == Some(r1)),
                    "Transformer {} references unknown intermediate {}",
                    i,
                    r1
                );
            }
            if let Some(r2) = t.result_index_2 {
                assert!(
                    r2 < self.intermediate_results.len()
                        || self
                            .transformers
                            .iter()
                            .any(|tt| tt.result_store_index == Some(r2)),
                    "Transformer {} references unknown intermediate {}",
                    i,
                    r2
                );
            }
        }
    }

    #[cfg(debug_assertions)]
    fn validate_logic_links(&self) {
        for (i, t) in self.transformers.iter().enumerate() {
            match t.transformer_type {
                ColumnTransformerType::MathOperation(_) => {
                    assert!(
                        t.next.is_none(),
                        "Math transformer {} unexpectedly has a logical next",
                        i
                    );
                }
                ColumnTransformerType::ComparerOperation(_) => {}
            }
        }
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

    // Helper for a row view
    struct TestRow([MemoryBlock; 2]);
    impl RowBlocks for TestRow {
        fn mb_at(&self, idx: usize) -> &MemoryBlock {
            &self.0[idx]
        }
        fn len(&self) -> usize {
            self.0.len()
        }
    }

    #[test]
    fn test_transform_on_row_math_add() {
        let i32_data_1 = create_memory_block_from_i32(10);
        let i32_data_2 = create_memory_block_from_i32(20);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1.clone(),
            ColumnTransformerType::MathOperation(MathOperation::Add),
            None,
        );
        transformer.setup_column_2(i32_data_2.clone());

        let row = TestRow([i32_data_1.clone(), i32_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter) {
            Either::Left(result) => {
                let result_slice = result.into_slice();
                let result_value = i32::from_le_bytes(result_slice.try_into().unwrap());
                assert_eq!(result_value, 30);
            }
            _ => panic!("Expected MemoryBlock result"),
        }
    }

    #[test]
    fn test_transform_on_row_comparer_equals() {
        let i32_data_1 = create_memory_block_from_i32(42);
        let i32_data_2 = create_memory_block_from_i32(42);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1.clone(),
            ColumnTransformerType::ComparerOperation(ComparerOperation::Equals),
            None,
        );
        transformer.setup_column_2(i32_data_2.clone());

        let row = TestRow([i32_data_1.clone(), i32_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter) {
            Either::Right(result) => {
                assert!(result.0);
            }
            _ => panic!("Expected bool result"),
        }
    }

    #[test]
    fn test_transform_on_row_comparer_not_equals() {
        let i32_data_1 = create_memory_block_from_i32(42);
        let i32_data_2 = create_memory_block_from_i32(43);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1.clone(),
            ColumnTransformerType::ComparerOperation(ComparerOperation::NotEquals),
            None,
        );
        transformer.setup_column_2(i32_data_2.clone());

        let row = TestRow([i32_data_1.clone(), i32_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter) {
            Either::Right(result) => {
                assert!(result.0);
            }
            _ => panic!("Expected bool result"),
        }
    }

    #[test]
    #[should_panic(expected = "Next operation is not supported for MathOperation")]
    fn test_transform_on_row_math_with_next_panic() {
        let i32_data_1 = create_memory_block_from_i32(10);
        let i32_data_2 = create_memory_block_from_i32(20);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1.clone(),
            ColumnTransformerType::MathOperation(MathOperation::Add),
            Some(Next::And),
        );
        transformer.setup_column_2(i32_data_2.clone());

        let row = TestRow([i32_data_1.clone(), i32_data_2.clone()]);
        let empty_inter = [];
        transformer.transform_on_row(&row, &empty_inter);
    }

    #[test]
    fn test_transform_on_row_comparer_contains() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("world");

        let mut transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1.clone(),
            ColumnTransformerType::ComparerOperation(ComparerOperation::Contains),
            None,
        );
        transformer.setup_column_2(string_data_2.clone());

        let row = TestRow([string_data_1.clone(), string_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter) {
            Either::Right(result) => {
                assert!(result.0);
            }
            _ => panic!("Expected bool result"),
        }
    }

    #[test]
    fn test_transform_on_row_comparer_not_contains() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("planet");

        let mut transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1.clone(),
            ColumnTransformerType::ComparerOperation(ComparerOperation::Contains),
            None,
        );
        transformer.setup_column_2(string_data_2.clone());

        let row = TestRow([string_data_1.clone(), string_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter) {
            Either::Right(result) => assert!(!result.0),
            _ => panic!("Expected bool result"),
        }
    }

    #[test]
    fn test_processor_complex_mixed_true() {
        // Row layout: [id(i32), name(str), age(u8), email(str), department(str), bank_balance(f64)]
        let id = create_memory_block_from_i32(42);
        let name = create_memory_block_from_string("John Doe");
        let age = create_memory_block_from_u8(30);
        let email = create_memory_block_from_string("john.doe@example.com");
        let department = create_memory_block_from_string("Engineering");
        let bank_balance = create_memory_block_from_f64(1000.43);
        let row = [
            id.clone(),
            name.clone(),
            age.clone(),
            email.clone(),
            department.clone(),
            bank_balance.clone(),
        ];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        // (id - 2 = 40 AND name CONTAINS 'John') OR (bank_balance >= 1000.43 AND department STARTSWITH 'Eng' AND email ENDSWITH 'example.com')
        let t_id_minus_2 = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::DirectWithIndex(id.clone(), 0),
            create_memory_block_from_i32(2),
            MathOperation::Subtract,
        );

        proc.add_comparison(
            DbType::I32,
            ComparisonOperand::Intermediate(t_id_minus_2),
            create_memory_block_from_i32(40),
            ComparerOperation::Equals,
            Some(Next::And),
        );

        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(name.clone(), 1),
            create_memory_block_from_string("John"),
            ComparerOperation::Contains,
            Some(Next::Or),
        );

        proc.add_comparison(
            DbType::F64,
            ComparisonOperand::DirectWithIndex(bank_balance.clone(), 5),
            create_memory_block_from_f64(1000.43),
            ComparerOperation::GreaterOrEquals,
            Some(Next::And),
        );

        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(department.clone(), 4),
            create_memory_block_from_string("Eng"),
            ComparerOperation::StartsWith,
            Some(Next::And),
        );

        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(email.clone(), 3),
            create_memory_block_from_string("example.com"),
            ComparerOperation::EndsWith,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let res = proc.execute_row(&row, &mut bool_buf);
        assert!(res);
    }

    #[test]
    fn test_processor_intermediate_math_chain_true() {
        // Compute: ((10 + 5) * 2 - 10) / 1 = 20
        let a = create_memory_block_from_i32(10);
        let b = create_memory_block_from_i32(5);
        let c = create_memory_block_from_i32(2);
        let d = create_memory_block_from_i32(1);
        let row = [a.clone(), b.clone(), c.clone(), d.clone()];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        let add_idx = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::DirectWithIndex(a.clone(), 0),
            ComparisonOperand::DirectWithIndex(b.clone(), 1),
            MathOperation::Add,
        );
        let mul_idx = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::Intermediate(add_idx),
            ComparisonOperand::DirectWithIndex(c.clone(), 2),
            MathOperation::Multiply,
        );
        let sub_idx = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::Intermediate(mul_idx),
            create_memory_block_from_i32(10),
            MathOperation::Subtract,
        );
        let div_idx = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::Intermediate(sub_idx),
            ComparisonOperand::DirectWithIndex(d.clone(), 3),
            MathOperation::Divide,
        );

        proc.add_comparison(
            DbType::I32,
            ComparisonOperand::Intermediate(div_idx),
            create_memory_block_from_i32(20),
            ComparerOperation::Equals,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let res = proc.execute_row(&row, &mut bool_buf);
        assert!(res);
    }

    #[test]
    fn test_processor_or_groups_false() {
        // (id = 0 AND name CONTAINS 'Nope') OR (bank_balance < 1000.0 AND department STARTSWITH 'Sales')
        let id = create_memory_block_from_i32(42);
        let name = create_memory_block_from_string("John Doe");
        let email = create_memory_block_from_string("john.doe@example.com");
        let department = create_memory_block_from_string("Engineering");
        let bank_balance = create_memory_block_from_f64(1000.43);
        let row = [
            id.clone(),
            name.clone(),
            email.clone(),
            department.clone(),
            bank_balance.clone(),
        ];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        proc.add_comparison(
            DbType::I32,
            ComparisonOperand::DirectWithIndex(id.clone(), 0),
            create_memory_block_from_i32(0),
            ComparerOperation::Equals,
            Some(Next::And),
        );
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(name.clone(), 1),
            create_memory_block_from_string("Nope"),
            ComparerOperation::Contains,
            Some(Next::Or),
        );
        proc.add_comparison(
            DbType::F64,
            ComparisonOperand::DirectWithIndex(bank_balance.clone(), 4),
            create_memory_block_from_f64(1000.0),
            ComparerOperation::Less,
            Some(Next::And),
        );
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(department.clone(), 3),
            create_memory_block_from_string("Sales"),
            ComparerOperation::StartsWith,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let res = proc.execute_row(&row, &mut bool_buf);
        assert!(!res);
    }

    #[test]
    fn test_processor_precedence_without_parentheses_true() {
        // name CONTAINS 'Nope' OR age*2 = 60 AND email ENDSWITH 'example.com' -> false OR (true AND true) => true
        let age = create_memory_block_from_u8(30);
        let email = create_memory_block_from_string("john.doe@example.com");
        let name = create_memory_block_from_string("John Doe");
        let two = create_memory_block_from_u8(2);
        let row = [name.clone(), age.clone(), email.clone(), two.clone()];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        // name CONTAINS 'Nope'
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(name.clone(), 0),
            create_memory_block_from_string("Nope"),
            ComparerOperation::Contains,
            Some(Next::Or),
        );

        // age*2 = 60
        let mul_idx = proc.add_math_operation(
            DbType::U8,
            ComparisonOperand::DirectWithIndex(age.clone(), 1),
            ComparisonOperand::DirectWithIndex(two.clone(), 3),
            MathOperation::Multiply,
        );
        proc.add_comparison(
            DbType::U8,
            ComparisonOperand::Intermediate(mul_idx),
            create_memory_block_from_u8(60),
            ComparerOperation::Equals,
            Some(Next::And),
        );

        // email ENDSWITH 'example.com'
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(email.clone(), 2),
            create_memory_block_from_string("example.com"),
            ComparerOperation::EndsWith,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let res = proc.execute_row(&row, &mut bool_buf);
        assert!(res);
    }

    #[test]
    fn test_processor_used_column_indices() {
        // Build a plan that references id(0), name(1), bank_balance(5)
        let id = create_memory_block_from_i32(42);
        let name = create_memory_block_from_string("John Doe");
        let bank_balance = create_memory_block_from_f64(1000.43);
        let forty = create_memory_block_from_i32(40);
        let row = [
            id.clone(),
            name.clone(),
            create_memory_block_from_u8(1),
            create_memory_block_from_string("x@x"),
            create_memory_block_from_string("Eng"),
            bank_balance.clone(),
        ];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        // id - 2 = 40
        let t_id_minus_2 = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::DirectWithIndex(id.clone(), 0),
            create_memory_block_from_i32(2),
            MathOperation::Subtract,
        );
        proc.add_comparison(
            DbType::I32,
            ComparisonOperand::Intermediate(t_id_minus_2),
            forty,
            ComparerOperation::Equals,
            Some(Next::Or),
        );

        // name CONTAINS 'John'
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(name.clone(), 1),
            create_memory_block_from_string("John"),
            ComparerOperation::Contains,
            Some(Next::Or),
        );

        // bank_balance >= 1000.0
        proc.add_comparison(
            DbType::F64,
            ComparisonOperand::DirectWithIndex(bank_balance.clone(), 5),
            create_memory_block_from_f64(1000.0),
            ComparerOperation::GreaterOrEquals,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let _ = proc.execute_row(&row, &mut bool_buf);

        let used = proc.get_used_column_indices();
        assert!(used.contains(&0));
        assert!(used.contains(&1));
        assert!(used.contains(&5));
        assert_eq!(used.len(), 3);
    }
}
