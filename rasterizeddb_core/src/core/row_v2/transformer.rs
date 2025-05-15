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

    pub fn transform(&self) -> Either<MemoryBlock, bool> {  
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