use itertools::Either;

use crate::{core::db_type::DbType, memory_pool::MemoryBlock};

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
        match self.transformer_type {
            ColumnTransformerType::MathOperation(ref operation) => {
                if self.next.is_some() {
                    panic!("Next operation is not supported for MathOperation");
                }

                match operation {
                    MathOperation::Add => {
                        // Perform addition
                    }
                    MathOperation::Subtract => {
                        // Perform subtraction
                    }
                    MathOperation::Multiply => {
                        // Perform multiplication
                    }
                    MathOperation::Divide => {
                        // Perform division
                    }
                    MathOperation::Exponent => {
                        // Perform exponentiation
                    }
                    MathOperation::Root => {
                        // Perform root operation
                    }
                }
            },
            ColumnTransformerType::ComparerOperation(ref operation) => {
                match operation {
                    ComparerOperation::Equals => {
                        // Perform equals operation
                    }
                    ComparerOperation::NotEquals => {
                        // Perform not equals operation
                    }
                    ComparerOperation::Contains => {
                        // Perform contains operation
                    }
                    ComparerOperation::StartsWith => {
                        // Perform starts with operation
                    }
                    ComparerOperation::EndsWith => {
                        // Perform ends with operation
                    }
                    ComparerOperation::Greater => {
                        // Perform greater operation
                    }
                    ComparerOperation::GreaterOrEquals => {
                        // Perform greater or equals operation
                    }
                    ComparerOperation::Less => {
                        // Perform less operation
                    }
                    ComparerOperation::LessOrEquals => {
                        // Perform less or equals operation
                    }
                }
            }
        };

        Either::Left(MemoryBlock::from_vec(vec![]))
    }
}