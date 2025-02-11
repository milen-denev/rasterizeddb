use std::pin::Pin;

use crate::core::column::Column;

#[derive(Debug, PartialEq, Clone)]
pub enum ComparerOperation {
    Equals, // Done
    NotEquals, // Done
    Contains, // Done
    StartsWith, // Done
    EndsWith, // Done
    Greater, // Done
    GreaterOrEquals, // Done
    Less, // Done
    LessOrEquals // Done
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

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Column(u32),
    Math(MathOperation),
    Value(Column),
    Operation(ComparerOperation)
}