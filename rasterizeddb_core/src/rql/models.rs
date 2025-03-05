use crate::core::column::Column;

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Column(u32),
    Value(Column),
    Operation(ComparerOperation),
    Math(MathOperation)
}