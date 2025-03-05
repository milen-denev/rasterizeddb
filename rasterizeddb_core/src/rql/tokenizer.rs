use std::cell::LazyCell;

use crate::core::column::Column;

use super::models::{ComparerOperation, MathOperation, Next, Token};

const ZERO_VALUE: LazyCell<Column> = LazyCell::new(|| Column::new(0).unwrap());

pub(crate) fn evaluate_column_result(
    required_columns: &Vec<(u32, Column)>,
    evaluation_tokens: &mut Vec<(Vec<Token>, Option<Next>)>,
    token_results: &mut Vec<(bool, Option<Next>)>,
) -> bool {
    token_results.clear();
    token_results.reserve(evaluation_tokens.len());
    
    for (tokens, next) in evaluation_tokens.iter() {
        // Fast path for empty or invalid token lists
        if tokens.is_empty() {
            token_results.push((false, next.clone()));
            continue;
        }
        
        // Find the operation and validate token structure
        // Expect tokens to be in form: [left_operands..., operation, right_operands...]
        let mut op_idx = None;
        for (idx, token) in tokens.iter().enumerate() {
            if let Token::Operation(_) = token {
                op_idx = Some(idx);
                break;
            }
        }
        
        let op_idx = match op_idx {
            Some(idx) if idx > 0 && idx < tokens.len() - 1 => idx,
            _ => {
                // Invalid token structure - need operation with left & right operands
                token_results.push((false, next.clone()));
                continue;
            }
        };
        
        // Get left side value
        let left = match evaluate_side(&tokens[..op_idx], required_columns) {
            Some(val) => val,
            None => {
                token_results.push((false, next.clone()));
                continue;
            }
        };
        
        // Get operation
        let op = match &tokens[op_idx] {
            Token::Operation(op) => op,
            _ => unreachable!("Already checked this is an operation token"),
        };
        
        // Get right side value
        let right = match evaluate_side(&tokens[op_idx+1..], required_columns) {
            Some(val) => val,
            None => {
                token_results.push((false, next.clone()));
                continue;
            }
        };
        
        // Apply comparison operation
        let result = match op {
            ComparerOperation::Equals => left.equals(&right),
            ComparerOperation::NotEquals => left.not_equal(&right),
            ComparerOperation::Greater => left.greater_than(&right),
            ComparerOperation::Less => left.less_than(&right),
            ComparerOperation::GreaterOrEquals => left.greater_or_equals(&right),
            ComparerOperation::LessOrEquals => left.less_or_equals(&right),
            ComparerOperation::Contains => left.contains(&right),
            ComparerOperation::StartsWith => left.starts_with(&right),
            ComparerOperation::EndsWith => left.ends_with(&right),
        };
        
        token_results.push((result, next.clone()));
    }
    
    get_final_result(token_results)
}

// Helper function to evaluate one side of an operation (left or right)
fn evaluate_side(tokens: &[Token], required_columns: &Vec<(u32, Column)>) -> Option<Column> {
    // No tokens, no result
    if tokens.is_empty() {
        return None;
    }
    
    // Handle simple case of just one token (column or value)
    if tokens.len() == 1 {
        return match &tokens[0] {
            Token::Column(col_id) => {
                required_columns.iter()
                    .find(|(id, _)| *id == *col_id)
                    .map(|(_, col)| col.clone())
            },
            Token::Value(val) => Some(val.clone()),
            _ => None,  // Operation or math without operands is invalid
        };
    }
    
    // Get initial value
    let mut result = match &tokens[0] {
        Token::Column(col_id) => {
            match required_columns.iter().find(|(id, _)| *id == *col_id) {
                Some((_, col)) => col.clone(),
                None => return None,  // Column not found
            }
        },
        Token::Value(val) => val.clone(),
        _ => return None,  // Unexpected token
    };
    
    // Process all math operations in sequence
    let mut i = 1;
    while i < tokens.len() {
        // We expect tokens to be in the pattern [value, math_op, value, math_op, ...]
        // So at position i we should have a math operation
        match &tokens[i] {
            Token::Math(op) => {
                // Verify we have a value after this math operation
                if i + 1 >= tokens.len() {
                    return None;  // Missing right operand
                }
                
                // Get the right operand
                let right = match &tokens[i + 1] {
                    Token::Column(col_id) => {
                        match required_columns.iter().find(|(id, _)| *id == *col_id) {
                            Some((_, col)) => col,
                            None => return None,  // Column not found
                        }
                    },
                    Token::Value(val) => val,
                    _ => return None,  // Invalid right operand
                };
                
                // Apply the math operation
                match op {
                    MathOperation::Add => result.add(right),
                    MathOperation::Subtract => result.subtract(right),
                    MathOperation::Multiply => result.multiply(right),
                    MathOperation::Divide => {
                        // Check for division by zero
                        if right.equals(&ZERO_VALUE) {
                            return None;
                        }
                        result.divide(right);
                    },
                    _ => return None,  // Unsupported operation
                }
                
                i += 2;  // Skip the math op and right operand we just processed
            },
            _ => return None,  // Expected math operation but got something else
        }
    }
    
    Some(result)
}

fn get_final_result(token_results: &mut Vec<(bool, Option<Next>)>) -> bool {
    let mut final_result = false;

    let total_results_len = token_results.len();

    if token_results.len() > 1 {
        let mut previous: Next = Next::And;
        let mut next_one: Next = Next::And;

        for first in token_results.iter().take(1) {
            final_result = first.0;
            previous = first.1.clone().unwrap();
        }

        for token_result in token_results.iter().skip(1).take(total_results_len - 2) {
            if let Some(next) = token_result.1.clone() {
                if previous == Next::And {
                    final_result = final_result && token_result.0;
                } else {
                    final_result = final_result || token_result.0;
                }
                previous = next_one;
                next_one = next;
            }
        }

        for last in token_results.iter().skip(total_results_len - 1).take(1) {
            if next_one == Next::And {
                final_result = final_result && last.0;
            } else {
                final_result = final_result || last.0;
            }
        }
    } else {
        for token_result in token_results {
            final_result = token_result.0;
        }
    }

    final_result 
}