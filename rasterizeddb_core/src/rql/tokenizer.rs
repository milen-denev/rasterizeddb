use std::{cell::LazyCell, pin::Pin};

use crate::core::{column::Column, db_type::DbType};

use super::models::{ComparerOperation, MathOperation, Next, Token};

const ZERO_VALUE: LazyCell<Column> = LazyCell::new(|| {
    Column::new(0).unwrap()
});

pub(crate) fn evaluate_column_result(
    required_columns: &mut Vec<(u32, Column)>, 
    evaluation_tokens: &mut Vec<(Vec<Token>, Option<Next>)>,
    token_results: &mut Vec<(bool, Option<Next>)>) -> bool {
    
    let mut iter = evaluation_tokens.iter_mut();

    while let Some(tokens) = iter.next() {
        let mut current_value: Option<Column> = None;
        let mut token_iter = tokens.0.iter_mut();

        let evalaution_result = loop {  
            if let Some(token) = token_iter.next() {
                match token {
                    Token::Column(column_id) => {
                        // Get the value associated with the column_id
                        if let Some((_, column)) = required_columns.iter().find(|(id, _)| *id == *column_id) {
                            current_value = Some(column.clone());
                        } else {
                            continue; // Column ID not found
                        }
                    }
                    Token::Math(operation) => {
                        if let Some(ref mut left_value) = current_value.as_mut() {
                            // Get the next token for the right operand
                            let iter_result = token_iter.next();
                            if let Some(Token::Value(column)) = iter_result {
                                if column.data_type == DbType::TBD {
                                    column.into_regular(left_value.data_type.clone());
                                }

                                let right_value = column;
                                
                                // Perform the math operation
                                match operation {
                                    MathOperation::Add => left_value.add(&right_value),
                                    MathOperation::Subtract => left_value.subtract(&right_value),
                                    MathOperation::Multiply => left_value.multiply(&right_value),
                                    MathOperation::Divide => {
                                        if right_value.equals(&ZERO_VALUE)  {
                                            panic!("Division with zero is not allowed.")
                                        }
                                        left_value.divide(&right_value)
                                    }
                                    _ => todo!()
                                };
                            }  else if let Some(Token::Column(column)) = iter_result {
                                let right_value = column;
    
                                if let Some((_, column)) = required_columns.iter_mut().find(|(id, _)| *id == *right_value) {
                                    if column.data_type == DbType::TBD {
                                        column.into_regular(left_value.data_type.clone());
                                    }

                                    let right_value = column;
            
                                    // Perform the math operation
                                    match operation {
                                        MathOperation::Add => left_value.add(&right_value),
                                        MathOperation::Subtract => left_value.subtract(&right_value),
                                        MathOperation::Multiply => left_value.multiply(&right_value),
                                        MathOperation::Divide => {
                                            if right_value.equals(&ZERO_VALUE)  {
                                                panic!("Division with zero is not allowed.")
                                            }
                                            left_value.divide(&right_value)
                                        }
                                        _ => todo!()
                                    };
                                } else {
                                    continue; // Column ID not found
                                }
                            } else {
                                continue; // Missing operand
                            }
                        } else {
                            continue;
                        }
                    }
                    Token::Operation(op) => {
                    
                        if let Some(ref mut left_value) = current_value.as_mut() {


                            let next_token = token_iter.next();
                            // Get the next token for the right operand
                            if let Some(Token::Value(ref mut column)) = next_token {
                                if column.data_type == DbType::TBD {
                                    column.into_regular(left_value.data_type.clone());
                                }

                                let right_value = column;

                                // Perform the comparison
                                let result = match op {
                                    ComparerOperation::Equals => left_value.equals(&right_value),
                                    ComparerOperation::NotEquals =>  left_value.not_equal(&right_value),
                                    ComparerOperation::Greater => left_value.greater_than(&right_value),
                                    ComparerOperation::Less => left_value.less_than(&right_value),
                                    ComparerOperation::GreaterOrEquals => left_value.greater_or_equals(&right_value),
                                    ComparerOperation::LessOrEquals => left_value.less_or_equals(&right_value),
                                    ComparerOperation::Contains => left_value.contains(&right_value),
                                    ComparerOperation::StartsWith => left_value.starts_with(&right_value),
                                    ComparerOperation::EndsWith => left_value.ends_with(&right_value),
                                };

                                break result // Return the result of the comparison
                            } else if let Some(Token::Column(ref mut column)) = next_token {
                                let right_value = column;

                                if let Some((_, ref mut column)) = required_columns.iter_mut().find(|(id, _)| *id == *right_value) {
                                    if column.data_type == DbType::TBD {
                                        column.into_regular(left_value.data_type.clone());
                                    }

                                    let right_value = column;
    
                                    // Perform the comparison
                                    let result = match op {
                                        ComparerOperation::Equals => left_value.equals(&right_value),
                                        ComparerOperation::NotEquals =>  left_value.not_equal(&right_value),
                                        ComparerOperation::Greater => left_value.greater_than(&right_value),
                                        ComparerOperation::Less => left_value.less_than(&right_value),
                                        ComparerOperation::GreaterOrEquals => left_value.greater_or_equals(&right_value),
                                        ComparerOperation::LessOrEquals => left_value.less_or_equals(&right_value),
                                        ComparerOperation::Contains => left_value.contains(&right_value),
                                        ComparerOperation::StartsWith => left_value.starts_with(&right_value),
                                        ComparerOperation::EndsWith => left_value.ends_with(&right_value),
                                    };
    
                                    break result
                                } else {
                                    continue; // Column ID not found
                                }
                            } else {
                                continue;
                            }
                        } else {
                            continue; // Missing left operand
                        }
                    }
                    Token::Value(ref mut column_value) => {
                        current_value = Some(column_value.clone());
                    }
                }
            } else {
                break false
            }
        };

        token_results.push((evalaution_result, tokens.1.clone()));
    }

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
    
    final_result // Default to false if no comparison was made
}