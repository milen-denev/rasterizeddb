use crate::{
    core::{
        column::Column,
        hashing::get_hash}, 
        POSITIONS_CACHE
    };

use super::{helpers::whitespace_spec_splitter, models::{ComparerOperation, MathOperation, Next, Token}};

#[allow(unused_variables)]
#[allow(unused_mut)]
pub fn parse_rql(query: &str) -> Result<DatabaseAction, String> {
    let hash = get_hash(query);

    if let Some(file_positions) = POSITIONS_CACHE.get(&hash) {
        let db_action = DatabaseAction {
            table_name: "".to_string(),
            parser_result: ParserResult::HashIndexes(file_positions)
        };
        return Ok(db_action);
    }

    let begin_result = query.find("BEGIN");
    let begin;

    if begin_result.is_none() {
        return Err("BEGIN statement is missing.".into())
    } else {
        begin = begin_result.unwrap();
    }

    let select_result = query.find("SELECT");
    //let select;

    if select_result.is_none() {
        return Err("SELECT statement is missing.".into())
    } else {
        //select = select_result.unwrap();
    }

    //println!("{}", select);

    let where_result = query.find("WHERE");
    let mut where_i: usize = 0;
    let select_all: bool;

    if where_result.is_none() {
        select_all = true;
    } else {
        select_all = false;
        where_i = where_result.unwrap();
    }

    let return_result = query.find("RETURN");
    let mut return_i: usize = 0;
    let return_all: bool;

    if return_result.is_none() {
        return_all = true;
    } else {
        return_all = false;
        return_i = return_result.unwrap();
    }

    let end_result = query.find("END");
    let end;

    if end_result.is_none() {
        return Err("END statement is missing.".into())
    } else {
        end = end_result.unwrap();
    }

    let limit_result = query.find("LIMIT");
    let mut limit_i64 = i64::MAX;

    if let Some(limit_start) = limit_result {
        // Find the starting index of the number after "LIMIT"
        let limit_str = &query[limit_start + 5..].trim(); // +5 to skip "LIMIT"
        
        // Find the end of the number using a space or newline as a delimiter
        let limit_end = limit_str.find(|c: char| c.is_whitespace()).unwrap_or(limit_str.len());
        
        // Extract the substring and parse it as i64
        let limit_value = &limit_str[..limit_end];
        if let Ok(limit_number) = str::parse::<i64>(limit_value) {
            if limit_number <= 0 {
                limit_i64 = i64::MAX;
            } else {
                limit_i64 = limit_number;
            }
        } else {
            return Err("LIMIT statement is invalid.".into());
        }
    }

    // Step 1: Parse WHERE clause
    if where_result.is_some() && return_result.is_some()  && return_all && select_all {
        let select_clause = &query[begin + 5..where_i].trim();
        let where_clause = &query[where_i + 5..return_i].trim();
        todo!()
    } else if return_result.is_none() && where_result.is_some() {
        let select_clause = &query[begin + 5..where_i].trim();
        let where_clause = &query[where_i + 5..end].trim();

        let mut where_tokens = whitespace_spec_splitter(where_clause);

        let mut index_token: u32 = 0; 
        let mut tokens_vector: Vec<(Vec<Token>, Option<Next>)> = Vec::default();
        let mut token_vector: Vec<Token> = Vec::default();

        let mut where_tokens_iter = where_tokens.into_iter();

        let  mut double_continue = false;
        
        while let Some(token) = where_tokens_iter.next() {
            if double_continue {
                double_continue = false;
                continue;
            }

            if token.eq("LIMIT") {
                double_continue = true;
                continue;
            }

            index_token += token.len() as u32;

            if token.starts_with("COL(") {
                let column_end = token.find(")").unwrap();
                let column_index = str::parse::<u32>(&token[4..column_end].trim()).unwrap();
                let val = Token::Column(column_index);
                token_vector.push(val);
            } else if token.eq("*") {
                let val = Token::Math(MathOperation::Multiply);
                token_vector.push(val);
            }  else if token.eq("-") {
                let val = Token::Math(MathOperation::Subtract);
                token_vector.push(val);
            } else if token.eq("/") {
                let val = Token::Math(MathOperation::Divide);
                token_vector.push(val);
            } else if token.eq("+") {
                let val = Token::Math(MathOperation::Add);
                token_vector.push(val);
            } else if token.eq("=") {
                let val = Token::Operation(ComparerOperation::Equals);
                token_vector.push(val);
            } else if token.eq(">") {
                let val = Token::Operation(ComparerOperation::Greater);
                token_vector.push(val);
            } else if token.eq(">=") {
                let val = Token::Operation(ComparerOperation::GreaterOrEquals);
                token_vector.push(val);
            } else if token.eq("!=") {
                let val = Token::Operation(ComparerOperation::NotEquals);
                token_vector.push(val);
            } else if token.eq("<") {
                let val = Token::Operation(ComparerOperation::Less);
                token_vector.push(val);
            } else if token.eq("<=") {
                let val = Token::Operation(ComparerOperation::LessOrEquals);
                token_vector.push(val);
            } else if token.eq("AND") {
                let mut new_vector: Vec<Token> = Vec::with_capacity(token_vector.len());
                new_vector.append(&mut token_vector);
                tokens_vector.push((new_vector, Some(Next::And)));
            } else if token.eq("OR") {
                let mut new_vector: Vec<Token> = Vec::with_capacity(token_vector.len());
                new_vector.append(&mut token_vector);
                tokens_vector.push((new_vector, Some(Next::Or)));
            } else if token.starts_with('\'') && token.ends_with('\'') {
                let string = &token[1..token.len() -1];
                let val = Token::Value(Box::new(Column::new(string).unwrap()));
                token_vector.push(val);
            } else if !token.contains(".") {
                if let Ok(token_number) = str::parse::<i128>(&token) {
                    let val = Token::Value(Box::new(Column::new(token_number).unwrap()));
                    token_vector.push(val);
                } else {
                    panic!()
                }
            } else if token.contains(".") {
                if let Ok(token_number) = str::parse::<f64>(&token) {
                    let val = Token::Value(Box::new(Column::new(token_number).unwrap()));
                    token_vector.push(val);
                } else {
                    panic!()
                }
            }
        }

        tokens_vector.push((token_vector, None));

        return Ok(DatabaseAction { 
            table_name: "".to_string(),
            parser_result: ParserResult::EvaluationTokens(EvaluationResult {
                query_hash: hash,
                tokens: tokens_vector,
                limit: limit_i64
            })
        });
    } else {
        let select_clause = &query[begin + 5..end].trim();
        todo!()
    }
}

pub struct DatabaseAction {
    table_name: String,
    parser_result: ParserResult
}

pub enum ParserResult {
    HashIndexes(Vec<(u64, u32)>),
    EvaluationTokens(EvaluationResult)
}

pub struct EvaluationResult {
    pub query_hash: u64,
    pub tokens: Vec<(Vec<Token>, Option<Next>)>,
    pub limit: i64
}