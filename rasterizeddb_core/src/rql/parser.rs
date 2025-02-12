use crate::{
    core::{
        column::Column, db_type::DbType, hashing::get_hash, row::InsertOrUpdateRow}, memory_pool::Chunk, POSITIONS_CACHE
    };

use super::{helpers::whitespace_spec_splitter, models::{ComparerOperation, MathOperation, Next, Token}};

#[allow(unused_variables)]
#[allow(unused_mut)]
pub fn parse_rql(query: &str) -> Result<DatabaseAction, String> {
    let hash = get_hash(query);

    if let Some(file_positions) = POSITIONS_CACHE.get(&hash) {
        let db_action = DatabaseAction {
            table_name: "".to_string(),
            parser_result: ParserResult::CachedHashIndexes(file_positions)
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

    
    let end_result = query.find("END");
    let end;

    if end_result.is_none() {
        return Err("END statement is missing.".into())
    } else {
        end = end_result.unwrap();
    }

    //CREATE TABLE
    if let Some(create_start) = query.find("CREATE TABLE") {
        let create_str = query[create_start + 12..].trim(); // Skip "CREATE TABLE"

        // Extract table name
        let table_name_end = create_str.find(|c: char| c == '(').ok_or("Missing '(' in CREATE TABLE statement")?;
        let table_name = create_str[..table_name_end].trim();

        if table_name.is_empty() {
            return Err("Table name is missing in CREATE TABLE statement.".into());
        }

        // Extract options between parentheses
        let options_start = create_str.find('(').unwrap() + 1;
        let options_end = create_str.find(')').ok_or("Missing ')' in CREATE TABLE statement")?;
        let options = &create_str[options_start..options_end].trim();

        // Split options and parse TRUE/FALSE
        let mut option_values = options.split(',').map(|s| s.trim());
        let compressed = match option_values.next() {
            Some("TRUE") => true,
            Some("FALSE") => false,
            _ => return Err("Invalid value for compression option in CREATE TABLE.".into()),
        };

        let immutable = match option_values.next() {
            Some("TRUE") => true,
            Some("FALSE") => false,
            _ => return Err("Invalid value for immutability option in CREATE TABLE.".into()),
        };

        // Ensure no extra arguments are present
        if option_values.next().is_some() {
            return Err("Unexpected extra arguments in CREATE TABLE statement.".into());
        }

        return Ok(DatabaseAction {
            table_name: table_name.to_string(),
            parser_result: ParserResult::CreateTable((table_name.to_string(), compressed, immutable)),
        });
    }

    // Handle DROP TABLE
    if let Some(drop_start) = query.find("DROP TABLE") {
        let drop_str = query[drop_start + 10..].trim(); // Skip "DROP TABLE"

        // Extract table name
        let table_name = drop_str.trim();

        if table_name.is_empty() {
            return Err("Table name is missing in DROP TABLE statement.".into());
        }

        return Ok(DatabaseAction {
            table_name: table_name.to_string(),
            parser_result: ParserResult::DropTable(table_name.to_string()),
        });
    }

    let select_result = query.find("SELECT");
    let mut select_table = String::new();

    if select_result.is_none() {
        return Err("SELECT statement is missing.".into())
    }

    if let Some(select_start) = select_result {
        // Find the starting index of the number after "SELECT FROM"
        let select_from_str = query[select_start + (6 + 4)..].trim(); // +6 +4 to skip "SELECT FROM"

        let select_from_end = select_from_str.find(|c: char| c.is_whitespace()).unwrap_or(select_from_str.len());
        
        let select_value = &select_from_str[..select_from_end];
        
        select_table.push_str(select_value);
    }

    //println!("{}", select_table);

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
    let return_all: bool;

    if return_result.is_none() {
        return_all = true;
    } else {
        return_all = false;
    }

    // Get LIMIT
    let limit_result = query.find("LIMIT");
    let mut limit_i64 = i64::MAX;

    if let Some(limit_start) = limit_result {
        // Find the starting index of the number after "LIMIT"
        let limit_str = query[limit_start + 5..].trim(); // +5 to skip "LIMIT"
        
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

    //RETURN ALL & SELECT ALL scenario
    if return_all && select_all {
        let select_clause = &query[begin + 6..].trim();
        return Ok(DatabaseAction { 
            table_name: select_table,
            parser_result: ParserResult::QueryEvaluationTokens(EvaluationResult {
                query_hash: hash,
                tokens: Vec::default(),
                limit: limit_i64,
                select_all: true
            })
        });
    } 
    
    // RETURN ALL and WHERE clause scenario
    if return_all && where_result.is_some() {
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

            if token == " " || token.is_empty() {
                continue;
            }

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
                let column = Column::from_chunk(14, Chunk::from_vec(string.as_bytes().to_vec()));
                let val = Token::Value(column);
                token_vector.push(val);
            } else if !token.contains(".") {
                let result_i128 = str::parse::<i128>(&token.trim());
                if let Ok(token_number) = result_i128 {
                    let column = Column::from_chunk(5, Chunk::from_vec(token_number.to_le_bytes().to_vec()));
                    let val = Token::Value(column);
                    token_vector.push(val);
                } else {
                    panic!("Error parsing token number: {}, error: {}", token, result_i128.unwrap_err());
                }
            } else if token.contains(".") {
                if let Ok(token_number) = str::parse::<f64>(&token) {
                    let column = Column::from_chunk(12, Chunk::from_vec(token_number.to_le_bytes().to_vec()));
                    let val = Token::Value(column);
                    token_vector.push(val);
                } else {
                    panic!()
                }
            } else if token == "TRUE" {
                let column = Column::from_chunk(12, Chunk::from_vec([true as u8].to_vec()));
                let val = Token::Value(column);
                token_vector.push(val);
            } else if token == "FALSE" {
                let column = Column::from_chunk(12, Chunk::from_vec([false as u8].to_vec()));
                let val = Token::Value(column);
                token_vector.push(val);
            }
        }

        tokens_vector.push((token_vector, None));

        return Ok(DatabaseAction { 
            table_name: select_table,
            parser_result: ParserResult::QueryEvaluationTokens(EvaluationResult {
                query_hash: hash,
                tokens: tokens_vector,
                limit: limit_i64,
                select_all: false
            })
        });
    }

    panic!("Operation not supported")
}

pub struct DatabaseAction {
    pub table_name: String,
    pub parser_result: ParserResult
}

pub enum ParserResult {
    CreateTable((String,bool, bool)),
    DropTable(String),
    InsertEvaluationTokens(InsertEvaluationResult),
    UpdateEvaluationTokens(EvaluationResult),
    DeleteEvaluationTokens(EvaluationResult),
    QueryEvaluationTokens(EvaluationResult),
    CachedHashIndexes(Vec<(u64, u32)>)
}

pub struct EvaluationResult {
    pub query_hash: u64,
    pub tokens: Vec<(Vec<Token>, Option<Next>)>,
    pub limit: i64,
    pub select_all: bool
}

pub struct InsertEvaluationResult {
    pub rows: Vec<InsertOrUpdateRow>
}