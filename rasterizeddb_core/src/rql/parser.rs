use crate::{
    core::{column::Column, hashing::get_hash, row::InsertOrUpdateRow},
    memory_pool::MemoryChunk
};

#[cfg(feature = "enable_index_caching")]
use crate::POSITIONS_CACHE;

use super::{
    helpers::whitespace_spec_splitter,
    models::{ComparerOperation, MathOperation, Next, Token},
};

#[allow(unused_variables)]
#[allow(unused_mut)]
#[inline(always)]
pub fn parse_rql(query: &str) -> Result<DatabaseAction, String> {
    let hash = get_hash(query);

    let begin_result = query.find("BEGIN");
    let begin;

    if begin_result.is_none() {
        return Err("BEGIN statement is missing.".into());
    } else {
        begin = begin_result.unwrap();
    }

    let end_result = query.find("END");
    let end;

    if end_result.is_none() {
        return Err("END statement is missing.".into());
    } else {
        end = end_result.unwrap();
    }

    //CREATE TABLE
    if let Some(create_start) = query.find("CREATE TABLE") {
        let create_str = query[create_start + 12..].trim(); // Skip "CREATE TABLE"

        // Extract table name
        let table_name_end = create_str
            .find(|c: char| c == '(')
            .ok_or("Missing '(' in CREATE TABLE statement")?;
        let table_name = create_str[..table_name_end].trim();

        if table_name.is_empty() {
            return Err("Table name is missing in CREATE TABLE statement.".into());
        }

        // Extract options between parentheses
        let options_start = create_str.find('(').unwrap() + 1;
        let options_end = create_str
            .find(')')
            .ok_or("Missing ')' in CREATE TABLE statement")?;
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
            parser_result: ParserResult::CreateTable((
                table_name.to_string(),
                compressed,
                immutable,
            )),
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
    
    // Handle INSERT INTO
    if let Some(insert_start) = query.find("INSERT INTO") {
        let insert_str = query[insert_start + 11..].trim(); // Skip "INSERT INTO"
        
        // Extract table name
        let table_name_end = insert_str
            .find(|c: char| c == '(')
            .ok_or("Missing '(' in INSERT INTO statement")?;
        let table_name = insert_str[..table_name_end].trim();
        
        if table_name.is_empty() {
            return Err("Table name is missing in INSERT INTO statement.".into());
        }
        
        // Extract column specifications
        let columns_start = insert_str.find('(').unwrap() + 1;
        let columns_end = insert_str
            .find("))")
            .ok_or("Missing ')' in INSERT INTO statement")?;

        let columns_spec = &insert_str[columns_start..columns_end + 1].trim();
        
        // Parse column specifications
        let column_specs: Vec<&str> = columns_spec
            .split(',')
            .map(|s| s.trim())
            .collect();

        // Parse column types into a vector of data types
        let mut data_types: Vec<&str> = Vec::new();
        for col_spec in &column_specs {
            if !col_spec.starts_with("COL(") || !col_spec.ends_with(")") {
            return Err(format!("Invalid column specification: {}", col_spec).into());
            }
            
            let type_name = &col_spec[4..col_spec.len()-1];
            data_types.push(type_name);
        }
            
        // Find VALUES statements
        let values_part = &query[insert_start + 11 + table_name_end + columns_end + 2 - columns_start + 2..end];
        
        // Parse each VALUES statement
        let mut rows: Vec<InsertOrUpdateRow> = Vec::new();
        for values_line in values_part.lines() {
            let values_line = values_line.trim();
            if values_line.starts_with("VALUES") {
                // Extract values between parentheses
                let values_start = values_line.find('(').ok_or("Missing '(' in VALUES statement")? + 1;
                let values_end = values_line.find(')').ok_or("Missing ')' in VALUES statement")?;
                let values_str = &values_line[values_start..values_end];
                
                // Split values
                let mut raw_values: Vec<String> = Vec::new();
                let mut current_value = String::new();
                let mut in_string = false;
                
                for c in values_str.chars() {
                    if c == '\'' && !in_string {
                        in_string = true;
                        current_value.push(c);
                    } else if c == '\'' && in_string {
                        in_string = false;
                        current_value.push(c);
                    } else if c == ',' && !in_string {
                        // Process completed value
                        raw_values.push(current_value.trim().to_string());
                        current_value.clear();
                    } else {
                        current_value.push(c);
                    }
                }
                
                // Add the last value if exists
                if !current_value.is_empty() {
                    raw_values.push(current_value.trim().to_string());
                }
                
                // Check that values match columns
                if raw_values.len() != data_types.len() {
                    return Err(format!(
                        "Number of values ({}) does not match number of columns ({})", 
                        raw_values.len(), data_types.len()
                    ).into());
                }
                
                // Create properly typed columns from values
                let mut columns: Vec<Column> = Vec::with_capacity(raw_values.len());
                
                for (i, val) in raw_values.iter().enumerate() {
                    let column = parse_typed_value(val, data_types[i])?;
                    columns.push(column);
                }
                
                // Process columns to create InsertOrUpdateRow
                let mut column_data: Vec<u8> = Vec::new();
                for column in columns {
                    let mut column_bytes = column.content.to_vec();
                    column_data.append(&mut column_bytes);
                }
                
                // Create row
                let row = InsertOrUpdateRow { columns_data: column_data };
                rows.push(row);
            }
        }
        
        return Ok(DatabaseAction {
            table_name: table_name.to_string(),
            parser_result: ParserResult::InsertEvaluationTokens(InsertEvaluationResult { rows }),
        });
    }
    
    // Handle DELETE FROM
    if let Some(delete_start) = query.find("DELETE FROM") {
        let delete_str = query[delete_start + 11..].trim(); // Skip "DELETE FROM"
        
        // Extract table name
        let table_name_end = delete_str
            .find(|c: char| c.is_whitespace())
            .unwrap_or(delete_str.len());
        let table_name = delete_str[..table_name_end].trim();
        
        if table_name.is_empty() {
            return Err("Table name is missing in DELETE FROM statement.".into());
        }
        
        // Parse WHERE clause (reuse existing where clause parsing)
        let where_result = query.find("WHERE");
        if where_result.is_none() {
            return Err("WHERE statement is missing in DELETE.".into());
        }
        
        let where_i = where_result.unwrap();
        let where_clause = &query[where_i + 5..end].trim();
        
        let mut where_tokens = whitespace_spec_splitter(where_clause);
        let mut index_token: u32 = 0;
        let mut tokens_vector: Vec<(Vec<Token>, Option<Next>)> = Vec::default();
        let mut token_vector: Vec<Token> = Vec::default();
        
        let mut where_tokens_iter = where_tokens.into_iter();
        
        let mut double_continue = false;
        
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
            } else if token.eq("-") {
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
                let string = &token[1..token.len() - 1];
                let column = Column::from_chunk(14, MemoryChunk::from_vec(string.as_bytes().to_vec()));
                let val = Token::Value(column);
                token_vector.push(val);
            } else if !token.contains(".") {
                let result_i128 = str::parse::<i128>(&token.trim());
                if let Ok(token_number) = result_i128 {
                    let column = Column::create_temp(true, token_number.to_le_bytes().to_vec());
                    let val = Token::Value(column);
                    token_vector.push(val);
                } else {
                    panic!(
                        "Error parsing token number: {}, error: {}",
                        token,
                        result_i128.unwrap_err()
                    );
                }
            } else if token.contains(".") {
                if let Ok(token_number) = str::parse::<f64>(&token) {
                    let column = Column::create_temp(false, token_number.to_le_bytes().to_vec());
                    let val = Token::Value(column);
                    token_vector.push(val);
                } else {
                    panic!()
                }
            } else if token == "TRUE" {
                let column = Column::create_temp(true, [1].to_vec());
                let val = Token::Value(column);
                token_vector.push(val);
            } else if token == "FALSE" {
                let column = Column::create_temp(true, [0].to_vec());
                let val = Token::Value(column);
                token_vector.push(val);
            }
        }
        
        tokens_vector.push((token_vector, None));
        
        return Ok(DatabaseAction {
            table_name: table_name.to_string(),
            parser_result: ParserResult::DeleteEvaluationTokens(EvaluationResult {
                query_hash: hash,
                tokens: tokens_vector,
                limit: i64::MAX, // No limit for DELETE
                select_all: false,
            }),
        });
    }

    let select_result = query.find("SELECT");
    let mut select_table = String::new();

    if select_result.is_none() {
        return Err("SELECT statement is missing.".into());
    }

    if let Some(select_start) = select_result {
        // Find the starting index of the number after "SELECT FROM"
        let select_from_str = query[select_start + (6 + 1 + 4)..].trim(); // +6 +4 to skip "SELECT FROM"

        let select_from_end = select_from_str
            .find(|c: char| c.is_whitespace())
            .unwrap_or(select_from_str.len());

        let select_value = &select_from_str[..select_from_end];

        select_table.push_str(select_value);
    }

    #[cfg(feature = "enable_index_caching")]
    if let Some(file_positions) = POSITIONS_CACHE.get(&hash) {
        let db_action = DatabaseAction {
            table_name: select_table.to_string(),
            parser_result: ParserResult::CachedHashIndexes(file_positions),
        };
        return Ok(db_action);
    }

    let rebuild_indexes_result = query.find("REBUILD_INDEXES");
   
    if rebuild_indexes_result.is_some() {
        return Ok(DatabaseAction {
            table_name: select_table.clone(),
            parser_result: ParserResult::RebuildIndexes(select_table),
        });
    }
    
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
        let limit_end = limit_str
            .find(|c: char| c.is_whitespace())
            .unwrap_or(limit_str.len());

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
                select_all: true,
            }),
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

        let mut double_continue = false;

        let mut column_type = String::default();

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
                let column_type_and_index = token[4..column_end].trim();
                let column_type_and_index_split: Vec<&str> = column_type_and_index.split(',').collect();
                let column_index = str::parse::<u32>(column_type_and_index_split[0]).unwrap();
                column_type = column_type_and_index_split[1].to_string();
                let val = Token::Column(column_index);
                token_vector.push(val);
            } else if token.eq("*") {
                let val = Token::Math(MathOperation::Multiply);
                token_vector.push(val);
            } else if token.eq("-") {
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
                let string = &token[1..token.len() - 1];
                let column = Column::from_chunk(14, MemoryChunk::from_vec(string.as_bytes().to_vec()));
                let val = Token::Value(column);
                token_vector.push(val);
            } else {
                if let Ok(column) = parse_typed_value(&token, &column_type) {
                    let val = Token::Value(column);
                    token_vector.push(val);
                } else {
                    panic!()
                }
            }
        }

        tokens_vector.push((token_vector, None));

        return Ok(DatabaseAction {
            table_name: select_table,
            parser_result: ParserResult::QueryEvaluationTokens(EvaluationResult {
                query_hash: hash,
                tokens: tokens_vector,
                limit: limit_i64,
                select_all: false,
            }),
        });
    }

    panic!("Operation not supported")
}

// Helper function to parse a value string into a Column with specific type
fn parse_typed_value(value_str: &str, data_type: &str) -> std::result::Result<Column, String> {
    match data_type {
        "I8" => {
            match value_str.parse::<i8>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse I8 value '{}': {}", value_str, e))
            }
        },
        "I16" => {
            match value_str.parse::<i16>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse I16 value '{}': {}", value_str, e))
            }
        },
        "I32" => {
            match value_str.parse::<i32>() {
                Ok(num) => { Ok(Column::new_without_type(num).unwrap()) },
                Err(e) => Err(format!("Failed to parse I32 value '{}': {}", value_str, e))
            }
        },
        "I64" => {
            match value_str.parse::<i64>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse I64 value '{}': {}", value_str, e))
            }
        },
        "I128" => {
            match value_str.parse::<i128>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse I128 value '{}': {}", value_str, e))
            }
        },
        "U8" => {
            match value_str.parse::<u8>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse U8 value '{}': {}", value_str, e))
            }
        },
        "U16" => {
            match value_str.parse::<u16>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse U16 value '{}': {}", value_str, e))
            }
        },
        "U32" => {
            match value_str.parse::<u32>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse U32 value '{}': {}", value_str, e))
            }
        },
        "U64" => {
            match value_str.parse::<u64>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse U64 value '{}': {}", value_str, e))
            }
        },
        "U128" => {
            match value_str.parse::<u128>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse U128 value '{}': {}", value_str, e))
            }
        },
        "F32" => {
            match value_str.parse::<f32>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse F32 value '{}': {}", value_str, e))
            }
        },
        "F64" => {
            match value_str.parse::<f64>() {
                Ok(num) => Ok(Column::new_without_type(num).unwrap()),
                Err(e) => Err(format!("Failed to parse F64 value '{}': {}", value_str, e))
            }
        },
        "STRING" => {
            // Handle string values (strip quotes if present)
            let string_value = if value_str.starts_with('\'') && value_str.ends_with('\'') {
                &value_str[1..value_str.len()-1]
            } else {
                value_str
            };
            Ok(Column::new_without_type(string_value.to_string()).unwrap())
        },
        "BOOL" => {
            match value_str.to_lowercase().as_str() {
                "true" | "1" => Ok(Column::new_without_type(true).unwrap()),
                "false" | "0" => Ok(Column::new_without_type(false).unwrap()),
                _ => Err(format!("Failed to parse BOOL value: {}", value_str))
            }
        },
        "CHAR" => {
            if value_str.starts_with('\'') && value_str.ends_with('\'') && value_str.chars().count() == 3 {
                let ch = value_str.chars().nth(1).unwrap();
                Ok(Column::new_without_type(ch).unwrap())
            } else {
                Err(format!("Invalid CHAR value: {}", value_str))
            }
        },
        _ => Err(format!("Unsupported data type: {}", data_type))
    }.map_err(|e| e.to_string())
}

pub struct DatabaseAction {
    pub table_name: String,
    pub parser_result: ParserResult,
}

pub enum ParserResult {
    CreateTable((String, bool, bool)),
    DropTable(String),
    InsertEvaluationTokens(InsertEvaluationResult),
    UpdateEvaluationTokens(EvaluationResult),
    DeleteEvaluationTokens(EvaluationResult),
    QueryEvaluationTokens(EvaluationResult),
    CachedHashIndexes(Vec<(u64, u32)>),
    RebuildIndexes(String),
}

pub struct EvaluationResult {
    pub query_hash: u64,
    pub tokens: Vec<(Vec<Token>, Option<Next>)>,
    pub limit: i64,
    pub select_all: bool,
}

pub struct InsertEvaluationResult {
    pub rows: Vec<InsertOrUpdateRow>,
}
