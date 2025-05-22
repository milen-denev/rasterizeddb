use smallvec::SmallVec;

use crate::{core::{db_type::DbType}, memory_pool::MemoryBlock};
use  crate::memory_pool::MEMORY_POOL;
use super::schema::SchemaField;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String),
    Number(NumericValue),
    StringLit(String),
    Op(String),
    LPar,
    RPar,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NumericValue {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    F32(f32),
    F64(f64),
}

// Improved token parsing with better string operation handling
#[inline(always)]
pub fn tokenize(s: &str, schema: &SmallVec<[SchemaField; 20]>) -> SmallVec<[Token; 36]> {
    let mut out = SmallVec::new();
    let mut i = 0;
    let cs: Vec<char> = s.chars().collect();
    
    // Track context for field association to maintain type consistency
    let mut current_field_context: Option<(String, DbType)> = None;
    let mut expression_type_context: Option<DbType> = None;
    let mut expression_stack: Vec<Option<DbType>> = Vec::new();
    let mut in_parentheses = 0;
    
    // Create a case-insensitive lookup map for field names to types
    let mut field_map: std::collections::HashMap<String, (String, DbType)> = std::collections::HashMap::new();
    for field in schema.iter() {
        // Store both canonical name and type
        field_map.insert(field.name.clone(), (field.name.clone(), field.db_type.clone()));
    }
    
    // Create sets of known keywords for faster checking
    let logical_ops: std::collections::HashSet<&str> = ["and", "or"].iter().cloned().collect();
    let string_ops: std::collections::HashSet<&str> = ["contains", "startswith", "endswith"].iter().cloned().collect();
    
    // Helper to determine if a minus sign should be considered part of a number
    #[inline(always)]
    fn is_negative_number_start(tokens: &[Token], cs: &[char], i: usize) -> bool {
        // Check if the next character is a digit
        let next_is_digit = i + 1 < cs.len() && cs[i + 1].is_ascii_digit();
        if !next_is_digit {
            return false;
        }
        
        // Check if this is the start or if preceding token makes this likely to be a negative number
        i == 0 || match tokens.last() {
            Some(Token::Op(_)) => true,       // After operator like =, <, >
            Some(Token::LPar) => true,        // After opening parenthesis
            None => true,                     // At the beginning
            _ => false,                       // After identifiers or other tokens
        }
    }
    
    // Helper function to determine numeric type based on context and value
    #[inline(always)]
    fn parse_numeric_value(
        value_str: &str, 
        field_context: &Option<(String, DbType)>,
        expression_context: &Option<DbType>,
    ) -> NumericValue {
        // Always prioritize expression context for consistent typing in expressions
        if let Some(db_type) = expression_context {
            return parse_by_db_type(value_str, db_type);
        }
        
        // Next try the field context
        if let Some((_, db_type)) = field_context {
            return parse_by_db_type(value_str, db_type);
        }
        
        // Default parsing logic when no context is available
        parse_default(value_str)
    }

    // After parsing any numeric literal:
    if expression_type_context.is_none() && current_field_context.is_some() {
        expression_type_context = Some(current_field_context.as_ref().unwrap().1.clone());
    }
    
    // Helper to parse based on DbType
    #[inline(always)]
    fn parse_by_db_type(value_str: &str, db_type: &DbType) -> NumericValue {
        match db_type {
            DbType::I8 => NumericValue::I8(value_str.parse().unwrap_or_default()),
            DbType::I16 => NumericValue::I16(value_str.parse().unwrap_or_default()),
            DbType::I32 => NumericValue::I32(value_str.parse().unwrap_or_default()),
            DbType::I64 => NumericValue::I64(value_str.parse().unwrap_or_default()),
            DbType::I128 => NumericValue::I128(value_str.parse().unwrap_or_default()),
            DbType::U8 => NumericValue::U8(value_str.parse().unwrap_or_default()),
            DbType::U16 => NumericValue::U16(value_str.parse().unwrap_or_default()),
            DbType::U32 => NumericValue::U32(value_str.parse().unwrap_or_default()),
            DbType::U64 => NumericValue::U64(value_str.parse().unwrap_or_default()),
            DbType::U128 => NumericValue::U128(value_str.parse().unwrap_or_default()),
            DbType::F32 => NumericValue::F32(value_str.parse().unwrap_or_default()),
            DbType::F64 => NumericValue::F64(value_str.parse().unwrap_or_default()),
            _ => parse_default(value_str),
        }
    }
    
    // Default parsing logic for numbers without context
    #[inline(always)]
    fn parse_default(value_str: &str) -> NumericValue {
        if value_str.contains('.') {
            NumericValue::F64(value_str.parse().unwrap_or_default())
        } else {
            let parsed_num: i64 = value_str.parse().unwrap_or_default();
            if parsed_num >= i32::MIN as i64 && parsed_num <= i32::MAX as i64 {
                NumericValue::I32(parsed_num as i32)
            } else {
                NumericValue::I64(parsed_num)
            }
        }
    }
    
    // Process tokens and maintain type context
    while i < cs.len() {
        match cs[i] {
            c if c.is_whitespace() => i += 1,
            
            '\'' => {
                // Handle string literals
                let mut j = i + 1;
                while j < cs.len() && cs[j] != '\'' {
                    j += 1;
                }
                if j >= cs.len() {
                    panic!("Unclosed string literal starting at position {}", i);
                }
                let lit: String = cs[i+1..j].iter().collect();
                out.push(Token::StringLit(lit));
                i = j + 1;
                
                // After a string literal, set expression context to STRING
                expression_type_context = Some(DbType::STRING);
                current_field_context = None;
            },
            
            c if c.is_ascii_digit() => {
                // Handle numeric literals with context awareness
                let start = i;
                let mut has_decimal = false;
                while i < cs.len() && (cs[i].is_ascii_digit() || (!has_decimal && cs[i] == '.')) {
                    if cs[i] == '.' {
                        has_decimal = true;
                    }
                    i += 1;
                }
                
                let num_str = cs[start..i].iter().collect::<String>();
                let numeric_value = parse_numeric_value(
                    &num_str, 
                    &current_field_context, 
                    &expression_type_context
                );
                
                // Store the numeric value's type in expression context
                let num_type = numeric_value_to_db_type(&numeric_value);
                if expression_type_context.is_none() {
                    expression_type_context = Some(num_type);
                }
                
                out.push(Token::Number(numeric_value));
            },
            
            '-' if is_negative_number_start(&out, &cs, i) => {
                // Handle negative numbers with context awareness
                let start = i;
                i += 1;
                
                let mut has_decimal = false;
                while i < cs.len() && (cs[i].is_ascii_digit() || (!has_decimal && cs[i] == '.')) {
                    if cs[i] == '.' {
                        has_decimal = true;
                    }
                    i += 1;
                }
                
                let num_str = cs[start..i].iter().collect::<String>();
                let numeric_value = parse_numeric_value(
                    &num_str, 
                    &current_field_context, 
                    &expression_type_context
                );
                
                // Store the numeric value's type in expression context
                let num_type = numeric_value_to_db_type(&numeric_value);
                if expression_type_context.is_none() {
                    expression_type_context = Some(num_type);
                }
                
                out.push(Token::Number(numeric_value));
            },
            
            c if is_id_start(c) => {
                // Handle identifiers with strict validation
                let start = i;
                while i < cs.len() && is_id_part(cs[i]) { i += 1 }
                let id = cs[start..i].iter().collect::<String>();
                let id_lower = id.clone(); // Convert to lowercase for case-insensitive comparison
                
                // Check for special string operators (case-insensitive)
                if string_ops.contains(id_lower.as_str()) {
                    // Store normalized uppercase for string operators
                    out.push(Token::Ident(id_lower));
                    continue;
                }
                
                // Check for logical operators (case-insensitive)
                if logical_ops.contains(id_lower.as_str()) {
                    // Store normalized uppercase for logical operators
                    out.push(Token::Ident(id_lower.to_uppercase()));
                    // Logical operators reset expression context
                    expression_type_context = None;
                    current_field_context = None;
                    continue;
                }
                
                // Check if this is a field name - strict validation
                if let Some((canonical_name, field_type)) = field_map.get(&id_lower) {
                    // Store canonical name from schema
                    current_field_context = Some((canonical_name.clone(), field_type.clone()));
                    expression_type_context = Some(field_type.clone());
                    
                    // Update expression stack for parenthesized expressions
                    if in_parentheses > 0 {
                        if expression_stack.len() < in_parentheses {
                            expression_stack.push(Some(field_type.clone()));
                        } else {
                            expression_stack[in_parentheses - 1] = Some(field_type.clone());
                        }
                    }
                    
                    // Push the canonical name from the schema
                    out.push(Token::Ident(canonical_name.clone()));
                } else {
                    // STRICT VALIDATION: If this looks like a field but isn't in the schema, panic
                    // Only consider it a field attempt if it's not followed by a comparison operator
                    if i < cs.len() {
                        // Skip whitespace to find next non-whitespace character
                        let mut next_i = i;
                        while next_i < cs.len() && cs[next_i].is_whitespace() {
                            next_i += 1;
                        }
                        
                        // If the next character could indicate this is a field in a condition, validate it
                        if next_i < cs.len() {
                            let next_char = cs[next_i];
                            if ['=', '>', '<', '!'].contains(&next_char) || 
                               (next_i + 8 < cs.len() && cs[next_i..next_i+8].iter().collect::<String>() == "CONTAINS") ||
                               (next_i + 10 < cs.len() && cs[next_i..next_i+10].iter().collect::<String>() == "STARTSWITH") ||
                               (next_i + 8 < cs.len() && cs[next_i..next_i+8].iter().collect::<String>() == "ENDSWITH") {
                                panic!("Unknown column: '{}' is not defined in the schema", id);
                            }
                        }
                    }
                    
                    // Not a known field but might be some other identifier, push as is
                    out.push(Token::Ident(id.clone()));
                }
            },
            
            '(' => {
                out.push(Token::LPar);
                i += 1;
                in_parentheses += 1;
                
                // Save the current expression context before entering new parentheses
                // This ensures we preserve the context from outer expressions
                if expression_type_context.is_some() {
                    if in_parentheses > expression_stack.len() {
                        expression_stack.push(expression_type_context.clone());
                    } else {
                        expression_stack[in_parentheses - 1] = expression_type_context.clone();
                    }
                }
                // Don't reset the expression context when entering parentheses
            },

            ')' => {
                out.push(Token::RPar);
                i += 1;
                
                if in_parentheses > 0 {
                    // Restore the expression context from before this parenthesis group
                    if in_parentheses <= expression_stack.len() && expression_stack[in_parentheses - 1].is_some() {
                        expression_type_context = expression_stack[in_parentheses - 1].clone();
                    }
                    in_parentheses -= 1;
                }
                
                // Don't reset field context or expression context after closing parenthesis
            },
            
            '<'|'>'|'!'|'=' => {
                // Save the expression context before the comparison operator
                let saved_context = expression_type_context.clone();
                
                let mut op = cs[i].to_string();
                if i+1 < cs.len() && cs[i+1] == '=' {
                    op.push('=');
                    i += 2;
                } else { 
                    i += 1; 
                }
                out.push(Token::Op(op));
                
                // Preserve the expression context across the comparison operator
                expression_type_context = saved_context;
                // Reset field context since we're moving to the right side of the comparison
                current_field_context = None;
            },
            
            '+'|'-'|'*'|'/' => {
                // For arithmetic operators, preserve the expression context
                let op_str = cs[i].to_string();
                out.push(Token::Op(op_str));
                i += 1;
                
                // Don't reset contexts - we want to maintain the expression type
            },
            
            ',' => {
                i += 1;
                // Reset contexts after a comma
                current_field_context = None;
                expression_type_context = None;
            },
            
            _ => panic!("unexpected character: '{}'", cs[i]),
        }
    }
    
    out
}

#[inline(always)]
fn is_numeric_type(db_type: &DbType) -> bool {
    matches!(db_type, 
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128 |
        DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128 |
        DbType::F32 | DbType::F64
    )
}

// Improved type promotion for numeric types with better handling of edge cases
pub fn promote_numeric_types(type1: DbType, type2: DbType) -> Option<DbType> {
    // We can only promote numeric types
    if !is_numeric_type(&type1) || !is_numeric_type(&type2) {
        return None;
    }
    
    // If types are the same, no promotion needed
    if type1 == type2 {
        return Some(type1);
    }

    // Special case: when comparing field with literal, preserve field type
    // This is crucial for operations like "age + 10" where age is U8
    // and we want the result to be U8, not some wider type
    match (&type1, &type2) {
        // For operations with field-specific types like U8, U64, etc.
        // preserve the field type for most accurate processing
        (DbType::U8, DbType::I32) => return Some(DbType::U8),
        (DbType::I32, DbType::U8) => return Some(DbType::U8),
        
        (DbType::U16, DbType::I32) => return Some(DbType::U16),
        (DbType::I32, DbType::U16) => return Some(DbType::U16),
        
        (DbType::U32, DbType::I32) => return Some(DbType::U32),
        (DbType::I32, DbType::U32) => return Some(DbType::U32),
        
        (DbType::U64, DbType::I32) => return Some(DbType::U64),
        (DbType::I32, DbType::U64) => return Some(DbType::U64),
        
        (DbType::U128, DbType::I32) => return Some(DbType::U128),
        (DbType::I32, DbType::U128) => return Some(DbType::U128),
        
        // Handle typical int literal case against other numeric types
        (DbType::I32, _) if is_numeric_type(&type2) => return Some(type2),
        (_, DbType::I32) if is_numeric_type(&type1) => return Some(type1),
        
        // Standard promotion rules for other cases
        _ => {}
    }

    // Get ranks for standard type promotion
    let rank1 = get_type_rank(&type1);
    let rank2 = get_type_rank(&type2);

    // Determine if either type is floating point
    let is_float1 = matches!(type1, DbType::F32 | DbType::F64);
    let is_float2 = matches!(type2, DbType::F32 | DbType::F64);
    
    // If either type is floating point, the result is floating point
    if is_float1 || is_float2 {
        if rank1 >= rank2 { Some(type1) } else { Some(type2) }
    } else {
        // For integer types, handle signed vs unsigned
        let is_signed1 = matches!(type1, DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128);
        let is_signed2 = matches!(type2, DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128);
        
        if is_signed1 && is_signed2 {
            // Both signed, take the larger one
            if rank1 >= rank2 { Some(type1) } else { Some(type2) }
        } else if !is_signed1 && !is_signed2 {
            // Both unsigned, take the larger one
            if rank1 >= rank2 { Some(type1) } else { Some(type2) }
        } else {
            // One signed, one unsigned - prefer the original field type if possible
            if (rank1 as i32 - rank2 as i32).abs() > 1 {
                if rank1 > rank2 { Some(type1) } else { Some(type2) }
            } else {
                // When ranks are close, use specific heuristics
                if is_signed1 { Some(type1) } else { Some(type2) }
            }
        }
    }
}

// Enhanced get_type_rank function with more precise ranking
#[inline(always)]
fn get_type_rank(db_type: &DbType) -> u8 {
    match db_type {
        DbType::F64 => 14,
        DbType::F32 => 13,
        DbType::I128 => 12,
        DbType::U128 => 11, 
        DbType::I64 => 10,
        DbType::U64 => 9,
        DbType::I32 => 8,
        DbType::U32 => 7,
        DbType::I16 => 6,
        DbType::U16 => 5,
        DbType::I8 => 4,
        DbType::U8 => 3,
        // Non-numeric types
        DbType::STRING => 2,
        DbType::DATETIME => 1, 
        DbType::CHAR | DbType::NULL => 0,
        _ => unreachable!("unknown type for promotion"),
    }
}

// General type promotion for comparison operations with better field type preservation
pub fn promote_types(type1: DbType, type2: DbType) -> DbType {
    // Special case for string comparison operations
    if type1 == DbType::STRING || type2 == DbType::STRING {
        return DbType::STRING;
    }
    
    // For numeric types, try to preserve the field type when possible
    if type1 == type2 {
        return type1;
    }
    
    // Special case: when one type is a standard integer literal (I32) and the other is a field type,
    // preserve the field type for more accurate processing
    if type1 == DbType::I32 && is_numeric_type(&type2) {
        return type2;
    }
    if type2 == DbType::I32 && is_numeric_type(&type1) {
        return type1;
    }
    
    // For other numeric types, use the numeric promotion logic
    if let Some(result_type) = promote_numeric_types(type1.clone(), type2.clone()) {
        return result_type;
    }
    
    // If we can't promote the types and they're not strings, this is an error
    panic!("Cannot promote types: {:?} and {:?} for comparison operation", type1, type2);
}

#[inline(always)]
fn is_id_start(c: char) -> bool { 
    c.is_alphabetic() || c == '_' 
}

#[inline(always)]
fn is_id_part(c: char)  -> bool { 
    c.is_alphanumeric() || c == '_' 
}

#[inline(always)]
pub fn numeric_value_to_db_type(nv: &NumericValue) -> DbType {
    match nv {
        NumericValue::I8(_) => DbType::I8,
        NumericValue::I16(_) => DbType::I16,
        NumericValue::I32(_) => DbType::I32,
        NumericValue::I64(_) => DbType::I64,
        NumericValue::I128(_) => DbType::I128,
        NumericValue::U8(_) => DbType::U8,
        NumericValue::U16(_) => DbType::U16,
        NumericValue::U32(_) => DbType::U32,
        NumericValue::U64(_) => DbType::U64,
        NumericValue::U128(_) => DbType::U128,
        NumericValue::F32(_) => DbType::F32,
        NumericValue::F64(_) => DbType::F64,
    }
}

#[inline(always)]
pub fn numeric_to_mb(v: &NumericValue) -> MemoryBlock {
    macro_rules! direct {
        ($val:expr) => {{
            let bytes = $val.to_le_bytes();
            let mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }};
    }
    let b = match v {
        NumericValue::I8(n)   => direct!(n),
        NumericValue::I16(n)  => direct!(n),
        NumericValue::I32(n)  => direct!(n),
        NumericValue::I64(n)  => direct!(n),
        NumericValue::I128(n) => direct!(n),
        NumericValue::U8(n)   => direct!(n),
        NumericValue::U16(n)  => direct!(n),
        NumericValue::U32(n)  => direct!(n),
        NumericValue::U64(n)  => direct!(n),
        NumericValue::U128(n) => direct!(n),
        NumericValue::F32(f)  => direct!(f),
        NumericValue::F64(f)  => direct!(f),
    };
    b
}

#[inline(always)]
pub fn str_to_mb(s: &str) -> MemoryBlock {
    let b = s.as_bytes();
    let mb = MEMORY_POOL.acquire(b.len());
    mb.into_slice_mut().copy_from_slice(b);
    mb
}

pub fn numeric_type_to_db_type(nv: &NumericValue) -> DbType {
    match nv {
        NumericValue::I8(_) => DbType::I8,
        NumericValue::I16(_) => DbType::I16,
        NumericValue::I32(_) => DbType::I32,
        NumericValue::I64(_) => DbType::I64,
        NumericValue::I128(_) => DbType::I128,
        NumericValue::U8(_) => DbType::U8,
        NumericValue::U16(_) => DbType::U16,
        NumericValue::U32(_) => DbType::U32,
        NumericValue::U64(_) => DbType::U64,
        NumericValue::U128(_) => DbType::U128,
        NumericValue::F32(_) => DbType::F32,
        NumericValue::F64(_) => DbType::F64,
    }
}