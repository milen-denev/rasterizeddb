use std::borrow::Cow;
use smallvec::SmallVec;

use crate::memory_pool::{MemoryBlock, MEMORY_POOL};
use crate::core::db_type::DbType;
use super::schema::SchemaField;
use super::transformer::{ComparerOperation, ComparisonOperand, MathOperation, Next, TransformerProcessor};

#[derive(Debug, Clone)]
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

pub fn parse_query<'a, 'b, 'c>(
    toks: &'a SmallVec<[Token; 36]>,
    columns: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    schema: &'a SmallVec<[SchemaField; 20]>,
    transformers: &'b mut TransformerProcessor<'b>,
) {
    let parser = QueryParser::new(toks, &columns, schema, transformers);
    parser.parse_where()
}

struct QueryParser<'a, 'b, 'c> {
    toks: &'a SmallVec<[Token; 36]>,
    pos: usize,
    cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    schema: &'a SmallVec<[SchemaField; 20]>,
    query_transformer: &'b mut TransformerProcessor<'b>,
}

impl<'a, 'b, 'c> QueryParser<'a, 'b, 'c> {
    fn new(
        tokens: &'a SmallVec<[Token; 36]>,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
        schema: &'a SmallVec<[SchemaField; 20]>,
        query_transformer: &'b mut TransformerProcessor<'b>,
    ) -> Self {
        Self { toks: tokens, pos: 0, cols, schema, query_transformer }
    }

    fn parse_where(mut self) {
        self.parse_or();
    }

    /// Parse OR expressions
    fn parse_or(&mut self) {
        self.parse_and();
        while let Some(Next::Or) = self.peek_logic() {
            self.next_logic();
            self.parse_and();
        }
    }

    /// Parse AND expressions
    fn parse_and(&mut self) {
        self.parse_comp_or_group();
        while let Some(Next::And) = self.peek_logic() {
            self.next_logic();
            self.parse_comp_or_group();
        }
    }

    /// Parse either a grouped boolean expression or a comparison
    fn parse_comp_or_group(&mut self) {
        if let Some(Token::LPar) = self.toks.get(self.pos) {
            if self.is_boolean_group(self.pos) {
                self.pos += 1; // consume '('
                self.parse_or();
                if !matches!(self.next().unwrap(), Token::RPar) {
                    panic!("expected closing parenthesis");
                }
                return;
            }
        }
        // default to comparison (handles arithmetic grouping)
        self.parse_comparison(); // parse_comparison now returns ()
    }

    /// Heuristic to check if parentheses at pos wrap a boolean expression
    fn is_boolean_group(&self, start: usize) -> bool {
        let mut depth = 0;
        let mut i = start;
        while i < self.toks.len() {
            match &self.toks[i] {
                Token::LPar => depth += 1,
                Token::RPar => {
                    depth -= 1;
                    if depth == 0 { break; }
                }
                Token::Op(o) if ["=","!=",">",">=","<","<=","CONTAINS","STARTSWITH","ENDSWITH"].contains(&o.as_str()) => return true,
                Token::Ident(t) if ["AND","OR"].contains(&t.to_uppercase().as_str()) => return true,
                _ => {}
            }
            i += 1;
        }
        false
    }

    fn parse_comparison(
        &mut self,
    ) { // Changed return type to ()
        let (left_op, left_type) = self.parse_expr(); // Now returns (ComparisonOperand, DbType)
        let op_token = self.next().unwrap();
        let op = match op_token {
            Token::Op(o) | Token::Ident(o) => o.to_uppercase(),
            _ => panic!("expected comparison op"),
        };
        let (right_op, right_type) = self.parse_expr(); // Now returns (ComparisonOperand, DbType)
        let cmp = match op.as_str() {
            "="  => ComparerOperation::Equals,
            "!=" => ComparerOperation::NotEquals,
            ">"  => ComparerOperation::Greater,
            ">=" => ComparerOperation::GreaterOrEquals,
            "<"  => ComparerOperation::Less,
            "<=" => ComparerOperation::LessOrEquals,
            "CONTAINS"    => ComparerOperation::Contains,
            "STARTSWITH"  => ComparerOperation::StartsWith,
            "ENDSWITH"    => ComparerOperation::EndsWith,
            _ => panic!("unknown cmp {}", op),
        };
        let next_logic_op = self.peek_logic();
        
        let determined_comparison_type = match cmp {
            ComparerOperation::Contains | 
            ComparerOperation::StartsWith | 
            ComparerOperation::EndsWith => DbType::STRING,
            _ => promote_types(left_type, right_type), // Use types from expressions
        };
        
        self.query_transformer.add_comparison(
            determined_comparison_type,
            left_op.clone(),
            right_op.clone(),
            cmp,
            next_logic_op.clone()
        );
    }        
    
    fn parse_expr(
        &mut self,
    ) -> (ComparisonOperand, DbType) { // Changed return type
        let (mut lhs_op, mut lhs_type) = self.parse_term();
        while let Some(op_str) = self.peek_op(&["+","-"]) {
            let math = if op_str=="+" { MathOperation::Add } else { MathOperation::Subtract };
            self.next(); // consume op
            let (rhs_op, rhs_type) = self.parse_term();
            
            let result_type = promote_types(lhs_type, rhs_type);
            let idx = self.query_transformer.add_math_operation(result_type.clone(), lhs_op.clone(), rhs_op.clone(), math);
            lhs_op = ComparisonOperand::Intermediate(idx);
            lhs_type = result_type; // Update type to the result type of the operation
        }
        (lhs_op, lhs_type)
    }    
    
    fn parse_term(
        &mut self
    ) -> (ComparisonOperand, DbType) { // Changed return type
        let (mut lhs_op, mut lhs_type) = self.parse_factor();
        while let Some(op_str) = self.peek_op(&["*","/"]) {
            let math = if op_str=="*" { MathOperation::Multiply } else { MathOperation::Divide };
            self.next();
            let (rhs_op, rhs_type) = self.parse_factor();

            let result_type = promote_types(lhs_type, rhs_type);
            let idx = self.query_transformer.add_math_operation(result_type.clone(), lhs_op.clone(), rhs_op.clone(), math);
            lhs_op = ComparisonOperand::Intermediate(idx);
            lhs_type = result_type; // Update type to the result type of the operation
        }
        (lhs_op, lhs_type)
    }

   fn parse_factor(
        &mut self
    ) -> (ComparisonOperand, DbType) { // Changed return type
        match self.next().unwrap() {
            Token::Number(n) => {
                let db_type = numeric_value_to_db_type(&n);
                let mb = numeric_to_mb(n);
                (ComparisonOperand::Direct(mb), db_type)
            }
            Token::StringLit(s) => {
                let mb = str_to_mb(&s);
                (ComparisonOperand::Direct(mb), DbType::STRING)
            }
            Token::Ident(name) => {
                // It's important that self.schema is available and populated
                let field_schema = self.schema.iter().find(|f| f.name == name)
                    .unwrap_or_else(|| panic!("unknown column schema for {}", name));
                let mb = self.cols.iter().find(|(n, _)| n == &name)
                    .map(|(_, mb)| mb.clone())
                    .unwrap_or_else(|| panic!("unknown column data for {}", name));
                (ComparisonOperand::Direct(mb), field_schema.db_type.clone())
            }
            Token::LPar => {
                let (inner_op, inner_type) = self.parse_expr(); // parse_expr now returns tuple
                if !matches!(self.next().unwrap(), Token::RPar) {
                    panic!("expected closing parenthesis after grouped expression");
                }
                (inner_op, inner_type)
            }
            t => panic!("unexpected factor: {:?}", t),
        }
    }

    fn peek_logic(&self) -> Option<Next> {
        if let Some(Token::Ident(t)) = self.toks.get(self.pos) {
            match t.to_uppercase().as_str() {
                "AND" => Some(Next::And),
                "OR"  => Some(Next::Or),
                _ => None
            }
        } else { None }
    }

    fn next_logic(&mut self) { self.pos+=1; }

    fn peek_op(&self, a: &[&str]) -> Option<String> {
        if let Some(Token::Op(o)) = self.toks.get(self.pos) {
            if a.contains(&o.as_str()) { return Some(o.clone()) }
        }
        None
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.toks.get(self.pos).cloned();
        self.pos += 1;
        t
    }

}

fn numeric_value_to_db_type(nv: &NumericValue) -> DbType {
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

// Helper to get a rank for type promotion (higher is "larger" or more general)
fn get_type_rank(db_type: &DbType) -> u8 {
    match db_type {
        DbType::F64 => 10,
        DbType::F32 => 9,
        DbType::I128 => 8,
        // Note: U128 vs I128 promotion needs careful thought for actual ops if mixing signed/unsigned
        DbType::U128 => 7, 
        DbType::I64 => 6,
        DbType::U64 => 5,
        DbType::I32 => 4,
        DbType::U32 => 3,
        DbType::I16 => 2,
        DbType::U16 => 1,
        DbType::I8 | DbType::U8 => 0, 
        // Non-numeric types, or types not typically involved in arithmetic promotion
        DbType::STRING | DbType::DATETIME | DbType::CHAR | DbType::NULL => 0, // Or a distinct low rank / handle separately
        _ => panic!("unknown type for promotion"),
    }
}

// Simple type promotion logic for arithmetic and comparison operations
fn promote_types(type1: DbType, type2: DbType) -> DbType {
    if type1 == DbType::STRING || type2 == DbType::STRING {
        // For arithmetic, this would be an error. For comparisons, it might be allowed if one is string.
        // The parse_comparison handles string-specific ops separately.
        // If this is reached for '=', '>', etc. and one is string, it implies string comparison.
        // However, numeric promotion shouldn't yield STRING.
        if type1 == DbType::STRING && type2 == DbType::STRING {
            return DbType::STRING;
        }
        // This case (e.g. number vs string in arithmetic) should ideally be an error earlier
        // or handled by specific casting rules if allowed.
        panic!("Cannot promote types: {:?} and {:?} for generic arithmetic/comparison. String involved.", type1, type2);
    }

    let rank1 = get_type_rank(&type1);
    let rank2 = get_type_rank(&type2);

    if rank1 >= rank2 { type1 } else { type2 }
}

#[inline(always)]
pub fn tokenize(s: &str, schema: &SmallVec<[SchemaField; 20]>) -> SmallVec<[Token; 36]> {
    let mut out = SmallVec::new();
    let mut i = 0;
    let cs: Vec<char> = s.chars().collect();
    
    // Helper function to determine numeric type based on field name and value
    #[inline(always)]
    fn parse_numeric_value(value_str: &str, field_name: Option<&str>, schema: &SmallVec<[SchemaField; 20]>) -> NumericValue {
        // Try to find the field in schema
        if let Some(field_name) = field_name {
            if let Some(field) = schema.iter().find(|f| f.name == field_name) {
                // Parse according to the field's database type
                return match field.db_type {
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
                    _ => NumericValue::I32(value_str.parse().unwrap_or_default()), // Default fallback
                };
            }
        }
        
        // Default behavior: determine type based on the format of the number
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
    
    // Track context for field association
    let mut current_field: Option<String> = None;
    
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
    
    while i < cs.len() {
        match cs[i] {
            c if c.is_whitespace() => i += 1,
            '\'' => {
                // find closing quote safely
                let mut j = i + 1;
                while j < cs.len() && cs[j] != '\'' {
                    j += 1;
                }
                let lit: String = cs[i+1..j].iter().collect();
                out.push(Token::StringLit(lit));
                i = j + 1;
            }
            c if c.is_ascii_digit() => {
                let start = i;
                // Handle floating point numbers
                let mut has_decimal = false;
                while i < cs.len() && (cs[i].is_ascii_digit() || (!has_decimal && cs[i] == '.')) {
                    if cs[i] == '.' {
                        has_decimal = true;
                    }
                    i += 1;
                }
                
                let num_str = cs[start..i].iter().collect::<String>();
                let numeric_value = parse_numeric_value(&num_str, current_field.as_deref(), schema);
                out.push(Token::Number(numeric_value));
            }
            '-' if is_negative_number_start(&out, &cs, i) => {
                // Handle negative numbers
                let start = i;  // Start includes the minus sign
                i += 1;  // Move past the minus sign
                
                // Parse the digits and potential decimal point
                let mut has_decimal = false;
                while i < cs.len() && (cs[i].is_ascii_digit() || (!has_decimal && cs[i] == '.')) {
                    if cs[i] == '.' {
                        has_decimal = true;
                    }
                    i += 1;
                }
                
                let num_str = cs[start..i].iter().collect::<String>();
                let numeric_value = parse_numeric_value(&num_str, current_field.as_deref(), schema);
                out.push(Token::Number(numeric_value));
            }
            c if is_id_start(c) => {
                let start = i;
                while i < cs.len() && is_id_part(cs[i]) { i += 1 }
                let id = cs[start..i].iter().collect::<String>();
                
                // Store identifier as potential field name for next numeric value
                if schema.iter().any(|field| field.name == id) {
                    current_field = Some(id.clone());
                }
                
                out.push(Token::Ident(id));
            }
            '(' => { out.push(Token::LPar); i += 1 }
            ')' => { out.push(Token::RPar); i += 1 }
            '<'|'>'|'!'|'=' => {
                let mut op = cs[i].to_string();
                if i+1 < cs.len() && cs[i+1] == '=' {
                    op.push('=');
                    i += 2;
                } else { i += 1; }
                out.push(Token::Op(op));
            }
            '+'|'-'|'*'|'/' => {
                out.push(Token::Op(cs[i].to_string()));
                i += 1;
            }
            ',' => { i += 1; current_field = None; } // Reset current field on comma
            'A'..='Z'|'a'..='z' => {
                // Handle keywords like AND, OR
                let start = i;
                while i < cs.len() && is_id_part(cs[i]) { i += 1 }
                let keyword = cs[start..i].iter().collect::<String>();
                
                // If it's a logical operator, reset the current field context
                if keyword.to_uppercase() == "AND" || keyword.to_uppercase() == "OR" {
                    current_field = None;
                }
                
                out.push(Token::Ident(keyword));
            }
            _ => panic!("unexpected char {}", cs[i]),
        }
    }
    
    out
}

#[inline(always)]
fn is_id_start(c: char) -> bool { c.is_alphabetic() || c == '_' }

#[inline(always)]
fn is_id_part(c: char)  -> bool { c.is_alphanumeric() || c == '_' }

#[inline(always)]
fn numeric_to_mb(v: NumericValue) -> MemoryBlock {
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
fn str_to_mb(s: &str) -> MemoryBlock {
    let b = s.as_bytes();
    let mb = MEMORY_POOL.acquire(b.len());
    mb.into_slice_mut().copy_from_slice(b);
    mb
}

pub fn tokenize_for_test(s: &str, schema: &SmallVec<[SchemaField; 20]>) -> SmallVec<[Token; 36]> {
    tokenize(s, schema)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::cell::UnsafeCell;
    use std::sync::LazyLock;
    use smallvec::SmallVec;

    use crate::core::db_type::DbType;
    use crate::core::row_v2::concurrent_processor::Buffer;
    use crate::core::row_v2::query_parser::{parse_query, tokenize_for_test, NumericValue};
    use crate::core::row_v2::row::Row;
    use crate::core::row_v2::schema::SchemaField;
    use crate::core::row_v2::transformer::TransformerProcessor;
    use crate::memory_pool::{MemoryBlock, MEMORY_POOL};

    fn create_memory_block_from_i8(value: i8) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_i16(value: i16) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_i64(value: i64) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u8(value: u8) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u16(value: u16) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u32(value: u32) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u64(value: u64) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u128(value: u128) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_i128(value: i128) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_f32(value: f32) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_i32(value: i32) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_string(value: &str) -> MemoryBlock {
        let bytes = value.as_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(bytes);
        data
    }

    fn create_memory_block_from_f64(value: f64) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_schema() -> SmallVec<[SchemaField; 20]> {
        smallvec::smallvec![
            SchemaField::new("id".to_string(), DbType::I32, 4 , 0, 0, false),
            SchemaField::new("age".to_string(), DbType::U8, 1 , 0, 0, false),
            SchemaField::new("salary".to_string(), DbType::I32, 4 , 0, 0, false),
            SchemaField::new("name".to_string(), DbType::STRING, 4 + 8 , 0, 0, false),
            SchemaField::new("department".to_string(), DbType::STRING, 4 + 8 , 0, 0, false),
            SchemaField::new("bank_balance".to_string(), DbType::F64, 8 , 0, 0, false),
            SchemaField::new("credit_balance".to_string(), DbType::F32, 4 , 0, 0, false),
            SchemaField::new("net_assets".to_string(), DbType::I64, 8 , 0, 0, false),
            SchemaField::new("earth_position".to_string(), DbType::I8, 1, 0, 0, false),
            SchemaField::new("test_1".to_string(), DbType::U32, 4, 0, 0, false),
            SchemaField::new("test_2".to_string(), DbType::U16, 2, 0, 0, false),
            SchemaField::new("test_3".to_string(), DbType::I128, 16, 0, 0, false),
            SchemaField::new("test_4".to_string(), DbType::U128, 16, 0, 0, false),
            SchemaField::new("test_5".to_string(), DbType::I16, 2, 0, 0, false),
            SchemaField::new("test_6".to_string(), DbType::U64, 8, 0, 0, false),
        ]
    }

    fn create_schema_2() -> SmallVec<[SchemaField; 20]> {
        smallvec::smallvec![
            SchemaField::new("id".to_string(), DbType::U128, 16, 0, 0, false),
            SchemaField::new("name".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("age".to_string(), DbType::U8, 1, 0, 0, false),
            SchemaField::new("email".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("phone".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("is_active".to_string(), DbType::U8, 1, 0, 0, false),
            SchemaField::new("bank_balance".to_string(), DbType::F64, 8, 0, 0, false),
            SchemaField::new("married".to_string(), DbType::U8, 1, 0, 0, false),
            SchemaField::new("birth_date".to_string(), DbType::STRING, 12, 0, 0, false), // Assuming STRING, could be DATETIME
            SchemaField::new("last_login".to_string(), DbType::STRING, 12, 0, 0, false), // Assuming STRING, could be DATETIME
            SchemaField::new("last_purchase".to_string(), DbType::STRING, 12, 0, 0, false), // Assuming STRING, could be DATETIME
            SchemaField::new("last_purchase_amount".to_string(), DbType::F32, 4, 0, 0, false),
            SchemaField::new("last_purchase_date".to_string(), DbType::STRING, 12, 0, 0, false), // Assuming STRING, could be DATETIME
            SchemaField::new("last_purchase_location".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("last_purchase_method".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("last_purchase_category".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("last_purchase_subcategory".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("last_purchase_description".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("last_purchase_status".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("last_purchase_id".to_string(), DbType::U64, 8, 0, 0, false),
            SchemaField::new("last_purchase_notes".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("net_assets".to_string(), DbType::F64, 8, 0, 0, false),
            SchemaField::new("department".to_string(), DbType::STRING, 12, 0, 0, false),
            SchemaField::new("salary".to_string(), DbType::F32, 4, 0, 0, false),
        ]
    }

    fn setup_test_columns<'a>() -> SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>  {
        let mut columns = SmallVec::new();

        static ID: &str = "id";
        static AGE: &str = "age";
        static SALARY: &str = "salary";
        static NAME: &str = "name";
        static DEPARTMENT: &str = "department";
        static BANK_BALANCE: &str = "bank_balance";
        static CREDIT_BALANCE: &str = "credit_balance";
        static NET_ASSETS: &str = "net_assets";
        static EARTH_POSITION: &str = "earth_position";
        static TEST_1: &str = "test_1";
        static TEST_2: &str = "test_2";
        static TEST_3: &str = "test_3";
        static TEST_4: &str = "test_4";
        static TEST_5: &str = "test_5";
        static TEST_6: &str = "test_6";

        columns.push((Cow::Borrowed(ID), create_memory_block_from_i32(42)));
        columns.push((Cow::Borrowed(AGE), create_memory_block_from_u8(30)));
        columns.push((Cow::Borrowed(SALARY), create_memory_block_from_i32(50000)));
        columns.push((Cow::Borrowed(NAME), create_memory_block_from_string("John Doe")));
        columns.push((Cow::Borrowed(DEPARTMENT), create_memory_block_from_string("Engineering")));
        columns.push((Cow::Borrowed(BANK_BALANCE), create_memory_block_from_f64(1000.43)));
        columns.push((Cow::Borrowed(CREDIT_BALANCE), create_memory_block_from_f32(100.50)));
        columns.push((Cow::Borrowed(NET_ASSETS), create_memory_block_from_i64(200000)));
        columns.push((Cow::Borrowed(EARTH_POSITION), create_memory_block_from_i8(-50)));
        columns.push((Cow::Borrowed(TEST_1), create_memory_block_from_u32(100)));
        columns.push((Cow::Borrowed(TEST_2), create_memory_block_from_u16(200)));
        columns.push((Cow::Borrowed(TEST_3), create_memory_block_from_i128(i128::MAX)));
        columns.push((Cow::Borrowed(TEST_4), create_memory_block_from_u128(u128::MAX)));
        columns.push((Cow::Borrowed(TEST_5), create_memory_block_from_i16(300)));
        columns.push((Cow::Borrowed(TEST_6), create_memory_block_from_u64(400)));

        columns
    }

    fn setup_test_columns_2<'a>() -> SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>  {
        let mut columns = SmallVec::new();

        static ID: &str = "id";
        static NAME: &str = "name";
        static AGE: &str = "age";
        static EMAIL: &str = "email";
        static PHONE: &str = "phone";
        static IS_ACTIVE: &str = "is_active";
        static BANK_BALANCE: &str = "bank_balance";
        static MARRIED: &str = "married";
        static BIRTH_DATE: &str = "birth_date";
        static LAST_LOGIN: &str = "last_login";
        static LAST_PURCHASE: &str = "last_purchase";
        static LAST_PURCHASE_AMOUNT: &str = "last_purchase_amount";
        static LAST_PURCHASE_DATE: &str = "last_purchase_date";
        static LAST_PURCHASE_LOCATION: &str = "last_purchase_location";
        static LAST_PURCHASE_METHOD: &str = "last_purchase_method";
        static LAST_PURCHASE_CATEGORY: &str = "last_purchase_category";
        static LAST_PURCHASE_SUBCATEGORY: &str = "last_purchase_subcategory";
        static LAST_PURCHASE_DESCRIPTION: &str = "last_purchase_description";
        static LAST_PURCHASE_STATUS: &str = "last_purchase_status";
        static LAST_PURCHASE_ID: &str = "last_purchase_id";
        static LAST_PURCHASE_NOTES: &str = "last_purchase_notes";
        static NET_ASSETS: &str = "net_assets";
        static DEPARTMENT: &str = "department";
        static SALARY: &str = "salary";

        columns.push((Cow::Borrowed(ID), create_memory_block_from_u128(42_433_943_354)));
        columns.push((Cow::Borrowed(NAME), create_memory_block_from_string("John Doe")));
        columns.push((Cow::Borrowed(AGE), create_memory_block_from_u8(30)));
        columns.push((Cow::Borrowed(EMAIL), create_memory_block_from_string("john.doe@example.com")));
        columns.push((Cow::Borrowed(PHONE), create_memory_block_from_string("123-456-7890")));
        columns.push((Cow::Borrowed(IS_ACTIVE), create_memory_block_from_u8(1)));
        columns.push((Cow::Borrowed(BANK_BALANCE), create_memory_block_from_f64(1000.43)));
        columns.push((Cow::Borrowed(MARRIED), create_memory_block_from_u8(0)));
        columns.push((Cow::Borrowed(BIRTH_DATE), create_memory_block_from_string("1990-01-01")));
        columns.push((Cow::Borrowed(LAST_LOGIN), create_memory_block_from_string("2023-10-01")));
        columns.push((Cow::Borrowed(LAST_PURCHASE), create_memory_block_from_string("2023-09-30")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_AMOUNT), create_memory_block_from_f32(15_540.75)));
        columns.push((Cow::Borrowed(LAST_PURCHASE_DATE), create_memory_block_from_string("2023-09-30")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_LOCATION), create_memory_block_from_string("Online")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_METHOD), create_memory_block_from_string("Credit Card")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_CATEGORY), create_memory_block_from_string("Electronics")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_SUBCATEGORY), create_memory_block_from_string("Computers")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_DESCRIPTION), create_memory_block_from_string("Gaming Laptop")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_STATUS), create_memory_block_from_string("Completed")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_ID), create_memory_block_from_u64(1001)));
        columns.push((Cow::Borrowed(LAST_PURCHASE_NOTES), create_memory_block_from_string("No issues")));
        columns.push((Cow::Borrowed(NET_ASSETS), create_memory_block_from_f64(8_449_903_733.92)));
        columns.push((Cow::Borrowed(DEPARTMENT), create_memory_block_from_string("Engineering")));
        columns.push((Cow::Borrowed(SALARY), create_memory_block_from_f32(4955.58)));

        columns
    }

    #[test]
    fn test_parse_simple_equals() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "id = 42";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers, 
            &mut buffer.intermediate_results
        ));

        parse_query(&tokens, columns, &schema, unsafe { &mut *transformer.get() });

        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer).clone());
    }


    #[test]
    fn test_parse_simple_not_equals() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "id != 50";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_simple_greater_than() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age > 25";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_simple_less_than() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age < 40";

       let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_greater_or_equal() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age >= 30";

      let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_less_or_equal() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age <= 30";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_math_operation() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age + 10 = 40";

       let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_multiple_math_operations() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age * 2 + 10 = 70";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_parentheses() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "(age + 10) * 2 = 80";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_string_contains() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "name CONTAINS 'John'";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_string_starts_with() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "name STARTSWITH 'John'";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_string_ends_with() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "name ENDSWITH 'Doe'";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_logical_and() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age > 25 AND salary = 50000";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_logical_or() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age < 25 OR salary = 50000";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_complex_query() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age > 25 AND (salary = 50000 OR name CONTAINS 'Jane')";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_tokenize() {
        use crate::core::row_v2::query_parser::Token;
        
        let tokens = tokenize_for_test("
            age >= 30 
            AND 
            name CONTAINS 'John'
            AND
            bank_balance > 1000.00
            ", &create_schema());
        
        assert_eq!(tokens.len(), 11);
        
        // Validate token types and values
        match &tokens[0] {
            Token::Ident(s) => assert_eq!(s, "age"),
            _ => panic!("Expected identifier token"),
        }
        
        match &tokens[1] {
            Token::Op(s) => assert_eq!(s, ">="),
            _ => panic!("Expected operator token"),
        }
        
        match &tokens[2] {
            Token::Number(n) => assert_eq!(*n, NumericValue::U8(30)),
            _ => panic!("Expected number token"),
        }
        
        match &tokens[3] {
            Token::Ident(s) => assert_eq!(s.to_uppercase(), "AND"),
            _ => panic!("Expected AND token"),
        }
        
        match &tokens[4] {
            Token::Ident(s) => assert_eq!(s, "name"),
            _ => panic!("Expected identifier token"),
        }
        
        match &tokens[5] {
            Token::Ident(s) => assert_eq!(s.to_uppercase(), "CONTAINS"),
            _ => panic!("Expected CONTAINS token"),
        }
        
        match &tokens[6] {
            Token::StringLit(s) => assert_eq!(s, "John"),
            _ => panic!("Expected string literal token"),
        }

        match &tokens[7] {
            Token::Ident(s) => assert_eq!(s.to_uppercase(), "AND"),
            _ => panic!("Expected AND token"),
        }

        match &tokens[8] {
            Token::Ident(s) => assert_eq!(s, "bank_balance"),
            _ => panic!("Expected identifier token"),
        }

        match &tokens[9] {
            Token::Op(s) => assert_eq!(s, ">"),
            _ => panic!("Expected operator token"),
        }

        match &tokens[10] {
            Token::Number(s) => assert_eq!(s, &NumericValue::F64(1000.00)),
            _ => panic!("Expected number token"),
        }
    }

    
    #[test]
    fn test_string_equality_type_inference() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "name = 'John Doe'";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_string_op_type_inference() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "department STARTSWITH 'Eng'";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_mixed_types_in_different_comparisons_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "id > 10 AND name = 'Test'"; // id > 10 (T), name = 'Test' (F) -> F
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_arithmetic_precedence_div_add() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "salary / 2 + 100 = 25100"; // 50000/2 + 100 = 25000 + 100 = 25100
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_arithmetic_precedence_literal_first_div_add() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "10 + salary / 2 = 25010"; // 10 + 50000/2 = 10 + 25000 = 25010
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_logical_precedence_and_then_or() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // (id=42 (T) AND age > 20 (T)) OR salary < 60000 (T) -> (T AND T) OR T -> T OR T -> T
        let query = "id = 42 AND age > 20 OR salary < 60000";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_logical_precedence_or_then_and_parsed() { // Parser structure implies AND has higher precedence
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // id = 10 (F) OR (age > 20 (T) AND salary = 50000 (T)) -> F OR (T AND T) -> F OR T -> T
        let query = "id = 10 OR age > 20 AND salary = 50000";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }
    
    #[test]
    fn test_explicit_logical_grouping_and_first() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "(id = 42 AND age > 20) OR salary < 60000"; // Same as test_logical_precedence_and_then_or
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_explicit_logical_grouping_or_first_with_and() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "id = 10 OR (age > 20 AND salary = 50000)"; // Same as test_logical_precedence_or_then_and_parsed
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_complex_boolean_with_grouping() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // (name STARTSWITH 'J' (T) OR department = 'Sales' (F)) AND age < 35 (T)
        // (T OR F) AND T -> T AND T -> T
        let query = "(name STARTSWITH 'J' OR department = 'Sales') AND age < 35";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_complex_arithmetic_nested_parentheses() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // ((30+5)*2 - 10)/3 = (35*2 - 10)/3 = (70-10)/3 = 60/3 = 20
        let query = "( (age + 5) * 2 - 10 ) / 3 = 20";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_nested_boolean_groups() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // (id=42 (T) AND (age>25 (T) OR age<20 (F))) OR department='Engineering' (T)
        // (T AND (T OR F)) OR T -> (T AND T) OR T -> T OR T -> T
        let query = "(id = 42 AND (age > 25 OR age < 20)) OR department = 'Engineering'";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_column_compared_to_itself() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "id = id"; // 42 = 42

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }
    
    #[test]
    fn test_literal_on_left_arithmetic_on_right() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "10 = id - 32"; // 10 = 42 - 32 -> 10 = 10
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_multiple_conditions_on_same_string_column() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // name = 'John Doe' (T) AND name != 'Jane Doe' (T) -> T
        let query = "name = 'John Doe' AND name != 'Jane Doe'";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_arithmetic_group_followed_by_and() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // (age + 10) > 30 (T, 40 > 30) AND id = 42 (T) -> T
        let query = "(age + 10) > 30 AND id = 42";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_multiple_string_operations_with_and() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // department ENDSWITH 'ing' (T) AND name CONTAINS 'oh' (T) -> T
        let query = "department ENDSWITH 'ing' AND name CONTAINS 'oh'";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_boolean_groups_with_and_or_combination() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // (age*2=60 (T) OR age/2=15 (T)) -> (T OR T) -> T
        // AND
        // (salary > 40000 (T) AND salary < 60000 (T)) -> (T AND T) -> T
        // T AND T -> T
        let query = "(age * 2 = 60 OR age / 2 = 15) AND (salary > 40000 AND salary < 60000)";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_tokenize_keywords_as_idents_and_ops() { // Existing test_tokenize is good, this adds a bit more
        use crate::core::row_v2::query_parser::Token;

        let tokens = tokenize_for_test("age >= 30 AND name CONTAINS 'John' OR id STARTSWITH 'prefix'", &create_schema());

        assert_eq!(tokens.len(), 11);
        
        match &tokens[0] { Token::Ident(s) => assert_eq!(s, "age"), _ => panic!("Expected ident") }
        match &tokens[1] { Token::Op(s) => assert_eq!(s, ">="), _ => panic!("Expected op") }
        match &tokens[2] { Token::Number(n) => assert_eq!(*n, NumericValue::U8(30)), _ => panic!("Expected num") }
        match &tokens[3] { Token::Ident(s) => assert_eq!(s.to_uppercase(), "AND"), _ => panic!("Expected AND") }
        match &tokens[4] { Token::Ident(s) => assert_eq!(s, "name"), _ => panic!("Expected ident") }
        match &tokens[5] { Token::Ident(s) => assert_eq!(s.to_uppercase(), "CONTAINS"), _ => panic!("Expected CONTAINS") }
        match &tokens[6] { Token::StringLit(s) => assert_eq!(s, "John"), _ => panic!("Expected str lit") }
        match &tokens[7] { Token::Ident(s) => assert_eq!(s.to_uppercase(), "OR"), _ => panic!("Expected OR") }
        match &tokens[8] { Token::Ident(s) => assert_eq!(s, "id"), _ => panic!("Expected ident") }
        match &tokens[9] { Token::Ident(s) => assert_eq!(s.to_uppercase(), "STARTSWITH"), _ => panic!("Expected STARTSWITH") }
        match &tokens[10] { Token::StringLit(s) => assert_eq!(s, "prefix"), _ => panic!("Expected str lit") }
    }

    // --- 20 not true tests ---

    #[test]
    fn test_parse_simple_equals_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "id = 100"; // 42 = 100 -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_simple_string_equals_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "name = 'NonExistent'"; // 'John Doe' = 'NonExistent' -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_simple_greater_than_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age > 30"; // 30 > 30 -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_simple_less_than_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "salary < 50000"; // 50000 < 50000 -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_simple_not_equals_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "id != 42"; // 42 != 42 -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_string_contains_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "name CONTAINS 'XYZ'"; // 'John Doe' CONTAINS 'XYZ' -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_string_starts_with_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "department STARTSWITH 'Sci'"; // 'Engineering' STARTSWITH 'Sci' -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_string_ends_with_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "name ENDSWITH 'Smith'"; // 'John Doe' ENDSWITH 'Smith' -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_math_operation_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age + 10 = 50"; // 30 + 10 = 40; 40 = 50 -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_multiple_math_operations_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age * 2 + 10 = 60"; // 30 * 2 + 10 = 70; 70 = 60 -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_parentheses_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "(age + 10) * 2 = 70"; // (30 + 10) * 2 = 80; 80 = 70 -> False
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_logical_and_false_first_cond() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age > 35 AND salary = 50000"; // (30 > 35 -> F) AND (50000 = 50000 -> T) -> F
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_logical_and_false_second_cond() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age > 25 AND salary = 10000"; // (30 > 25 -> T) AND (50000 = 10000 -> F) -> F
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_logical_and_false_both_conds() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age > 35 AND salary = 10000"; // (F) AND (F) -> F
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_parse_logical_or_false_both_conds() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "age < 20 OR salary = 10000"; // (30 < 20 -> F) OR (50000 = 10000 -> F) -> F
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    #[should_panic(expected =r#"Invalid byte slice: TryFromSliceError(())"#)]
    fn test_column_vs_column_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "id = age"; // Should fail, not same type
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_literal_on_left_arithmetic_on_right_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "10 = id - 30"; // 10 = (42 - 30) -> 10 = 12 -> False

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_complex_query_false_condition() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // age > 25 (T) AND (salary = 10000 (F) OR name CONTAINS 'Jane' (F)) -> T AND (F OR F) -> T AND F -> F
        let query = "age > 25 AND (salary = 10000 OR name CONTAINS 'Jane')";

        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    #[should_panic(expected ="unknown column schema for AgE")]
    fn test_case_insensitive_keywords_with_false_outcome() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // AgE > 35 (F) aNd SaLaRy = 50000 (T) -> F
        let query = "AgE > 35 aNd SaLaRy = 50000";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_query_with_extra_whitespace_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // id = 100 (F) AND name CONTAINS 'John' (T) -> F
        let query = "id   =  100   AND  name   CONTAINS   'John'";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_float_equality_true() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "bank_balance = 1000.43";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_float_equality_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "bank_balance = 1000.44";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_float_greater_than_true() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "bank_balance > 1000.0";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_float_less_than_with_arithmetic_false() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "bank_balance - 0.43 < 1000.0"; // 1000.43 - 0.43 = 1000.0; 1000.0 < 1000.0 is false
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_float_comparison_with_logical_and_true() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        let query = "bank_balance >= 1000.43 AND age = 30"; // T AND T -> T
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_various_numeric_types_and_logical_ops_true() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // credit_balance (100.50) > 100.0 (T)
        // AND net_assets (200000) < 200001 (T)
        // AND earth_position (-50) = -50 (T)
        // T AND T AND T -> T
        let query = "credit_balance > 100.0 AND net_assets < 200001 AND earth_position = -50";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_large_and_unsigned_types_with_grouping_true() {
        let schema = create_schema(); 
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns());
            columns_box
        });
        let columns = &*COLUMNS;
        // test_1 (100) = 100 (T)
        // OR test_2 (200) > 300 (F) -> T OR F -> T
        // AND
        // test_3 (MAX) = test_3 (T)
        // AND test_4 (MAX) > 0 (T) -> T AND T -> T
        // (T) AND (T) -> T
        let query = "(test_1 = 100 OR test_2 > 300) AND (test_3 = test_3 AND test_4 > 0)";
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &create_schema(), unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

     fn _setup_test_columns43242<'a>() -> SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>  {
        let mut columns = SmallVec::new();

        static ID: &str = "id";
        static NAME: &str = "name";
        static AGE: &str = "age";
        static EMAIL: &str = "email";
        static PHONE: &str = "phone";
        static IS_ACTIVE: &str = "is_active";
        static BANK_BALANCE: &str = "bank_balance";
        static MARRIED: &str = "married";
        static BIRTH_DATE: &str = "birth_date";
        static LAST_LOGIN: &str = "last_login";
        static LAST_PURCHASE: &str = "last_purchase";
        static LAST_PURCHASE_AMOUNT: &str = "last_purchase_amount";
        static LAST_PURCHASE_DATE: &str = "last_purchase_date";
        static LAST_PURCHASE_LOCATION: &str = "last_purchase_location";
        static LAST_PURCHASE_METHOD: &str = "last_purchase_method";
        static LAST_PURCHASE_CATEGORY: &str = "last_purchase_category";
        static LAST_PURCHASE_SUBCATEGORY: &str = "last_purchase_subcategory";
        static LAST_PURCHASE_DESCRIPTION: &str = "last_purchase_description";
        static LAST_PURCHASE_STATUS: &str = "last_purchase_status";
        static LAST_PURCHASE_ID: &str = "last_purchase_id";
        static LAST_PURCHASE_NOTES: &str = "last_purchase_notes";
        static NET_ASSETS: &str = "net_assets";
        static DEPARTMENT: &str = "department";
        static SALARY: &str = "salary";

        columns.push((Cow::Borrowed(ID), create_memory_block_from_u128(42_433_943_354)));
        columns.push((Cow::Borrowed(NAME), create_memory_block_from_string("John Doe")));
        columns.push((Cow::Borrowed(AGE), create_memory_block_from_u8(30)));
        columns.push((Cow::Borrowed(EMAIL), create_memory_block_from_string("john.doe@example.com")));
        columns.push((Cow::Borrowed(PHONE), create_memory_block_from_string("123-456-7890")));
        columns.push((Cow::Borrowed(IS_ACTIVE), create_memory_block_from_u8(1)));
        columns.push((Cow::Borrowed(BANK_BALANCE), create_memory_block_from_f64(1000.43)));
        columns.push((Cow::Borrowed(MARRIED), create_memory_block_from_u8(0)));
        columns.push((Cow::Borrowed(BIRTH_DATE), create_memory_block_from_string("1990-01-01")));
        columns.push((Cow::Borrowed(LAST_LOGIN), create_memory_block_from_string("2023-10-01")));
        columns.push((Cow::Borrowed(LAST_PURCHASE), create_memory_block_from_string("2023-09-30")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_AMOUNT), create_memory_block_from_f32(15_540.75)));
        columns.push((Cow::Borrowed(LAST_PURCHASE_DATE), create_memory_block_from_string("2023-09-30")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_LOCATION), create_memory_block_from_string("Online")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_METHOD), create_memory_block_from_string("Credit Card")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_CATEGORY), create_memory_block_from_string("Electronics")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_SUBCATEGORY), create_memory_block_from_string("Computers")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_DESCRIPTION), create_memory_block_from_string("Gaming Laptop")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_STATUS), create_memory_block_from_string("Completed")));
        columns.push((Cow::Borrowed(LAST_PURCHASE_ID), create_memory_block_from_u64(1001)));
        columns.push((Cow::Borrowed(LAST_PURCHASE_NOTES), create_memory_block_from_string("No issues")));
        columns.push((Cow::Borrowed(NET_ASSETS), create_memory_block_from_f64(8_449_903_733.92)));
        columns.push((Cow::Borrowed(DEPARTMENT), create_memory_block_from_string("Engineering")));
        columns.push((Cow::Borrowed(SALARY), create_memory_block_from_f32(4955.58)));

        columns
    }

    #[test]
    fn test_very_complex_query_1_true() {
        let schema = create_schema_2(); 
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns_2());
            columns_box
        });
        let columns = &*COLUMNS_2;

        let query = r##"
            (id = 42 AND name CONTAINS 'John') OR
            (age > 25 AND salary < 50000.0) OR
            (bank_balance >= 1000.43 AND net_assets > 800000) OR
            (department STARTSWITH 'Eng' AND email ENDSWITH 'example.com')
        "##;
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, &schema, unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

}
