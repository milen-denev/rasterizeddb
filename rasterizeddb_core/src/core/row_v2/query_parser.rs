use std::collections::HashMap;
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

// For testing only
#[cfg(test)]
pub fn tokenize_for_test(s: &str, schema: &Vec<SchemaField>) -> Vec<Token> {
    tokenize(s, schema)
}

pub fn parse_query(
    query: &str,
    columns: &HashMap<String, MemoryBlock>,
    schema: &Vec<SchemaField>,
) -> TransformerProcessor {
    let parser = QueryParser::new(query, columns, schema);
    parser.parse_where()
}

struct QueryParser<'a> {
    toks: Vec<Token>,
    pos: usize,
    cols: &'a HashMap<String, MemoryBlock>,
    schema: &'a Vec<SchemaField>,
}

impl<'a> QueryParser<'a> {
    fn new(s: &str, cols: &'a HashMap<String, MemoryBlock>, schema: &'a Vec<SchemaField>) -> Self {
        let toks = tokenize(s, schema);
        Self { toks, pos: 0, cols, schema }
    }

    fn parse_where(mut self) -> TransformerProcessor {
        let mut proc = TransformerProcessor::new();
        self.parse_or(&mut proc);
        proc
    }

    /// Parse OR expressions
    fn parse_or(&mut self, proc: &mut TransformerProcessor) {
        self.parse_and(proc);
        while let Some(Next::Or) = self.peek_logic() {
            self.next_logic();
            self.parse_and(proc);
        }
    }

    /// Parse AND expressions
    fn parse_and(&mut self, proc: &mut TransformerProcessor) {
        self.parse_comp_or_group(proc);
        while let Some(Next::And) = self.peek_logic() {
            self.next_logic();
            self.parse_comp_or_group(proc);
        }
    }

    /// Parse either a grouped boolean expression or a comparison
    fn parse_comp_or_group(&mut self, proc: &mut TransformerProcessor) {
        if let Some(Token::LPar) = self.toks.get(self.pos) {
            if self.is_boolean_group(self.pos) {
                self.pos += 1; // consume '('
                self.parse_or(proc);
                if !matches!(self.next().unwrap(), Token::RPar) {
                    panic!("expected closing parenthesis");
                }
                return;
            }
        }
        // default to comparison (handles arithmetic grouping)
        self.parse_comparison(proc); // parse_comparison now returns ()
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
        proc: &mut TransformerProcessor
    ) { // Changed return type to ()
        let (left_op, left_type) = self.parse_expr(proc); // Now returns (ComparisonOperand, DbType)
        let op_token = self.next().unwrap();
        let op = match op_token {
            Token::Op(o) | Token::Ident(o) => o.to_uppercase(),
            _ => panic!("expected comparison op"),
        };
        let (right_op, right_type) = self.parse_expr(proc); // Now returns (ComparisonOperand, DbType)
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
        
        proc.add_comparison(
            determined_comparison_type,
            left_op.clone(),
            right_op.clone(),
            cmp,
            next_logic_op.clone()
        );
    }        
    
    fn parse_expr(
        &mut self,
        proc: &mut TransformerProcessor
    ) -> (ComparisonOperand, DbType) { // Changed return type
        let (mut lhs_op, mut lhs_type) = self.parse_term(proc);
        while let Some(op_str) = self.peek_op(&["+","-"]) {
            let math = if op_str=="+" { MathOperation::Add } else { MathOperation::Subtract };
            self.next(); // consume op
            let (rhs_op, rhs_type) = self.parse_term(proc);
            
            let result_type = promote_types(lhs_type, rhs_type);
            let idx = proc.add_math_operation(result_type.clone(), lhs_op.clone(), rhs_op.clone(), math);
            lhs_op = ComparisonOperand::Intermediate(idx);
            lhs_type = result_type; // Update type to the result type of the operation
        }
        (lhs_op, lhs_type)
    }    
    
    fn parse_term(
        &mut self,
        proc: &mut TransformerProcessor
    ) -> (ComparisonOperand, DbType) { // Changed return type
        let (mut lhs_op, mut lhs_type) = self.parse_factor(proc);
        while let Some(op_str) = self.peek_op(&["*","/"]) {
            let math = if op_str=="*" { MathOperation::Multiply } else { MathOperation::Divide };
            self.next();
            let (rhs_op, rhs_type) = self.parse_factor(proc);

            let result_type = promote_types(lhs_type, rhs_type);
            let idx = proc.add_math_operation(result_type.clone(), lhs_op.clone(), rhs_op.clone(), math);
            lhs_op = ComparisonOperand::Intermediate(idx);
            lhs_type = result_type; // Update type to the result type of the operation
        }
        (lhs_op, lhs_type)
    }

   fn parse_factor(
        &mut self,
        proc: &mut TransformerProcessor
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
                let mb = self.cols.get(&name)
                    .unwrap_or_else(|| panic!("unknown column data for {}", name))
                    .clone();
                (ComparisonOperand::Direct(mb), field_schema.db_type.clone())
            }
            Token::LPar => {
                let (inner_op, inner_type) = self.parse_expr(proc); // parse_expr now returns tuple
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

fn tokenize(s: &str, schema: &Vec<SchemaField>) -> Vec<Token> {
    let mut out = Vec::new();
    let mut i = 0;
    let cs: Vec<char> = s.chars().collect();
    
    // Helper function to determine numeric type based on field name and value
    fn parse_numeric_value(value_str: &str, field_name: Option<&str>, schema: &Vec<SchemaField>) -> NumericValue {
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

fn is_id_start(c: char) -> bool { c.is_alphabetic() || c=='_' }
fn is_id_part(c: char)  -> bool { c.is_alphanumeric() || c=='_' }

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

fn str_to_mb(s: &str) -> MemoryBlock {
    let b = s.as_bytes();
    let mb = MEMORY_POOL.acquire(b.len());
    mb.into_slice_mut().copy_from_slice(b);
    mb
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::core::db_type::DbType;
    use crate::core::row_v2::query_parser::{parse_query, tokenize_for_test, NumericValue};
    use crate::core::row_v2::schema::SchemaField;
    use crate::memory_pool::{MemoryBlock, MEMORY_POOL};

    fn create_schema() -> Vec<SchemaField> {
        vec![
            SchemaField::new("id".to_string(), DbType::I32, 4 , 0, 0, false),
            SchemaField::new("age".to_string(), DbType::I32, 4 , 0, 0, false),
            SchemaField::new("salary".to_string(), DbType::I32, 4 , 0, 0, false),
            SchemaField::new("name".to_string(), DbType::STRING, 4 + 8 , 0, 0, false),
            SchemaField::new("department".to_string(), DbType::STRING, 4 + 8 , 0, 0, false),
            SchemaField::new("bank_balance".to_string(), DbType::F64, 8 , 0, 0, false),
        ]
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

    fn setup_test_columns() -> HashMap<String, MemoryBlock> {
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), create_memory_block_from_i32(42));
        columns.insert("age".to_string(), create_memory_block_from_i32(30));
        columns.insert("salary".to_string(), create_memory_block_from_i32(50000));
        columns.insert("name".to_string(), create_memory_block_from_string("John Doe"));
        columns.insert("department".to_string(), create_memory_block_from_string("Engineering"));
        columns.insert("bank_balance".to_string(), create_memory_block_from_f64(1000.43));
        columns
    }

    #[test]
    fn test_parse_simple_equals() {
        let columns = setup_test_columns();
        let query = "id = 42";
        
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_simple_not_equals() {
        let columns = setup_test_columns();
        let query = "id != 50";
        
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_simple_greater_than() {
        let columns = setup_test_columns();
        let query = "age > 25";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_simple_less_than() {
        let columns = setup_test_columns();
        let query = "age < 40";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_greater_or_equal() {
        let columns = setup_test_columns();
        let query = "age >= 30";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_less_or_equal() {
        let columns = setup_test_columns();
        let query = "age <= 30";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_math_operation() {
        let columns = setup_test_columns();
        let query = "age + 10 = 40";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_multiple_math_operations() {
        let columns = setup_test_columns();
        let query = "age * 2 + 10 = 70";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_parentheses() {
        let columns = setup_test_columns();
        let query = "(age + 10) * 2 = 80";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_string_contains() {
        let columns = setup_test_columns();
        let query = "name CONTAINS 'John'";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_string_starts_with() {
        let columns = setup_test_columns();
        let query = "name STARTSWITH 'John'";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_string_ends_with() {
        let columns = setup_test_columns();
        let query = "name ENDSWITH 'Doe'";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_logical_and() {
        let columns = setup_test_columns();
        let query = "age > 25 AND salary = 50000";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_logical_or() {
        let columns = setup_test_columns();
        let query = "age < 25 OR salary = 50000";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_complex_query() {
        let columns = setup_test_columns();
        let query = "age > 25 AND (salary = 50000 OR name CONTAINS 'Jane')";

        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
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
            Token::Number(n) => assert_eq!(*n, NumericValue::I32(30)),
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
        let columns = setup_test_columns();
        let query = "name = 'John Doe'";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_string_op_type_inference() {
        let columns = setup_test_columns();
        let query = "department STARTSWITH 'Eng'";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_mixed_types_in_different_comparisons_false() {
        let columns = setup_test_columns();
        let query = "id > 10 AND name = 'Test'"; // id > 10 (T), name = 'Test' (F) -> F
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_arithmetic_precedence_div_add() {
        let columns = setup_test_columns();
        let query = "salary / 2 + 100 = 25100"; // 50000/2 + 100 = 25000 + 100 = 25100
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_arithmetic_precedence_literal_first_div_add() {
        let columns = setup_test_columns();
        let query = "10 + salary / 2 = 25010"; // 10 + 50000/2 = 10 + 25000 = 25010
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_logical_precedence_and_then_or() {
        let columns = setup_test_columns();
        // (id=42 (T) AND age > 20 (T)) OR salary < 60000 (T) -> (T AND T) OR T -> T OR T -> T
        let query = "id = 42 AND age > 20 OR salary < 60000";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_logical_precedence_or_then_and_parsed() { // Parser structure implies AND has higher precedence
        let columns = setup_test_columns();
        // id = 10 (F) OR (age > 20 (T) AND salary = 50000 (T)) -> F OR (T AND T) -> F OR T -> T
        let query = "id = 10 OR age > 20 AND salary = 50000";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }
    
    #[test]
    fn test_explicit_logical_grouping_and_first() {
        let columns = setup_test_columns();
        let query = "(id = 42 AND age > 20) OR salary < 60000"; // Same as test_logical_precedence_and_then_or
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_explicit_logical_grouping_or_first_with_and() {
        let columns = setup_test_columns();
        let query = "id = 10 OR (age > 20 AND salary = 50000)"; // Same as test_logical_precedence_or_then_and_parsed
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_complex_boolean_with_grouping() {
        let columns = setup_test_columns();
        // (name STARTSWITH 'J' (T) OR department = 'Sales' (F)) AND age < 35 (T)
        // (T OR F) AND T -> T AND T -> T
        let query = "(name STARTSWITH 'J' OR department = 'Sales') AND age < 35";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_complex_arithmetic_nested_parentheses() {
        let columns = setup_test_columns();
        // ((30+5)*2 - 10)/3 = (35*2 - 10)/3 = (70-10)/3 = 60/3 = 20
        let query = "( (age + 5) * 2 - 10 ) / 3 = 20";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_nested_boolean_groups() {
        let columns = setup_test_columns();
        // (id=42 (T) AND (age>25 (T) OR age<20 (F))) OR department='Engineering' (T)
        // (T AND (T OR F)) OR T -> (T AND T) OR T -> T OR T -> T
        let query = "(id = 42 AND (age > 25 OR age < 20)) OR department = 'Engineering'";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_column_compared_to_itself() {
        let columns = setup_test_columns();
        let query = "id = id"; // 42 = 42
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }
    
    #[test]
    fn test_literal_on_left_arithmetic_on_right() {
        let columns = setup_test_columns();
        let query = "10 = id - 32"; // 10 = 42 - 32 -> 10 = 10
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_multiple_conditions_on_same_string_column() {
        let columns = setup_test_columns();
        // name = 'John Doe' (T) AND name != 'Jane Doe' (T) -> T
        let query = "name = 'John Doe' AND name != 'Jane Doe'";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_arithmetic_group_followed_by_and() {
        let columns = setup_test_columns();
        // (age + 10) > 30 (T, 40 > 30) AND id = 42 (T) -> T
        let query = "(age + 10) > 30 AND id = 42";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_multiple_string_operations_with_and() {
        let columns = setup_test_columns();
        // department ENDSWITH 'ing' (T) AND name CONTAINS 'oh' (T) -> T
        let query = "department ENDSWITH 'ing' AND name CONTAINS 'oh'";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_very_complex_mixed_query() {
        let columns = setup_test_columns();
        // (age + salary/1000 > 70) -> (30 + 50 > 70) -> 80 > 70 (T)
        // AND
        // (name STARTSWITH 'J' (T) OR department = 'HR' (F)) -> (T OR F) -> T
        // T AND T -> T
        let query = "(age + salary / 1000 > 70) AND (name STARTSWITH 'J' OR department = 'HR')";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_boolean_groups_with_and_or_combination() {
        let columns = setup_test_columns();
        // (age*2=60 (T) OR age/2=15 (T)) -> (T OR T) -> T
        // AND
        // (salary > 40000 (T) AND salary < 60000 (T)) -> (T AND T) -> T
        // T AND T -> T
        let query = "(age * 2 = 60 OR age / 2 = 15) AND (salary > 40000 AND salary < 60000)";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_complex_arithmetic_on_both_sides_of_comparison() {
        let columns = setup_test_columns();
        // salary - 10000 = 50000 - 10000 = 40000
        // age * 1000 + 10000 = 30 * 1000 + 10000 = 30000 + 10000 = 40000
        // 40000 = 40000 -> T
        let query = "salary - 10000 = age * 1000 + 10000";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_tokenize_keywords_as_idents_and_ops() { // Existing test_tokenize is good, this adds a bit more
        use crate::core::row_v2::query_parser::Token;

        let tokens = tokenize_for_test("age >= 30 AND name CONTAINS 'John' OR id STARTSWITH 'prefix'", &create_schema());

        assert_eq!(tokens.len(), 11);
        
        match &tokens[0] { Token::Ident(s) => assert_eq!(s, "age"), _ => panic!("Expected ident") }
        match &tokens[1] { Token::Op(s) => assert_eq!(s, ">="), _ => panic!("Expected op") }
        match &tokens[2] { Token::Number(n) => assert_eq!(*n, NumericValue::I32(30)), _ => panic!("Expected num") }
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
        let columns = setup_test_columns();
        let query = "id = 100"; // 42 = 100 -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_simple_string_equals_false() {
        let columns = setup_test_columns();
        let query = "name = 'NonExistent'"; // 'John Doe' = 'NonExistent' -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_simple_greater_than_false() {
        let columns = setup_test_columns();
        let query = "age > 30"; // 30 > 30 -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_simple_less_than_false() {
        let columns = setup_test_columns();
        let query = "salary < 50000"; // 50000 < 50000 -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_simple_not_equals_false() {
        let columns = setup_test_columns();
        let query = "id != 42"; // 42 != 42 -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_string_contains_false() {
        let columns = setup_test_columns();
        let query = "name CONTAINS 'XYZ'"; // 'John Doe' CONTAINS 'XYZ' -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_string_starts_with_false() {
        let columns = setup_test_columns();
        let query = "department STARTSWITH 'Sci'"; // 'Engineering' STARTSWITH 'Sci' -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_string_ends_with_false() {
        let columns = setup_test_columns();
        let query = "name ENDSWITH 'Smith'"; // 'John Doe' ENDSWITH 'Smith' -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_math_operation_false() {
        let columns = setup_test_columns();
        let query = "age + 10 = 50"; // 30 + 10 = 40; 40 = 50 -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_multiple_math_operations_false() {
        let columns = setup_test_columns();
        let query = "age * 2 + 10 = 60"; // 30 * 2 + 10 = 70; 70 = 60 -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_parentheses_false() {
        let columns = setup_test_columns();
        let query = "(age + 10) * 2 = 70"; // (30 + 10) * 2 = 80; 80 = 70 -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_logical_and_false_first_cond() {
        let columns = setup_test_columns();
        let query = "age > 35 AND salary = 50000"; // (30 > 35 -> F) AND (50000 = 50000 -> T) -> F
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_logical_and_false_second_cond() {
        let columns = setup_test_columns();
        let query = "age > 25 AND salary = 10000"; // (30 > 25 -> T) AND (50000 = 10000 -> F) -> F
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_logical_and_false_both_conds() {
        let columns = setup_test_columns();
        let query = "age > 35 AND salary = 10000"; // (F) AND (F) -> F
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_logical_or_false_both_conds() {
        let columns = setup_test_columns();
        let query = "age < 20 OR salary = 10000"; // (30 < 20 -> F) OR (50000 = 10000 -> F) -> F
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_column_vs_column_false() {
        let columns = setup_test_columns();
        let query = "id = age"; // 42 = 30 -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_literal_on_left_arithmetic_on_right_false() {
        let columns = setup_test_columns();
        let query = "10 = id - 30"; // 10 = (42 - 30) -> 10 = 12 -> False
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_complex_query_false_condition() {
        let columns = setup_test_columns();
        // age > 25 (T) AND (salary = 10000 (F) OR name CONTAINS 'Jane' (F)) -> T AND (F OR F) -> T AND F -> F
        let query = "age > 25 AND (salary = 10000 OR name CONTAINS 'Jane')";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    #[should_panic(expected ="unknown column schema for AgE")]
    fn test_case_insensitive_keywords_with_false_outcome() {
        let columns = setup_test_columns();
        // AgE > 35 (F) aNd SaLaRy = 50000 (T) -> F
        let query = "AgE > 35 aNd SaLaRy = 50000";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_query_with_extra_whitespace_false() {
        let columns = setup_test_columns();
        // id = 100 (F) AND name CONTAINS 'John' (T) -> F
        let query = "id   =  100   AND  name   CONTAINS   'John'";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_float_equality_true() {
        let columns = setup_test_columns();
        let query = "bank_balance = 1000.43";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_float_equality_false() {
        let columns = setup_test_columns();
        let query = "bank_balance = 1000.44";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_float_greater_than_true() {
        let columns = setup_test_columns();
        let query = "bank_balance > 1000.0";
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }

    #[test]
    fn test_float_less_than_with_arithmetic_false() {
        let columns = setup_test_columns();
        let query = "bank_balance - 0.43 < 1000.0"; // 1000.43 - 0.43 = 1000.0; 1000.0 < 1000.0 is false
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(!processor.execute());
    }

    #[test]
    fn test_float_comparison_with_logical_and_true() {
        let columns = setup_test_columns();
        let query = "bank_balance >= 1000.43 AND age = 30"; // T AND T -> T
        let mut processor = parse_query(query, &columns, &create_schema());
        assert!(processor.execute());
    }
}
