use std::borrow::Cow;
use ahash::{HashSet, HashSetExt};
use smallvec::SmallVec;

use crate::memory_pool::MemoryBlock;
use crate::core::db_type::DbType;
use super::schema::SchemaField;
use super::query_tokenizer::{numeric_to_mb, numeric_value_to_db_type, str_to_mb, tokenize, Token};
use super::transformer::{ComparerOperation, ComparisonOperand, MathOperation, Next, TransformerProcessor};

pub struct QueryParser<'a, 'b> {
    toks: &'a SmallVec<[Token; 36]>,
    pos: usize,
    query_transformer: &'b mut TransformerProcessor<'b>,
    first_iteration: bool, // Fixed: singular form
    // Store column indices that are actually used
    used_column_indices: HashSet<usize>,
}

impl<'a, 'b> QueryParser<'a, 'b> {
    pub fn new(
        tokens: &'a SmallVec<[Token; 36]>,
        query_transformer: &'b mut TransformerProcessor<'b>,
    ) -> Self {
        Self { 
            toks: tokens, 
            pos: 0, 
            query_transformer, 
            first_iteration: true, // Fixed: singular form
            used_column_indices: HashSet::new() 
        }
    }

    /// Reset parser for processing new column data while keeping the optimization
    pub fn reset_for_new_data(&mut self) {
        // Don't reset first_iteration - we want to keep using the fast path
        // The transformers are already built, we just need new column data
    }

    /// Reset parser completely (forces reparsing)
    pub fn reset_completely(&mut self) {
        self.pos = 0;
        self.first_iteration = true;
        self.used_column_indices.clear();
    }

    pub fn execute<'c>(&mut self, cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>) -> Result<(), String> {
        if self.first_iteration {
            self.first_iteration = false;
            self.pos = 0;
            self.used_column_indices.clear();
            
            self.parse_or(cols)?;
            
            // This is fine - query with only literals doesn't need column optimization
            #[cfg(debug_assertions)]
            if self.used_column_indices.is_empty() {
                println!("Query contains only literal values - no column optimization needed");
            } else {
                println!("First iteration complete. Used columns: {:?}", self.used_column_indices);
            }
        } else {
            // Fast path: validate column count and update data
            if cols.len() == 0 {
                return Err("No columns provided for fast path".to_string());
            }
            
            // Validate that all used column indices are still valid
            let max_index = self.used_column_indices.iter().max().copied().unwrap_or(0);
            if max_index >= cols.len() {
                return Err(format!("Column index {} out of bounds (max: {})", max_index, cols.len() - 1));
            }
            
            #[cfg(debug_assertions)]
            println!("Fast path: updating {} transformers with new column data", 
                self.query_transformer.transformers.len());
            
            // Create the column slice for fast update
            let vec_blocks: SmallVec<[MemoryBlock; 20]> = cols.iter()
                .map(|(_, mb)| mb.clone())
                .collect();

            let slice: &[MemoryBlock] = &vec_blocks;

            self.query_transformer.replace_row_inputs(slice);
        }
        
        Ok(())
    }

    pub fn reset_for_next_execution(&mut self) {
        // Reset position but keep first_iteration = false and used_column_indices
        self.pos = 0;
        // Reset intermediate results but keep transformers
        self.query_transformer.reset_intermediate_results();
    }
    
    /// Get debug information about the parser state
    pub fn debug_info(&self) -> String {
        format!(
            "QueryParser {{ first_iteration: {}, pos: {}, used_columns: {:?}, transformers: {} }}",
            self.first_iteration,
            self.pos,
            self.used_column_indices,
            self.query_transformer.transformers.len()
        )
    }

    /// Check if the parser is in optimization mode
    pub fn is_optimized(&self) -> bool {
        !self.first_iteration && !self.used_column_indices.is_empty()
    }

    /// Parse OR expressions
    fn parse_or<'c>(&mut self, cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>) -> Result<(), String> {
        self.parse_and(cols)?;

        // After parsing each AND group, check if there's an OR
        while let Some(Next::Or) = self.peek_logic() {
            // Before consuming the OR, we need to set the last comparison's next operation
            self.set_last_comparison_next_op(Some(Next::Or));
            
            self.next_logic(); // consume OR token
            self.parse_and(cols)?;
        }
        
        Ok(())
    }

    /// Parse AND expressions  
    fn parse_and<'c>(&mut self, cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>) -> Result<(), String> {
        self.parse_comp_or_group(cols)?;
        
        while let Some(Next::And) = self.peek_logic() {
            // Set the previous comparison's next operation to AND
            self.set_last_comparison_next_op(Some(Next::And));
            
            self.next_logic(); // consume AND token
            self.parse_comp_or_group(cols)?;
        }
        
        Ok(())
    }
        
    // Add this helper method to set the next operation on the last added comparison
    fn set_last_comparison_next_op(&mut self, next_op: Option<Next>) {
        if let Some(last_transformer) = self.query_transformer.transformers.last_mut() {
            last_transformer.next = next_op;
        }
    }

    /// Parse either a grouped boolean expression or a comparison
    fn parse_comp_or_group<'c>(&mut self, cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>) -> Result<(), String> {
        if let Some(Token::LPar) = self.toks.get(self.pos) {
            // Check if this is a boolean group
            if self.is_boolean_group(self.pos) {
                self.pos += 1; // consume '('
                self.parse_or(cols)?; // Parse the boolean expression inside parentheses
                if !matches!(self.next().ok_or("Unexpected end of token stream")?, 
                        Token::RPar) {
                    return Err("expected closing parenthesis".to_string());
                }
                return Ok(());
            }
        }
        // default to comparison (handles arithmetic grouping)
        self.parse_comparison(cols)
    }

    /// Improved heuristic to check if parentheses at pos wrap a boolean expression
    fn is_boolean_group(&self, start: usize) -> bool {
        let mut depth = 0;
        let mut i = start;
        let mut has_comparison = false;
        let mut has_boolean_keyword = false;
        
        while i < self.toks.len() {
            match &self.toks[i] {
                Token::LPar => depth += 1,
                Token::RPar => {
                    depth -= 1;
                    if depth == 0 { 
                        return has_comparison || has_boolean_keyword; 
                    }
                },
                Token::Op(o) if depth >= 1 && ["=","!=",">",">=","<","<="].contains(&o.as_str()) => {
                    has_comparison = true;
                },
                Token::Next(n) if depth >= 1 => {
                    if ["AND", "OR"].contains(&n.as_str()) {
                        has_boolean_keyword = true;
                    }
                },
                Token::Ident(t) if depth >= 1 => {
                    let upper = &t.0;
                    if ["AND", "OR"].contains(&upper.as_str()) {
                        has_boolean_keyword = true;
                    } else if ["CONTAINS", "STARTSWITH", "ENDSWITH"].contains(&upper.as_str()) {
                        has_comparison = true;
                    }
                },
                _ => {}
            }
            i += 1;
        }
        
        false
    }

    fn parse_comparison<'c>(&mut self, cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>) -> Result<(), String> {
        let (left_op, left_type) = self.parse_expr(cols)?;
        
        // Handle both Token::Op and Token::Ident for operators
        let op_token = self.next().ok_or("Unexpected end of token stream")?;
        let op = match op_token {
            Token::Op(o) => o,
            Token::Ident((o, _, _)) => o,
            _ => return Err(format!("expected comparison operator, got {:?}", op_token)),
        };
        
        let (right_op, right_type) = self.parse_expr(cols)?;
        
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
            _ => return Err(format!("unknown comparison operator: {}", op)),
        };
        
        // Better type handling for string operations
        let determined_comparison_type = match cmp {
            ComparerOperation::Contains | 
            ComparerOperation::StartsWith | 
            ComparerOperation::EndsWith => {
                if left_type == DbType::STRING || right_type == DbType::STRING {
                    DbType::STRING
                } else {
                    DbType::STRING
                }
            },
            _ => left_type, 
        };

        // Add the comparison with None initially - the logic operations will be set later
        self.query_transformer.add_comparison(
            determined_comparison_type,
            left_op,
            right_op,
            cmp,
            None // Will be set by parse_or/parse_and when they know what comes next
        );
        
        Ok(())
    }

    fn parse_expr<'c>(&mut self, cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>) -> Result<(ComparisonOperand, DbType), String> {
        let (mut lhs_op, mut lhs_type) = self.parse_term(cols)?;
        
        while let Some(op_str) = self.peek_op(&["+","-"]) {
            let math = if op_str == "+" { MathOperation::Add } else { MathOperation::Subtract };
            self.next(); // consume op
            let (rhs_op, _) = self.parse_term(cols)?;
            
            let idx = self.query_transformer.add_math_operation(lhs_type.clone(), lhs_op, rhs_op, math);
            lhs_op = ComparisonOperand::Intermediate(idx);
            lhs_type = lhs_type;
        }
        
        Ok((lhs_op, lhs_type))
    }
    
    fn parse_term<'c>(&mut self, cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>) -> Result<(ComparisonOperand, DbType), String> {
        let (mut lhs_op, mut lhs_type) = self.parse_factor(cols)?;
        
        while let Some(op_str) = self.peek_op(&["*","/"]) {
            let math = if op_str == "*" { MathOperation::Multiply } else { MathOperation::Divide };
            self.next();
            let (rhs_op, _) = self.parse_factor(cols)?;

            let idx = self.query_transformer.add_math_operation(lhs_type.clone(), lhs_op, rhs_op, math);
            lhs_op = ComparisonOperand::Intermediate(idx);
            lhs_type = lhs_type;
        }
        
        Ok((lhs_op, lhs_type))
    }

    fn parse_factor<'c>(&mut self, cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>) -> Result<(ComparisonOperand, DbType), String> {
        let token = self.next().ok_or("Unexpected end of token stream")?;
        
        match token {
            Token::Number(n) => {
                let db_type = numeric_value_to_db_type(&n);
                let mb = numeric_to_mb(&n);
                Ok((ComparisonOperand::Direct(mb), db_type))
            }
            Token::StringLit(s) => {
                let mb = str_to_mb(&s);
                Ok((ComparisonOperand::Direct(mb), DbType::STRING))
            }
            Token::Ident((_name, db_type, write_order)) => {
                // Validate bounds before unsafe access
                // if write_order as usize >= cols.len() {
                //     return Err(format!("Column index {} out of bounds (max: {})", write_order, cols.len() - 1));
                // }
                
                // Safe column data retrieval
                let mb = cols[write_order as usize].1.clone();

                // Track which columns are used during first iteration
                if self.first_iteration {
                    self.used_column_indices.insert(write_order as usize);
                }

                Ok((ComparisonOperand::DirectWithIndex(mb, write_order as usize), db_type))
            }
            Token::LPar => {
                let (inner_op, inner_type) = self.parse_expr(cols)?;
                if !matches!(self.next().ok_or("Unexpected end of token stream")?, 
                        Token::RPar) {
                    return Err("expected closing parenthesis after grouped expression".to_string());
                }
                Ok((inner_op, inner_type))
            }
            t => Err(format!("unexpected factor: {:?}", t)),
        }
    }

    fn peek_logic(&self) -> Option<Next> {
        if let Some(token) = self.toks.get(self.pos) {
            match token {
                Token::Next(next_val) => {
                    match next_val.as_str() {
                        "AND" => Some(Next::And),
                        "OR" => Some(Next::Or),
                        _ => None
                    }
                },
                Token::Ident((t, _, _)) => {
                    match t.as_str() {
                        "AND" => Some(Next::And),
                        "OR" => Some(Next::Or),
                        _ => None
                    }
                },
                _ => None
            }
        } else { 
            None 
        }
    }

    fn next_logic(&mut self) { 
        self.pos += 1; 
    }

    fn peek_op(&self, a: &[&str]) -> Option<String> {
        if let Some(Token::Op(o)) = self.toks.get(self.pos) {
            if a.contains(&o.as_str()) { 
                return Some(o.clone()) 
            }
        }
        None
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.toks.get(self.pos).cloned();
        self.pos += 1;
        t
    }
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
    use crate::core::row_v2::query_parser::QueryParser;
    use crate::core::row_v2::row::Row;
    use crate::core::row_v2::schema::SchemaField;
    //use crate::core::row_v2::token_processor::TokenProcessor;
    use crate::core::row_v2::query_tokenizer::{tokenize, NumericValue, Token};
    use crate::core::row_v2::transformer::{ColumnTransformer, Next, TransformerProcessor};
    use crate::memory_pool::{MemoryBlock, MEMORY_POOL};

    struct Buffer<'a> {
        pub _hashtable_buffer: UnsafeCell<SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>>,
        pub _row: Row,
        pub transformers: SmallVec<[ColumnTransformer; 36]>,
        pub intermediate_results: SmallVec<[MemoryBlock; 20]>,
        pub bool_buffer: SmallVec<[(bool, Option<Next>); 20]>
    }

    fn tokenize_for_test(s: &str, schema: &SmallVec<[SchemaField; 20]>) -> SmallVec<[Token; 36]> {
        tokenize(s, schema)
    }

    fn parse_query<'a, 'b, 'c>(
        toks: &'a SmallVec<[Token; 36]>,
        columns: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
        transformers: &'b mut TransformerProcessor<'b>,
    ) {
        let mut parser = QueryParser::new(toks, transformers);
        _ = parser.execute(columns);
    }

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
            SchemaField::new("age".to_string(), DbType::U8, 1 , 0, 1, false),
            SchemaField::new("salary".to_string(), DbType::I32, 4 , 0, 2, false),
            SchemaField::new("name".to_string(), DbType::STRING, 4 + 8 , 0, 3, false),
            SchemaField::new("department".to_string(), DbType::STRING, 4 + 8 , 0, 4, false),
            SchemaField::new("bank_balance".to_string(), DbType::F64, 8 , 0, 5, false),
            SchemaField::new("credit_balance".to_string(), DbType::F32, 4 , 0, 6, false),
            SchemaField::new("net_assets".to_string(), DbType::I64, 8 , 0, 7, false),
            SchemaField::new("earth_position".to_string(), DbType::I8, 1, 0, 8, false),
            SchemaField::new("test_1".to_string(), DbType::U32, 4, 0, 9, false),
            SchemaField::new("test_2".to_string(), DbType::U16, 2, 0, 10, false),
            SchemaField::new("test_3".to_string(), DbType::I128, 16, 0, 11, false),
            SchemaField::new("test_4".to_string(), DbType::U128, 16, 0, 12, false),
            SchemaField::new("test_5".to_string(), DbType::I16, 2, 0, 13, false),
            SchemaField::new("test_6".to_string(), DbType::U64, 8, 0, 14, false),
        ]
    }

    fn create_schema_2() -> SmallVec<[SchemaField; 20]> {
        smallvec::smallvec![
            SchemaField::new("id".to_string(), DbType::U128, 16, 0, 0, false),
            SchemaField::new("name".to_string(), DbType::STRING, 12, 0, 1, false),
            SchemaField::new("age".to_string(), DbType::U8, 1, 0, 2, false),
            SchemaField::new("email".to_string(), DbType::STRING, 12, 0, 3, false),
            SchemaField::new("phone".to_string(), DbType::STRING, 12, 0, 4, false),
            SchemaField::new("is_active".to_string(), DbType::U8, 1, 0, 5, false),
            SchemaField::new("bank_balance".to_string(), DbType::F64, 8, 0, 6, false),
            SchemaField::new("married".to_string(), DbType::U8, 1, 0, 0, false),
            SchemaField::new("birth_date".to_string(), DbType::STRING, 12, 0, 7, false), // Assuming STRING, could be DATETIME
            SchemaField::new("last_login".to_string(), DbType::STRING, 12, 0, 8, false), // Assuming STRING, could be DATETIME
            SchemaField::new("last_purchase".to_string(), DbType::STRING, 12, 0, 9, false), // Assuming STRING, could be DATETIME
            SchemaField::new("last_purchase_amount".to_string(), DbType::F32, 4, 0, 10, false),
            SchemaField::new("last_purchase_date".to_string(), DbType::STRING, 12, 0, 11, false), // Assuming STRING, could be DATETIME
            SchemaField::new("last_purchase_location".to_string(), DbType::STRING, 12, 0, 12, false),
            SchemaField::new("last_purchase_method".to_string(), DbType::STRING, 12, 0, 13, false),
            SchemaField::new("last_purchase_category".to_string(), DbType::STRING, 12, 0, 14, false),
            SchemaField::new("last_purchase_subcategory".to_string(), DbType::STRING, 12, 0, 15, false),
            SchemaField::new("last_purchase_description".to_string(), DbType::STRING, 12, 0, 16, false),
            SchemaField::new("last_purchase_status".to_string(), DbType::STRING, 12, 0, 17, false),
            SchemaField::new("last_purchase_id".to_string(), DbType::U64, 8, 0, 18, false),
            SchemaField::new("last_purchase_notes".to_string(), DbType::STRING, 12, 0, 19, false),
            SchemaField::new("net_assets".to_string(), DbType::F64, 8, 0, 20, false),
            SchemaField::new("department".to_string(), DbType::STRING, 12, 0, 21, false),
            SchemaField::new("salary".to_string(), DbType::F32, 4, 0, 22, false),
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers, 
            &mut buffer.intermediate_results
        ));

        parse_query(&tokens, columns, unsafe { &mut *transformer.get() });

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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            Token::Ident(s) => assert_eq!(s.0, "age"),
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
            Token::Next(s) => assert_eq!(s, "AND"),
            _ => panic!("Expected AND token"),
        }
        
        match &tokens[4] {
            Token::Ident(s) => assert_eq!(s.0, "name"),
            _ => panic!("Expected identifier token"),
        }
        
        match &tokens[5] {
            Token::Ident(s) => assert_eq!(s.0.to_uppercase(), "CONTAINS"),
            _ => panic!("Expected CONTAINS token"),
        }
        
        match &tokens[6] {
            Token::StringLit(s) => assert_eq!(s, "John"),
            _ => panic!("Expected string literal token"),
        }

        match &tokens[7] {
            Token::Next(s) => assert_eq!(s, "AND"),
            _ => panic!("Expected AND token"),
        }

        match &tokens[8] {
            Token::Ident(s) => assert_eq!(s.0, "bank_balance"),
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_tokenize_keywords_as_idents_and_ops() { // Existing test_tokenize is good, this adds a bit more
        use crate::core::row_v2::query_parser::Token;

        let tokens = tokenize_for_test("age >= 30 AND name CONTAINS 'John' OR id STARTSWITH 'prefix'", &create_schema());

        assert_eq!(tokens.len(), 11);
        
        match &tokens[0] { Token::Ident(s) => assert_eq!(s.0, "age"), _ => panic!("Expected ident") }
        match &tokens[1] { Token::Op(s) => assert_eq!(s, ">="), _ => panic!("Expected op") }
        match &tokens[2] { Token::Number(n) => assert_eq!(*n, NumericValue::U8(30)), _ => panic!("Expected num") }
        match &tokens[3] { Token::Next(s) => assert_eq!(s, "AND"), _ => panic!("Expected AND") }
        match &tokens[4] { Token::Ident(s) => assert_eq!(s.0, "name"), _ => panic!("Expected ident") }
        match &tokens[5] { Token::Ident(s) => assert_eq!(s.0.to_uppercase(), "CONTAINS"), _ => panic!("Expected CONTAINS") }
        match &tokens[6] { Token::StringLit(s) => assert_eq!(s, "John"), _ => panic!("Expected str lit") }
        match &tokens[7] { Token::Next(s) => assert_eq!(s, "OR"), _ => panic!("Expected OR") }
        match &tokens[8] { Token::Ident(s) => assert_eq!(s.0, "id"), _ => panic!("Expected ident") }
        match &tokens[9] { Token::Ident(s) => assert_eq!(s.0.to_uppercase(), "STARTSWITH"), _ => panic!("Expected STARTSWITH") }
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    #[should_panic(expected ="assertion `left == right` failed\n  left: 1\n right: 4")]
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
        assert!(!transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    #[should_panic(expected ="Unknown column: 'AgE' is not defined in the schema")]
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });
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
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });

        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    #[test]
    fn test_ultra_complex_query_1_true() {
        let schema = create_schema_2(); 
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns_2());
            columns_box
        });
        let columns = &*COLUMNS_2;

        let query = r##"
            (id - 1 = 41 AND name CONTAINS 'John') OR
            (age + 10 > 15 AND salary - 1000 < 49000.0) OR
            (bank_balance >= 1000.43 AND net_assets + 40000 > 400000) OR
            (department STARTSWITH 'Eng' AND email ENDSWITH 'example.com')
        "##;
        
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });

        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }

    
    #[test]
    fn test_ultra_complex_query_no_parentheses_true() {
        let schema = create_schema_2(); 
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> = LazyLock::new(|| {
            let columns_box = Box::new(setup_test_columns_2());
            columns_box
        });
        let columns = &*COLUMNS_2;

        let query = r##"
            id - 1 = 41 AND name CONTAINS 'John' OR
            age + 10 > 15 AND salary - 1000 < 49000.0
        "##;
        
        let tokens = tokenize_for_test(query, &schema);

        let mut buffer = Buffer {
            _hashtable_buffer: UnsafeCell::new(SmallVec::new()),
            _row: Row::default(),
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let mut transformer = UnsafeCell::new(TransformerProcessor::new(&mut buffer.transformers, &mut buffer.intermediate_results));

        parse_query(&tokens, &columns, unsafe { &mut *transformer.get() });

        assert!(transformer.get_mut().execute(&mut buffer.bool_buffer));
    }
}