use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::HashMap;

use super::query_tokenizer::{Token, numeric_to_mb, numeric_value_to_db_type, str_to_mb, tokenize};

use crate::core::db_type::DbType;
use crate::core::processor::transformer::{ComparerOperation, ComparisonOperand, MathOperation, Next, TransformerProcessor};
use crate::core::row::schema::SchemaField;
use crate::memory_pool::MemoryBlock;

pub struct QueryParser<'a, 'b> {
    toks: &'a SmallVec<[Token; 36]>,
    pos: usize,
    query_transformer: &'b mut TransformerProcessor<'b>,
    first_iteration: bool,
    // Keep sorted & unique on first parse
    used_column_indices: SmallVec<[usize; 16]>,
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
            first_iteration: true,
            used_column_indices: SmallVec::new(),
        }
    }

    #[inline]
    pub fn reset_for_new_data(&mut self) {
        // Intentionally empty: keep used_column_indices and plan; no per-row cloning anymore.
    }

    pub fn reset_completely(&mut self) {
        self.pos = 0;
        self.first_iteration = true;
        self.used_column_indices.clear();
        // No tmp buffers to clear anymore
    }

    /// Build the transformer plan on the first call; fast path is now a no-op.
    pub fn execute<'c>(
        &mut self,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(), String> {
        if self.first_iteration {
            self.first_iteration = false;
            self.pos = 0;
            self.used_column_indices.clear();

            self.parse_or(cols)?;

            if !self.used_column_indices.is_empty() {
                self.used_column_indices.sort_unstable();
                self.used_column_indices.dedup();
            }
        } else {
            // Fast path: nothing to do; inputs will be resolved on-the-fly during evaluation.
            // Still validate input length for safety.
            if let Some(&max_index) = self.used_column_indices.last() {
                if max_index >= cols.len() {
                    return Err(format!(
                        "Column index {} out of bounds (max: {})",
                        max_index,
                        cols.len() - 1
                    ));
                }
            }
        }

        Ok(())
    }

    /// Evaluate against a concrete row without cloning.
    /// This will parse/build on first call and then run the processor in zero-copy mode.
    pub fn evaluate_row<'c>(
        &mut self,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
        comparison_results: &mut SmallVec<[(bool, Option<Next>); 20]>,
    ) -> Result<bool, String> {
        // Ensure the plan is built
        self.execute(cols)?;
        // Build a write_order-aligned view of the provided columns to ensure correct index access
        // Map input names to their positions once
        let mut name_to_pos: HashMap<&str, usize> = HashMap::with_capacity(cols.len());
        for (i, (name, _)) in cols.iter().enumerate() {
            name_to_pos.insert(name.as_ref(), i);
        }

        // Determine max write_order used by this query and gather (write_order, name) pairs from tokens
        let mut max_idx: usize = 0;
        let mut pairs: SmallVec<[(usize, &str); 20]> = SmallVec::new();
        for t in self.toks.iter() {
            if let Token::Ident((name, _t, w)) = t {
                let n = name.as_str();
                if let Some(_) = name_to_pos.get(n) {
                    let wi = *w as usize;
                    if wi > max_idx {
                        max_idx = wi;
                    }
                    pairs.push((wi, n));
                }
            }
        }

        // Create aligned vector sized to max write_order + 1 and fill only referenced slots
        let mut aligned: SmallVec<[MemoryBlock; 24]> = SmallVec::new();
        aligned.resize(max_idx.saturating_add(1), MemoryBlock::default());
        for (wi, n) in pairs.into_iter() {
            if let Some(&pos) = name_to_pos.get(n) {
                aligned[wi] = cols[pos].1.clone();
            }
        }

        // Execute on the aligned slice
        let res = self
            .query_transformer
            .execute_row(&aligned[..], comparison_results);
        Ok(res)
    }

    #[inline]
    pub fn reset_for_next_execution(&mut self) {
        self.pos = 0;
        self.query_transformer.reset_intermediate_results();
    }

    pub fn debug_info(&self) -> String {
        format!(
            "QueryParser {{ first_iteration: {}, pos: {}, used_columns: {:?}, transformers: {} }}",
            self.first_iteration,
            self.pos,
            self.used_column_indices,
            self.query_transformer.transformers.len()
        )
    }

    #[inline]
    pub fn is_optimized(&self) -> bool {
        !self.first_iteration && !self.used_column_indices.is_empty()
    }

    // ---------------- Parsing ----------------

    fn parse_or<'c>(
        &mut self,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(), String> {
        self.parse_and(cols)?;
        while let Some(Next::Or) = self.peek_logic() {
            self.set_last_comparison_next_op(Some(Next::Or));
            self.next_logic();
            self.parse_and(cols)?;
        }
        Ok(())
    }

    fn parse_and<'c>(
        &mut self,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(), String> {
        self.parse_comp_or_group(cols)?;
        while let Some(Next::And) = self.peek_logic() {
            self.set_last_comparison_next_op(Some(Next::And));
            self.next_logic();
            self.parse_comp_or_group(cols)?;
        }
        Ok(())
    }

    #[inline(always)]
    fn set_last_comparison_next_op(&mut self, next_op: Option<Next>) {
        if let Some(last_transformer) = self.query_transformer.transformers.last_mut() {
            last_transformer.next = next_op;
        }
    }

    fn parse_comp_or_group<'c>(
        &mut self,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(), String> {
        if let Some(Token::LPar) = self.toks.get(self.pos) {
            if self.is_boolean_group(self.pos) {
                self.pos += 1; // consume '('
                self.parse_or(cols)?;
                match self.next() {
                    Some(Token::RPar) => return Ok(()),
                    _ => return Err("expected closing parenthesis".to_string()),
                }
            }
        }
        self.parse_comparison(cols)
    }

    fn is_boolean_group(&self, start: usize) -> bool {
        let mut depth = 0usize;
        let mut i = start;
        let mut has_comp = false;
        let mut has_logic = false;

        const CMP_OPS: [&str; 6] = ["=", "!=", ">", ">=", "<", "<="];
        const LOGIC_KWS: [&str; 2] = ["AND", "OR"];
        const TEXT_CMPS: [&str; 3] = ["CONTAINS", "STARTSWITH", "ENDSWITH"];

        let toks_len = self.toks.len();
        while i < toks_len {
            match &self.toks[i] {
                Token::LPar => depth += 1,
                Token::RPar => {
                    if depth == 0 {
                        return false;
                    }
                    depth -= 1;
                    if depth == 0 {
                        return has_comp || has_logic;
                    }
                }
                Token::Op(o) if depth >= 1 => {
                    let s = o.as_str();
                    if CMP_OPS.contains(&s) {
                        has_comp = true;
                        if has_logic {
                            return true;
                        }
                    }
                }
                Token::Next(n) if depth >= 1 => {
                    let s = n.as_str();
                    if LOGIC_KWS.contains(&s) {
                        has_logic = true;
                        if has_comp {
                            return true;
                        }
                    }
                }
                Token::Ident((t, _, _)) if depth >= 1 => {
                    let s = t.as_str();
                    if LOGIC_KWS.contains(&s) {
                        has_logic = true;
                        if has_comp {
                            return true;
                        }
                    } else if TEXT_CMPS.contains(&s) {
                        has_comp = true;
                        if has_logic {
                            return true;
                        }
                    }
                }
                _ => {}
            }
            i += 1;
        }
        false
    }

    fn parse_comparison<'c>(
        &mut self,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(), String> {
        let (left_op, left_type) = self.parse_expr(cols)?;
        let op_token = self
            .next()
            .ok_or_else(|| "Unexpected end of token stream".to_string())?;

        let op_str: &str = match &op_token {
            Token::Op(o) => o.as_str(),
            Token::Ident((o, _, _)) => o.as_str(),
            _ => return Err(format!("expected comparison operator, got {:?}", op_token)),
        };

        let (right_op, right_type) = self.parse_expr(cols)?;

        let cmp = match op_str {
            "=" => ComparerOperation::Equals,
            "!=" => ComparerOperation::NotEquals,
            ">" => ComparerOperation::Greater,
            ">=" => ComparerOperation::GreaterOrEquals,
            "<" => ComparerOperation::Less,
            "<=" => ComparerOperation::LessOrEquals,
            "CONTAINS" => ComparerOperation::Contains,
            "STARTSWITH" => ComparerOperation::StartsWith,
            "ENDSWITH" => ComparerOperation::EndsWith,
            _ => return Err(format!("unknown comparison operator: {}", op_str)),
        };

        let comparison_type = match cmp {
            ComparerOperation::Contains
            | ComparerOperation::StartsWith
            | ComparerOperation::EndsWith => {
                if left_type == DbType::STRING || right_type == DbType::STRING {
                    DbType::STRING
                } else {
                    DbType::STRING
                }
            }
            _ => left_type,
        };

        self.query_transformer
            .add_comparison(comparison_type, left_op, right_op, cmp, None);
        Ok(())
    }

    fn parse_expr<'c>(
        &mut self,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(ComparisonOperand, DbType), String> {
        let (mut lhs_op, lhs_type) = self.parse_term(cols)?;
        while let Some(op_str) = self.peek_op(&["+", "-"]) {
            self.next(); // consume op
            let (rhs_op, _) = self.parse_term(cols)?;
            let math = if op_str == "+" {
                MathOperation::Add
            } else {
                MathOperation::Subtract
            };
            let idx =
                self.query_transformer
                    .add_math_operation(lhs_type.clone(), lhs_op, rhs_op, math);
            lhs_op = ComparisonOperand::Intermediate(idx);
        }
        Ok((lhs_op, lhs_type))
    }

    fn parse_term<'c>(
        &mut self,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(ComparisonOperand, DbType), String> {
        let (mut lhs_op, lhs_type) = self.parse_factor(cols)?;
        while let Some(op_str) = self.peek_op(&["*", "/"]) {
            self.next();
            let (rhs_op, _) = self.parse_factor(cols)?;
            let math = if op_str == "*" {
                MathOperation::Multiply
            } else {
                MathOperation::Divide
            };
            let idx =
                self.query_transformer
                    .add_math_operation(lhs_type.clone(), lhs_op, rhs_op, math);
            lhs_op = ComparisonOperand::Intermediate(idx);
        }
        Ok((lhs_op, lhs_type))
    }

    fn parse_factor<'c>(
        &mut self,
        cols: &'c SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(ComparisonOperand, DbType), String> {
        let token = self
            .next()
            .ok_or_else(|| "Unexpected end of token stream".to_string())?;

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
                let idx = write_order as usize;
                if idx >= cols.len() {
                    return Err(format!(
                        "Column index {} out of bounds (max: {})",
                        idx,
                        cols.len() - 1
                    ));
                }
                // Clone only during the first parse; no per-row cloning afterwards.
                let mb = cols[idx].1.clone();

                if self.first_iteration {
                    self.used_column_indices.push(idx);
                }

                Ok((ComparisonOperand::DirectWithIndex(mb, idx), db_type))
            }
            Token::LPar => {
                let (inner_op, inner_type) = self.parse_expr(cols)?;
                match self.next() {
                    Some(Token::RPar) => Ok((inner_op, inner_type)),
                    _ => Err("expected closing parenthesis after grouped expression".to_string()),
                }
            }
            t => Err(format!("unexpected factor: {:?}", t)),
        }
    }

    #[inline(always)]
    fn peek_logic(&self) -> Option<Next> {
        self.toks.get(self.pos).and_then(|token| match token {
            Token::Next(n) => match n.as_str() {
                "AND" => Some(Next::And),
                "OR" => Some(Next::Or),
                _ => None,
            },
            Token::Ident((t, _, _)) => match t.as_str() {
                "AND" => Some(Next::And),
                "OR" => Some(Next::Or),
                _ => None,
            },
            _ => None,
        })
    }

    #[inline(always)]
    fn next_logic(&mut self) {
        self.pos += 1;
    }

    #[inline(always)]
    fn peek_op(&self, allowed: &[&str]) -> Option<String> {
        match self.toks.get(self.pos) {
            Some(Token::Op(o)) if allowed.contains(&o.as_str()) => Some(o.clone()),
            _ => None,
        }
    }

    #[inline(always)]
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
    use smallvec::SmallVec;
    use std::borrow::Cow;
    use std::cell::UnsafeCell;
    use std::sync::LazyLock;

    use crate::core::db_type::DbType;
    use crate::core::processor::transformer::{ColumnTransformer, Next, TransformerProcessor};
    use crate::core::row::schema::SchemaField;
    use crate::core::tokenizer::query_parser::QueryParser;
    use crate::core::tokenizer::query_tokenizer::{tokenize, NumericValue, Token};
    use crate::memory_pool::{MEMORY_POOL, MemoryBlock};

    // Buffer structure for tests
    struct Buffer {
        pub transformers: SmallVec<[ColumnTransformer; 36]>,
        pub intermediate_results: SmallVec<[MemoryBlock; 20]>,
        pub bool_buffer: SmallVec<[(bool, Option<Next>); 20]>,
    }

    // Helper functions for memory blocks
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
            SchemaField::new("id".to_string(), DbType::I32, 4, 0, 0, false),
            SchemaField::new("age".to_string(), DbType::U8, 1, 0, 1, false),
            SchemaField::new("salary".to_string(), DbType::I32, 4, 0, 2, false),
            SchemaField::new("name".to_string(), DbType::STRING, 4 + 8, 0, 3, false),
            SchemaField::new("department".to_string(), DbType::STRING, 4 + 8, 0, 4, false),
            SchemaField::new("bank_balance".to_string(), DbType::F64, 8, 0, 5, false),
            SchemaField::new("credit_balance".to_string(), DbType::F32, 4, 0, 6, false),
            SchemaField::new("net_assets".to_string(), DbType::I64, 8, 0, 7, false),
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
            SchemaField::new(
                "last_purchase_amount".to_string(),
                DbType::F32,
                4,
                0,
                10,
                false
            ),
            SchemaField::new(
                "last_purchase_date".to_string(),
                DbType::STRING,
                12,
                0,
                11,
                false
            ), // Assuming STRING, could be DATETIME
            SchemaField::new(
                "last_purchase_location".to_string(),
                DbType::STRING,
                12,
                0,
                12,
                false
            ),
            SchemaField::new(
                "last_purchase_method".to_string(),
                DbType::STRING,
                12,
                0,
                13,
                false
            ),
            SchemaField::new(
                "last_purchase_category".to_string(),
                DbType::STRING,
                12,
                0,
                14,
                false
            ),
            SchemaField::new(
                "last_purchase_subcategory".to_string(),
                DbType::STRING,
                12,
                0,
                15,
                false
            ),
            SchemaField::new(
                "last_purchase_description".to_string(),
                DbType::STRING,
                12,
                0,
                16,
                false
            ),
            SchemaField::new(
                "last_purchase_status".to_string(),
                DbType::STRING,
                12,
                0,
                17,
                false
            ),
            SchemaField::new("last_purchase_id".to_string(), DbType::U64, 8, 0, 18, false),
            SchemaField::new(
                "last_purchase_notes".to_string(),
                DbType::STRING,
                12,
                0,
                19,
                false
            ),
            SchemaField::new("net_assets".to_string(), DbType::F64, 8, 0, 20, false),
            SchemaField::new("department".to_string(), DbType::STRING, 12, 0, 21, false),
            SchemaField::new("salary".to_string(), DbType::F32, 4, 0, 22, false),
        ]
    }

    fn setup_test_columns<'a>() -> SmallVec<[(Cow<'a, str>, MemoryBlock); 20]> {
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
        columns.push((
            Cow::Borrowed(NAME),
            create_memory_block_from_string("John Doe"),
        ));
        columns.push((
            Cow::Borrowed(DEPARTMENT),
            create_memory_block_from_string("Engineering"),
        ));
        columns.push((
            Cow::Borrowed(BANK_BALANCE),
            create_memory_block_from_f64(1000.43),
        ));
        columns.push((
            Cow::Borrowed(CREDIT_BALANCE),
            create_memory_block_from_f32(100.50),
        ));
        columns.push((
            Cow::Borrowed(NET_ASSETS),
            create_memory_block_from_i64(200000),
        ));
        columns.push((
            Cow::Borrowed(EARTH_POSITION),
            create_memory_block_from_i8(-50),
        ));
        columns.push((Cow::Borrowed(TEST_1), create_memory_block_from_u32(100)));
        columns.push((Cow::Borrowed(TEST_2), create_memory_block_from_u16(200)));
        columns.push((
            Cow::Borrowed(TEST_3),
            create_memory_block_from_i128(i128::MAX),
        ));
        columns.push((
            Cow::Borrowed(TEST_4),
            create_memory_block_from_u128(u128::MAX),
        ));
        columns.push((Cow::Borrowed(TEST_5), create_memory_block_from_i16(300)));
        columns.push((Cow::Borrowed(TEST_6), create_memory_block_from_u64(400)));

        columns
    }

    fn setup_test_columns_2<'a>() -> SmallVec<[(Cow<'a, str>, MemoryBlock); 20]> {
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

        columns.push((
            Cow::Borrowed(ID),
            create_memory_block_from_u128(42_433_943_354),
        ));
        columns.push((
            Cow::Borrowed(NAME),
            create_memory_block_from_string("John Doe"),
        ));
        columns.push((Cow::Borrowed(AGE), create_memory_block_from_u8(30)));
        columns.push((
            Cow::Borrowed(EMAIL),
            create_memory_block_from_string("john.doe@example.com"),
        ));
        columns.push((
            Cow::Borrowed(PHONE),
            create_memory_block_from_string("123-456-7890"),
        ));
        columns.push((Cow::Borrowed(IS_ACTIVE), create_memory_block_from_u8(1)));
        columns.push((
            Cow::Borrowed(BANK_BALANCE),
            create_memory_block_from_f64(1000.43),
        ));
        columns.push((Cow::Borrowed(MARRIED), create_memory_block_from_u8(0)));
        columns.push((
            Cow::Borrowed(BIRTH_DATE),
            create_memory_block_from_string("1990-01-01"),
        ));
        columns.push((
            Cow::Borrowed(LAST_LOGIN),
            create_memory_block_from_string("2023-10-01"),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE),
            create_memory_block_from_string("2023-09-30"),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_AMOUNT),
            create_memory_block_from_f32(15_540.75),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_DATE),
            create_memory_block_from_string("2023-09-30"),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_LOCATION),
            create_memory_block_from_string("Online"),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_METHOD),
            create_memory_block_from_string("Credit Card"),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_CATEGORY),
            create_memory_block_from_string("Electronics"),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_SUBCATEGORY),
            create_memory_block_from_string("Computers"),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_DESCRIPTION),
            create_memory_block_from_string("Gaming Laptop"),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_STATUS),
            create_memory_block_from_string("Completed"),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_ID),
            create_memory_block_from_u64(1001),
        ));
        columns.push((
            Cow::Borrowed(LAST_PURCHASE_NOTES),
            create_memory_block_from_string("No issues"),
        ));
        columns.push((
            Cow::Borrowed(NET_ASSETS),
            create_memory_block_from_f64(8_449_903_733.92),
        ));
        columns.push((
            Cow::Borrowed(DEPARTMENT),
            create_memory_block_from_string("Engineering"),
        ));
        columns.push((Cow::Borrowed(SALARY), create_memory_block_from_f32(4955.58)));

        columns
    }

    #[test]
    fn test_logical_precedence_or_then_and_parsed() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "id = 10 OR age > 20 AND salary = 50000";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_explicit_logical_grouping_and_first() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "(id = 42 AND age > 20) OR salary < 60000";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_explicit_logical_grouping_or_first_with_and() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "id = 10 OR (age > 20 AND salary = 50000)";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_complex_boolean_with_grouping() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "(name STARTSWITH 'J' OR department = 'Sales') AND age < 35";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_complex_arithmetic_nested_parentheses() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "( (age + 5) * 2 - 10 ) / 3 = 20";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_nested_boolean_groups() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "(id = 42 AND (age > 25 OR age < 20)) OR department = 'Engineering'";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_column_compared_to_itself() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "id = id";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_literal_on_left_arithmetic_on_right() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "10 = id - 32";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_multiple_conditions_on_same_string_column() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "name = 'John Doe' AND name != 'Jane Doe'";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_arithmetic_group_followed_by_and() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "(age + 10) > 30 AND id = 42";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_multiple_string_operations_with_and() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "department ENDSWITH 'ing' AND name CONTAINS 'oh'";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_boolean_groups_with_and_or_combination() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "(age * 2 = 60 OR age / 2 = 15) AND (salary > 40000 AND salary < 60000)";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_tokenize_keywords_as_idents_and_ops() {
        let tokens = tokenize(
            "age >= 30 AND name CONTAINS 'John' OR id STARTSWITH 'prefix'",
            &create_schema(),
        );
        assert_eq!(tokens.len(), 11);
        match &tokens[0] {
            Token::Ident(s) => assert_eq!(s.0, "age"),
            _ => panic!("Expected ident"),
        }
        match &tokens[1] {
            Token::Op(s) => assert_eq!(s, ">="),
            _ => panic!("Expected op"),
        }
        match &tokens[2] {
            Token::Number(n) => assert_eq!(
                *n,
                NumericValue::U8(30)
            ),
            _ => panic!("Expected num"),
        }
        match &tokens[3] {
            Token::Next(s) => assert_eq!(s, "AND"),
            _ => panic!("Expected AND"),
        }
        match &tokens[4] {
            Token::Ident(s) => assert_eq!(s.0, "name"),
            _ => panic!("Expected ident"),
        }
        match &tokens[5] {
            Token::Ident(s) => assert_eq!(s.0.to_uppercase(), "CONTAINS"),
            _ => panic!("Expected CONTAINS"),
        }
        match &tokens[6] {
            Token::StringLit(s) => assert_eq!(s, "John"),
            _ => panic!("Expected str lit"),
        }
        match &tokens[7] {
            Token::Next(s) => assert_eq!(s, "OR"),
            _ => panic!("Expected OR"),
        }
        match &tokens[8] {
            Token::Ident(s) => assert_eq!(s.0, "id"),
            _ => panic!("Expected ident"),
        }
        match &tokens[9] {
            Token::Ident(s) => assert_eq!(s.0.to_uppercase(), "STARTSWITH"),
            _ => panic!("Expected STARTSWITH"),
        }
        match &tokens[10] {
            Token::StringLit(s) => assert_eq!(s, "prefix"),
            _ => panic!("Expected str lit"),
        }
    }

    // --- 20 not true tests ---

    #[test]
    fn test_parse_simple_equals_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "id = 100";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_simple_string_equals_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "name = 'NonExistent'";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_simple_greater_than_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age > 30";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_simple_less_than_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "salary < 50000";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_simple_not_equals_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "id != 42";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_string_contains_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "name CONTAINS 'XYZ'";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_string_starts_with_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "department STARTSWITH 'Sci'";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_string_ends_with_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "name ENDSWITH 'Smith'";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_math_operation_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age + 10 = 50";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_multiple_math_operations_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age * 2 + 10 = 60";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_parentheses_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "(age + 10) * 2 = 70";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_logical_and_false_first_cond() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age > 35 AND salary = 50000";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_logical_and_false_second_cond() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age > 25 AND salary = 10000";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_logical_and_false_both_conds() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age > 35 AND salary = 10000";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_parse_logical_or_false_both_conds() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age < 20 OR salary = 10000";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    #[should_panic]
    fn test_column_vs_column_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "id = age";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let _ = parser
            .evaluate_row(columns, &mut buffer.bool_buffer)
            .unwrap();
    }

    #[test]
    fn test_parse_simple_equals() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "id = 42";
        let tokens = tokenize(query, &schema);
        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_parse_simple_not_equals() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "id != 50";
        let tokens = tokenize(query, &schema);
        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_parse_simple_greater_than() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age > 25";
        let tokens = tokenize(query, &schema);
        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_parse_simple_less_than() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age < 40";
        let tokens = tokenize(query, &schema);
        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_parse_greater_or_equal() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age >= 30";
        let tokens = tokenize(query, &schema);
        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_parse_less_or_equal() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age <= 30";
        let tokens = tokenize(query, &schema);
        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_parse_math_operation() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age + 10 = 40";
        let tokens = tokenize(query, &schema);
        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_parse_multiple_math_operations() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "age * 2 + 10 = 70";
        let tokens = tokenize(query, &schema);
        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_literal_on_left_arithmetic_on_right_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "10 = id - 30"; // 10 = (42 - 30) -> 10 = 12 -> False
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_complex_query_false_condition() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        // age > 25 (T) AND (salary = 10000 (F) OR name CONTAINS 'Jane' (F)) -> T AND (F OR F) -> T AND F -> F
        let query = "age > 25 AND (salary = 10000 OR name CONTAINS 'Jane')";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    #[should_panic]
    fn test_case_insensitive_keywords_with_false_outcome() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        // AgE > 35 (F) aNd SaLaRy = 50000 (T) -> F (should fail: AgE not found)
        let query = "AgE > 35 aNd SaLaRy = 50000";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let _ = parser
            .evaluate_row(columns, &mut buffer.bool_buffer)
            .unwrap();
    }

    #[test]
    fn test_query_with_extra_whitespace_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        // id = 100 (F) AND name CONTAINS 'John' (T) -> F
        let query = "id   =  100   AND  name   CONTAINS   'John'";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_float_equality_true() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "bank_balance = 1000.43";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_float_equality_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "bank_balance = 1000.44";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_float_greater_than_true() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "bank_balance > 1000.0";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_float_less_than_with_arithmetic_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "bank_balance - 0.43 < 1000.0"; // 1000.43 - 0.43 = 1000.0; 1000.0 < 1000.0 is false
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_float_comparison_with_logical_and_true() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "bank_balance >= 1000.43 AND age = 30"; // T AND T -> T
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_various_numeric_types_and_logical_ops_true() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "credit_balance > 100.0 AND net_assets < 200001 AND earth_position = -50";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_large_and_unsigned_types_with_grouping_true() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        let query = "(test_1 = 100 OR test_2 > 300) AND (test_3 = test_3 AND test_4 > 0)";
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_very_complex_query_1_true() {
        let schema = create_schema_2();
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns_2()));
        let columns = &*COLUMNS_2;

        let query = r#"
            (id = 42 AND name CONTAINS 'John') OR
            (age > 25 AND salary < 50000.0) OR
            (bank_balance >= 1000.43 AND net_assets > 800000) OR
            (department STARTSWITH 'Eng' AND email ENDSWITH 'example.com')
        "#;

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_ultra_complex_query_1_true() {
        let schema = create_schema_2();
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns_2()));
        let columns = &*COLUMNS_2;

        let query = r#"
            (id - 1 = 41 AND name CONTAINS 'John') OR
            (age + 10 > 15 AND salary - 1000 < 49000.0) OR
            (bank_balance >= 1000.43 AND net_assets + 40000 > 400000) OR
            (department STARTSWITH 'Eng' AND email ENDSWITH 'example.com')
        "#;

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_ultra_complex_query_no_parentheses_true() {
        let schema = create_schema_2();
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns_2()));
        let columns = &*COLUMNS_2;

        let query = r#"
            id - 1 = 41 AND name CONTAINS 'John' OR
            age + 10 > 15 AND salary - 1000 < 49000.0
        "#;

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };

        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));

        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_ultra_complex_mixed_int_and_string_true() {
        // Mix many numeric types and string predicates with nested groups; overall should be true
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;

        let query = r#"
            (
                (test_6 / 4 = 100 AND test_1 * 2 + 200 = 400) OR
                (id - 2 = 40 AND salary - 5000 = 45000)
            ) AND (
                department STARTSWITH 'Eng' AND name ENDSWITH 'Doe'
            )
        "#;

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_ultra_complex_long_chain_or_and_true() {
        // Long chain where AND binds tighter than OR; at least one OR group is true
        let schema = create_schema_2();
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns_2()));
        let columns = &*COLUMNS_2;

        let query = r#"
            id < 10 AND name CONTAINS 'Nope' OR
            age * 2 = 60 AND salary + 100 < 50000.0 OR
            department STARTSWITH 'Eng' AND email ENDSWITH 'example.com' OR
            net_assets > 1.0
        "#;

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_deeply_nested_parentheses_arithmetic_true() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;

        // ((((age * 2) + 10) - 40) / 1) = 30  -> with age=30
        let query = r#"((((age * 2) + 10) - 40) / 1) = 30"#;

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_negative_numbers_and_mixed_ops_true() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;

        let query = r#"
            (earth_position = -50 AND earth_position + 100 = 50) AND
            (name CONTAINS 'John' OR department ENDSWITH 'Sales')
        "#;

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_mixed_float_math_and_string_true() {
        // Use schema 2 floats; ensure division and comparisons work
        let schema = create_schema_2();
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns_2()));
        let columns = &*COLUMNS_2;

        let query = r#"
            (last_purchase_amount / 2.0 > 7000.0 AND email ENDSWITH 'example.com') OR
            (bank_balance = 1000.43 AND name STARTSWITH 'John')
        "#;

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_compound_false_overall() {
        // Many clauses but all OR groups evaluate to false
        let schema = create_schema_2();
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns_2()));
        let columns = &*COLUMNS_2;

        let query = r#"
            (name CONTAINS 'XYZ' AND department STARTSWITH 'Sal') OR
            (age < 10 AND salary > 60000.0) OR
            (bank_balance < 1000.0 AND net_assets < 1000.0)
        "#;

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), false);
    }

    #[test]
    fn test_id_arithmetic_both_sides_true() {
        // Check arithmetic on both sides of comparator
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;

        let query = r#"id - 2 = 2 * 20"#; // 40 = 40
        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_mix_without_parentheses_precedence_true() {
        // Verify AND precedence over OR on a mixed expression
        let schema = create_schema_2();
        static COLUMNS_2: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns_2()));
        let columns = &*COLUMNS_2;

        let query = r#"
            name CONTAINS 'Nope' OR age * 2 = 60 AND email ENDSWITH 'example.com'
        "#; // false OR (true AND true) -> true

        let tokens = tokenize(query, &schema);

        let mut buffer = Buffer {
            transformers: SmallVec::new(),
            intermediate_results: SmallVec::new(),
            bool_buffer: SmallVec::new(),
        };
        let transformer = UnsafeCell::new(TransformerProcessor::new(
            &mut buffer.transformers,
            &mut buffer.intermediate_results,
        ));
        let mut parser = QueryParser::new(&tokens, unsafe { &mut *transformer.get() });
        let res = parser.evaluate_row(columns, &mut buffer.bool_buffer);
        assert_eq!(res.unwrap(), true);
    }
}
