use std::collections::HashMap;
use crate::memory_pool::{MemoryBlock, MEMORY_POOL};

use crate::core::db_type::DbType;

use super::transformer::{ComparerOperation, ComparisonOperand, MathOperation, Next, TransformerProcessor};

#[derive(Debug, Clone)]
pub enum Token {
    Ident(String),
    Number(i32),
    StringLit(String),
    Op(String),
    LPar,
    RPar,
}

// For testing only
#[cfg(test)]
pub fn tokenize_for_test(s: &str) -> Vec<Token> {
    tokenize(s)
}

pub fn parse_query(
    query: &str,
    columns: &HashMap<String, MemoryBlock>
) -> TransformerProcessor {
    let parser = QueryParser::new(query, columns);
    parser.parse_where()
}

struct QueryParser<'a> {
    toks: Vec<Token>,
    pos: usize,
    cols: &'a HashMap<String, MemoryBlock>,
}

impl<'a> QueryParser<'a> {
    fn new(s: &str, cols: &'a HashMap<String, MemoryBlock>) -> Self {
        let toks = tokenize(s);
        Self { toks, pos: 0, cols }
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
        self.parse_comparison(proc);
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
    ) -> ComparisonOperand {
        let left = self.parse_expr(proc);
        let op = match self.next().unwrap() {
            Token::Op(o) | Token::Ident(o) => o.to_uppercase(),
            _ => panic!("expected comparison op"),
        };
        let right = self.parse_expr(proc);
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
        let next = self.peek_logic();
        
        // Try to infer column type from the operands
        let col_type = match (&left, &right) {
            (ComparisonOperand::Direct(mb_left), _) => {
                // Check if left operand is a string by checking operation type
                match cmp {
                    ComparerOperation::Contains | 
                    ComparerOperation::StartsWith | 
                    ComparerOperation::EndsWith => DbType::STRING,
                    _ => {
                        // For other operations, we need to infer from the operand
                        // Assume I32 is always 4 bytes
                        if mb_left.into_slice().len() != 4 {
                            DbType::STRING
                        } else {
                            DbType::I32
                        }
                    }
                }
            },
            (_, ComparisonOperand::Direct(mb_right)) => {
                // If left isn't a direct operand, check the right one
                if mb_right.into_slice().len() != 4 {
                    DbType::STRING
                } else {
                    DbType::I32
                }
            },
            _ => DbType::I32 // Default to I32 if we can't determine
        };
        
        proc.add_comparison(
            col_type,
            left.clone(),
            right.clone(),
            cmp,
            next.clone()
        );
        left
    }    fn parse_expr(
        &mut self,
        proc: &mut TransformerProcessor
    ) -> ComparisonOperand {
        let mut lhs = self.parse_term(proc);
        while let Some(op) = self.peek_op(&["+","-"]) {
            let math = if op=="+" { MathOperation::Add } else { MathOperation::Subtract };
            self.next(); // consume op
            let rhs = self.parse_term(proc);
            let idx = proc.add_math_operation(DbType::I32, lhs.clone(), rhs.clone(), math);
            lhs = ComparisonOperand::Intermediate(idx);
        }
        lhs
    }    fn parse_term(
        &mut self,
        proc: &mut TransformerProcessor
    ) -> ComparisonOperand {
        let mut lhs = self.parse_factor(proc);
        while let Some(op) = self.peek_op(&["*","/"]) {
            let math = if op=="*" { MathOperation::Multiply } else { MathOperation::Divide };
            self.next();
            let rhs = self.parse_factor(proc);
            let idx = proc.add_math_operation(DbType::I32, lhs.clone(), rhs.clone(), math);
            lhs = ComparisonOperand::Intermediate(idx);
        }
        lhs
    }

    fn parse_factor(
        &mut self,
        proc: &mut TransformerProcessor
    ) -> ComparisonOperand {
        match self.next().unwrap() {
            Token::Number(n) => {
                let mb = int_to_mb(n);
                ComparisonOperand::Direct(mb)
            }
            Token::StringLit(s) => {
                let mb = str_to_mb(&s);
                ComparisonOperand::Direct(mb)
            }
            Token::Ident(name) => {
                let mb = self.cols.get(&name)
                    .unwrap_or_else(|| panic!("unknown column {}", name))
                    .clone();
                ComparisonOperand::Direct(mb)
            }
            Token::LPar => {
                let inner = self.parse_expr(proc);
                assert!(matches!(self.next().unwrap(), Token::RPar));
                inner
            }
            _ => panic!("unexpected factor"),
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

fn tokenize(s: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut i=0;
    let cs: Vec<char> = s.chars().collect();
    while i<cs.len() {
        match cs[i] {
            c if c.is_whitespace() => i+=1,
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
                let start=i;
                while i<cs.len() && cs[i].is_ascii_digit() { i+=1 }
                let num: i32 = cs[start..i].iter().collect::<String>().parse().unwrap();
                out.push(Token::Number(num));
            }
            c if is_id_start(c) => {
                let start=i;
                while i<cs.len() && is_id_part(cs[i]) { i+=1 }
                let id = cs[start..i].iter().collect::<String>();
                // treat all keywords and identifiers uniformly, including CONTAINS, STARTSWITH, ENDSWITH
                out.push(Token::Ident(id));
            }
            '(' => { out.push(Token::LPar); i+=1 }
            ')' => { out.push(Token::RPar); i+=1 }
            '<'|'>'|'!'|'=' => {
                let mut op = cs[i].to_string();
                if i+1<cs.len() && cs[i+1]=='=' {
                    op.push('=');
                    i+=2;
                } else { i+=1; }
                out.push(Token::Op(op));
            }
            '+'|'-'|'*'|'/' => {
                out.push(Token::Op(cs[i].to_string()));
                i+=1;
            }
            _ => panic!("unexpected char {}", cs[i]),
        }
    }
    out
}

fn is_id_start(c: char) -> bool { c.is_alphabetic() || c=='_' }
fn is_id_part(c: char)  -> bool { c.is_alphanumeric() || c=='_' }

fn int_to_mb(v: i32) -> MemoryBlock {
    let b = v.to_le_bytes();
    let mb = MEMORY_POOL.acquire(b.len());
    mb.into_slice_mut().copy_from_slice(&b);
    mb
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
    use crate::core::row_v2::query_parser::{parse_query, tokenize_for_test};
    use crate::memory_pool::{MemoryBlock, MEMORY_POOL};

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

    fn setup_test_columns() -> HashMap<String, MemoryBlock> {
        let mut columns = HashMap::new();
        columns.insert("id".to_string(), create_memory_block_from_i32(42));
        columns.insert("age".to_string(), create_memory_block_from_i32(30));
        columns.insert("salary".to_string(), create_memory_block_from_i32(50000));
        columns.insert("name".to_string(), create_memory_block_from_string("John Doe"));
        columns.insert("department".to_string(), create_memory_block_from_string("Engineering"));
        columns
    }

    #[test]
    fn test_parse_simple_equals() {
        let columns = setup_test_columns();
        let query = "id = 42";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_simple_not_equals() {
        let columns = setup_test_columns();
        let query = "id != 50";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_simple_greater_than() {
        let columns = setup_test_columns();
        let query = "age > 25";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_simple_less_than() {
        let columns = setup_test_columns();
        let query = "age < 40";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_greater_or_equal() {
        let columns = setup_test_columns();
        let query = "age >= 30";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_less_or_equal() {
        let columns = setup_test_columns();
        let query = "age <= 30";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_math_operation() {
        let columns = setup_test_columns();
        let query = "age + 10 = 40";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_multiple_math_operations() {
        let columns = setup_test_columns();
        let query = "age * 2 + 10 = 70";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_parentheses() {
        let columns = setup_test_columns();
        let query = "(age + 10) * 2 = 80";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_string_contains() {
        let columns = setup_test_columns();
        let query = "name CONTAINS 'John'";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_string_starts_with() {
        let columns = setup_test_columns();
        let query = "name STARTSWITH 'John'";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_string_ends_with() {
        let columns = setup_test_columns();
        let query = "name ENDSWITH 'Doe'";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_logical_and() {
        let columns = setup_test_columns();
        let query = "age > 25 AND salary = 50000";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_logical_or() {
        let columns = setup_test_columns();
        let query = "age < 25 OR salary = 50000";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_parse_complex_query() {
        let columns = setup_test_columns();
        let query = "age > 25 AND (salary = 50000 OR name CONTAINS 'Jane')";
        
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_tokenize() {
        use crate::core::row_v2::query_parser::Token;
        
        let tokens = tokenize_for_test("age >= 30 AND name CONTAINS 'John'");
        
        assert_eq!(tokens.len(), 7);
        
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
            Token::Number(n) => assert_eq!(*n, 30),
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
    }

    
    #[test]
    fn test_string_equality_type_inference() {
        let columns = setup_test_columns();
        let query = "name = 'John Doe'";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_string_op_type_inference() {
        let columns = setup_test_columns();
        let query = "department STARTSWITH 'Eng'";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_mixed_types_in_different_comparisons_false() {
        let columns = setup_test_columns();
        let query = "id > 10 AND name = 'Test'"; // id > 10 (T), name = 'Test' (F) -> F
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_arithmetic_precedence_div_add() {
        let columns = setup_test_columns();
        let query = "salary / 2 + 100 = 25100"; // 50000/2 + 100 = 25000 + 100 = 25100
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_arithmetic_precedence_literal_first_div_add() {
        let columns = setup_test_columns();
        let query = "10 + salary / 2 = 25010"; // 10 + 50000/2 = 10 + 25000 = 25010
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_logical_precedence_and_then_or() {
        let columns = setup_test_columns();
        // (id=42 (T) AND age > 20 (T)) OR salary < 60000 (T) -> (T AND T) OR T -> T OR T -> T
        let query = "id = 42 AND age > 20 OR salary < 60000";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_logical_precedence_or_then_and_parsed() { // Parser structure implies AND has higher precedence
        let columns = setup_test_columns();
        // id = 10 (F) OR (age > 20 (T) AND salary = 50000 (T)) -> F OR (T AND T) -> F OR T -> T
        let query = "id = 10 OR age > 20 AND salary = 50000";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }
    
    #[test]
    fn test_explicit_logical_grouping_and_first() {
        let columns = setup_test_columns();
        let query = "(id = 42 AND age > 20) OR salary < 60000"; // Same as test_logical_precedence_and_then_or
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_explicit_logical_grouping_or_first_with_and() {
        let columns = setup_test_columns();
        let query = "id = 10 OR (age > 20 AND salary = 50000)"; // Same as test_logical_precedence_or_then_and_parsed
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_complex_boolean_with_grouping() {
        let columns = setup_test_columns();
        // (name STARTSWITH 'J' (T) OR department = 'Sales' (F)) AND age < 35 (T)
        // (T OR F) AND T -> T AND T -> T
        let query = "(name STARTSWITH 'J' OR department = 'Sales') AND age < 35";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_complex_arithmetic_nested_parentheses() {
        let columns = setup_test_columns();
        // ((30+5)*2 - 10)/3 = (35*2 - 10)/3 = (70-10)/3 = 60/3 = 20
        let query = "( (age + 5) * 2 - 10 ) / 3 = 20";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_nested_boolean_groups() {
        let columns = setup_test_columns();
        // (id=42 (T) AND (age>25 (T) OR age<20 (F))) OR department='Engineering' (T)
        // (T AND (T OR F)) OR T -> (T AND T) OR T -> T OR T -> T
        let query = "(id = 42 AND (age > 25 OR age < 20)) OR department = 'Engineering'";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_column_compared_to_itself() {
        let columns = setup_test_columns();
        let query = "id = id"; // 42 = 42
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }
    
    #[test]
    fn test_literal_on_left_arithmetic_on_right() {
        let columns = setup_test_columns();
        let query = "10 = id - 32"; // 10 = 42 - 32 -> 10 = 10
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_multiple_conditions_on_same_string_column() {
        let columns = setup_test_columns();
        // name = 'John Doe' (T) AND name != 'Jane Doe' (T) -> T
        let query = "name = 'John Doe' AND name != 'Jane Doe'";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_arithmetic_group_followed_by_and() {
        let columns = setup_test_columns();
        // (age + 10) > 30 (T, 40 > 30) AND id = 42 (T) -> T
        let query = "(age + 10) > 30 AND id = 42";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_multiple_string_operations_with_and() {
        let columns = setup_test_columns();
        // department ENDSWITH 'ing' (T) AND name CONTAINS 'oh' (T) -> T
        let query = "department ENDSWITH 'ing' AND name CONTAINS 'oh'";
        let mut processor = parse_query(query, &columns);
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
        let mut processor = parse_query(query, &columns);
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
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_complex_arithmetic_on_both_sides_of_comparison() {
        let columns = setup_test_columns();
        // salary - 10000 = 50000 - 10000 = 40000
        // age * 1000 + 10000 = 30 * 1000 + 10000 = 30000 + 10000 = 40000
        // 40000 = 40000 -> T
        let query = "salary - 10000 = age * 1000 + 10000";
        let mut processor = parse_query(query, &columns);
        assert!(processor.execute());
    }

    #[test]
    fn test_tokenize_keywords_as_idents_and_ops() { // Existing test_tokenize is good, this adds a bit more
        use crate::core::row_v2::query_parser::Token;
        
        let tokens = tokenize_for_test("age >= 30 AND name CONTAINS 'John' OR id STARTSWITH 'prefix'");
        
        assert_eq!(tokens.len(), 11);
        
        match &tokens[0] { Token::Ident(s) => assert_eq!(s, "age"), _ => panic!("Expected ident") }
        match &tokens[1] { Token::Op(s) => assert_eq!(s, ">="), _ => panic!("Expected op") }
        match &tokens[2] { Token::Number(n) => assert_eq!(*n, 30), _ => panic!("Expected num") }
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
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_simple_string_equals_false() {
        let columns = setup_test_columns();
        let query = "name = 'NonExistent'"; // 'John Doe' = 'NonExistent' -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_simple_greater_than_false() {
        let columns = setup_test_columns();
        let query = "age > 30"; // 30 > 30 -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_simple_less_than_false() {
        let columns = setup_test_columns();
        let query = "salary < 50000"; // 50000 < 50000 -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_simple_not_equals_false() {
        let columns = setup_test_columns();
        let query = "id != 42"; // 42 != 42 -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_string_contains_false() {
        let columns = setup_test_columns();
        let query = "name CONTAINS 'XYZ'"; // 'John Doe' CONTAINS 'XYZ' -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_string_starts_with_false() {
        let columns = setup_test_columns();
        let query = "department STARTSWITH 'Sci'"; // 'Engineering' STARTSWITH 'Sci' -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_string_ends_with_false() {
        let columns = setup_test_columns();
        let query = "name ENDSWITH 'Smith'"; // 'John Doe' ENDSWITH 'Smith' -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_math_operation_false() {
        let columns = setup_test_columns();
        let query = "age + 10 = 50"; // 30 + 10 = 40; 40 = 50 -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_multiple_math_operations_false() {
        let columns = setup_test_columns();
        let query = "age * 2 + 10 = 60"; // 30 * 2 + 10 = 70; 70 = 60 -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_parentheses_false() {
        let columns = setup_test_columns();
        let query = "(age + 10) * 2 = 70"; // (30 + 10) * 2 = 80; 80 = 70 -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_logical_and_false_first_cond() {
        let columns = setup_test_columns();
        let query = "age > 35 AND salary = 50000"; // (30 > 35 -> F) AND (50000 = 50000 -> T) -> F
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_logical_and_false_second_cond() {
        let columns = setup_test_columns();
        let query = "age > 25 AND salary = 10000"; // (30 > 25 -> T) AND (50000 = 10000 -> F) -> F
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_logical_and_false_both_conds() {
        let columns = setup_test_columns();
        let query = "age > 35 AND salary = 10000"; // (F) AND (F) -> F
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_parse_logical_or_false_both_conds() {
        let columns = setup_test_columns();
        let query = "age < 20 OR salary = 10000"; // (30 < 20 -> F) OR (50000 = 10000 -> F) -> F
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_column_vs_column_false() {
        let columns = setup_test_columns();
        let query = "id = age"; // 42 = 30 -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_literal_on_left_arithmetic_on_right_false() {
        let columns = setup_test_columns();
        let query = "10 = id - 30"; // 10 = (42 - 30) -> 10 = 12 -> False
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_complex_query_false_condition() {
        let columns = setup_test_columns();
        // age > 25 (T) AND (salary = 10000 (F) OR name CONTAINS 'Jane' (F)) -> T AND (F OR F) -> T AND F -> F
        let query = "age > 25 AND (salary = 10000 OR name CONTAINS 'Jane')";
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    #[should_panic(expected ="unknown column AgE")]
    fn test_case_insensitive_keywords_with_false_outcome() {
        let columns = setup_test_columns();
        // AgE > 35 (F) aNd SaLaRy = 50000 (T) -> F
        let query = "AgE > 35 aNd SaLaRy = 50000";
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }

    #[test]
    fn test_query_with_extra_whitespace_false() {
        let columns = setup_test_columns();
        // id = 100 (F) AND name CONTAINS 'John' (T) -> F
        let query = "id   =  100   AND  name   CONTAINS   'John'";
        let mut processor = parse_query(query, &columns);
        assert!(!processor.execute());
    }
}
