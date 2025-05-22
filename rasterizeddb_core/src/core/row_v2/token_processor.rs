use smallvec::SmallVec;

use crate::{core::db_type::DbType, memory_pool::MemoryBlock};
use super::{tokenizer::{numeric_to_mb, numeric_type_to_db_type, str_to_mb, NumericValue, Token}, transformer::{ComparerOperation, ComparisonOperand, MathOperation, Next, TransformerProcessor}};

pub struct TokenProcessor {
    tokens: SmallVec<[Token; 36]>,
    position: usize,
}

impl TokenProcessor {
    pub fn new(tokens: SmallVec<[Token; 36]>) -> Self {
        println!("TokenProcessor initialized with: {:?}", tokens); 
        Self {
            tokens,
            position: 0,
        }
    }

    pub fn process<'a>(&mut self, processor: &mut TransformerProcessor<'a>) -> Result<(), String> {
        // Process entire query
        self.process_expression(processor)?;
        
        // Verify we've consumed all tokens
        if self.position < self.tokens.len() {
            return Err(format!("Unexpected tokens at position {}", self.position));
        }
        
        Ok(())
    }

    fn process_expression<'a>(&mut self, processor: &mut TransformerProcessor<'a>) -> Result<(), String> {
        // Process first condition group
        self.process_condition_group(processor)?;
        
        // Process additional condition groups connected by OR
        while self.position < self.tokens.len() && self.peek() == Some(&Token::Ident("OR".to_string())) {
            self.consume(); // Consume OR token
            self.process_condition_group(processor)?;
        }
        
        Ok(())
    }

    fn process_condition_group<'a>(&mut self, processor: &mut TransformerProcessor<'a>) -> Result<(), String> {
        // Expect left parenthesis
        self.expect(Token::LPar)?;
        
        // Process first condition
        self.process_condition(processor, true)?;
        
        // Check if there's a second condition connected by AND
        if self.peek() == Some(&Token::Ident("AND".to_string())) {
            self.consume(); // Consume AND token
            self.process_condition(processor, false)?;
        }
        
        // Expect right parenthesis
        self.expect(Token::RPar)?;
        
        Ok(())
    }

    fn process_condition<'a>(&mut self, processor: &mut TransformerProcessor<'a>, is_first_in_group: bool) -> Result<(), String> {
        // Process left side of the comparison (could be a field or math expression)
        let (left_operand, left_type) = self.parse_expression()?;
        
        // Get operation type
        let operation = match self.consume() {
            Some(Token::Op(op)) => self.map_operation(&op)?,
            Some(Token::Ident(op)) => self.map_special_operation(&op)?,
            token => return Err(format!("Expected operation, got {:?}", token)),
        };
        
        // Process right side of the comparison (could be a literal value or math expression)
        let (right_operand, right_type) = self.parse_expression()?;
        
        // Ensure both sides have compatible types
        if !self.are_types_compatible(&left_type, &right_type) {
            return Err(format!("Type mismatch: {:?} and {:?}", left_type, right_type));
        }
        
        // Determine final DB type based on operation and operands
        let final_type = self.determine_result_type(&left_type, &right_type);
        
        // Determine next connector
        let next = self.determine_next_connector()?;
        
        // Add comparison to processor
        processor.add_comparison(
            final_type,
            left_operand,  // Left operand (field or result of math expression)
            right_operand, // Right operand (literal value or result of math expression)
            operation,
            next,
        );
        
        Ok(())
    }
    
    // Parse an expression (field name, literal value, or math expression)
    fn parse_expression(&mut self) -> Result<(MemoryBlock, DbType), String> {
        match self.peek() {
            Some(Token::Ident(_)) => {
                // It's a field reference
                let field_name = match self.consume() {
                    Some(Token::Ident(name)) => name,
                    _ => unreachable!(), // We already checked it's an Ident
                };
                
                // Check for math operations after the field name
                if self.is_math_operator(self.peek()) {
                    // Start a math expression with this field as the left operand
                    self.parse_math_expression(field_name)
                } else {
                    // It's just a simple field reference
                    // In a real implementation, you'd determine the field type from your schema
                    // For demonstration, we're using STRING as a default type
                    let field_value = create_string_memory_block(&field_name);
                    Ok((field_value, DbType::STRING))
                }
            },
            Some(Token::Number(num)) => {
                // It's a literal number value
                self.consume(); // Consume the number token
                let (value, db_type) = self.convert_numeric_value(num.clone())?;
                
                // Check for math operations after the number
                if self.is_math_operator(self.peek()) {
                    // Start a math expression with this number as the left operand
                    self.parse_math_expression_with_initial_value(value, db_type)
                } else {
                    // It's just a simple numeric literal
                    Ok((value, db_type))
                }
            },
            Some(Token::StringLit(s)) => {
                // It's a string literal
                self.consume(); // Consume the string token
                let value = create_string_memory_block(s);
                Ok((value, DbType::STRING))
            },
            Some(Token::LPar) => {
                // It's a parenthesized expression
                self.consume(); // Consume left parenthesis
                let result = self.parse_expression()?;
                self.expect(Token::RPar)?; // Expect closing parenthesis
                Ok(result)
            },
            token => Err(format!("Expected expression, got {:?}", token)),
        }
    }
    
    // Check if token is a math operator
    fn is_math_operator(&self, token: Option<&Token>) -> bool {
        match token {
            Some(Token::Op(op)) => match op.as_str() {
                "+" | "-" | "*" | "/" | "^" => true,
                _ => false,
            },
            _ => false,
        }
    }
    
    // Parse a math expression starting with a field name
    fn parse_math_expression(&mut self, left_field_name: String) -> Result<(MemoryBlock, DbType), String> {
        // In a real implementation, you'd determine the field type from your schema
        // For demonstration, we're using a numeric type as a default for math operations
        let left_operand = create_memory_block(1i32); // Placeholder for field value
        let left_type = DbType::I32; // Placeholder for field type
        
        self.parse_math_expression_with_initial_value(left_operand, left_type)
    }
    
    // Parse a math expression with an initial left value
    fn parse_math_expression_with_initial_value(
        &mut self, 
        left_operand: MemoryBlock, 
        left_type: DbType,
        processor: &mut TransformerProcessor
    ) -> Result<(MemoryBlock, DbType), String> {
        // Get the math operation
        let operation = match self.consume() {
            Some(Token::Op(op)) => self.map_math_operation(&op)?,
            token => return Err(format!("Expected math operation, got {:?}", token)),
        };
        
        // Parse the right operand of the math operation
        let (right_operand, right_type) = self.parse_expression()?;
        
        // Ensure both operands have compatible types for the math operation
        if !self.are_types_compatible_for_math(&left_type, &right_type) {
            return Err(format!("Type mismatch for math operation: {:?} and {:?}", left_type, right_type));
        }
        
        // Determine result type of the math operation
        let result_type = self.determine_math_result_type(&left_type, &right_type);
        
        // In a real implementation, you'd store intermediate results for math operations
        // For our example, we'll create a processor that performs the math operation
        let math_result_idx = processor.add_math_operation(
            result_type.clone(),
            left_operand,
            right_operand,
            operation,
        );
        
        // Return a reference to the intermediate result
        Ok((ComparisonOperand::Intermediate(math_result_idx), result_type))
    }
    
    // Map string to MathOperation
    fn map_math_operation(&self, op: &str) -> Result<MathOperation, String> {
        match op {
            "+" => Ok(MathOperation::Add),
            "-" => Ok(MathOperation::Subtract),
            "*" => Ok(MathOperation::Multiply),
            "/" => Ok(MathOperation::Divide),
            "^" => Ok(MathOperation::Exponent),
            _ => Err(format!("Unsupported math operation: {}", op)),
        }
    }
    
    // Check if two types are compatible for comparison
    fn are_types_compatible(&self, type1: &DbType, type2: &DbType) -> bool {
        match (type1, type2) {
            // String operations
            (DbType::STRING, DbType::STRING) => true,
            
            // Numeric operations - allow mixing numeric types
            (DbType::I8, _) | (_, DbType::I8) |
            (DbType::I16, _) | (_, DbType::I16) |
            (DbType::I32, _) | (_, DbType::I32) |
            (DbType::I64, _) | (_, DbType::I64) |
            (DbType::I128, _) | (_, DbType::I128) |
            (DbType::U8, _) | (_, DbType::U8) |
            (DbType::U16, _) | (_, DbType::U16) |
            (DbType::U32, _) | (_, DbType::U32) |
            (DbType::U64, _) | (_, DbType::U64) |
            (DbType::U128, _) | (_, DbType::U128) |
            (DbType::F32, _) | (_, DbType::F32) |
            (DbType::F64, _) | (_, DbType::F64) => true,
            
            _ => false,
        }
    }
    
    // Check if two types are compatible for math operations
    fn are_types_compatible_for_math(&self, type1: &DbType, type2: &DbType) -> bool {
        match (type1, type2) {
            // Only numeric types are compatible for math
            (DbType::I8, _) | (_, DbType::I8) |
            (DbType::I16, _) | (_, DbType::I16) |
            (DbType::I32, _) | (_, DbType::I32) |
            (DbType::I64, _) | (_, DbType::I64) |
            (DbType::I128, _) | (_, DbType::I128) |
            (DbType::U8, _) | (_, DbType::U8) |
            (DbType::U16, _) | (_, DbType::U16) |
            (DbType::U32, _) | (_, DbType::U32) |
            (DbType::U64, _) | (_, DbType::U64) |
            (DbType::U128, _) | (_, DbType::U128) |
            (DbType::F32, _) | (_, DbType::F32) |
            (DbType::F64, _) | (_, DbType::F64) => true,
            
            _ => false,
        }
    }
    
    // Determine the result type of a math operation
    fn determine_math_result_type(&self, type1: &DbType, type2: &DbType) -> DbType {
        match (type1, type2) {
            // Floating point has precedence
            (DbType::F64, _) | (_, DbType::F64) => DbType::F64,
            (DbType::F32, _) | (_, DbType::F32) => DbType::F32,
            
            // Then signed integers by size
            (DbType::I128, _) | (_, DbType::I128) => DbType::I128,
            (DbType::I64, _) | (_, DbType::I64) => DbType::I64,
            (DbType::I32, _) | (_, DbType::I32) => DbType::I32,
            (DbType::I16, _) | (_, DbType::I16) => DbType::I16,
            (DbType::I8, _) | (_, DbType::I8) => DbType::I8,
            
            // Then unsigned integers by size
            (DbType::U128, _) | (_, DbType::U128) => DbType::U128,
            (DbType::U64, _) | (_, DbType::U64) => DbType::U64,
            (DbType::U32, _) | (_, DbType::U32) => DbType::U32,
            (DbType::U16, _) | (_, DbType::U16) => DbType::U16,
            (DbType::U8, _) | (_, DbType::U8) => DbType::U8,
            
            // Default case (shouldn't happen with type checking)
            _ => DbType::I32,
        }
    }
    
    // Convert NumericValue to MemoryBlock and DbType
    fn convert_numeric_value(&self, value: NumericValue) -> Result<(MemoryBlock, DbType), String> {
        Ok((numeric_to_mb(&value), numeric_type_to_db_type(&value)))
    }
    
    // Determine the final result type for comparison operations
    fn determine_result_type(&self, type1: &DbType, type2: &DbType) -> DbType {
        // For comparison operations, we want to use the higher precision type
        self.determine_math_result_type(type1, type2)
    }
    
    fn map_operation(&self, op: &str) -> Result<ComparerOperation, String> {
        match op {
            "=" => Ok(ComparerOperation::Equals),
            "!=" => Ok(ComparerOperation::NotEquals),
            ">" => Ok(ComparerOperation::Greater),
            ">=" => Ok(ComparerOperation::GreaterOrEquals),
            "<" => Ok(ComparerOperation::Less),
            "<=" => Ok(ComparerOperation::LessOrEquals),
            _ => Err(format!("Unsupported operation: {}", op)),
        }
    }

    // Updated method to use is_first_in_group when determining next connector
    fn determine_next_connector(&self, is_first_in_group: bool) -> Result<Option<Next>, String> {
        // Check for right parenthesis followed by OR or end of input
        if self.peek() == Some(&Token::RPar) {
            if !is_first_in_group {
                // This is the second condition in a group (after AND)
                if self.peek_ahead(1) == Some(&Token::Ident("OR".to_string())) {
                    // If there's another group after this one
                    Ok(Some(Next::Or))
                } else if self.peek_ahead(1) == None {
                    // If this is the last group
                    Ok(None)
                } else {
                    Err(format!("Unexpected token after RPar: {:?}", self.peek_ahead(1)))
                }
            } else {
                // This is the only condition in a group (no AND)
                if self.peek_ahead(1) == Some(&Token::Ident("OR".to_string())) {
                    Ok(Some(Next::Or))
                } else if self.peek_ahead(1) == None {
                    Ok(None)
                } else {
                    Err(format!("Unexpected token after RPar: {:?}", self.peek_ahead(1)))
                }
            }
        } else if self.peek() == Some(&Token::Ident("AND".to_string())) {
            // This condition is followed by AND within the same group
            Ok(Some(Next::And))
        } else {
            Err(format!("Unexpected token: {:?}", self.peek()))
        }
    }

    fn map_special_operation(&self, op: &str) -> Result<ComparerOperation, String> {
        match op {
            "CONTAINS" => Ok(ComparerOperation::Contains),
            "STARTSWITH" => Ok(ComparerOperation::StartsWith),
            "ENDSWITH" => Ok(ComparerOperation::EndsWith),
            _ => Err(format!("Unsupported special operation: {}", op)),
        }
    }

    fn parse_value(&mut self) -> Result<(MemoryBlock, DbType), String> {
        match self.consume() {
            Some(Token::Number(num_val)) => {
                Ok((numeric_to_mb(&num_val), numeric_type_to_db_type(&num_val)))
            },
            Some(Token::StringLit(s)) => {
                let memory = str_to_mb(&s);
                Ok((memory, DbType::STRING))
            },
            token => Err(format!("Expected value, got {:?}", token)),
        }
    }

    fn peek(&self) -> Option<&Token> {
        if self.position < self.tokens.len() {
            Some(&self.tokens[self.position])
        } else {
            None
        }
    }

    fn peek_ahead(&self, offset: usize) -> Option<&Token> {
        if self.position + offset < self.tokens.len() {
            Some(&self.tokens[self.position + offset])
        } else {
            None
        }
    }

    fn consume(&mut self) -> Option<Token> {
        if self.position < self.tokens.len() {
            let token = self.tokens[self.position].clone();
            self.position += 1;
            Some(token)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: Token) -> Result<(), String> {
        match self.consume() {
            Some(token) if token == expected => Ok(()),
            Some(token) => Err(format!("Expected {:?}, got {:?}", expected, token)),
            None => Err("Unexpected end of input".to_string()),
        }
    }
}