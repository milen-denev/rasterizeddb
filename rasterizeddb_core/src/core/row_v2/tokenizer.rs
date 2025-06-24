use smallvec::SmallVec;
use std::arch::x86_64::*;
use std::collections::HashMap;

use crate::{core::{db_type::DbType}, memory_pool::MemoryBlock};
use crate::memory_pool::MEMORY_POOL;
use super::schema::SchemaField;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident(String),
    Number(NumericValue),
    StringLit(String),
    Op(String),
    Next(String),
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

static mut CHAR_CLASS_TABLE: [u8; 256] = [0; 256];
static mut INIT_DONE: bool = false;

const CHAR_WHITESPACE: u8 = 1;
const CHAR_DIGIT: u8 = 2;
const CHAR_ALPHA: u8 = 4;
const CHAR_UNDERSCORE: u8 = 8;
const CHAR_OPERATOR: u8 = 16;
const CHAR_QUOTE: u8 = 32;
const CHAR_PAREN: u8 = 64;

#[cold]
fn init_char_table() {
    unsafe {
        if INIT_DONE {
            return;
        }
        
        for i in 0..256 {
            let c = i as u8 as char;
            let mut class = 0u8;
            
            if c.is_whitespace() { class |= CHAR_WHITESPACE; }
            if c.is_ascii_digit() { class |= CHAR_DIGIT; }
            if c.is_alphabetic() { class |= CHAR_ALPHA; }
            if c == '_' { class |= CHAR_UNDERSCORE; }
            if matches!(c, '<' | '>' | '!' | '=' | '+' | '-' | '*' | '/') { class |= CHAR_OPERATOR; }
            if c == '\'' { class |= CHAR_QUOTE; }
            if matches!(c, '(' | ')') { class |= CHAR_PAREN; }
            
            CHAR_CLASS_TABLE[i] = class;
        }
        
        INIT_DONE = true;
    }
}

#[inline(always)]
fn get_char_class(c: u8) -> u8 {
    unsafe { CHAR_CLASS_TABLE[c as usize] }
}

#[inline(always)]
fn is_whitespace_fast(c: u8) -> bool {
    get_char_class(c) & CHAR_WHITESPACE != 0
}

#[inline(always)]
fn is_digit_fast(c: u8) -> bool {
    get_char_class(c) & CHAR_DIGIT != 0
}

#[inline(always)]
fn is_id_start_fast(c: u8) -> bool {
    get_char_class(c) & (CHAR_ALPHA | CHAR_UNDERSCORE) != 0
}

#[inline(always)]
fn is_id_part_fast(c: u8) -> bool {
    get_char_class(c) & (CHAR_ALPHA | CHAR_DIGIT | CHAR_UNDERSCORE) != 0
}

// SIMD-optimized whitespace skipping
#[target_feature(enable = "avx2")]
unsafe fn skip_whitespace_simd(bytes: &[u8], mut pos: usize) -> usize {
    if pos >= bytes.len() {
        return pos;
    }

    // Process 32 bytes at a time with AVX2
    while pos + 32 <= bytes.len() {
        let chunk = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(pos) as *const __m256i) };
        
        // Create masks for different whitespace characters
        let space_mask = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b' ' as i8));
        let tab_mask = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'\t' as i8));
        let newline_mask = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'\n' as i8));
        let cr_mask = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'\r' as i8));
        
        // Combine all whitespace masks
        let ws_mask = _mm256_or_si256(
            _mm256_or_si256(space_mask, tab_mask),
            _mm256_or_si256(newline_mask, cr_mask)
        );
        
        let mask_int = _mm256_movemask_epi8(ws_mask) as u32;
        
        if mask_int == 0xFFFFFFFF {
            // All 32 bytes are whitespace
            pos += 32;
        } else if mask_int == 0 {
            // No whitespace found
            break;
        } else {
            // Find first non-whitespace
            let first_non_ws = mask_int.trailing_ones() as usize;
            pos += first_non_ws;
            break;
        }
    }
    
    // Handle remaining bytes
    while pos < bytes.len() && is_whitespace_fast(bytes[pos]) {
        pos += 1;
    }
    
    pos
}

// SIMD-optimized digit scanning
#[target_feature(enable = "avx2")]
unsafe fn scan_digits_simd(bytes: &[u8], mut pos: usize) -> usize {
    if pos >= bytes.len() {
        return pos;
    }

    // Process 32 bytes at a time
    while pos + 32 <= bytes.len() {
        let chunk = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(pos) as *const __m256i) };
        
        // Check for digits (0-9)
        let ge_0 = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8(b'0' as i8 - 1));
        let le_9 = _mm256_cmpgt_epi8(_mm256_set1_epi8(b'9' as i8 + 1), chunk);
        let digit_mask = _mm256_and_si256(ge_0, le_9);
        
        let mask_int = _mm256_movemask_epi8(digit_mask) as u32;
        
        if mask_int == 0xFFFFFFFF {
            // All 32 bytes are digits
            pos += 32;
        } else if mask_int == 0 {
            // No digits found
            break;
        } else {
            // Find first non-digit
            let first_non_digit = mask_int.trailing_ones() as usize;
            pos += first_non_digit;
            break;
        }
    }
    
    // Handle remaining bytes
    while pos < bytes.len() && is_digit_fast(bytes[pos]) {
        pos += 1;
    }
    
    pos
}

// SIMD-optimized identifier scanning
#[target_feature(enable = "avx2")]
#[allow(non_snake_case)]
unsafe fn scan_identifier_simd(bytes: &[u8], mut pos: usize) -> usize {
    if pos >= bytes.len() {
        return pos;
    }

    // Process 32 bytes at a time
    while pos + 32 <= bytes.len() {
        let chunk = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(pos) as *const __m256i) };
        
        // Check for letters (A-Z, a-z)
        let ge_A = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8(b'A' as i8 - 1));
        let le_Z = _mm256_cmpgt_epi8(_mm256_set1_epi8(b'Z' as i8 + 1), chunk);
        let upper_mask = _mm256_and_si256(ge_A, le_Z);
        
        let ge_a = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8(b'a' as i8 - 1));
        let le_z = _mm256_cmpgt_epi8(_mm256_set1_epi8(b'z' as i8 + 1), chunk);
        let lower_mask = _mm256_and_si256(ge_a, le_z);
        
        // Check for digits
        let ge_0 = _mm256_cmpgt_epi8(chunk, _mm256_set1_epi8(b'0' as i8 - 1));
        let le_9 = _mm256_cmpgt_epi8(_mm256_set1_epi8(b'9' as i8 + 1), chunk);
        let digit_mask = _mm256_and_si256(ge_0, le_9);
        
        // Check for underscore
        let underscore_mask = _mm256_cmpeq_epi8(chunk, _mm256_set1_epi8(b'_' as i8));
        
        // Combine all valid identifier character masks
        let id_mask = _mm256_or_si256(
            _mm256_or_si256(upper_mask, lower_mask),
            _mm256_or_si256(digit_mask, underscore_mask)
        );
        
        let mask_int = _mm256_movemask_epi8(id_mask) as u32;
        
        if mask_int == 0xFFFFFFFF {
            // All 32 bytes are identifier characters
            pos += 32;
        } else if mask_int == 0 {
            // No identifier characters found
            break;
        } else {
            // Find first non-identifier character
            let first_non_id = mask_int.trailing_ones() as usize;
            pos += first_non_id;
            break;
        }
    }
    
    // Handle remaining bytes
    while pos < bytes.len() && is_id_part_fast(bytes[pos]) {
        pos += 1;
    }
    
    pos
}

// Pre-compute field lookup map for better cache locality
struct FieldLookup {
    map: HashMap<String, (String, DbType)>,
    logical_ops: HashMap<String, String>,
    string_ops: HashMap<String, String>,
}

impl FieldLookup {
    fn new(schema: &SmallVec<[SchemaField; 20]>) -> Self {
        let mut map = HashMap::with_capacity(schema.len());
        for field in schema.iter() {
            map.insert(field.name.clone(), (field.name.clone(), field.db_type.clone()));
        }
        
        let mut logical_ops = HashMap::new();
        logical_ops.insert("and".to_string(), "AND".to_string());
        logical_ops.insert("or".to_string(), "OR".to_string());
        
        let mut string_ops = HashMap::new();
        string_ops.insert("contains".to_string(), "CONTAINS".to_string());
        string_ops.insert("startswith".to_string(), "STARTSWITH".to_string());
        string_ops.insert("endswith".to_string(), "ENDSWITH".to_string());

        Self { map, logical_ops, string_ops }
    }
}

#[inline(always)]
pub fn tokenize(s: &str, schema: &SmallVec<[SchemaField; 20]>) -> SmallVec<[Token; 36]> {
    init_char_table();
    
    let mut out = SmallVec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    
    // Pre-compute lookup structures
    let field_lookup = FieldLookup::new(schema);
    
    // Track context for field association to maintain type consistency
    let mut current_field_context: Option<(String, DbType)> = None;
    let mut expression_type_context: Option<DbType> = None;
    let mut expression_stack: Vec<Option<DbType>> = Vec::new();
    let mut in_parentheses = 0;
    
    // Helper to determine if a minus sign should be considered part of a number
    #[inline(always)]
    fn is_negative_number_start(tokens: &[Token], bytes: &[u8], i: usize) -> bool {
        let next_is_digit = i + 1 < bytes.len() && is_digit_fast(bytes[i + 1]);
        if !next_is_digit {
            return false;
        }
        
        i == 0 || match tokens.last() {
            Some(Token::Op(_)) => true,
            Some(Token::LPar) => true,
            None => true,
            _ => false,
        }
    }
    
    // Process tokens with SIMD optimization
    while i < bytes.len() {
        // Skip whitespace using SIMD
        i = unsafe { skip_whitespace_simd(bytes, i) };
        if i >= bytes.len() {
            break;
        }
        
        let c = bytes[i];
        
        match c {
            b'\'' => {
                // Handle string literals
                let start = i + 1;
                let mut j = start;
                while j < bytes.len() && bytes[j] != b'\'' {
                    j += 1;
                }
                if j >= bytes.len() {
                    panic!("Unclosed string literal starting at position {}", i);
                }
                
                // SAFETY: We know this is valid UTF-8 since it came from a &str
                let lit = unsafe { 
                    std::str::from_utf8_unchecked(&bytes[start..j]).to_string()
                };
                out.push(Token::StringLit(lit));
                i = j + 1;
                
                expression_type_context = Some(DbType::STRING);
                current_field_context = None;
            },
            
            _ if is_digit_fast(c) => {
                // Handle numeric literals with SIMD optimization
                let start = i;
                i = unsafe { scan_digits_simd(bytes, i) };

                if i < bytes.len() && bytes[i] == b'.' {
                    i += 1;
                    i = unsafe { scan_digits_simd(bytes, i) };
                }
                
                let num_str = unsafe {
                    std::str::from_utf8_unchecked(&bytes[start..i])
                };
                
                let numeric_value = parse_numeric_value(
                    num_str, 
                    &current_field_context, 
                    &expression_type_context
                );
                
                let num_type = numeric_value_to_db_type(&numeric_value);
                if expression_type_context.is_none() {
                    expression_type_context = Some(num_type);
                }
                
                out.push(Token::Number(numeric_value));
            },
            
            b'-' if is_negative_number_start(&out, bytes, i) => {
                // Handle negative numbers
                let start = i;
                i += 1;
                i = unsafe { scan_digits_simd(bytes, i) };
                
                if i < bytes.len() && bytes[i] == b'.' {
                    i += 1;
                    i = unsafe { scan_digits_simd(bytes, i) };
                }
                
                let num_str = unsafe {
                    std::str::from_utf8_unchecked(&bytes[start..i])
                };
                
                let numeric_value = parse_numeric_value(
                    num_str, 
                    &current_field_context, 
                    &expression_type_context
                );
                
                let num_type = numeric_value_to_db_type(&numeric_value);
                if expression_type_context.is_none() {
                    expression_type_context = Some(num_type);
                }
                
                out.push(Token::Number(numeric_value));
            },
            
            _ if is_id_start_fast(c) => {
                // Handle identifiers with SIMD optimization
                let start = i;
                i = unsafe { scan_identifier_simd(bytes, i) };
                
                let id = unsafe {
                    std::str::from_utf8_unchecked(&bytes[start..i])
                };

                let id_lower: String = id.to_lowercase();
                
                // Check for special operators using pre-computed maps
                if let Some(normalized) = field_lookup.string_ops.get(&id_lower) {
                    out.push(Token::Ident(normalized.clone()));
                    continue;
                }

                if let Some(normalized) = field_lookup.logical_ops.get(&id_lower) {
                    out.push(Token::Next(normalized.clone()));  // Changed from Token::Ident to Token::Next
                    expression_type_context = None;
                    current_field_context = None;
                    continue;
                }
                
                // Check if this is a field name
                if let Some((canonical_name, field_type)) = field_lookup.map.get(&id_lower) {
                    current_field_context = Some((canonical_name.clone(), field_type.clone()));
                    expression_type_context = Some(field_type.clone());
                    
                    if in_parentheses > 0 {
                        if expression_stack.len() < in_parentheses {
                            expression_stack.push(Some(field_type.clone()));
                        } else {
                            expression_stack[in_parentheses - 1] = Some(field_type.clone());
                        }
                    }
                    
                    out.push(Token::Ident(canonical_name.clone()));
                } else {
                    // Validate field reference
                    if i < bytes.len() {
                        let mut next_i = i;
                        next_i = unsafe { skip_whitespace_simd(bytes, next_i) };
                        
                        if next_i < bytes.len() {
                            let next_char = bytes[next_i];
                            if matches!(next_char, b'=' | b'>' | b'<' | b'!') || 
                               (next_i + 8 <= bytes.len() && &bytes[next_i..next_i+8] == b"CONTAINS") ||
                               (next_i + 10 <= bytes.len() && &bytes[next_i..next_i+10] == b"STARTSWITH") ||
                               (next_i + 8 <= bytes.len() && &bytes[next_i..next_i+8] == b"ENDSWITH") {
                                panic!("Unknown column: '{}' is not defined in the schema", id);
                            }
                        }
                    }
                    
                    out.push(Token::Ident(id.to_string()));
                }
            },
            
            b'(' => {
                out.push(Token::LPar);
                i += 1;
                in_parentheses += 1;
                
                if expression_type_context.is_some() {
                    if in_parentheses > expression_stack.len() {
                        expression_stack.push(expression_type_context.clone());
                    } else {
                        expression_stack[in_parentheses - 1] = expression_type_context.clone();
                    }
                }
            },

            b')' => {
                out.push(Token::RPar);
                i += 1;
                
                if in_parentheses > 0 {
                    if in_parentheses <= expression_stack.len() && expression_stack[in_parentheses - 1].is_some() {
                        expression_type_context = expression_stack[in_parentheses - 1].clone();
                    }
                    in_parentheses -= 1;
                }
            },
            
            b'<'|b'>'|b'!'|b'=' => {
                let saved_context = expression_type_context.clone();
                
                let mut op_end = i + 1;
                if op_end < bytes.len() && bytes[op_end] == b'=' {
                    op_end += 1;
                }
                
                let op = unsafe {
                    std::str::from_utf8_unchecked(&bytes[i..op_end]).to_string()
                };
                out.push(Token::Op(op));
                i = op_end;
                
                expression_type_context = saved_context;
                current_field_context = None;
            },
            
            b'+'|b'-'|b'*'|b'/' => {
                let op_str = unsafe {
                    std::str::from_utf8_unchecked(&bytes[i..i+1]).to_string()
                };
                out.push(Token::Op(op_str));
                i += 1;
            },
            
            b',' => {
                i += 1;
                current_field_context = None;
                expression_type_context = None;
            },
            
            _ => panic!("unexpected character: '{}'", c as char),
        }
    }
    
    out
}

// Rest of the helper functions remain the same but with optimized implementations

#[inline(always)]
fn parse_numeric_value(
    value_str: &str, 
    field_context: &Option<(String, DbType)>,
    expression_context: &Option<DbType>,
) -> NumericValue {
    if let Some(db_type) = expression_context {
        return parse_by_db_type(value_str, db_type);
    }
    
    if let Some((_, db_type)) = field_context {
        return parse_by_db_type(value_str, db_type);
    }
    
    parse_default(value_str)
}

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

// Keep all the rest of the functions exactly the same
#[inline(always)]
fn is_numeric_type(db_type: &DbType) -> bool {
    matches!(db_type, 
        DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128 |
        DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128 |
        DbType::F32 | DbType::F64
    )
}

#[inline(always)]
pub fn promote_numeric_types(type1: DbType, type2: DbType) -> Option<DbType> {
    if !is_numeric_type(&type1) || !is_numeric_type(&type2) {
        return None;
    }
    
    if type1 == type2 {
        return Some(type1);
    }

    match (&type1, &type2) {
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
        
        (DbType::I32, _) if is_numeric_type(&type2) => return Some(type2),
        (_, DbType::I32) if is_numeric_type(&type1) => return Some(type1),
        
        _ => {}
    }

    let rank1 = get_type_rank(&type1);
    let rank2 = get_type_rank(&type2);

    let is_float1 = matches!(type1, DbType::F32 | DbType::F64);
    let is_float2 = matches!(type2, DbType::F32 | DbType::F64);
    
    if is_float1 || is_float2 {
        if rank1 >= rank2 { Some(type1) } else { Some(type2) }
    } else {
        let is_signed1 = matches!(type1, DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128);
        let is_signed2 = matches!(type2, DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128);
        
        if is_signed1 && is_signed2 {
            if rank1 >= rank2 { Some(type1) } else { Some(type2) }
        } else if !is_signed1 && !is_signed2 {
            if rank1 >= rank2 { Some(type1) } else { Some(type2) }
        } else {
            if (rank1 as i32 - rank2 as i32).abs() > 1 {
                if rank1 > rank2 { Some(type1) } else { Some(type2) }
            } else {
                if is_signed1 { Some(type1) } else { Some(type2) }
            }
        }
    }
}

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
        DbType::STRING => 2,
        DbType::DATETIME => 1, 
        DbType::CHAR | DbType::NULL => 0,
        _ => unreachable!("unknown type for promotion"),
    }
}

#[inline(always)]
pub fn promote_types(type1: DbType, type2: DbType) -> DbType {
    if type1 == DbType::STRING || type2 == DbType::STRING {
        return DbType::STRING;
    }
    
    if type1 == type2 {
        return type1;
    }
    
    if type1 == DbType::I32 && is_numeric_type(&type2) {
        return type2;
    }
    if type2 == DbType::I32 && is_numeric_type(&type1) {
        return type1;
    }
    
    if let Some(result_type) = promote_numeric_types(type1.clone(), type2.clone()) {
        return result_type;
    }
    
    panic!("Cannot promote types: {:?} and {:?} for comparison operation", type1, type2);
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

#[inline(always)]
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_tokenize_identifiers() {
        let mut schema = SmallVec::new();
        schema.push(SchemaField {
            name: "field2".to_string(),
            db_type: DbType::F64,
            is_deleted: false,
            size: 0,
            offset: 0,
            write_order: 0,
            is_unique: false,
        });
        schema.push(SchemaField {
            name: "field2".to_string(),
            db_type: DbType::F64,
            is_deleted: false,
            size: 0,
            offset: 0,
            write_order: 0,
            is_unique: false,
        });
        let tokens = tokenize("field1 + field2", &schema);
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], Token::Ident("field1".to_string()));
        assert_eq!(tokens[1], Token::Op("+".to_string()));
        assert_eq!(tokens[2], Token::Ident("field2".to_string()));
    }

    #[test]
    fn test_tokenize_numbers() {
        let schema = SmallVec::new();
        let tokens = tokenize("123 + 456", &schema);
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], Token::Number(NumericValue::I32(123)));
        assert_eq!(tokens[1], Token::Op("+".to_string()));
        assert_eq!(tokens[2], Token::Number(NumericValue::I32(456)));
    }

    #[test]
    fn test_large_query() {
        let schema = create_schema_2();
        let query = r##"
            (id = 42 AND name CONTAINS 'John') OR
            (age > 25 AND salary < 50000.0) OR
            (bank_balance >= 1000.43 AND net_assets > 800000) OR
            (department STARTSWITH 'Eng' AND email ENDSWITH 'example.com')
        "##;
        let tokens = tokenize(query, &schema);
        assert_eq!(tokens.len(), 39);
        assert_eq!(tokens[0], Token::LPar);
        assert_eq!(tokens[1], Token::Ident("id".to_string()));
        assert_eq!(tokens[2], Token::Op("=".to_string()));
        assert_eq!(tokens[3], Token::Number(NumericValue::U128(42)));
        assert_eq!(tokens[4], Token::Next("AND".to_string()));
        assert_eq!(tokens[5], Token::Ident("name".to_string()));
        assert_eq!(tokens[6], Token::Ident("CONTAINS".to_string()));
        assert_eq!(tokens[7], Token::StringLit("John".to_string()));
        assert_eq!(tokens[8], Token::RPar);
        assert_eq!(tokens[9], Token::Next("OR".to_string()));
        assert_eq!(tokens[10], Token::LPar);
        assert_eq!(tokens[11], Token::Ident("age".to_string()));
        assert_eq!(tokens[12], Token::Op(">".to_string()));
        assert_eq!(tokens[13], Token::Number(NumericValue::U8(25)));
        assert_eq!(tokens[14], Token::Next("AND".to_string()));
        assert_eq!(tokens[15], Token::Ident("salary".to_string()));
        assert_eq!(tokens[16], Token::Op("<".to_string()));
        assert_eq!(tokens[17], Token::Number(NumericValue::F32(50000.0)));
        assert_eq!(tokens[18], Token::RPar);
        assert_eq!(tokens[19], Token::Next("OR".to_string()));
        assert_eq!(tokens[20], Token::LPar);
        assert_eq!(tokens[21], Token::Ident("bank_balance".to_string()));
        assert_eq!(tokens[22], Token::Op(">=".to_string()));
        assert_eq!(tokens[23], Token::Number(NumericValue::F64(1000.43)));
        assert_eq!(tokens[24], Token::Next("AND".to_string()));
        assert_eq!(tokens[25], Token::Ident("net_assets".to_string()));
        assert_eq!(tokens[26], Token::Op(">".to_string()));
        assert_eq!(tokens[27], Token::Number(NumericValue::F64(800000.0)));
        assert_eq!(tokens[28], Token::RPar);
        assert_eq!(tokens[29], Token::Next("OR".to_string()));
        assert_eq!(tokens[30], Token::LPar);
        assert_eq!(tokens[31], Token::Ident("department".to_string()));
        assert_eq!(tokens[32], Token::Ident("STARTSWITH".to_string()));
        assert_eq!(tokens[33], Token::StringLit("Eng".to_string()));
        assert_eq!(tokens[34], Token::Next("AND".to_string()));
        assert_eq!(tokens[35], Token::Ident("email".to_string()));
        assert_eq!(tokens[36], Token::Ident("ENDSWITH".to_string()));
        assert_eq!(tokens[37], Token::StringLit("example.com".to_string()));
        assert_eq!(tokens[38], Token::RPar);
    }
}