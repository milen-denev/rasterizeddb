use smallvec::SmallVec;
use std::{arch::x86_64::*, borrow::Cow};

use crate::core::row::schema::SchemaField;
use crate::memory_pool::MEMORY_POOL;
use crate::{core::db_type::DbType, memory_pool::MemoryBlock};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Ident((String, DbType, u64)),
    /// `doc->>'k'` — a value inside a document column, not a column of its own.
    ///
    /// Carries the *document's* `write_order`, because that is the column a row
    /// has to fetch; the steps are walked against its bytes per row. Keeping it
    /// a distinct token rather than a synthetic `Ident` is deliberate: an
    /// `Ident` is a promise that a physical column of that name and type
    /// exists, and the filter index believes that promise.
    JsonPath {
        write_order: u64,
        steps: Vec<crate::core::jsonb::Step>,
        as_text: bool,
    },
    Number(NumericValue),
    StringLit(String),
    Op(String),
    Next(String),
    LPar,
    RPar,
    /// `::bigint` applied to the operand before it.
    ///
    /// Kept as a token rather than resolved where it is read, because what it
    /// applies to may be a parenthesised group whose extent the parser knows
    /// and the tokenizer does not.
    Cast(DbType),
    /// An operand the expression evaluator computes per row — a `CASE`, a
    /// `||` chain, a `%` — which the rewrite lifted out of the clause whole.
    /// See `predicate_rewrite::lift_evaluated_operands`.
    ///
    /// Neither a column nor a literal, so every extractor that plans from
    /// tokens treats the comparison holding it as opaque.
    Evaluated(std::sync::Arc<crate::core::processor::transformer::EvaluatedOperand>),
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

const CHAR_WHITESPACE: u8 = 1;
const CHAR_DIGIT: u8 = 2;
const CHAR_ALPHA: u8 = 4;
const CHAR_UNDERSCORE: u8 = 8;
const CHAR_OPERATOR: u8 = 16;
const CHAR_QUOTE: u8 = 32;
const CHAR_PAREN: u8 = 64;

// Built at compile time so the per-byte lookup needs no init check and no
// synchronisation. The byte is classified as the Latin-1 character it names,
// which is what `(i as u8 as char).is_whitespace()` / `.is_alphabetic()` do;
// `char_class_table_matches_char_methods` pins the two in agreement.
const fn char_class_of(b: u8) -> u8 {
    let mut class = 0u8;
    // Unicode White_Space within U+0000..=U+00FF.
    if matches!(b, 0x09..=0x0D | 0x20 | 0x85 | 0xA0) {
        class |= CHAR_WHITESPACE;
    }
    if b.is_ascii_digit() {
        class |= CHAR_DIGIT;
    }
    // Unicode Alphabetic within U+0000..=U+00FF.
    if b.is_ascii_alphabetic()
        || matches!(b, 0xAA | 0xB5 | 0xBA | 0xC0..=0xD6 | 0xD8..=0xF6 | 0xF8..=0xFF)
    {
        class |= CHAR_ALPHA;
    }
    if b == b'_' {
        class |= CHAR_UNDERSCORE;
    }
    if matches!(b, b'<' | b'>' | b'!' | b'=' | b'+' | b'-' | b'*' | b'/') {
        class |= CHAR_OPERATOR;
    }
    if b == b'\'' {
        class |= CHAR_QUOTE;
    }
    if matches!(b, b'(' | b')') {
        class |= CHAR_PAREN;
    }
    class
}

const CHAR_CLASS_TABLE: [u8; 256] = {
    let mut table = [0u8; 256];
    let mut i = 0;
    while i < 256 {
        table[i] = char_class_of(i as u8);
        i += 1;
    }
    table
};

#[inline(always)]
fn get_char_class(c: u8) -> u8 {
    CHAR_CLASS_TABLE[c as usize]
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
            _mm256_or_si256(newline_mask, cr_mask),
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
            _mm256_or_si256(digit_mask, underscore_mask),
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

/// The column a name in a predicate refers to: matched exactly, and the
/// last of that name in schema order when several share it (a dropped column
/// keeps its name). A linear scan: schemas are short, and building a map per
/// tokenization cost more than every lookup it served.
fn field_named<'a>(schema: &'a SmallVec<[SchemaField; 20]>, name: &str) -> Option<&'a SchemaField> {
    schema.iter().rev().find(|f| f.name == name)
}

/// The operators the tokenizer reads as words, by their lowercase spelling.
fn string_op(lower: &str) -> Option<&'static str> {
    Some(match lower {
        "contains" => "CONTAINS",
        "startswith" => "STARTSWITH",
        "endswith" => "ENDSWITH",
        // The case-insensitive counterparts that `ILIKE` lowers to. Without
        // these the tokenizer would treat them as unknown identifiers, which it
        // panics on.
        "icontains" => "ICONTAINS",
        "istartswith" => "ISTARTSWITH",
        "iendswith" => "IENDSWITH",
        "iequals" => "IEQUALS",
        // `LIKE`/`ILIKE` reach the tokenizer only for patterns the rewrite could
        // not reduce to one of the above — an interior `%`, or a `_`.
        "like" => "LIKE",
        "ilike" => "ILIKE",
        // `SIMILAR TO`, folded to one word by `predicate_rewrite` so it fits
        // this table — the tokenizer reads one identifier at a time.
        "similarto" => "SIMILARTO",
        // `IS NULL` / `IS NOT NULL` over a JSON path, in PostgreSQL's own
        // one-word spellings. Postfix: nothing follows them.
        "isnull" => "ISNULL",
        "notnull" => "NOTNULL",
        _ => return None,
    })
}

fn logical_op(lower: &str) -> Option<&'static str> {
    match lower {
        "and" => Some("AND"),
        "or" => Some("OR"),
        _ => None,
    }
}

#[inline(always)]
/// Tokenizes a predicate, or says why it cannot.
///
/// Returns an error rather than panicking. A predicate naming something that is
/// not a column is an ordinary client mistake — a keyword the clause splitter
/// left behind, a function the engine does not have — and it used to take down
/// the worker thread, which dropped the client's connection with no message at
/// all. Four separate parser bugs surfaced that way in one day, each appearing
/// as a lost connection rather than as the error it was.
pub fn try_tokenize<'a>(
    s: &str,
    schema: &'a SmallVec<[SchemaField; 20]>,
) -> Result<SmallVec<[Token; 36]>, String> {
    tokenize_inner(s, schema)
}

/// Panicking wrapper, for callers that have already validated their input.
///
/// Prefer [`try_tokenize`]: anything reachable from a client should report the
/// error rather than die.
pub fn tokenize<'a>(s: &str, schema: &'a SmallVec<[SchemaField; 20]>) -> SmallVec<[Token; 36]> {
    match tokenize_inner(s, schema) {
        Ok(tokens) => tokens,
        Err(e) => panic!("{}", e),
    }
}

/// Where a JSON path following a column name ends, or `None` when none does.
///
/// Only the extent is decided here; what the steps *are* is
/// `jsonb::parse_path_expression`'s answer, so there is one definition of what
/// a path means rather than two that can disagree.
fn json_path_extent(bytes: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    let mut end = None;
    loop {
        let mut j = i;
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        // Longest first: `->>` before `->`, `#>>` before `#>`.
        let after_op = if bytes[j..].starts_with(b"->>") || bytes[j..].starts_with(b"#>>") {
            j + 3
        } else if bytes[j..].starts_with(b"->") || bytes[j..].starts_with(b"#>") {
            j + 2
        } else {
            return end;
        };

        let mut k = after_op;
        while k < bytes.len() && bytes[k].is_ascii_whitespace() {
            k += 1;
        }
        if k >= bytes.len() {
            return end;
        }
        if bytes[k] == b'\'' {
            // A quoted key, in which `''` is one quote and not the end.
            k += 1;
            loop {
                if k >= bytes.len() {
                    return end;
                }
                if bytes[k] == b'\'' {
                    if bytes.get(k + 1) == Some(&b'\'') {
                        k += 2;
                        continue;
                    }
                    k += 1;
                    break;
                }
                k += 1;
            }
        } else {
            let digits_at = if bytes[k] == b'-' { k + 1 } else { k };
            let mut d = digits_at;
            while d < bytes.len() && bytes[d].is_ascii_digit() {
                d += 1;
            }
            if d == digits_at {
                return end;
            }
            k = d;
        }
        end = Some(k);
        i = k;
    }
}

fn tokenize_inner<'a>(
    s: &str,
    schema: &'a SmallVec<[SchemaField; 20]>,
) -> Result<SmallVec<[Token; 36]>, String> {
    let mut out = SmallVec::new();
    let bytes = s.as_bytes();
    let mut i = 0;

    // Track context for field association to maintain type consistency
    let mut current_field_context: Option<(Cow<'a, String>, DbType, u64)> = None;
    let mut expression_type_context: Option<DbType> = None;
    let mut expression_stack: Vec<Option<DbType>> = Vec::new();
    let mut in_parentheses = 0;
    // Fractions a float does not hold, by token index, for the literal
    // written before the NUMERIC column it is compared with (see the end).
    let mut lossy_fractions: Vec<(usize, String)> = Vec::new();

    // Helper to determine if a minus sign should be considered part of a number
    #[inline(always)]
    fn is_negative_number_start(tokens: &[Token], bytes: &[u8], i: usize) -> bool {
        let next_is_digit = i + 1 < bytes.len() && is_digit_fast(bytes[i + 1]);
        if !next_is_digit {
            return false;
        }

        i == 0
            || match tokens.last() {
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
            // An operand the rewrite lifted for the expression evaluator.
            crate::core::tokenizer::predicate_rewrite::LIFTED_OPEN => {
                let (text, end) = crate::core::tokenizer::predicate_rewrite::read_lifted(s, i)
                    .ok_or_else(|| {
                        "a lifted expression in the WHERE clause is malformed".to_string()
                    })?;
                let operand =
                    crate::core::processor::transformer::EvaluatedOperand::new(&text, schema)?;
                expression_type_context = Some(operand.db_type.clone());
                current_field_context = None;
                out.push(Token::Evaluated(std::sync::Arc::new(operand)));
                i = end;
            }

            b'\'' => {
                // A string literal. `''` inside one is an escaped quote, not
                // the end: `'O''Brien'` is one literal, and reading it as two
                // left `Brien` to be reported as an unknown identifier.
                let mut lit = String::new();
                let mut segment = i + 1;
                let mut j = segment;
                loop {
                    while j < bytes.len() && bytes[j] != b'\'' {
                        j += 1;
                    }
                    if j >= bytes.len() {
                        return Err(format!(
                            "unclosed string literal starting at position {}",
                            i
                        ));
                    }
                    lit.push_str(&s[segment..j]);
                    if bytes.get(j + 1) == Some(&b'\'') {
                        lit.push('\'');
                        j += 2;
                        segment = j;
                        continue;
                    }
                    break;
                }
                out.push(Token::StringLit(lit));
                i = j + 1;

                expression_type_context = Some(DbType::STRING);
                current_field_context = None;
            }

            _ if is_digit_fast(c) => {
                // Handle numeric literals with SIMD optimization
                let start = i;
                i = unsafe { scan_digits_simd(bytes, i) };

                if i < bytes.len() && bytes[i] == b'.' {
                    i += 1;
                    i = unsafe { scan_digits_simd(bytes, i) };
                }
                i = exponent_end(bytes, i);

                let num_str = unsafe { std::str::from_utf8_unchecked(&bytes[start..i]) };

                let numeric_value =
                    parse_numeric_value(num_str, &current_field_context, &expression_type_context)?;

                let num_type = numeric_value_to_db_type(&numeric_value);
                let numeric_context = [
                    expression_type_context.as_ref(),
                    current_field_context.as_ref().map(|(_, t, _)| t),
                ]
                .contains(&Some(&DbType::DECIMAL));
                if expression_type_context.is_none() {
                    expression_type_context = Some(num_type);
                }

                if numeric_value_is_lossy_fraction(num_str, &numeric_value) {
                    lossy_fractions.push((out.len(), num_str.to_string()));
                }
                out.push(number_token(num_str, numeric_value, numeric_context));
            }

            b'-' if is_negative_number_start(&out, bytes, i) => {
                // Handle negative numbers
                let start = i;
                i += 1;
                i = unsafe { scan_digits_simd(bytes, i) };

                if i < bytes.len() && bytes[i] == b'.' {
                    i += 1;
                    i = unsafe { scan_digits_simd(bytes, i) };
                }
                i = exponent_end(bytes, i);

                let num_str = unsafe { std::str::from_utf8_unchecked(&bytes[start..i]) };

                let numeric_value =
                    parse_numeric_value(num_str, &current_field_context, &expression_type_context)?;

                let num_type = numeric_value_to_db_type(&numeric_value);
                let numeric_context = [
                    expression_type_context.as_ref(),
                    current_field_context.as_ref().map(|(_, t, _)| t),
                ]
                .contains(&Some(&DbType::DECIMAL));
                if expression_type_context.is_none() {
                    expression_type_context = Some(num_type);
                }

                if numeric_value_is_lossy_fraction(num_str, &numeric_value) {
                    lossy_fractions.push((out.len(), num_str.to_string()));
                }
                out.push(number_token(num_str, numeric_value, numeric_context));
            }

            _ if is_id_start_fast(c) => {
                // Handle identifiers with SIMD optimization
                let start = i;
                i = unsafe { scan_identifier_simd(bytes, i) };

                let id = unsafe { std::str::from_utf8_unchecked(&bytes[start..i]) };

                // ASCII, the common case, needs no lowered copy. Anything else
                // is lowered as Unicode lowers it, as it always was: a
                // non-ASCII letter can lower to an ASCII one.
                let lowered;
                let id_lower: &str = if id.is_ascii() && !id.bytes().any(|b| b.is_ascii_uppercase()) {
                    id
                } else {
                    lowered = id.to_lowercase();
                    &lowered
                };

                // Check for special operators
                if let Some(normalized) = string_op(id_lower) {
                    // For string operations, we need to determine the type from context or schema
                    // Since these are operators, we might want to use the current expression type
                    // A string operator with nothing before it — `WHERE like 'x'`,
                    // or a column that happens to share an operator's name —
                    // has no operand type to take; refused rather than unwrapped.
                    let Some(op_type) = expression_type_context.clone() else {
                        return Err(format!("operator {} has no left-hand operand", normalized));
                    };
                    out.push(Token::Ident((normalized.to_string(), op_type, 0)));
                    continue;
                }

                if let Some(normalized) = logical_op(id_lower) {
                    out.push(Token::Next(normalized.to_string()));
                    expression_type_context = None;
                    current_field_context = None;
                    continue;
                }

                // `NOT` before one comparison, which the rewrite emits only
                // where an operator has no complement to become. An `Ident`
                // rather than a `Next`, so the index extractors read it as part
                // of the comparison it negates — an opaque one — rather than
                // as a connective they do not know.
                if id_lower == "not" && field_named(schema, id).is_none() {
                    out.push(Token::Ident(("NOT".to_string(), DbType::BOOL, 0)));
                    expression_type_context = None;
                    current_field_context = None;
                    continue;
                }

                // Check if this is a field name in the schema
                if let Some(field) = field_named(schema, id) {
                    let (canonical_name, field_type, write_order) =
                        (Cow::Borrowed(&field.name), &field.db_type, &field.write_order);
                    current_field_context = Some((
                        canonical_name.clone(),
                        field_type.clone(),
                        write_order.clone(),
                    ));
                    expression_type_context = Some(field_type.clone());

                    if in_parentheses > 0 {
                        if expression_stack.len() < in_parentheses {
                            expression_stack.push(Some(field_type.clone()));
                        } else {
                            expression_stack[in_parentheses - 1] = Some(field_type.clone());
                        }
                    }

                    // A path following the name — `doc->>'k'` — makes this a
                    // reach into a document rather than a column reference.
                    // The extent is found here and handed to the one path
                    // parser, so the tokenizer cannot drift from what `SELECT`
                    // and a future index resolve the same text to.
                    if let Some(end) = json_path_extent(bytes, i) {
                        let text = unsafe { std::str::from_utf8_unchecked(&bytes[start..end]) };
                        if let Some(path) = crate::core::jsonb::parse_path_expression(text) {
                            i = end;
                            // `->>` is text; `->` yields a document.
                            let result_type = if path.as_text {
                                DbType::STRING
                            } else {
                                DbType::JSONB
                            };
                            expression_type_context = Some(result_type.clone());
                            current_field_context = None;
                            out.push(Token::JsonPath {
                                write_order: *write_order,
                                steps: path.steps,
                                as_text: path.as_text,
                            });
                            continue;
                        }
                    }

                    out.push(Token::Ident((
                        canonical_name.to_string(),
                        field_type.clone(),
                        write_order.clone(),
                    )));
                } else if let Some(db_type) = typed_literal_type(&id, bytes, i) {
                    // `POINT '(1,2)'`, `inet '10.0.0.0/8'`, `INTERVAL '1 day'`
                    // — PostgreSQL's typed-literal syntax, which is defined to
                    // mean exactly what `'…'::type` means.
                    //
                    // So it is lexed as exactly that: the string, then a cast.
                    // Compiling it to the form the parser already handles is
                    // what keeps one implementation of what a cast does, rather
                    // than a second that could disagree with it.
                    let mut j = unsafe { skip_whitespace_simd(bytes, i) };
                    let mut lit = String::new();
                    let mut segment = j + 1;
                    j += 1;
                    loop {
                        while j < bytes.len() && bytes[j] != b'\'' {
                            j += 1;
                        }
                        if j >= bytes.len() {
                            return Err(format!(
                                "unclosed string literal after the type name '{}'",
                                id
                            ));
                        }
                        lit.push_str(&s[segment..j]);
                        if bytes.get(j + 1) == Some(&b'\'') {
                            lit.push('\'');
                            j += 2;
                            segment = j;
                            continue;
                        }
                        break;
                    }
                    out.push(Token::StringLit(lit));
                    out.push(Token::Cast(db_type.clone()));
                    i = j + 1;
                    expression_type_context = Some(db_type);
                    current_field_context = None;
                } else {
                    // Validate field reference
                    if i < bytes.len() {
                        let mut next_i = i;
                        next_i = unsafe { skip_whitespace_simd(bytes, next_i) };

                        if next_i < bytes.len() {
                            let next_char = bytes[next_i];
                            if matches!(next_char, b'=' | b'>' | b'<' | b'!')
                                || (next_i + 8 <= bytes.len()
                                    && &bytes[next_i..next_i + 8] == b"CONTAINS")
                                || (next_i + 10 <= bytes.len()
                                    && &bytes[next_i..next_i + 10] == b"STARTSWITH")
                                || (next_i + 8 <= bytes.len()
                                    && &bytes[next_i..next_i + 8] == b"ENDSWITH")
                            {
                                return Err(format!(
                                    "unknown column '{}' is not defined in the schema",
                                    id
                                ));
                            }
                        }
                    }

                    // For identifiers not found in schema, this should probably be an error
                    // since all valid fields should be in the schema
                    // But if you want to allow unknown identifiers, you could use a default type
                    // Right after a value, a word is where an operator or a
                    // keyword goes: `x = 1 LIMT 1`, `x = 1 ANDD v = 2`, `0x10`.
                    // A syntax error (42601), not an unknown column.
                    if matches!(
                        out.last(),
                        Some(Token::Number(_) | Token::StringLit(_) | Token::Ident(_) | Token::RPar | Token::JsonPath { .. })
                    ) {
                        return Err(format!("syntax error at or near \"{}\"", id));
                    }
                    return Err(format!(
                        "unknown identifier '{}' is not defined in the schema",
                        id
                    ));
                }
            }

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
            }

            b')' => {
                out.push(Token::RPar);
                i += 1;

                if in_parentheses > 0 {
                    if in_parentheses <= expression_stack.len()
                        && expression_stack[in_parentheses - 1].is_some()
                    {
                        expression_type_context = expression_stack[in_parentheses - 1].clone();
                    }
                    in_parentheses -= 1;
                }
            }

            // PostgreSQL's structural operators, matched before the ordered
            // comparisons below so `<@` is not cut into `<` and a stray `@`.
            // The list is longest-first, which is what keeps `<<=` whole.
            _ if crate::core::tokenizer::predicate_rewrite::PG_OPERATORS
                .iter()
                .any(|sym| bytes[i..].starts_with(sym.as_bytes())) =>
            {
                let symbol = crate::core::tokenizer::predicate_rewrite::PG_OPERATORS
                    .iter()
                    .find(|sym| bytes[i..].starts_with(sym.as_bytes()))
                    .expect("the guard just found one");
                out.push(Token::Op((*symbol).to_string()));
                i += symbol.len();
                current_field_context = None;
            }

            b'<' | b'>' | b'!' | b'=' => {
                let saved_context = expression_type_context.clone();

                let mut op_end = i + 1;
                if op_end < bytes.len() {
                    // Two-character operators.
                    // - <=, >=, !=, ==
                    // - <> (SQL not-equals)
                    if bytes[op_end] == b'=' {
                        op_end += 1;
                    } else if bytes[i] == b'<' && bytes[op_end] == b'>' {
                        op_end += 1;
                    }
                }

                let op = unsafe { std::str::from_utf8_unchecked(&bytes[i..op_end]).to_string() };
                out.push(Token::Op(op));
                i = op_end;

                expression_type_context = saved_context;
                current_field_context = None;
            }

            b'+' | b'-' | b'*' | b'/' => {
                let op_str = unsafe { std::str::from_utf8_unchecked(&bytes[i..i + 1]).to_string() };
                out.push(Token::Op(op_str));
                i += 1;
            }

            b',' => {
                i += 1;
                current_field_context = None;
                expression_type_context = None;
            }

            // `::bigint` — a cast applied to whatever precedes it. The parser
            // decides what that is; here it only has to become a token.
            b':' if bytes.get(i + 1) == Some(&b':') => {
                let mut k = i + 2;
                while k < bytes.len() && (bytes[k] as char).is_whitespace() {
                    k += 1;
                }
                let name_start = k;
                while k < bytes.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
                    k += 1;
                }
                let name = unsafe { std::str::from_utf8_unchecked(&bytes[name_start..k]) };
                let Some(db_type) = crate::core::rql::expr::db_type_of_cast(name) else {
                    return Err(format!("unknown type '{}' in a cast", name));
                };
                expression_type_context = Some(db_type.clone());
                out.push(Token::Cast(db_type));
                i = k;
            }

            _ => return Err(format!("unexpected character '{}'", c as char)),
        }
    }

    // `0.12345678912300000001 > a`: read before the column, so with no
    // NUMERIC beside it, the literal became a float that is another number.
    // Kept as its text, as `number_token` keeps one written after the column.
    for (at, text) in lossy_fractions {
        let compared_with_numeric = matches!(out.get(at + 1), Some(Token::Op(_)))
            && matches!(out.get(at + 2), Some(Token::Ident((_, DbType::DECIMAL, _))));
        if compared_with_numeric && matches!(out.get(at), Some(Token::Number(NumericValue::F64(_)))) {
            out[at] = Token::StringLit(text);
        }
    }
    Ok(out)
}

/// The type of a `TYPE 'literal'` at this position, or `None` when the
/// identifier is not a type name or is not followed by a string.
///
/// Both conditions matter. A column may legitimately be called `path` or
/// `line`, and treating the name as a type wherever it appeared would take a
/// column reference away from a schema that has one — so the schema is
/// consulted first (this runs in the `else` of that lookup) and a quote must
/// follow.
fn typed_literal_type(id: &str, bytes: &[u8], at: usize) -> Option<DbType> {
    let db_type = crate::core::rql::expr::db_type_of_cast(id)?;
    let mut j = at;
    while j < bytes.len() && (bytes[j] as char).is_whitespace() {
        j += 1;
    }
    if bytes.get(j) == Some(&b'\'') {
        Some(db_type)
    } else {
        None
    }
}

// Rest of the helper functions remain the same but with optimized implementations

#[inline(always)]
fn parse_numeric_value<'a>(
    value_str: &str,
    field_context: &Option<(Cow<'a, String>, DbType, u64)>,
    expression_context: &Option<DbType>,
) -> Result<NumericValue, String> {
    if let Some(db_type) = expression_context {
        return parse_by_db_type(value_str, db_type);
    }

    if let Some((_, db_type, _)) = field_context {
        return parse_by_db_type(value_str, db_type);
    }

    parse_default(value_str)
}

/// The token for a numeric literal: the number as it was read, except a
/// fraction beside a NUMERIC that a float cannot hold, which keeps its text.
///
/// A fraction is read as an `f64`, and the planner re-reads that at the
/// column's scale. Past a float's digits the re-read is another number:
/// `nm = 1234567890123456.78` compared `1234567890123456.80` and returned that
/// row instead, and so did `nm IN (SELECT nm …)`, whose values are written into
/// the statement as text and read again here. The text is compared exactly,
/// as a quoted number is.
///
/// Decided on the digits, not at the engine's nine places: past them both
/// readings were "not a number" alike, so `a = 0.12345678912300000001`
/// became 0.123456789123 and matched that row of a `NUMERIC(20,12)`.
fn number_token(text: &str, value: NumericValue, numeric_context: bool) -> Token {
    if let NumericValue::F64(f) = value {
        if numeric_context && text.contains(['.', 'e', 'E']) && !float_holds_exactly(text) {
            let written = crate::core::rql::expr::decimal_digits(text).map(normalise_digits);
            let read = crate::core::rql::expr::decimal_digits(&format!("{}", f)).map(normalise_digits);
            // More digits than an i128 holds is never what the float is.
            if written != read && (written.is_some() || is_plain_decimal(text)) {
                return Token::StringLit(text.to_string());
            }
        }
    }
    Token::Number(value)
}

/// A fraction read as a float that is not the number written.
fn numeric_value_is_lossy_fraction(text: &str, value: &NumericValue) -> bool {
    let NumericValue::F64(f) = value else {
        return false;
    };
    if float_holds_exactly(text) {
        return false;
    }
    let written = crate::core::rql::expr::decimal_digits(text).map(normalise_digits);
    if written.is_none() {
        return is_plain_decimal(text);
    }
    written != crate::core::rql::expr::decimal_digits(&format!("{}", f)).map(normalise_digits)
}

/// Whether `text` is a plain decimal (`[+-]digits[.digits]`, no exponent)
/// of at most fifteen digits, which an `f64` reads and prints back as the
/// same digits (`f64::DIGITS`): the float round-trip in [`number_token`]
/// and [`numeric_value_is_lossy_fraction`] cannot find a difference, so it
/// is not made — it formatted a float and read two decimals for every
/// numeric literal of a statement on a NUMERIC column.
fn float_holds_exactly(text: &str) -> bool {
    let body = text.strip_prefix(['-', '+']).unwrap_or(text);
    let mut digits = 0usize;
    let mut points = 0usize;
    for b in body.bytes() {
        match b {
            b'0'..=b'9' => digits += 1,
            b'.' => points += 1,
            _ => return false,
        }
    }
    digits > 0 && points <= 1 && digits <= f64::DIGITS as usize
}

/// Digits with at most one point, an optional sign and exponent: a number
/// as written, however long.
fn is_plain_decimal(text: &str) -> bool {
    let body = text.trim_start_matches(['-', '+']);
    let mantissa = body.split(['e', 'E']).next().unwrap_or("");
    !mantissa.is_empty()
        && mantissa.bytes().any(|b| b.is_ascii_digit())
        && mantissa.bytes().all(|b| b.is_ascii_digit() || b == b'.')
        && mantissa.bytes().filter(|b| *b == b'.').count() <= 1
}

/// `m × 10^e` with the trailing zeros of `m` moved into `e`, so equal
/// numbers compare equal however they were written.
fn normalise_digits((mut m, mut e): (i128, i32)) -> (i128, i32) {
    if m == 0 {
        return (0, 0);
    }
    while m % 10 == 0 {
        m /= 10;
        e += 1;
    }
    (m, e)
}

/// Parses the literal as the type of the column it is compared with.
///
/// A literal the type cannot hold is an error, as it is in PostgreSQL. It
/// used to parse as the type's default: `WHERE age > 300` on a `U8` column
/// became `age > 0` and returned every row, and `WHERE id = 100000` on an
/// `I16` column matched the rows whose id is 0 — a wrong answer reported as
/// success, with nothing in the statement to show for it.
#[inline(always)]
fn parse_by_db_type(value_str: &str, db_type: &DbType) -> Result<NumericValue, String> {
    fn parse<T: std::str::FromStr>(value_str: &str, db_type: &DbType) -> Result<T, String> {
        value_str
            .parse()
            .map_err(|_| format!("value {} is out of range for type {:?}", value_str, db_type))
    }
    // A fractional literal beside an integer is a fraction, not an integer that
    // failed to parse: `i * 2.5` and `age > 2.5` are numeric in PostgreSQL. It
    // stays a float, and the planner meets the two at a common type.
    let integer = !matches!(db_type, DbType::F32 | DbType::F64);
    if integer && value_str.contains(['.', 'e', 'E']) {
        return parse_default(value_str);
    }
    // An integer the column's type cannot hold keeps its own type — `int4`
    // or `int8`, as PostgreSQL types the constant — and the planner meets the
    // two at a common type. Refusing it made `n * 3000000000 > 20000000000`
    // an error over an `int4` column, where PostgreSQL answers it, and the
    // comparison `age > 300` over a one-byte column is simply false there.
    // What must never happen is the literal read as the column's zero, which
    // this does not do.
    if integer
        && !matches!(db_type, DbType::I128 | DbType::U128)
        && value_str.parse::<i64>().is_ok()
        && !fits_integer_type(value_str, db_type)
    {
        return parse_default(value_str);
    }
    Ok(match db_type {
        DbType::I8 => NumericValue::I8(parse(value_str, db_type)?),
        DbType::I16 => NumericValue::I16(parse(value_str, db_type)?),
        DbType::I32 => NumericValue::I32(parse(value_str, db_type)?),
        DbType::I64 => NumericValue::I64(parse(value_str, db_type)?),
        DbType::I128 => NumericValue::I128(parse(value_str, db_type)?),
        DbType::U8 => NumericValue::U8(parse(value_str, db_type)?),
        DbType::U16 => NumericValue::U16(parse(value_str, db_type)?),
        DbType::U32 => NumericValue::U32(parse(value_str, db_type)?),
        DbType::U64 => NumericValue::U64(parse(value_str, db_type)?),
        DbType::U128 => NumericValue::U128(parse(value_str, db_type)?),
        DbType::F32 => NumericValue::F32(parse(value_str, db_type)?),
        DbType::F64 => NumericValue::F64(parse(value_str, db_type)?),
        _ => return parse_default(value_str),
    })
}

/// Whether an integer literal parses as `db_type` itself.
fn fits_integer_type(value_str: &str, db_type: &DbType) -> bool {
    match db_type {
        DbType::I8 => value_str.parse::<i8>().is_ok(),
        DbType::I16 => value_str.parse::<i16>().is_ok(),
        DbType::I32 => value_str.parse::<i32>().is_ok(),
        DbType::I64 => value_str.parse::<i64>().is_ok(),
        DbType::U8 => value_str.parse::<u8>().is_ok(),
        DbType::U16 => value_str.parse::<u16>().is_ok(),
        DbType::U32 => value_str.parse::<u32>().is_ok(),
        DbType::U64 => value_str.parse::<u64>().is_ok(),
        _ => true,
    }
}

/// Past an exponent, `e5`, `E-10` or `e+3`, that follows a number ending at
/// `i`; `i` itself when none does. It was read as a word of its own, so
/// `f > 1e-10` asked for a column `e`.
fn exponent_end(bytes: &[u8], i: usize) -> usize {
    if !matches!(bytes.get(i), Some(b'e' | b'E')) {
        return i;
    }
    let mut j = i + 1;
    if matches!(bytes.get(j), Some(b'+' | b'-')) {
        j += 1;
    }
    let digits = j;
    while bytes.get(j).is_some_and(u8::is_ascii_digit) {
        j += 1;
    }
    // A letter after the digits makes it a word (`1e5x`), not an exponent.
    if j == digits || bytes.get(j).is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_') {
        return i;
    }
    j
}

#[inline(always)]
fn parse_default(value_str: &str) -> Result<NumericValue, String> {
    let out_of_range = || format!("numeric literal {} is out of range", value_str);
    if value_str.contains(['.', 'e', 'E']) {
        Ok(NumericValue::F64(
            value_str.parse().map_err(|_| out_of_range())?,
        ))
    } else {
        let parsed_num: i64 = value_str.parse().map_err(|_| out_of_range())?;
        if parsed_num >= i32::MIN as i64 && parsed_num <= i32::MAX as i64 {
            Ok(NumericValue::I32(parsed_num as i32))
        } else {
            Ok(NumericValue::I64(parsed_num))
        }
    }
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
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            mb
        }};
    }
    let b = match v {
        NumericValue::I8(n) => direct!(n),
        NumericValue::I16(n) => direct!(n),
        NumericValue::I32(n) => direct!(n),
        NumericValue::I64(n) => direct!(n),
        NumericValue::I128(n) => direct!(n),
        NumericValue::U8(n) => direct!(n),
        NumericValue::U16(n) => direct!(n),
        NumericValue::U32(n) => direct!(n),
        NumericValue::U64(n) => direct!(n),
        NumericValue::U128(n) => direct!(n),
        NumericValue::F32(f) => direct!(f),
        NumericValue::F64(f) => direct!(f),
    };
    b
}

#[inline(always)]
pub fn str_to_mb(s: &str) -> MemoryBlock {
    let b = s.as_bytes();
    let mut mb = MEMORY_POOL.acquire(b.len());
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

    /// Every decimal `float_holds_exactly` lets past the round-trip is one
    /// the round-trip finds equal: the shortcut never hides a lossy literal.
    ///
    /// Mutation: the fifteen-digit bound raised to seventeen.
    #[test]
    fn a_decimal_a_float_holds_skips_nothing_the_round_trip_would_catch() {
        let mut texts: Vec<String> = Vec::new();
        let mut seed: u64 = 0x5eed;
        for _ in 0..20_000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let digits = 1 + (seed >> 59) as usize % 17;
            let point = (seed >> 40) as usize % (digits + 1);
            let mut text = String::new();
            let mut s = seed;
            for d in 0..digits {
                if d == point && d > 0 {
                    text.push('.');
                }
                text.push(char::from(b'0' + (s % 10) as u8));
                s /= 10;
                if s == 0 {
                    s = seed ^ 0x9e3779b97f4a7c15;
                }
            }
            texts.push(text);
        }
        texts.extend(["0.1", "1234567890.12345", "123456789012345", "0.000000000000001", "9999999999999.99"].map(String::from));
        let mut held = 0;
        for text in &texts {
            if float_holds_exactly(text) {
                held += 1;
                let f: f64 = text.parse().expect("a number");
                let written = crate::core::rql::expr::decimal_digits(text).map(normalise_digits);
                let read = crate::core::rql::expr::decimal_digits(&format!("{}", f)).map(normalise_digits);
                assert_eq!(written, read, "{text} read as {f}");
            }
        }
        assert!(held > 10_000, "only {held} held");
        assert!(!float_holds_exactly("1234567890123456.78"));
        assert!(!float_holds_exactly("1e5"));
    }

    use super::*;

    #[test]
    fn char_class_table_matches_char_methods() {
        for i in 0..256usize {
            let c = i as u8 as char;
            let mut expected = 0u8;
            if c.is_whitespace() {
                expected |= CHAR_WHITESPACE;
            }
            if c.is_ascii_digit() {
                expected |= CHAR_DIGIT;
            }
            if c.is_alphabetic() {
                expected |= CHAR_ALPHA;
            }
            if c == '_' {
                expected |= CHAR_UNDERSCORE;
            }
            if matches!(c, '<' | '>' | '!' | '=' | '+' | '-' | '*' | '/') {
                expected |= CHAR_OPERATOR;
            }
            if c == '\'' {
                expected |= CHAR_QUOTE;
            }
            if matches!(c, '(' | ')') {
                expected |= CHAR_PAREN;
            }
            assert_eq!(
                CHAR_CLASS_TABLE[i], expected,
                "byte 0x{i:02X} ({c:?}) classified differently from the char methods"
            );
        }
    }

    #[test]
    fn a_doubled_quote_is_an_escaped_quote_inside_a_literal() {
        let schema = create_schema_2();
        let toks = tokenize_inner("name = 'O''Brien'", &schema).expect("one literal");
        assert!(
            toks.iter()
                .any(|t| matches!(t, Token::StringLit(s) if s == "O'Brien")),
            "got {:?}",
            toks
        );
        let toks = tokenize_inner("name = ''''", &schema).expect("a lone quote");
        assert!(
            toks.iter()
                .any(|t| matches!(t, Token::StringLit(s) if s == "'"))
        );
        let toks = tokenize_inner("name = ''", &schema).expect("empty");
        assert!(
            toks.iter()
                .any(|t| matches!(t, Token::StringLit(s) if s.is_empty()))
        );
    }

    /// A literal the column's type cannot hold used to parse as that type's
    /// default, so `age > 300` on a `U8` column returned every row. It keeps
    /// its own integer type now, as PostgreSQL's constant does, rather than
    /// being refused — and never becomes the column's zero.
    #[test]
    fn an_out_of_range_literal_keeps_its_own_type_rather_than_read_as_zero() {
        let schema = create_schema_2();
        let toks = tokenize_inner("age > 300", &schema).expect("300 is an int4");
        assert!(toks.contains(&Token::Number(NumericValue::I32(300))), "{toks:?}");
        let toks = tokenize_inner("age = -1", &schema).expect("-1 is an int4");
        assert!(toks.contains(&Token::Number(NumericValue::I32(-1))), "{toks:?}");
        let toks = tokenize_inner("age > 200", &schema).expect("200 fits a U8");
        assert!(toks.contains(&Token::Number(NumericValue::U8(200))), "{toks:?}");
        assert!(tokenize_inner("age > 99999999999999999999", &schema).is_err());
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

    #[test]
    fn test_tokenize_identifiers() {
        let mut schema = SmallVec::new();
        schema.push(SchemaField {
            name: "field1".to_string(),
            db_type: DbType::F64,
            is_deleted: false,
            size: 0,
            offset: 0,
            write_order: 5,
            is_unique: false,
            scale: None,
        });
        schema.push(SchemaField {
            name: "field2".to_string(),
            db_type: DbType::F64,
            is_deleted: false,
            size: 0,
            offset: 0,
            write_order: 10,
            is_unique: false,
            scale: None,
        });
        let tokens = tokenize("field1 + field2", &schema);
        assert_eq!(tokens.len(), 3);
        assert_eq!(
            tokens[0],
            Token::Ident(("field1".to_string(), DbType::F64, 5))
        );
        assert_eq!(tokens[1], Token::Op("+".to_string()));
        assert_eq!(
            tokens[2],
            Token::Ident(("field2".to_string(), DbType::F64, 10))
        );
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
        assert_eq!(tokens[1], Token::Ident(("id".to_string(), DbType::U128, 0)));
        assert_eq!(tokens[2], Token::Op("=".to_string()));
        assert_eq!(tokens[3], Token::Number(NumericValue::U128(42)));
        assert_eq!(tokens[4], Token::Next("AND".to_string()));
        assert_eq!(
            tokens[5],
            Token::Ident(("name".to_string(), DbType::STRING, 1))
        );
        assert_eq!(
            tokens[6],
            Token::Ident(("CONTAINS".to_string(), DbType::STRING, 0))
        );
        assert_eq!(tokens[7], Token::StringLit("John".to_string()));
        assert_eq!(tokens[8], Token::RPar);
        assert_eq!(tokens[9], Token::Next("OR".to_string()));
        assert_eq!(tokens[10], Token::LPar);
        assert_eq!(tokens[11], Token::Ident(("age".to_string(), DbType::U8, 2)));
        assert_eq!(tokens[12], Token::Op(">".to_string()));
        assert_eq!(tokens[13], Token::Number(NumericValue::U8(25)));
        assert_eq!(tokens[14], Token::Next("AND".to_string()));
        assert_eq!(
            tokens[15],
            Token::Ident(("salary".to_string(), DbType::F32, 22))
        );
        assert_eq!(tokens[16], Token::Op("<".to_string()));
        assert_eq!(tokens[17], Token::Number(NumericValue::F32(50000.0)));
        assert_eq!(tokens[18], Token::RPar);
        assert_eq!(tokens[19], Token::Next("OR".to_string()));
        assert_eq!(tokens[20], Token::LPar);
        assert_eq!(
            tokens[21],
            Token::Ident(("bank_balance".to_string(), DbType::F64, 6))
        );
        assert_eq!(tokens[22], Token::Op(">=".to_string()));
        assert_eq!(tokens[23], Token::Number(NumericValue::F64(1000.43)));
        assert_eq!(tokens[24], Token::Next("AND".to_string()));
        assert_eq!(
            tokens[25],
            Token::Ident(("net_assets".to_string(), DbType::F64, 20))
        );
        assert_eq!(tokens[26], Token::Op(">".to_string()));
        assert_eq!(tokens[27], Token::Number(NumericValue::F64(800000.0)));
        assert_eq!(tokens[28], Token::RPar);
        assert_eq!(tokens[29], Token::Next("OR".to_string()));
        assert_eq!(tokens[30], Token::LPar);
        assert_eq!(
            tokens[31],
            Token::Ident(("department".to_string(), DbType::STRING, 21))
        );
        assert_eq!(
            tokens[32],
            Token::Ident(("STARTSWITH".to_string(), DbType::STRING, 0))
        );
        assert_eq!(tokens[33], Token::StringLit("Eng".to_string()));
        assert_eq!(tokens[34], Token::Next("AND".to_string()));
        assert_eq!(
            tokens[35],
            Token::Ident(("email".to_string(), DbType::STRING, 3))
        );
        assert_eq!(
            tokens[36],
            Token::Ident(("ENDSWITH".to_string(), DbType::STRING, 0))
        );
        assert_eq!(tokens[37], Token::StringLit("example.com".to_string()));
        assert_eq!(tokens[38], Token::RPar);
    }
}
