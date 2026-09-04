use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::HashMap;

use super::query_tokenizer::{Token, numeric_to_mb, numeric_value_to_db_type, str_to_mb, tokenize};

use crate::core::db_type::DbType;
use crate::core::processor::transformer::{
    ComparerOperation, ComparisonOperand, MathOperation, Next, TransformerProcessor,
};
use crate::core::row::schema::SchemaField;
use crate::memory_pool::{MEMORY_POOL, MemoryBlock};

pub struct QueryParser<'a, 'b> {
    toks: &'a SmallVec<[Token; 36]>,
    pos: usize,
    query_transformer: &'b mut TransformerProcessor<'b>,
    first_iteration: bool,
    // Keep sorted & unique on first parse
    used_column_indices: SmallVec<[usize; 16]>,
    /// The declared scale of each `NUMERIC(p, s)` column, by write order.
    ///
    /// A `DECIMAL` is stored as an `i128` of units scaled by the column's own
    /// `s`, while a literal is encoded at the engine default. Comparing the two
    /// without reconciling the scales is off by a power of ten — and answers,
    /// rather than failing. See [`Self::rescale_decimal_literal`].
    decimal_scales: SmallVec<[(usize, u32); 8]>,
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
            decimal_scales: SmallVec::new(),
        }
    }

    /// Tells the parser what scale each `NUMERIC(p, s)` column stores in.
    ///
    /// Without this every predicate over such a column is answered from a value
    /// a power of ten away from the stored one: `WHERE price > 50` over a
    /// `NUMERIC(10,2)` column returned no rows at all, and reported no error.
    /// A parser given no scales behaves as before, which is correct for a
    /// schema whose decimals all use the engine default.
    pub fn with_scales(mut self, schema: &[crate::core::row::schema::SchemaField]) -> Self {
        for field in schema {
            if field.db_type != DbType::DECIMAL {
                continue;
            }
            if let Some(scale) = field.scale {
                if scale != crate::core::value_codec::DECIMAL_SCALE {
                    self.decimal_scales
                        .push((field.write_order as usize, scale));
                }
            }
        }
        self
    }

    /// The declared scale of the column an operand names, when it differs from
    /// the engine default.
    fn scale_of(&self, operand: &ComparisonOperand) -> Option<u32> {
        let index = operand.get_column_index()?;
        self.decimal_scales
            .iter()
            .find(|(at, _)| *at == index)
            .map(|(_, scale)| *scale)
    }

    /// Re-encodes a literal at the scale the column it is compared against
    /// stores in.
    ///
    /// The literal is rendered back to text and parsed again rather than
    /// rescaled arithmetically, so one decimal reader decides what the text
    /// means — the same one the insert path uses.
    fn rescale_decimal_literal(
        literal: &ComparisonOperand,
        from: &DbType,
        scale: u32,
    ) -> Option<ComparisonOperand> {
        let ComparisonOperand::Direct(mb) = literal else {
            return None;
        };
        let text = crate::core::rql::subquery::value_to_literal(from, mb.into_slice())?;
        let text = crate::core::value_codec::unquote_sql(&text);
        let unscaled = crate::core::value_codec::parse_decimal_scaled(&text, scale)?;
        let bytes = unscaled.to_le_bytes();
        let mut out = MEMORY_POOL.acquire(bytes.len());
        out.into_slice_mut().copy_from_slice(&bytes);
        Some(ComparisonOperand::Direct(out))
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
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(), String> {
        if self.first_iteration {
            self.first_iteration = false;
            self.pos = 0;
            self.used_column_indices.clear();

            self.parse_or(cols)?;

            // `parse_or` stops at the first token that is not a connective.
            // Anything left is a predicate the clause asked for and the plan
            // does not contain — returned rows would be a superset, reported
            // as the answer.
            if self.pos < self.toks.len() {
                return Err(format!(
                    "unexpected {:?} after the end of the expression",
                    self.toks[self.pos]
                ));
            }

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
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
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
            .execute_row(&aligned[..], comparison_results)
            .map_err(|e| e.as_str().to_string())?;
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
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
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
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
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
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(), String> {
        if let Some(Token::LPar) = self.toks.get(self.pos) {
            if self.is_boolean_group(self.pos) {
                self.pos += 1; // consume '('
                // The bracketing is recorded so the evaluator can honour it;
                // the result chain alone cannot express grouping.
                self.query_transformer.open_group();
                let inner = self.parse_or(cols);
                self.query_transformer.close_group();
                inner?;
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

        const CMP_OPS: [&str; 7] = ["=", "!=", "<>", ">", ">=", "<", "<="];
        const LOGIC_KWS: [&str; 2] = ["AND", "OR"];
        const TEXT_CMPS: [&str; 10] = [
            "CONTAINS",
            "STARTSWITH",
            "ENDSWITH",
            "ICONTAINS",
            "ISTARTSWITH",
            "IENDSWITH",
            "IEQUALS",
            "LIKE",
            "ILIKE",
            "SIMILARTO",
        ];

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
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
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

        // A literal's type comes from how it was written, not from what it is
        // compared against: `200` is an `i32` and `'1111-…'` is text, even when
        // the column is a `DECIMAL` or a `UUID`. The evaluator does no
        // coercion, so without this the pair would fail as an operand-size
        // mismatch. Coercing the *literal* to the column's type, rather than
        // the other way round, keeps the column's bytes untouched — which is
        // what lets the filter index recognise the predicate.
        // A `NUMERIC(p, s)` column and a literal have to meet at the column's
        // scale, not the engine's default. Done before everything below,
        // because the ordinary reconciliation would otherwise encode the
        // literal at the default and the two would never compare equal.
        let (left_op, right_op, left_type, right_type) = {
            let left_scale = self.scale_of(&left_op);
            let right_scale = self.scale_of(&right_op);
            match (left_scale, right_scale) {
                (Some(scale), None) if right_type != DbType::DECIMAL || true => {
                    match Self::rescale_decimal_literal(&right_op, &right_type, scale) {
                        Some(rescaled) => (left_op, rescaled, left_type, DbType::DECIMAL),
                        None => (left_op, right_op, left_type, right_type),
                    }
                }
                (None, Some(scale)) => {
                    match Self::rescale_decimal_literal(&left_op, &left_type, scale) {
                        Some(rescaled) => (rescaled, right_op, DbType::DECIMAL, right_type),
                        None => (left_op, right_op, left_type, right_type),
                    }
                }
                // Two columns of the same declared scale need no work; two of
                // *different* scales cannot be compared byte-for-byte, and are
                // left to the reconciliation below to refuse.
                _ => (left_op, right_op, left_type, right_type),
            }
        };

        // A structural operator is not reconciled and not coerced. The two
        // sides are *meant* to differ: `sample_frequencies @> 100` compares an
        // `int4range` against an `int4`, and coercing the literal to the
        // column's type — which is what reconciliation does for every ordered
        // comparison — would turn the integer into a malformed range.
        if let Some(op) = crate::core::pg_ops::PgOp::from_symbol(op_str) {
            if !op.applies_to(&left_type) {
                return Err(format!(
                    "operator {} does not apply to a {} value",
                    op.symbol(),
                    left_type
                ));
            }
            // A literal written as text is re-encoded at the left operand's
            // type — `tags @> '{gift}'` is an array, `raw_payload @> '{"a":1}'`
            // is a document — because these operators compare *values*, and
            // comparing an array against the eight characters of its text form
            // answers nothing. When the text does not encode at that type the
            // literal is left alone, which is what keeps `range @> '2026-01-01'`
            // reaching the element reader.
            let (right_op, right_type) = match (&right_op, &right_type) {
                (ComparisonOperand::Direct(literal), DbType::STRING)
                    if left_type != DbType::STRING =>
                {
                    match std::str::from_utf8(literal.into_slice())
                        .ok()
                        .and_then(|text| crate::core::value_codec::encode_literal(&left_type, text))
                    {
                        Some(bytes) => {
                            let mut mb = MEMORY_POOL.acquire(bytes.len());
                            mb.into_slice_mut().copy_from_slice(&bytes);
                            (ComparisonOperand::Direct(mb), left_type.clone())
                        }
                        None => (right_op, right_type),
                    }
                }
                _ => (right_op, right_type),
            };

            return self.push_comparison(
                left_op,
                right_op,
                left_type,
                crate::core::processor::transformer::ComparerOperation::Pg { op, right_type },
            );
        }

        // A literal's type comes from how it was written, not from what it is
        // compared against: `200` is an `i32` and `'1111-…'` is text, even when
        // the column is a `DECIMAL` or a `UUID`. The evaluator does no
        // coercion, so without this the pair would fail as an operand-size
        // mismatch. Coercing the *literal* to the column's type, rather than
        // the other way round, keeps the column's bytes untouched — which is
        // what lets the filter index recognise the predicate.
        let (left_op, right_op, left_type, right_type) =
            Self::reconcile_operand_types(left_op, right_op, left_type, right_type)?;

        let cmp = match op_str {
            "=" => ComparerOperation::Equals,
            // `<>` is standard SQL for not-equal and is what most clients and
            // ORMs emit; `!=` is the PostgreSQL-accepted alias. Both map to the
            // same comparison.
            "!=" | "<>" => ComparerOperation::NotEquals,
            ">" => ComparerOperation::Greater,
            ">=" => ComparerOperation::GreaterOrEquals,
            "<" => ComparerOperation::Less,
            "<=" => ComparerOperation::LessOrEquals,
            "CONTAINS" => ComparerOperation::Contains,
            "STARTSWITH" => ComparerOperation::StartsWith,
            "ENDSWITH" => ComparerOperation::EndsWith,
            "IEQUALS" => ComparerOperation::EqualsCaseInsensitive,
            "ICONTAINS" => ComparerOperation::ContainsCaseInsensitive,
            "ISTARTSWITH" => ComparerOperation::StartsWithCaseInsensitive,
            "IENDSWITH" => ComparerOperation::EndsWithCaseInsensitive,
            "LIKE" => ComparerOperation::Like,
            "ILIKE" => ComparerOperation::LikeCaseInsensitive,
            "SIMILARTO" => ComparerOperation::SimilarTo,
            _ => return Err(format!("unknown comparison operator: {}", op_str)),
        };

        let comparison_type = match cmp {
            ComparerOperation::Contains
            | ComparerOperation::StartsWith
            | ComparerOperation::EndsWith
            | ComparerOperation::EqualsCaseInsensitive
            | ComparerOperation::ContainsCaseInsensitive
            | ComparerOperation::StartsWithCaseInsensitive
            | ComparerOperation::EndsWithCaseInsensitive
            | ComparerOperation::SimilarTo => {
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

    /// Records one comparison, with the left operand's type deciding how the
    /// bytes are read.
    ///
    /// Split out so the structural-operator branch above shares the emit step
    /// rather than reimplementing it.
    fn push_comparison(
        &mut self,
        left_op: ComparisonOperand,
        right_op: ComparisonOperand,
        left_type: DbType,
        cmp: ComparerOperation,
    ) -> Result<(), String> {
        self.query_transformer
            .add_comparison(left_type, left_op, right_op, cmp, None);
        Ok(())
    }

    /// Rewrites a literal operand to the type of the column it is compared
    /// against, so both sides reach the evaluator at the same width.
    ///
    /// Only a bare literal (`Direct`) is rewritten, and only when the other
    /// side is a column reference (`DirectWithIndex`). Two columns, or two
    /// literals, are left alone: there is no column type to adopt. Handling
    /// both orders means `1990 < id` behaves like `id > 1990`.
    fn reconcile_operand_types(
        left_op: ComparisonOperand,
        right_op: ComparisonOperand,
        left_type: DbType,
        right_type: DbType,
    ) -> Result<(ComparisonOperand, ComparisonOperand, DbType, DbType), String> {
        fn coerce(
            literal: &MemoryBlock,
            from: &DbType,
            to: &DbType,
        ) -> Result<MemoryBlock, String> {
            let bytes = crate::core::value_codec::coerce_literal(literal.into_slice(), from, to)
                .ok_or_else(|| format!("literal cannot be compared against a {} column", to))?;
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            Ok(mb)
        }

        match (&left_op, &right_op) {
            (ComparisonOperand::Direct(lit), ComparisonOperand::DirectWithIndex(_, _))
                if left_type != right_type =>
            {
                let mb = coerce(lit, &left_type, &right_type)?;
                let t = right_type.clone();
                Ok((ComparisonOperand::Direct(mb), right_op, t.clone(), t))
            }
            (ComparisonOperand::DirectWithIndex(_, _), ComparisonOperand::Direct(lit))
                if left_type != right_type =>
            {
                let mb = coerce(lit, &right_type, &left_type)?;
                let t = left_type.clone();
                Ok((left_op, ComparisonOperand::Direct(mb), t.clone(), t))
            }
            // A literal meeting a *path* needs the same reconciliation.
            //
            // Only column operands were handled, so `(doc->>'code')::bigint = 3`
            // put an eight-byte value against a literal written as a narrower
            // int and the evaluator refused the pair outright — "operand size
            // does not match the column type being evaluated" — for a
            // comparison that is perfectly well formed. The cast is precisely
            // what says which type to compare in, so the literal is re-encoded
            // in it exactly as it would be for a column of that type.
            //
            // Only when a cast set the type. An *uncast* path keeps the older
            // behaviour of comparing raw bytes, which is what `data->'k' <> ''`
            // relies on — coercing the empty string into JSONB there fails, and
            // that pair is how key-existence is answered.
            (ComparisonOperand::Direct(lit), ComparisonOperand::PathWithIndex(_, _, path))
                if left_type != right_type && path.cast.is_some() =>
            {
                let mb = coerce(lit, &left_type, &right_type)?;
                let t = right_type.clone();
                Ok((ComparisonOperand::Direct(mb), right_op, t.clone(), t))
            }
            (ComparisonOperand::PathWithIndex(_, _, path), ComparisonOperand::Direct(lit))
                if left_type != right_type && path.cast.is_some() =>
            {
                let mb = coerce(lit, &right_type, &left_type)?;
                let t = left_type.clone();
                Ok((left_op, ComparisonOperand::Direct(mb), t.clone(), t))
            }
            _ => Ok((left_op, right_op, left_type, right_type)),
        }
    }

    fn parse_expr<'c>(
        &mut self,
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(ComparisonOperand, DbType), String> {
        let (mut lhs_op, mut lhs_type) = self.parse_term(cols)?;
        while let Some(op_str) = self.peek_op(&["+", "-"]) {
            self.next(); // consume op
            let (rhs_op, rhs_type) = self.parse_term(cols)?;
            let adding = op_str == "+";

            // Temporal arithmetic is not arithmetic on the operands' bytes:
            // `timestamp - timestamp` is a span and `timestamp ± interval`
            // moves the calendar. Deciding it here, where both operand types
            // are known, is what lets the evaluator see one operation per
            // meaning rather than guess from a single type.
            let (math, result_type) = match (&lhs_type, &rhs_type) {
                (DbType::DATETIME, DbType::DATETIME) if !adding => {
                    (MathOperation::IntervalBetween, DbType::INTERVAL)
                }
                (DbType::DATETIME, DbType::INTERVAL) => (
                    if adding {
                        MathOperation::ShiftForward
                    } else {
                        MathOperation::ShiftBackward
                    },
                    DbType::DATETIME,
                ),
                _ => (
                    if adding {
                        MathOperation::Add
                    } else {
                        MathOperation::Subtract
                    },
                    lhs_type.clone(),
                ),
            };

            let idx =
                self.query_transformer
                    .add_math_operation(lhs_type.clone(), lhs_op, rhs_op, math);
            lhs_op = ComparisonOperand::Intermediate(idx);
            lhs_type = result_type;
        }
        Ok((lhs_op, lhs_type))
    }

    fn parse_term<'c>(
        &mut self,
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
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
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
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
                // `'…'::type`, and the typed-literal form `TYPE '…'` the
                // tokenizer compiles to it. The literal is encoded at that type
                // here, where the text is still in hand — a `POINT` compared as
                // the seven characters of `'(5,5)'` would answer nothing.
                //
                // A cast the encoder cannot honour leaves the literal as text
                // rather than failing, because the ordered comparisons below
                // reconcile a text literal against the column themselves, and
                // that path already handles the types this one does not.
                match self.peek_cast() {
                    Some(target) if target != DbType::STRING => {
                        self.pos += 1;
                        match crate::core::value_codec::encode_literal(&target, &s) {
                            Some(bytes) => {
                                let mut mb = MEMORY_POOL.acquire(bytes.len());
                                mb.into_slice_mut().copy_from_slice(&bytes);
                                Ok((ComparisonOperand::Direct(mb), target))
                            }
                            None => Ok((ComparisonOperand::Direct(str_to_mb(&s)), DbType::STRING)),
                        }
                    }
                    Some(_) => {
                        self.pos += 1;
                        Ok((ComparisonOperand::Direct(str_to_mb(&s)), DbType::STRING))
                    }
                    None => Ok((ComparisonOperand::Direct(str_to_mb(&s)), DbType::STRING)),
                }
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

                // `hardware_status::text` — a cast over the column itself. The
                // conversion is per row, so it travels with the operand rather
                // than being applied here to the first row's bytes.
                match self.peek_cast() {
                    Some(target) if target != db_type => {
                        self.pos += 1;
                        Ok((
                            ComparisonOperand::CastWithIndex(mb, idx, db_type, target.clone()),
                            target,
                        ))
                    }
                    Some(_) => {
                        self.pos += 1;
                        Ok((ComparisonOperand::DirectWithIndex(mb, idx), db_type))
                    }
                    None => Ok((ComparisonOperand::DirectWithIndex(mb, idx), db_type)),
                }
            }
            // A path into a document. The index is the *document* column's —
            // that is what the row has to fetch — and the transformer takes the
            // path from its bytes each row.
            Token::JsonPath {
                write_order,
                steps,
                as_text,
            } => {
                let idx = write_order as usize;
                if idx >= cols.len() {
                    return Err(format!(
                        "Column index {} out of bounds (max: {})",
                        idx,
                        cols.len() - 1
                    ));
                }
                if self.first_iteration {
                    self.used_column_indices.push(idx);
                }
                // A cast written on the path belongs to it: `doc->>'n'::int`
                // and `(doc->>'n')::int` both mean "read that value as an
                // int", and the comparison then happens in that type.
                let cast = match self.peek_cast() {
                    Some(t) => {
                        self.pos += 1;
                        Some(t)
                    }
                    None => None,
                };
                let path = crate::core::processor::transformer::JsonPathRef {
                    steps: std::sync::Arc::new(steps),
                    as_text,
                    cast: cast.clone(),
                };
                // Resolved once here so the first comparison has a value of the
                // right width; every row after it goes through
                // `update_column_data_by_index`.
                let mb = path.extract(cols[idx].1.into_slice());
                let db_type = match cast {
                    Some(t) => t,
                    None if as_text => DbType::STRING,
                    None => DbType::JSONB,
                };
                Ok((ComparisonOperand::PathWithIndex(mb, idx, path), db_type))
            }
            Token::LPar => {
                let (inner_op, inner_type) = self.parse_expr(cols)?;
                match self.next() {
                    Some(Token::RPar) => {
                        // `(doc->>'n')::bigint` — the cast follows the group and
                        // applies to what the group produced. Only a path can
                        // carry one, because only a path re-reads its bytes;
                        // over anything else it is refused rather than
                        // silently ignored, which would compare a value in the
                        // wrong type and match nothing.
                        match self.peek_cast() {
                            None => Ok((inner_op, inner_type)),
                            Some(target) => {
                                self.pos += 1;
                                match inner_op {
                                    ComparisonOperand::PathWithIndex(_, idx, path) => {
                                        let recast =
                                            crate::core::processor::transformer::JsonPathRef {
                                                steps: path.steps.clone(),
                                                as_text: path.as_text,
                                                cast: Some(target.clone()),
                                            };
                                        let mb = recast.extract(cols[idx].1.into_slice());
                                        Ok((
                                            ComparisonOperand::PathWithIndex(mb, idx, recast),
                                            target,
                                        ))
                                    }
                                    _ => {
                                        Err("a `::` cast over this expression is not supported yet"
                                            .to_string())
                                    }
                                }
                            }
                        }
                    }
                    _ => Err("expected closing parenthesis after grouped expression".to_string()),
                }
            }
            t => Err(format!("unexpected factor: {:?}", t)),
        }
    }

    /// A cast token sitting at the current position, if there is one.
    #[inline(always)]
    fn peek_cast(&self) -> Option<DbType> {
        match self.toks.get(self.pos) {
            Some(Token::Cast(t)) => Some(t.clone()),
            _ => None,
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
    use crate::core::tokenizer::query_tokenizer::{NumericValue, Token, tokenize};
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
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_i16(value: i16) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_i64(value: i64) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u8(value: u8) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u16(value: u16) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u32(value: u32) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u64(value: u64) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_u128(value: u128) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_i128(value: i128) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_f32(value: f32) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_i32(value: i32) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(&bytes);
        data
    }

    fn create_memory_block_from_string(value: &str) -> MemoryBlock {
        let bytes = value.as_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
        let slice = data.into_slice_mut();
        slice.copy_from_slice(bytes);
        data
    }

    fn create_memory_block_from_f64(value: f64) -> MemoryBlock {
        let bytes = value.to_le_bytes();
        let mut data = MEMORY_POOL.acquire(bytes.len());
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
            Token::Number(n) => assert_eq!(*n, NumericValue::U8(30)),
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
