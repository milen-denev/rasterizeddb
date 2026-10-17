use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::HashMap;

use super::query_tokenizer::{
    NumericValue, Token, numeric_to_mb, numeric_value_to_db_type, str_to_mb, tokenize,
};

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

    /// Whether a literal holds no more decimal places than `scale`, so that
    /// re-encoding it there loses nothing. Anything that is not a numeric
    /// literal answers `true`, leaving it to the paths that handled it before.
    ///
    /// A quoted number counts as well as a bare one: `v = '2.251'` is how a
    /// client that binds numerics as text writes it, and it was truncated to
    /// the column's two places and matched 2.25.
    fn literal_exact_at(literal: &ComparisonOperand, from: &DbType, scale: u32) -> bool {
        let ComparisonOperand::Direct(mb) = literal else {
            return true;
        };
        if !matches!(from, DbType::F32 | DbType::F64 | DbType::STRING) {
            return true;
        }
        let Some(text) = crate::core::rql::subquery::value_to_literal(from, mb.into_slice()) else {
            return true;
        };
        let text = crate::core::value_codec::unquote_sql(&text);
        if crate::core::value_codec::parse_decimal_scaled(&text, scale).is_none() {
            // Not a number: the paths below say so in their own words.
            return true;
        }
        // The places the value needs: the fraction's digits up to its last
        // non-zero one, less the exponent's shift. `2.2500` needs two, and
        // `1.5e-7` eight.
        let trimmed = text.trim();
        let at = trimmed.find(['e', 'E']).unwrap_or(trimmed.len());
        let Ok(exponent) = trimmed.get(at + 1..).filter(|e| !e.is_empty()).map_or(Ok(0), str::parse::<i64>) else {
            return true;
        };
        let fraction = trimmed[..at].split_once('.').map_or("", |(_, f)| f).trim_end_matches('0');
        (fraction.len() as i64 - exponent) <= i64::from(scale)
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

    /// One operand of `NUMERIC` arithmetic, brought to the engine scale.
    ///
    /// Arithmetic over decimals runs at the engine scale, as the expression
    /// evaluator's does (`expr::to_engine_scale`). A literal is re-encoded there
    /// once, now; a `NUMERIC(p, s)` column is lifted per row, which is the
    /// returned scale; an integer column is cast per row. An arithmetic result
    /// is already there. Anything else is refused by name rather than compared
    /// at the wrong width or scale.
    fn decimal_operand(
        &self,
        operand: ComparisonOperand,
        db_type: &DbType,
    ) -> Result<(ComparisonOperand, Option<u32>), String> {
        if *db_type == DbType::DECIMAL {
            let lift = match &operand {
                ComparisonOperand::DirectWithIndex(..) => self.scale_of(&operand),
                _ => None,
            };
            return Ok((operand, lift));
        }
        let numeric = matches!(
            db_type,
            DbType::I8
                | DbType::I16
                | DbType::I32
                | DbType::I64
                | DbType::I128
                | DbType::U8
                | DbType::U16
                | DbType::U32
                | DbType::U64
                | DbType::U128
                | DbType::F32
                | DbType::F64
        );
        match operand {
            // A quoted number too: `v = '2.251'` over a `NUMERIC(10,2)` comes
            // here when the column's scale cannot hold it.
            ComparisonOperand::Direct(_) if numeric || *db_type == DbType::STRING => {
                let rescaled = Self::rescale_decimal_literal(
                    &operand,
                    db_type,
                    crate::core::value_codec::DECIMAL_SCALE,
                )
                .ok_or_else(|| "a literal in NUMERIC arithmetic is not a number".to_string())?;
                Ok((rescaled, None))
            }
            ComparisonOperand::DirectWithIndex(mb, index)
                if numeric && !matches!(db_type, DbType::F32 | DbType::F64) =>
            {
                Ok((
                    ComparisonOperand::CastWithIndex(mb, index, db_type.clone(), DbType::DECIMAL),
                    None,
                ))
            }
            // An arithmetic result of another type — the `1 * 2` of
            // `v + 1 * 2`, or `id * 2` — converted per row, as a column is.
            ComparisonOperand::Intermediate(idx) if numeric => Ok((
                ComparisonOperand::IntermediateCast(idx, db_type.clone(), DbType::DECIMAL),
                None,
            )),
            _ => Err(format!(
                "a {} operand cannot be combined with a NUMERIC one here",
                db_type
            )),
        }
    }

    /// Both operands of an arithmetic step or comparison, when either is
    /// `NUMERIC`: see [`Self::decimal_operand`]. `None` when neither is.
    #[allow(clippy::type_complexity)]
    fn decimal_operands(
        &self,
        left: ComparisonOperand,
        left_type: &DbType,
        right: ComparisonOperand,
        right_type: &DbType,
    ) -> Result<
        Result<(ComparisonOperand, ComparisonOperand, Option<u32>, Option<u32>), (ComparisonOperand, ComparisonOperand)>,
        String,
    > {
        let literal = |o: &ComparisonOperand| matches!(o, ComparisonOperand::Direct(_));
        let float = |t: &DbType| matches!(t, DbType::F32 | DbType::F64);
        // A float column makes the step a float one, as PostgreSQL resolves
        // `numeric` against `float8`; `promote_numeric` converts the NUMERIC
        // side. Float *arithmetic* is not counted: it is usually two written
        // fractions (`v + 0.5 * 2`), which are `numeric` in PostgreSQL, and it
        // is converted to NUMERIC as it always was.
        if (float(left_type) && left.is_column()) || (float(right_type) && right.is_column()) {
            return Ok(Err((left, right)));
        }
        // A written fraction is `numeric` in PostgreSQL, so against an integer
        // column or an integer result it is NUMERIC arithmetic, which is exact:
        // `n * 0.1 = 0.3` held for no row as a float, and `n * 1.37 = 12.33`
        // missed 9. Two constants are left as they were, which keeps
        // `f = 0.5 * 3` comparing a float column against a float.
        let integer = |t: &DbType| {
            matches!(
                t,
                DbType::I8 | DbType::I16 | DbType::I32 | DbType::I64 | DbType::I128
                    | DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 | DbType::U128
            )
        };
        let fraction_meets_integer = (literal(&left) && float(left_type) && !literal(&right) && integer(right_type))
            || (literal(&right) && float(right_type) && !literal(&left) && integer(left_type));
        if *left_type != DbType::DECIMAL && *right_type != DbType::DECIMAL && !fraction_meets_integer {
            return Ok(Err((left, right)));
        }
        let (left, left_lift) = self.decimal_operand(left, left_type)?;
        let (right, right_lift) = self.decimal_operand(right, right_type)?;
        Ok(Ok((left, right, left_lift, right_lift)))
    }

    /// Brings two operands of different numeric types to one, for an
    /// arithmetic step or an ordered comparison that the evaluator would
    /// otherwise refuse as an operand-size mismatch: `0.5 * 2` multiplied an
    /// eight-byte float by a four-byte integer, and `i32col = i64col` compared
    /// unequal widths.
    ///
    /// A literal is re-encoded (once, now); a column or an arithmetic result is
    /// converted per row. The common type is the non-literal side's when the
    /// literal fits it — so a column's bytes stay untouched where possible —
    /// and otherwise the wider of the two: `F64` once a float is involved,
    /// `I64` (or `I128` past it) for integers. `NUMERIC` is not handled here; it
    /// has its own scale-aware path. Pairs that are not both plain numbers are
    /// returned unchanged.
    fn promote_numeric(
        left: ComparisonOperand,
        left_type: DbType,
        right: ComparisonOperand,
        right_type: DbType,
    ) -> (ComparisonOperand, ComparisonOperand, DbType, DbType) {
        fn int(t: &DbType) -> bool {
            matches!(
                t,
                DbType::I8
                    | DbType::I16
                    | DbType::I32
                    | DbType::I64
                    | DbType::I128
                    | DbType::U8
                    | DbType::U16
                    | DbType::U32
                    | DbType::U64
                    | DbType::U128
            )
        }
        fn float(t: &DbType) -> bool {
            matches!(t, DbType::F32 | DbType::F64)
        }
        // A NUMERIC arithmetic result against a float is a float, as
        // PostgreSQL resolves the pair; the result is converted per row. A
        // NUMERIC *column* is not, since its bytes are at its declared scale
        // and the conversion reads the engine's.
        let numeric_result = |o: &ComparisonOperand, t: &DbType, other: &DbType| {
            *t == DbType::DECIMAL
                && float(other)
                && matches!(o, ComparisonOperand::Intermediate(_) | ComparisonOperand::IntermediateCast(..))
        };
        let numeric_left = numeric_result(&left, &left_type, &right_type);
        let numeric_right = numeric_result(&right, &right_type, &left_type);
        let numeric = |t: &DbType| int(t) || float(t);
        if left_type == right_type
            || !(numeric(&left_type) || numeric_left)
            || !(numeric(&right_type) || numeric_right)
        {
            return (left, right, left_type, right_type);
        }
        let literal = |o: &ComparisonOperand| matches!(o, ComparisonOperand::Direct(_));
        let convertible = |o: &ComparisonOperand| {
            matches!(
                o,
                ComparisonOperand::Direct(_)
                    | ComparisonOperand::DirectWithIndex(..)
                    | ComparisonOperand::Intermediate(_)
                    | ComparisonOperand::IntermediateCast(..)
                    | ComparisonOperand::Evaluated(..)
            )
        };
        if !convertible(&left) || !convertible(&right) {
            return (left, right, left_type, right_type);
        }
        let wide = |a: &DbType, b: &DbType| -> DbType {
            if float(a) || float(b) {
                DbType::F64
            } else if [a, b]
                .iter()
                .any(|t| matches!(t, DbType::U64 | DbType::I128 | DbType::U128))
            {
                DbType::I128
            } else {
                DbType::I64
            }
        };
        let encode = |o: &ComparisonOperand, from: &DbType, to: &DbType| -> Option<ComparisonOperand> {
            match o {
                ComparisonOperand::Direct(mb) => {
                    let bytes =
                        crate::core::value_codec::coerce_literal(mb.into_slice(), from, to)?;
                    let mut out = MEMORY_POOL.acquire(bytes.len());
                    out.into_slice_mut().copy_from_slice(&bytes);
                    Some(ComparisonOperand::Direct(out))
                }
                ComparisonOperand::DirectWithIndex(mb, idx) => Some(
                    ComparisonOperand::CastWithIndex(mb.clone(), *idx, from.clone(), to.clone()),
                ),
                ComparisonOperand::Intermediate(idx) => Some(
                    ComparisonOperand::IntermediateCast(*idx, from.clone(), to.clone()),
                ),
                ComparisonOperand::IntermediateCast(idx, original, _) => Some(
                    ComparisonOperand::IntermediateCast(*idx, original.clone(), to.clone()),
                ),
                // Converted as it is evaluated, per row.
                ComparisonOperand::Evaluated(operand, _) => {
                    Some(ComparisonOperand::Evaluated(operand.clone(), to.clone()))
                }
                _ => None,
            }
        };
        // The non-literal side's type, when the literal fits it exactly — a
        // float literal into an integer column does not.
        let target = match (literal(&left), literal(&right)) {
            (true, false) if !(float(&left_type) && int(&right_type)) => {
                encode(&left, &left_type, &right_type).map(|_| right_type.clone())
            }
            (false, true) if !(float(&right_type) && int(&left_type)) => {
                encode(&right, &right_type, &left_type).map(|_| left_type.clone())
            }
            _ => None,
        }
        .unwrap_or_else(|| wide(&left_type, &right_type));
        let l = if left_type == target {
            Some(left.clone())
        } else {
            encode(&left, &left_type, &target)
        };
        let r = if right_type == target {
            Some(right.clone())
        } else {
            encode(&right, &right_type, &target)
        };
        match (l, r) {
            (Some(l), Some(r)) => (l, r, target.clone(), target),
            _ => (left, right, left_type, right_type),
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
        const TEXT_CMPS: [&str; 13] = [
            "ISNULL",
            "NOTNULL",
            "NOT",
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
                    // A structural operator is a comparison too: `(doc ? 'k')`
                    // is a condition, not an operand.
                    if CMP_OPS.contains(&s) || crate::core::pg_ops::PgOp::from_symbol(s).is_some() {
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
        // `NOT` before a comparison, which `rewrite_where` leaves only where
        // the operator has no complement to be rewritten to: `NOT t
        // STARTSWITH 'p'`, `NOT doc ? 'k'`. It negates this one comparison.
        let negated = matches!(self.toks.get(self.pos), Some(Token::Ident((n, _, _))) if n == "NOT");
        if negated {
            self.pos += 1;
        }
        let (left_op, left_type) = self.parse_expr(cols)?;
        let op_token = self
            .next()
            .ok_or_else(|| "Unexpected end of token stream".to_string())?;

        let op_str: &str = match &op_token {
            Token::Op(o) => o.as_str(),
            Token::Ident((o, _, _)) => o.as_str(),
            _ => return Err(format!("expected comparison operator, got {:?}", op_token)),
        };

        // `doc->>'a' ISNULL` / `NOTNULL`: a test of the path's value, with no
        // right operand. A column's NULL is its companion's, which the rewrite
        // tests instead, so only a path comes here.
        if op_str == "ISNULL" || op_str == "NOTNULL" {
            let want_null = op_str == "ISNULL";
            // An evaluated operand answers it the same way a path does: by
            // whether a value came back at all.
            if !matches!(
                left_op,
                ComparisonOperand::PathWithIndex(..) | ComparisonOperand::Evaluated(..)
            ) {
                return Err(format!(
                    "IS {}NULL is only supported over a column or a JSON path here",
                    if want_null { "" } else { "NOT " }
                ));
            }
            self.query_transformer.add_comparison(
                left_type,
                left_op,
                ComparisonOperand::Direct(MemoryBlock::default()),
                ComparerOperation::Equals,
                None,
            );
            self.query_transformer.set_last_null_test(want_null != negated);
            return Ok(());
        }

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
        //
        // Only when the literal is exact at that scale. Re-encoding truncates
        // the places past it, so `v = 2.251` over a `NUMERIC(10,2)` holding
        // 2.25 matched, and `v < 2.251` did not. A literal with more places
        // than the column is compared with both sides at the engine scale
        // instead, below — which the filter index declines to prune, as it
        // declines any literal it cannot encode exactly.
        let mut inexact_literal = false;
        let (left_op, right_op, left_type, right_type) = {
            let left_scale = self.scale_of(&left_op);
            let right_scale = self.scale_of(&right_op);
            match (left_scale, right_scale) {
                (Some(scale), None) if !Self::literal_exact_at(&right_op, &right_type, scale) => {
                    inexact_literal = true;
                    (left_op, right_op, left_type, right_type)
                }
                (None, Some(scale)) if !Self::literal_exact_at(&left_op, &left_type, scale) => {
                    inexact_literal = true;
                    (left_op, right_op, left_type, right_type)
                }
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
                // *different* scales are lifted to the engine scale below.
                _ => (left_op, right_op, left_type, right_type),
            }
        };

        // A `NUMERIC` comparison that cannot be answered on stored bytes at one
        // column's scale: either side is an arithmetic result — which is at the
        // engine scale — or both are columns of different declared scales.
        // Both sides are brought to the engine scale, the literal now and a
        // column per row. `v + 1 > 101` was refused here as an operand-size
        // mismatch, and `a > b` over `NUMERIC(10,2)` and `NUMERIC(12,4)`
        // compared integers a hundredfold apart and answered.
        //
        // The plain `column op literal` shape never comes here: it keeps the
        // literal at the column's own scale, which is what the filter index
        // hashes. Nor does an operator over text or a document.
        let mut operand_scales: (Option<u32>, Option<u32>) = (None, None);
        let comparing = matches!(op_str, "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=");
        let (left_op, right_op, left_type, right_type) = {
            let arithmetic = (left_op.is_intermediate() && left_type == DbType::DECIMAL)
                || (right_op.is_intermediate() && right_type == DbType::DECIMAL);
            let mixed_columns = left_type == DbType::DECIMAL
                && right_type == DbType::DECIMAL
                && left_op.is_column()
                && right_op.is_column()
                && self.scale_of(&left_op) != self.scale_of(&right_op);
            if comparing && (arithmetic || mixed_columns || inexact_literal) {
                match self.decimal_operands(left_op, &left_type, right_op, &right_type)? {
                    Ok((l, r, ls, rs)) => {
                        operand_scales = (ls, rs);
                        (l, r, DbType::DECIMAL, DbType::DECIMAL)
                    }
                    Err((l, r)) => (l, r, left_type, right_type),
                }
            } else {
                (left_op, right_op, left_type, right_type)
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
                    if left_type != DbType::STRING && !op.takes_text_operand() =>
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
                        // Except for a document. Text that is not JSON was
                        // kept as raw text and handed to the blob walk as if
                        // it were an encoding, so the answer depended on the
                        // row: `false` against an object, an error against an
                        // array. PostgreSQL refuses the literal before reading
                        // a row, and so does this.
                        None if left_type == DbType::JSONB => {
                            let text = String::from_utf8_lossy(literal.into_slice());
                            let reason = crate::core::jsonb::encode_text(&text)
                                .err()
                                .unwrap_or_default();
                            return Err(format!(
                                "invalid input syntax for type json: {} ({})",
                                crate::core::value_codec::quote_sql(&text),
                                reason
                            ));
                        }
                        None => (right_op, right_type),
                    }
                }
                _ => (right_op, right_type),
            };

            // A literal `?|` / `?&` key list is split now rather than per row.
            // `rewrite_where` has already refused a list that is not `'{…}'`;
            // the refusal here covers a plan that did not come through it,
            // which would otherwise fail per row, and only on non-NULL rows.
            let key_list = match (&right_op, op) {
                (
                    ComparisonOperand::Direct(literal),
                    crate::core::pg_ops::PgOp::KeyExistsAny | crate::core::pg_ops::PgOp::KeyExistsAll,
                ) => {
                    let text = std::str::from_utf8(literal.into_slice()).map_err(|_| {
                        format!("the key list of {} is not valid UTF-8", op.symbol())
                    })?;
                    Some(crate::core::pg_ops::parse_key_list(text).ok_or_else(|| {
                        format!(
                            "malformed array literal: {} (the right operand of {} is a text array, as in '{{a,b}}')",
                            crate::core::value_codec::quote_sql(text),
                            op.symbol()
                        )
                    })?)
                }
                _ => None,
            };

            self.push_comparison(
                left_op,
                right_op,
                left_type,
                crate::core::processor::transformer::ComparerOperation::Pg { op, right_type },
            )?;
            if let Some(keys) = key_list {
                self.query_transformer.set_last_key_list(keys);
            }
            if negated {
                self.query_transformer.set_last_negated();
            }
            return Ok(());
        }

        // A document is compared only with a document, or with a quoted
        // literal, which is read as one. PostgreSQL has no `jsonb = integer`,
        // `jsonb = boolean` or `jsonb = text`, and refuses them. Here the pair
        // reached the evaluator as a document comparison of whatever bytes the
        // other side held: an `i32` 1 begins with the tag of `false`, so
        // `doc->'a' = 1` matched `{"a":false}`.
        if comparing {
            if let Some(message) =
                Self::document_type_mismatch(op_str, &left_op, &left_type, &right_op, &right_type)
            {
                return Err(message);
            }
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
        // What reconciliation leaves mismatched — an arithmetic result against
        // a literal or a column, or two columns of different widths — meets at
        // a common numeric type. Ordered comparisons only.
        let (left_op, right_op, left_type, right_type) = if comparing {
            Self::promote_numeric(left_op, left_type, right_op, right_type)
        } else {
            (left_op, right_op, left_type, right_type)
        };
        if let Some(message) =
            Self::evaluated_type_mismatch(op_str, &left_op, &left_type, &right_op, &right_type)
        {
            return Err(message);
        }

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
        if operand_scales != (None, None) {
            self.query_transformer
                .set_last_operand_scales(operand_scales.0, operand_scales.1);
        }
        if negated {
            self.query_transformer.set_last_negated();
        }
        Ok(())
    }

    /// PostgreSQL's "operator does not exist" for a document compared with
    /// anything but a document or a quoted literal, or `None` when the pair is
    /// one PostgreSQL accepts.
    ///
    /// A quoted literal is text here, and is accepted because PostgreSQL
    /// resolves an untyped literal as `jsonb`. A text *column* or expression is
    /// not: `doc->'a' = t` is `jsonb = text`, which PostgreSQL refuses. The
    /// engine's own internal types are let through, since they do not come
    /// from anything a statement can write.
    fn document_type_mismatch(
        op_str: &str,
        left_op: &ComparisonOperand,
        left_type: &DbType,
        right_op: &ComparisonOperand,
        right_type: &DbType,
    ) -> Option<String> {
        let document = |t: &DbType| *t == DbType::JSONB;
        if document(left_type) == document(right_type) {
            return None;
        }
        let (other_op, other_type) = if document(left_type) {
            (right_op, right_type)
        } else {
            (left_op, left_type)
        };
        let accepted = match other_type {
            DbType::STRING => matches!(other_op, ComparisonOperand::Direct(_)),
            DbType::JSON | DbType::NULL | DbType::UNKNOWN => true,
            _ => false,
        };
        if accepted {
            return None;
        }
        // A literal written with a fraction is a `numeric` to PostgreSQL; the
        // engine parses it as a float.
        let name = |op: &ComparisonOperand, t: &DbType| match t {
            DbType::JSONB => "jsonb",
            DbType::F32 | DbType::F64 if matches!(op, ComparisonOperand::Direct(_)) => "numeric",
            _ => crate::core::value_codec::pg_input_type_name(t),
        };
        Some(format!(
            "operator does not exist: {} {} {}",
            name(left_op, left_type),
            if op_str == "!=" { "<>" } else { op_str },
            name(right_op, right_type)
        ))
    }

    /// The refusal for a computed operand that is still of a different type
    /// from the other side once reconciliation and numeric promotion are done,
    /// or `None` when the types agree.
    ///
    /// The comparison reads both sides' bytes as the left side's type, so a
    /// mismatch left here is answered on bytes of the wrong type: text read as
    /// a date's four bytes matched nothing under `=` and everything under
    /// `<>`. PostgreSQL has no operator between types of different categories
    /// (`date = text`, `boolean = text`, `text = integer`), and gets the same
    /// message. Within a category, PostgreSQL compares (`numeric = integer`,
    /// `character = text`) and the engine cannot yet, so that pair is refused
    /// as not supported rather than said not to exist.
    fn evaluated_type_mismatch(
        op_str: &str,
        left_op: &ComparisonOperand,
        left_type: &DbType,
        right_op: &ComparisonOperand,
        right_type: &DbType,
    ) -> Option<String> {
        let evaluated = |o: &ComparisonOperand| matches!(o, ComparisonOperand::Evaluated(..));
        // `bit` and `bit varying` share one encoding and one comparator, so
        // either reads the other's bytes as its own.
        let bits = |t: &DbType| matches!(t, DbType::BIT | DbType::VARBIT);
        if left_type == right_type
            || (bits(left_type) && bits(right_type))
            || !(evaluated(left_op) || evaluated(right_op))
        {
            return None;
        }
        let category = |t: &DbType| match t {
            DbType::I8
            | DbType::I16
            | DbType::I32
            | DbType::I64
            | DbType::I128
            | DbType::U8
            | DbType::U16
            | DbType::U32
            | DbType::U64
            | DbType::U128
            | DbType::F32
            | DbType::F64
            | DbType::DECIMAL => Some("number"),
            DbType::STRING | DbType::CHAR => Some("text"),
            _ => None,
        };
        let name = crate::core::value_codec::pg_input_type_name;
        let op = if op_str == "!=" { "<>" } else { op_str };
        Some(match (category(left_type), category(right_type)) {
            (Some(a), Some(b)) if a == b => format!(
                "comparing a {} value with a computed {} value is not supported yet",
                name(if evaluated(left_op) { right_type } else { left_type }),
                name(if evaluated(left_op) { left_type } else { right_type }),
            ),
            _ => format!(
                "operator does not exist: {} {} {}",
                name(left_type),
                op,
                name(right_type)
            ),
        })
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

        // A text literal read as a document, refused with PostgreSQL's
        // message when it is not JSON.
        fn json_literal(literal: &MemoryBlock) -> Result<MemoryBlock, String> {
            let text = String::from_utf8_lossy(literal.into_slice());
            let bytes = crate::core::jsonb::encode_text(&text).map_err(|reason| {
                format!(
                    "invalid input syntax for type json: {} ({})",
                    crate::core::value_codec::quote_sql(&text),
                    reason
                )
            })?;
            let mut mb = MEMORY_POOL.acquire(bytes.len());
            mb.into_slice_mut().copy_from_slice(&bytes);
            Ok(mb)
        }

        // A fractional literal against an integer column has no exact integer
        // form; it is left as it is for `promote_numeric`, which compares the
        // two as floats, rather than refused.
        let float_vs_int = |lit: &DbType, col: &DbType| {
            matches!(lit, DbType::F32 | DbType::F64)
                && matches!(
                    col,
                    DbType::I8
                        | DbType::I16
                        | DbType::I32
                        | DbType::I64
                        | DbType::I128
                        | DbType::U8
                        | DbType::U16
                        | DbType::U32
                        | DbType::U64
                        | DbType::U128
                )
        };
        let keeps_fraction = |lit: &MemoryBlock, from: &DbType, to: &DbType| {
            float_vs_int(from, to)
                && crate::core::value_codec::coerce_literal(lit.into_slice(), from, to).is_none()
        };
        match (&left_op, &right_op) {
            (ComparisonOperand::Direct(lit), ComparisonOperand::DirectWithIndex(_, _))
                if left_type != right_type && keeps_fraction(lit, &left_type, &right_type) =>
            {
                Ok((left_op, right_op, left_type, right_type))
            }
            (ComparisonOperand::DirectWithIndex(_, _), ComparisonOperand::Direct(lit))
                if left_type != right_type && keeps_fraction(lit, &right_type, &left_type) =>
            {
                Ok((left_op, right_op, left_type, right_type))
            }
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
            // An uncast `->` or `#>` yields a document, and PostgreSQL resolves
            // a quoted literal compared with one as a document too, `::jsonb`
            // or not — the rewrite drops a cast written on a literal anyway.
            // So `doc->'a' = '1'` compares two documents, by value. The text
            // used to be left as it was and compared with the encoded bytes,
            // which could never be equal: `=` matched nothing and `<>` matched
            // every row.
            //
            // Text that is not JSON is refused, as PostgreSQL refuses it. That
            // includes `doc->'k' <> ''`, which this engine once used to ask
            // whether a key exists; `?` is the operator for that.
            (ComparisonOperand::Direct(lit), ComparisonOperand::PathWithIndex(..))
                if left_type == DbType::STRING && right_type == DbType::JSONB =>
            {
                let mb = json_literal(lit)?;
                Ok((ComparisonOperand::Direct(mb), right_op, DbType::JSONB, DbType::JSONB))
            }
            (ComparisonOperand::PathWithIndex(..), ComparisonOperand::Direct(lit))
                if left_type == DbType::JSONB && right_type == DbType::STRING =>
            {
                let mb = json_literal(lit)?;
                Ok((left_op, ComparisonOperand::Direct(mb), DbType::JSONB, DbType::JSONB))
            }
            // A literal meeting an evaluated expression takes the expression's
            // type, as it would a column's: `CASE … END = 1` over a boolean,
            // `… = '1010'` over a bit string. Except a fraction against an
            // integer, which `promote_numeric` compares as floats instead.
            (ComparisonOperand::Direct(lit), ComparisonOperand::Evaluated(..))
                if left_type != right_type && !keeps_fraction(lit, &left_type, &right_type) =>
            {
                let mb = coerce(lit, &left_type, &right_type)?;
                let t = right_type.clone();
                Ok((ComparisonOperand::Direct(mb), right_op, t.clone(), t))
            }
            (ComparisonOperand::Evaluated(..), ComparisonOperand::Direct(lit))
                if left_type != right_type && !keeps_fraction(lit, &right_type, &left_type) =>
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

            // `NUMERIC` arithmetic runs at the engine scale; see
            // `decimal_operands`. Temporal arithmetic never involves one.
            let temporal = matches!(
                math,
                MathOperation::IntervalBetween | MathOperation::ShiftForward | MathOperation::ShiftBackward
            );
            let (lhs, rhs, scales, math_type, result_type) = match if temporal {
                Ok(Err((lhs_op, rhs_op)))
            } else {
                self.decimal_operands(lhs_op, &lhs_type, rhs_op, &rhs_type)
            }? {
                Ok((l, r, ls, rs)) => (l, r, Some((ls, rs)), DbType::DECIMAL, DbType::DECIMAL),
                Err((l, r)) if temporal => (l, r, None, lhs_type.clone(), result_type),
                Err((l, r)) => {
                    let (l, r, t, _) = Self::promote_numeric(l, lhs_type.clone(), r, rhs_type.clone());
                    (l, r, None, t.clone(), t)
                }
            };

            let idx =
                self.query_transformer
                    .add_math_operation(math_type, lhs, rhs, math);
            if let Some((ls, rs)) = scales {
                self.query_transformer.set_last_operand_scales(ls, rs);
            }
            lhs_op = ComparisonOperand::Intermediate(idx);
            lhs_type = result_type;
        }
        Ok((lhs_op, lhs_type))
    }

    fn parse_term<'c>(
        &mut self,
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(ComparisonOperand, DbType), String> {
        let (mut lhs_op, mut lhs_type) = self.parse_factor(cols)?;
        while let Some(op_str) = self.peek_op(&["*", "/"]) {
            self.next();
            let (rhs_op, rhs_type) = self.parse_factor(cols)?;
            let math = if op_str == "*" {
                MathOperation::Multiply
            } else {
                MathOperation::Divide
            };
            // `NUMERIC` arithmetic runs at the engine scale; see
            // `decimal_operands`.
            let (lhs, rhs, scales) =
                match self.decimal_operands(lhs_op, &lhs_type, rhs_op, &rhs_type)? {
                    Ok((l, r, ls, rs)) => {
                        lhs_type = DbType::DECIMAL;
                        (l, r, Some((ls, rs)))
                    }
                    Err((l, r)) => {
                        let (l, r, t, _) =
                            Self::promote_numeric(l, lhs_type.clone(), r, rhs_type.clone());
                        lhs_type = t;
                        (l, r, None)
                    }
                };
            let idx =
                self.query_transformer
                    .add_math_operation(lhs_type.clone(), lhs, rhs, math);
            if let Some((ls, rs)) = scales {
                self.query_transformer.set_last_operand_scales(ls, rs);
            }
            lhs_op = ComparisonOperand::Intermediate(idx);
        }
        Ok((lhs_op, lhs_type))
    }

    /// Negates a literal, widening an unsigned one to the signed type that can
    /// hold it — `-1` is tokenized as an unsigned `1`, and `0u64 - 1` would
    /// wrap rather than be negative.
    fn negate_numeric_inner(n: &NumericValue) -> Option<NumericValue> {
        Some(match *n {
            NumericValue::I8(v) => NumericValue::I8(v.checked_neg()?),
            NumericValue::I16(v) => NumericValue::I16(v.checked_neg()?),
            NumericValue::I32(v) => NumericValue::I32(v.checked_neg()?),
            NumericValue::I64(v) => NumericValue::I64(v.checked_neg()?),
            NumericValue::I128(v) => NumericValue::I128(v.checked_neg()?),
            NumericValue::U8(v) => NumericValue::I16(-(v as i16)),
            NumericValue::U16(v) => NumericValue::I32(-(v as i32)),
            NumericValue::U32(v) => NumericValue::I64(-(v as i64)),
            NumericValue::U64(v) => NumericValue::I128(-(v as i128)),
            NumericValue::U128(v) => NumericValue::I128(-(i128::try_from(v).ok()?)),
            NumericValue::F32(v) => NumericValue::F32(-v),
            NumericValue::F64(v) => NumericValue::F64(-v),
        })
    }

    fn parse_factor<'c>(
        &mut self,
        cols: &SmallVec<[(Cow<'c, str>, MemoryBlock); 20]>,
    ) -> Result<(ComparisonOperand, DbType), String> {
        let token = self
            .next()
            .ok_or_else(|| "Unexpected end of token stream".to_string())?;

        match token {
            // A signed literal: `WHERE queryid = -8455582427799197754`. The
            // tokenizer emits the sign as its own operator, so the parser saw a
            // factor beginning with `Op("-")` and refused the statement — while
            // the identical positive literal parsed. A leading `+` is accepted
            // for symmetry and means nothing.
            Token::Op(ref op) if op == "-" || op == "+" => {
                let negate = op == "-";
                let Some(Token::Number(n)) = self.next() else {
                    return Err(format!("expected a number after '{}'", op));
                };
                let n = if negate {
                    Self::negate_numeric_inner(&n)
                        .ok_or_else(|| "numeric literal is out of range".to_string())?
                } else {
                    n
                };
                let db_type = numeric_value_to_db_type(&n);
                let mb = numeric_to_mb(&n);
                Ok((ComparisonOperand::Direct(mb), db_type))
            }
            Token::Number(n) => {
                let db_type = numeric_value_to_db_type(&n);
                let mb = numeric_to_mb(&n);
                Ok((ComparisonOperand::Direct(mb), db_type))
            }
            // Computed per row by the expression evaluator, in its own type
            // unless the comparison asks for another.
            Token::Evaluated(operand) => {
                let db_type = operand.db_type.clone();
                Ok((ComparisonOperand::Evaluated(operand, db_type.clone()), db_type))
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
                let mb = path.extract(cols[idx].1.into_slice()).unwrap_or_default();
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
                                        let mb = recast
                                            .extract(cols[idx].1.into_slice())
                                            .unwrap_or_default();
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

    /// Two integer columns of different widths (`I32` against `U8`) compare
    /// as numbers. This used to panic on an operand-size mismatch, and the
    /// test pinned the panic.
    #[test]
    fn test_column_vs_column_false() {
        let schema = create_schema();
        static COLUMNS: LazyLock<Box<SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>>> =
            LazyLock::new(|| Box::new(setup_test_columns()));
        let columns = &*COLUMNS;
        for (query, want) in [("id = age", false), ("id > age", true), ("age < id", true)] {
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
            let got = parser
                .evaluate_row(columns, &mut buffer.bool_buffer)
                .unwrap();
            // id = 42, age = 30.
            assert_eq!(got, want, "{query}");
        }
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
