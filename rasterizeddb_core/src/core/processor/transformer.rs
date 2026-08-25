use ahash::{HashSet, HashSetExt};
use itertools::Either;
use smallvec::SmallVec;

use crate::{
    core::{db_type::DbType, helpers::smallvec_extensions::SmallVecExtensions, row::{error::EvalError, logical::perform_comparison_operation, math::perform_math_operation}},
    memory_pool::MemoryBlock,
};

#[derive(Debug)]
pub struct ColumnTransformer {
    pub column_type: DbType,
    pub column_1: MemoryBlock,
    pub column_2: Option<MemoryBlock>,
    pub transformer_type: ColumnTransformerType,
    pub next: Option<Next>,

    pub result_store_index: Option<usize>,
    pub result_index_1: Option<usize>,
    pub result_index_2: Option<usize>,

    pub column_1_index: Option<usize>,
    pub column_2_index: Option<usize>,

    /// Set when the operand is a path into a document rather than the column
    /// itself. The index still names the *document* column — that is what the
    /// row fetches — and the path is taken from its bytes each row.
    pub column_1_path: Option<JsonPathRef>,
    pub column_2_path: Option<JsonPathRef>,

    /// Parentheses opening immediately before this comparison, and closing
    /// immediately after it.
    ///
    /// The boolean results arrive as a flat chain, which is enough to evaluate
    /// an unparenthesised expression by precedence but loses the grouping.
    /// Without it, `A AND (B OR C)` flattens to `(A AND B) OR C` — a wrong
    /// answer rather than a slow one, and exactly the shape `x IN (…)`
    /// desugars into.
    ///
    /// Counts rather than a nesting depth, because depth cannot tell sibling
    /// groups apart: `(A AND B) AND (C OR D)` and `(A AND B AND C OR D)` have
    /// identical depths at every position.
    pub group_opens: u8,
    pub group_closes: u8,
}

impl Clone for ColumnTransformer {
    fn clone(&self) -> Self {
        Self {
            column_type: self.column_type.clone(),
            column_1: self.column_1.clone(),
            column_2: self.column_2.clone(),
            transformer_type: self.transformer_type.clone(),
            next: self.next.clone(),
            result_store_index: self.result_store_index,
            result_index_1: self.result_index_1,
            result_index_2: self.result_index_2,
            column_1_index: self.column_1_index,
            column_2_index: self.column_2_index,
            column_1_path: self.column_1_path.clone(),
            column_2_path: self.column_2_path.clone(),
            group_opens: self.group_opens,
            group_closes: self.group_closes,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnTransformerType {
    MathOperation(MathOperation),
    ComparerOperation(ComparerOperation),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComparerOperation {
    Equals,
    NotEquals,
    Contains,
    StartsWith,
    EndsWith,
    Greater,
    GreaterOrEquals,
    Less,
    LessOrEquals,
    /// Case-insensitive counterparts, which `ILIKE` lowers to.
    ///
    /// They are separate operators rather than a flag on the existing ones on
    /// purpose: the fingerprint index hashes raw bytes, so `'ACME'` and
    /// `'acme'` hash differently. Making `Equals` itself case-insensitive would
    /// silently drop rows from filtered queries, whereas a distinct operator is
    /// simply not recognised by the index and falls back to a full scan.
    EqualsCaseInsensitive,
    ContainsCaseInsensitive,
    /// SQL `LIKE` with a pattern the prefix/suffix/substring operators cannot
    /// express — an interior `%`, or any `_`. Carries the pattern as its
    /// operand, wildcards intact.
    Like,
    /// `ILIKE` for the same shapes.
    LikeCaseInsensitive,
    StartsWithCaseInsensitive,
    EndsWithCaseInsensitive,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Next {
    And,
    Or,
}

#[derive(Debug, PartialEq, Clone)]
pub enum MathOperation {
    Add,
    Subtract,
    Multiply,
    Divide,
    Exponent,
    Root,
}

// A small trait to view "row columns" without cloning.
// Implemented for &[MemoryBlock] and for SmallVec of (name, MemoryBlock) rows used by QueryParser.
pub trait RowBlocks {
    fn mb_at(&self, idx: usize) -> &MemoryBlock;
    fn len(&self) -> usize;
}

impl RowBlocks for [MemoryBlock] {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        &self[idx]
    }
    #[inline]
    fn len(&self) -> usize {
        <[MemoryBlock]>::len(self)
    }
}

// Allow fixed-size arrays of MemoryBlock to be used directly as a row view in tests and callers.
impl<const N: usize> RowBlocks for [MemoryBlock; N] {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        &self[idx]
    }
    #[inline]
    fn len(&self) -> usize {
        N
    }
}

impl<'a> RowBlocks for SmallVec<[(std::borrow::Cow<'a, str>, MemoryBlock); 20]> {
    #[inline]
    fn mb_at(&self, idx: usize) -> &MemoryBlock {
        &self[idx].1
    }
    #[inline]
    fn len(&self) -> usize {
        SmallVec::len(self)
    }
}

impl ColumnTransformer {
    pub fn new(
        column_type: DbType,
        column_1: MemoryBlock,
        transformer_type: ColumnTransformerType,
        next: Option<Next>,
    ) -> Self {
        Self {
            column_type,
            column_1,
            column_2: None,
            transformer_type,
            next,
            result_store_index: None,
            result_index_1: None,
            result_index_2: None,
            column_1_path: None,
            column_2_path: None,
            column_1_index: None,
            column_2_index: None,
            group_opens: 0,
            group_closes: 0,
        }
    }

    // New: resolve inputs on the fly (no per-row cloning)
    #[inline]
    fn resolve_inputs<'a, R: RowBlocks + ?Sized>(
        &'a self,
        row: &'a R,
        inter: &'a [MemoryBlock],
    ) -> (&'a MemoryBlock, &'a MemoryBlock) {
        let left: &MemoryBlock = if let Some(r1) = self.result_index_1 {
            &inter[r1]
        } else if let Some(i1) = self.column_1_index {
            row.mb_at(i1)
        } else {
            &self.column_1
        };

        let right: &MemoryBlock = if let Some(r2) = self.result_index_2 {
            &inter[r2]
        } else if let Some(i2) = self.column_2_index {
            row.mb_at(i2)
        } else {
            self.column_2
                .as_ref()
                .expect("column_2 must be set before transform")
        };

        (left, right)
    }

    // New: transform using a row view
    #[inline]
    pub fn transform_on_row<R: RowBlocks + ?Sized>(
        &self,
        row: &R,
        inter: &[MemoryBlock],
    ) -> Result<Either<MemoryBlock, (bool, Option<Next>)>, EvalError> {
        let (left_mb, right_mb) = self.resolve_inputs(row, inter);

        // A path operand's bytes are the *document*; what the comparison wants
        // is the value inside it. Resolved here rather than in either
        // `update_column_*`, because the hot path never calls those:
        // `resolve_inputs` borrows straight out of the row, so this is the only
        // point where a value that has to be *computed* can live long enough to
        // be compared.
        let left_owned;
        let input1 = match &self.column_1_path {
            Some(path) => {
                left_owned = path.extract(left_mb.into_slice());
                left_owned.into_slice()
            }
            None => left_mb.into_slice(),
        };
        let right_owned;
        let input2 = match &self.column_2_path {
            Some(path) => {
                right_owned = path.extract(right_mb.into_slice());
                right_owned.into_slice()
            }
            None => right_mb.into_slice(),
        };

        match self.transformer_type {
            ColumnTransformerType::MathOperation(ref operation) => {
                if self.next.is_some() {
                    panic!("Next operation is not supported for MathOperation");
                }
                Ok(Either::Left(perform_math_operation(
                    input1,
                    input2,
                    &self.column_type,
                    operation,
                )?))
            }
            ColumnTransformerType::ComparerOperation(ref operation) => {
                // A cast path with nothing to read is SQL NULL, and a
                // comparison with NULL is false.
                //
                // `(doc->>'code')::bigint = 3` has to answer *something* for a
                // row whose `code` is absent, or is JSON `null`, or holds text
                // that is not a number. The extract has no value to give, so it
                // yields empty bytes — and a fixed-width comparison refuses a
                // zero-length operand outright, failing the whole statement
                // with "operand size does not match the column type being
                // evaluated". One unreadable row killed a query that every
                // other row answered fine.
                //
                // PostgreSQL's answer for the first two is NULL, and a
                // comparison against NULL is false — the row simply does not
                // match. It errors only on the third, which is a narrower
                // divergence than failing all three, and far narrower than the
                // alternative of substituting the type's zeroes: an absent key
                // would then *match* `= 0`, quietly pulling rows into a result
                // they have nothing to do with.
                //
                // Only for a **cast** path. An uncast one compares its bytes as
                // they are, and empty is a meaningful value there — it is how
                // `data->'k' <> ''` answers key existence.
                let unreadable = self
                    .column_1_path
                    .as_ref()
                    .is_some_and(|p| p.cast.is_some() && input1.is_empty())
                    || self
                        .column_2_path
                        .as_ref()
                        .is_some_and(|p| p.cast.is_some() && input2.is_empty());
                if unreadable {
                    return Ok(Either::Right((false, self.next.clone())));
                }
                Ok(Either::Right((
                    perform_comparison_operation(input1, input2, &self.column_type, operation)?,
                    self.next.clone(),
                )))
            }
        }
    }

    #[inline]
    pub fn setup_column_2(&mut self, column_2: MemoryBlock) {
        self.column_2 = Some(column_2);
    }

    #[inline]
    pub fn clear_column_2(&mut self) {
        self.column_2 = None;
    }

    #[inline]
    pub fn set_column_1_index(&mut self, index: usize) {
        self.column_1_index = Some(index);
    }

    #[inline]
    pub fn set_column_2_index(&mut self, index: usize) {
        self.column_2_index = Some(index);
    }

    // These remain for completeness, but are no longer used on the hot path.
    pub fn update_column_data_by_index(&mut self, new_columns: &[MemoryBlock]) {
        if let Some(idx1) = self.column_1_index {
            if self.result_index_1.is_none() {
                let document = new_columns
                    .get(idx1)
                    .unwrap_or_else(|| panic!("Column index {} out of bounds", idx1));
                self.column_1 = match &self.column_1_path {
                    Some(path) => path.extract(document.into_slice()),
                    None => document.clone(),
                };
            }
        }
        if let Some(idx2) = self.column_2_index {
            if self.result_index_2.is_none() {
                let document = new_columns
                    .get(idx2)
                    .unwrap_or_else(|| panic!("Column index {} out of bounds", idx2));
                let mb = match &self.column_2_path {
                    Some(path) => path.extract(document.into_slice()),
                    None => document.clone(),
                };
                self.column_2 = Some(mb);
            }
        }
    }

    pub fn get_column_dependencies(&self) -> SmallVec<[usize; 2]> {
        let mut deps = SmallVec::new();
        if let Some(i) = self.column_1_index {
            if self.result_index_1.is_none() {
                deps.push(i);
            }
        }
        if let Some(i) = self.column_2_index {
            if self.result_index_2.is_none() {
                deps.push(i);
            }
        }
        deps
    }

    pub fn has_direct_column_dependencies(&self) -> bool {
        (self.column_1_index.is_some() && self.result_index_1.is_none())
            || (self.column_2_index.is_some() && self.result_index_2.is_none())
    }

    pub fn get_used_column_indices(&self) -> Vec<usize> {
        let mut v = Vec::new();
        if let Some(i) = self.column_1_index {
            if self.result_index_1.is_none() {
                v.push(i);
            }
        }
        if let Some(i) = self.column_2_index {
            if self.result_index_2.is_none() {
                v.push(i);
            }
        }
        v
    }

    pub fn uses_column_index(&self, column_index: usize) -> bool {
        (self.column_1_index == Some(column_index) && self.result_index_1.is_none())
            || (self.column_2_index == Some(column_index) && self.result_index_2.is_none())
    }

    /// The hot path's per-row update.
    ///
    /// `new_data` is the column's own bytes. When the operand is a path into a
    /// document, those bytes are the *document* and the value compared is what
    /// the path reaches inside it — resolved here, per row, because that is
    /// where the row's bytes arrive.
    pub fn update_column_by_index(&mut self, column_index: usize, new_data: MemoryBlock) {
        if self.column_1_index == Some(column_index) && self.result_index_1.is_none() {
            self.column_1 = match &self.column_1_path {
                Some(path) => path.extract(new_data.into_slice()),
                None => new_data.clone(),
            };
        }
        if self.column_2_index == Some(column_index) && self.result_index_2.is_none() {
            self.column_2 = Some(match &self.column_2_path {
                Some(path) => path.extract(new_data.into_slice()),
                None => new_data,
            });
        }
    }

    pub fn debug_info(&self) -> String {
        format!(
            "ColumnTransformer {{ type: {:?}, col1_idx: {:?}, col2_idx: {:?}, res_idx1: {:?}, res_idx2: {:?}, store_idx: {:?}, next: {:?} }}",
            self.transformer_type,
            self.column_1_index,
            self.column_2_index,
            self.result_index_1,
            self.result_index_2,
            self.result_store_index,
            self.next
        )
    }

    pub fn validate(&self) -> Result<(), String> {
        match self.transformer_type {
            ColumnTransformerType::MathOperation(_) => {
                if self.column_2.is_none() && self.result_index_2.is_none() {
                    return Err("Math operations require second operand".into());
                }
            }
            ColumnTransformerType::ComparerOperation(_) => {
                if self.column_2.is_none() && self.result_index_2.is_none() {
                    return Err("Comparison operations require second operand".into());
                }
            }
        }
        Ok(())
    }

    pub fn clear_column_indices(&mut self) {
        self.column_1_index = None;
        self.column_2_index = None;
    }
}

// TODO: Consider refactoring ComparisonOperand to remove DirectWithIndex and keep only Column(idx) for columns.
#[derive(Debug, Clone)]
pub enum ComparisonOperand {
    Direct(MemoryBlock),
    DirectWithIndex(MemoryBlock, usize),
    /// A path into the document column at this index — `doc->>'k'`. The index
    /// is the document's, and the path is taken from its bytes per row.
    PathWithIndex(MemoryBlock, usize, JsonPathRef),
    Intermediate(usize),
}

/// A path taken from a document column, resolved per row.
///
/// Shared rather than cloned per row: the steps are fixed by the query and a
/// predicate is evaluated once per row over a whole scan.
#[derive(Debug, Clone)]
pub struct JsonPathRef {
    pub steps: std::sync::Arc<Vec<crate::core::jsonb::Step>>,
    pub as_text: bool,
    /// A `::` cast applied to what the path reaches.
    ///
    /// `(data->>'code')::bigint = 3` extracts text and asks for it to be
    /// compared as a number. Without this the text `"3"` was compared against
    /// the number `3` and matched nothing, so the type has to travel with the
    /// operand and be applied to the bytes before the comparison sees them.
    pub cast: Option<crate::core::db_type::DbType>,
}

impl JsonPathRef {
    /// The value this path reaches in `document`, as the bytes a comparison
    /// should use.
    ///
    /// An absent path gives empty bytes, which is what a NULL column gives —
    /// so `doc->>'missing' = 'x'` is false rather than an error, as it is in
    /// PostgreSQL.
    pub fn extract(&self, document: &[u8]) -> MemoryBlock {
        let Some(found) = crate::core::jsonb::get_path(document, &self.steps) else {
            return MemoryBlock::default();
        };
        let bytes: Vec<u8> = if self.as_text {
            match crate::core::jsonb::as_text(found) {
                Some(text) => text.into_bytes(),
                None => return MemoryBlock::default(),
            }
        } else {
            found.to_vec()
        };
        // Re-read as the cast's type, so the comparison happens in it.
        //
        // An unreadable value gives empty bytes, which is what an absent path
        // gives — `(doc->>'k')::bigint = 3` on a `k` that is not a number is
        // then false rather than an error, matching how the uncast path treats
        // a missing key.
        let bytes = match &self.cast {
            None => bytes,
            Some(target) => {
                let value = crate::core::rql::expr::Value::Data {
                    bytes,
                    db_type: crate::core::db_type::DbType::STRING,
                };
                match crate::core::rql::expr::coerce_to_public(value, target) {
                    crate::core::rql::expr::Value::Data { bytes, db_type }
                        if db_type == *target =>
                    {
                        bytes
                    }
                    _ => return MemoryBlock::default(),
                }
            }
        };
        let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
        mb.into_slice_mut().copy_from_slice(&bytes);
        mb
    }
}

impl From<MemoryBlock> for ComparisonOperand {
    fn from(mem: MemoryBlock) -> Self {
        ComparisonOperand::Direct(mem)
    }
}
impl From<usize> for ComparisonOperand {
    fn from(idx: usize) -> Self {
        ComparisonOperand::Intermediate(idx)
    }
}

impl ComparisonOperand {
    /// Writes this operand into a transformer's first slot.
    ///
    /// One place per side rather than one arm per *pairing*: the pairings were
    /// a cartesian product, and a new operand shape had to be added to every
    /// one of them or it would silently take a wildcard.
    fn apply_left(&self, transformer: &mut ColumnTransformer) {
        match self {
            ComparisonOperand::Direct(mb) => transformer.column_1 = mb.clone(),
            ComparisonOperand::DirectWithIndex(mb, index) => {
                transformer.column_1 = mb.clone();
                transformer.set_column_1_index(*index);
            }
            ComparisonOperand::PathWithIndex(mb, index, path) => {
                transformer.column_1 = mb.clone();
                transformer.set_column_1_index(*index);
                transformer.column_1_path = Some(path.clone());
            }
            ComparisonOperand::Intermediate(idx) => transformer.result_index_1 = Some(*idx),
        }
    }

    /// The same for the second slot.
    fn apply_right(&self, transformer: &mut ColumnTransformer) {
        match self {
            ComparisonOperand::Direct(mb) => transformer.setup_column_2(mb.clone()),
            ComparisonOperand::DirectWithIndex(mb, index) => {
                transformer.setup_column_2(mb.clone());
                transformer.set_column_2_index(*index);
            }
            ComparisonOperand::PathWithIndex(mb, index, path) => {
                transformer.setup_column_2(mb.clone());
                transformer.set_column_2_index(*index);
                transformer.column_2_path = Some(path.clone());
            }
            ComparisonOperand::Intermediate(idx) => transformer.result_index_2 = Some(*idx),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransformerPlan {
    transformers: SmallVec<[ColumnTransformer; 36]>,
    max_result_store_index: usize,
}

impl ComparisonOperand {
    pub fn get_memory_block(&self) -> &MemoryBlock {
        match self {
            ComparisonOperand::Direct(mb) => mb,
            ComparisonOperand::DirectWithIndex(mb, _) => mb,
            ComparisonOperand::PathWithIndex(mb, _, _) => mb,
            ComparisonOperand::Intermediate(_) => {
                panic!("Intermediate has no direct memory block")
            }
        }
    }
    pub fn get_column_index(&self) -> Option<usize> {
        match self {
            ComparisonOperand::DirectWithIndex(_, i) => Some(*i),
            ComparisonOperand::PathWithIndex(_, i, _) => Some(*i),
            _ => None,
        }
    }
    pub fn is_column(&self) -> bool {
        matches!(self, ComparisonOperand::DirectWithIndex(_, _))
    }
    pub fn is_intermediate(&self) -> bool {
        matches!(self, ComparisonOperand::Intermediate(_))
    }
}

#[derive(Debug)]
pub struct TransformerProcessor<'a> {
    pub transformers: &'a mut SmallVec<[ColumnTransformer; 36]>,
    pub intermediate_results: &'a mut SmallVec<[MemoryBlock; 20]>,

    // New: cache maximum result index once (set when building plan)
    max_result_store_index: usize,

    /// The `WHERE` clause's boolean structure, compiled to postfix once with
    /// the plan so the per-row path is a stack walk with no allocation.
    ///
    /// Empty when the clause has no parentheses, where the flat sum-of-products
    /// walk is already correct and cheaper.
    comparison_program: SmallVec<[EvalOp; 24]>,
    /// Parentheses opening before the next comparison to be added.
    pending_opens: u8,
}

/// One step of the compiled boolean program, in postfix order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalOp {
    /// Push the comparison result at this index.
    Value(u16),
    And,
    Or,
}

impl<'a> TransformerProcessor<'a> {
    pub fn new(
        transformers: &'a mut SmallVec<[ColumnTransformer; 36]>,
        intermediate_results: &'a mut SmallVec<[MemoryBlock; 20]>,
    ) -> Self {
        transformers.clear();
        intermediate_results.clear();
        Self {
            transformers,
            intermediate_results,
            max_result_store_index: 0,
            comparison_program: SmallVec::new(),
            pending_opens: 0,
        }
    }

    #[inline]
    fn update_max_result_index(&mut self, idx: usize) {
        if idx > self.max_result_store_index {
            self.max_result_store_index = idx;
        }
    }

    pub fn snapshot_plan(&self) -> TransformerPlan {
        TransformerPlan {
            transformers: self.transformers.clone(),
            max_result_store_index: self.max_result_store_index,
        }
    }

    pub fn load_plan(&mut self, plan: &TransformerPlan) {
        self.transformers.clear();
        self.transformers.extend(plan.transformers.iter().cloned());
        self.intermediate_results.clear();
        self.max_result_store_index = plan.max_result_store_index;
        // A worker reusing a compiled plan must recompile the boolean program
        // with it, or its evaluation silently falls back to the flat form.
        self.refresh_comparison_program();
        self.ensure_intermediate_capacity();
    }

    pub fn add_math_operation(
        &mut self,
        column_type: DbType,
        left_operand_in: impl Into<ComparisonOperand>,
        right_operand_in: impl Into<ComparisonOperand>,
        operation: MathOperation,
    ) -> usize {
        let result_store_idx = self.intermediate_results.len();

        let left_op = left_operand_in.into();
        let right_op = right_operand_in.into();

        let placeholder = MemoryBlock::default();
        let mut transformer = ColumnTransformer::new(
            column_type,
            placeholder.clone(),
            ColumnTransformerType::MathOperation(operation),
            None,
        );
        transformer.setup_column_2(placeholder);

        left_op.apply_left(&mut transformer);
        right_op.apply_right(&mut transformer);

        transformer.result_store_index = Some(result_store_idx);

        self.transformers.push_back(transformer);
        self.intermediate_results.push(MemoryBlock::default());
        self.update_max_result_index(result_store_idx);

        result_store_idx
    }

    /// Notes that a boolean group opens before the next comparison.
    pub fn open_group(&mut self) {
        self.pending_opens = self.pending_opens.saturating_add(1);
    }

    /// Notes that the boolean group closes after the comparison just added.
    pub fn close_group(&mut self) {
        // An opening that never got a comparison cancels out rather than
        // leaving the program unbalanced.
        if self.pending_opens > 0 {
            self.pending_opens -= 1;
            return;
        }
        for t in self.transformers.iter_mut().rev() {
            if matches!(t.transformer_type, ColumnTransformerType::ComparerOperation(_)) {
                t.group_closes = t.group_closes.saturating_add(1);
                break;
            }
        }
        self.refresh_comparison_program();
    }

    /// Compiles the boolean structure to postfix by shunting-yard, so the
    /// per-row evaluation is a stack walk rather than a re-parse.
    ///
    /// Rebuilt whenever the plan changes, so it can never fall out of step with
    /// the results it indexes.
    fn refresh_comparison_program(&mut self) {
        self.comparison_program.clear();

        let comparisons: SmallVec<[(u8, u8, Option<Next>); 24]> = self
            .transformers
            .iter()
            .filter(|t| matches!(t.transformer_type, ColumnTransformerType::ComparerOperation(_)))
            .map(|t| (t.group_opens, t.group_closes, t.next.clone()))
            .collect();

        // With no parentheses the flat walk is already correct, and skipping
        // the program avoids the stack entirely on the common path.
        if comparisons.iter().all(|(o, c, _)| *o == 0 && *c == 0) {
            return;
        }

        #[derive(Clone, Copy, PartialEq, Eq)]
        enum StackOp {
            LParen,
            And,
            Or,
        }

        fn precedence(op: StackOp) -> u8 {
            match op {
                // AND binds tighter than OR, as in SQL.
                StackOp::And => 2,
                StackOp::Or => 1,
                StackOp::LParen => 0,
            }
        }

        let mut ops: SmallVec<[StackOp; 16]> = SmallVec::new();
        let mut out: SmallVec<[EvalOp; 24]> = SmallVec::new();

        for (index, (opens, closes, next)) in comparisons.iter().enumerate() {
            for _ in 0..*opens {
                ops.push(StackOp::LParen);
            }

            out.push(EvalOp::Value(index as u16));

            for _ in 0..*closes {
                while let Some(top) = ops.pop() {
                    match top {
                        StackOp::LParen => break,
                        StackOp::And => out.push(EvalOp::And),
                        StackOp::Or => out.push(EvalOp::Or),
                    }
                }
            }

            let Some(connective) = next else { continue };
            let incoming = match connective {
                Next::And => StackOp::And,
                Next::Or => StackOp::Or,
            };
            while let Some(top) = ops.last().copied() {
                if top == StackOp::LParen || precedence(top) < precedence(incoming) {
                    break;
                }
                ops.pop();
                out.push(match top {
                    StackOp::And => EvalOp::And,
                    StackOp::Or => EvalOp::Or,
                    StackOp::LParen => unreachable!("guarded above"),
                });
            }
            ops.push(incoming);
        }

        while let Some(top) = ops.pop() {
            match top {
                // Unbalanced input: the parser rejects it before here, so
                // rather than guess, fall back to the flat walk.
                StackOp::LParen => {
                    self.comparison_program.clear();
                    return;
                }
                StackOp::And => out.push(EvalOp::And),
                StackOp::Or => out.push(EvalOp::Or),
            }
        }

        self.comparison_program = out;
    }

    pub fn add_comparison(
        &mut self,
        column_type: DbType,
        left: impl Into<ComparisonOperand>,
        right: impl Into<ComparisonOperand>,
        operation: ComparerOperation,
        next: Option<Next>,
    ) {
        let left_operand = left.into();
        let right_operand = right.into();

        let placeholder = MemoryBlock::default();
        let mut transformer = ColumnTransformer::new(
            column_type,
            placeholder.clone(),
            ColumnTransformerType::ComparerOperation(operation),
            next,
        );
        transformer.setup_column_2(placeholder);

        left_operand.apply_left(&mut transformer);
        right_operand.apply_right(&mut transformer);

        transformer.group_opens = self.pending_opens;
        self.pending_opens = 0;
        self.transformers.push_back(transformer);
        self.refresh_comparison_program();
    }

    // New: Ensure intermediate_results has enough capacity once (no per-row scans).
    #[inline]
    fn ensure_intermediate_capacity(&mut self) {
        if self.intermediate_results.len() <= self.max_result_store_index {
            self.intermediate_results.resize(
                self.max_result_store_index.saturating_add(1),
                MemoryBlock::default(),
            );
        }
    }

    pub fn replace_row_inputs(&mut self, _new_columns: &[MemoryBlock]) {
        self.ensure_intermediate_capacity();
    }

    // New: execute directly with a row view (no cloning of inputs)
    pub fn execute_row<R: RowBlocks + ?Sized>(
        &mut self,
        row: &R,
        comparison_results: &mut SmallVec<[(bool, Option<Next>); 20]>,
    ) -> Result<bool, EvalError> {
        comparison_results.clear();
        self.ensure_intermediate_capacity();

        for t in self.transformers.iter() {
            match t.transform_on_row(row, &self.intermediate_results)? {
                Either::Left(result) => {
                    if let Some(store_idx) = t.result_store_index {
                        if store_idx >= self.intermediate_results.len() {
                            self.intermediate_results
                                .resize(store_idx + 1, MemoryBlock::default());
                        }
                        self.intermediate_results[store_idx] = result;
                    }
                }
                Either::Right(comp) => {
                    comparison_results.push(comp);
                }
            }
        }

        Ok(self.evaluate_comparison_results(comparison_results))
    }

    fn evaluate_comparison_results(&self, results: &[(bool, Option<Next>)]) -> bool {
        if results.is_empty() {
            return false;
        }

        // No program means no parentheses, where the flat walk is correct.
        if self.comparison_program.is_empty() {
            return Self::evaluate_flat(results);
        }

        let mut stack: SmallVec<[bool; 16]> = SmallVec::new();
        for op in self.comparison_program.iter() {
            match op {
                EvalOp::Value(i) => match results.get(*i as usize) {
                    Some((v, _)) => stack.push(*v),
                    // The program indexes a result that was not produced. That
                    // means plan and results disagree, so the safe answer is
                    // the flat walk rather than a guess.
                    None => return Self::evaluate_flat(results),
                },
                EvalOp::And => {
                    let (Some(b), Some(a)) = (stack.pop(), stack.pop()) else {
                        return Self::evaluate_flat(results);
                    };
                    stack.push(a && b);
                }
                EvalOp::Or => {
                    let (Some(b), Some(a)) = (stack.pop(), stack.pop()) else {
                        return Self::evaluate_flat(results);
                    };
                    stack.push(a || b);
                }
            }
        }

        stack.pop().unwrap_or(false)
    }

    /// Sum-of-products over the chain, correct when no parentheses are present:
    /// `A AND B OR C` is `(A AND B) OR C` in SQL too.
    fn evaluate_flat(results: &[(bool, Option<Next>)]) -> bool {
        let mut current_and = true;

        for (value, next) in results.iter() {
            current_and &= *value;

            match next {
                Some(Next::And) => {}
                Some(Next::Or) => {
                    if current_and {
                        return true;
                    }
                    current_and = true; // reset for next group
                }
                None => {
                    if current_and {
                        return true;
                    }
                    break;
                }
            }
        }

        false
    }

    pub fn get_used_column_indices(&self) -> HashSet<usize> {
        let mut s = HashSet::new();
        for t in self.transformers.iter() {
            if let Some(i) = t.column_1_index {
                if t.result_index_1.is_none() {
                    s.insert(i);
                }
            }
            if let Some(i) = t.column_2_index {
                if t.result_index_2.is_none() {
                    s.insert(i);
                }
            }
        }
        s
    }

    pub fn has_column_dependencies(&self) -> bool {
        self.transformers
            .iter()
            .any(|t| t.has_direct_column_dependencies())
    }

    pub fn reset_intermediate_results(&mut self) {
        self.intermediate_results.clear();
        if self.max_result_store_index > 0 {
            self.intermediate_results
                .resize(self.max_result_store_index + 1, MemoryBlock::default());
        }
    }

    // Debug validation helpers (call in debug builds after building transformer list)
    pub fn debug_validate(&self) {
        #[cfg(debug_assertions)]
        {
            self.validate_intermediate_dependencies();
            self.validate_logic_links();
        }
    }

    #[cfg(debug_assertions)]
    fn validate_intermediate_dependencies(&self) {
        for (i, t) in self.transformers.iter().enumerate() {
            if let Some(r1) = t.result_index_1 {
                assert!(
                    r1 < self.intermediate_results.len()
                        || self
                            .transformers
                            .iter()
                            .any(|tt| tt.result_store_index == Some(r1)),
                    "Transformer {} references unknown intermediate {}",
                    i,
                    r1
                );
            }
            if let Some(r2) = t.result_index_2 {
                assert!(
                    r2 < self.intermediate_results.len()
                        || self
                            .transformers
                            .iter()
                            .any(|tt| tt.result_store_index == Some(r2)),
                    "Transformer {} references unknown intermediate {}",
                    i,
                    r2
                );
            }
        }
    }

    #[cfg(debug_assertions)]
    fn validate_logic_links(&self) {
        for (i, t) in self.transformers.iter().enumerate() {
            match t.transformer_type {
                ColumnTransformerType::MathOperation(_) => {
                    assert!(
                        t.next.is_none(),
                        "Math transformer {} unexpectedly has a logical next",
                        i
                    );
                }
                ColumnTransformerType::ComparerOperation(_) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::MEMORY_POOL;

    fn create_memory_block_from_f64(value: f64) -> MemoryBlock {
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

    // Helper for a row view
    struct TestRow([MemoryBlock; 2]);
    impl RowBlocks for TestRow {
        fn mb_at(&self, idx: usize) -> &MemoryBlock {
            &self.0[idx]
        }
        fn len(&self) -> usize {
            self.0.len()
        }
    }

    #[test]
    fn test_transform_on_row_math_add() {
        let i32_data_1 = create_memory_block_from_i32(10);
        let i32_data_2 = create_memory_block_from_i32(20);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1.clone(),
            ColumnTransformerType::MathOperation(MathOperation::Add),
            None,
        );
        transformer.setup_column_2(i32_data_2.clone());

        let row = TestRow([i32_data_1.clone(), i32_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter).unwrap() {
            Either::Left(result) => {
                let result_slice = result.into_slice();
                let result_value = i32::from_le_bytes(result_slice.try_into().unwrap());
                assert_eq!(result_value, 30);
            }
            _ => panic!("Expected MemoryBlock result"),
        }
    }

    #[test]
    fn test_transform_on_row_comparer_equals() {
        let i32_data_1 = create_memory_block_from_i32(42);
        let i32_data_2 = create_memory_block_from_i32(42);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1.clone(),
            ColumnTransformerType::ComparerOperation(ComparerOperation::Equals),
            None,
        );
        transformer.setup_column_2(i32_data_2.clone());

        let row = TestRow([i32_data_1.clone(), i32_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter).unwrap() {
            Either::Right(result) => {
                assert!(result.0);
            }
            _ => panic!("Expected bool result"),
        }
    }

    #[test]
    fn test_transform_on_row_comparer_not_equals() {
        let i32_data_1 = create_memory_block_from_i32(42);
        let i32_data_2 = create_memory_block_from_i32(43);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1.clone(),
            ColumnTransformerType::ComparerOperation(ComparerOperation::NotEquals),
            None,
        );
        transformer.setup_column_2(i32_data_2.clone());

        let row = TestRow([i32_data_1.clone(), i32_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter).unwrap() {
            Either::Right(result) => {
                assert!(result.0);
            }
            _ => panic!("Expected bool result"),
        }
    }

    #[test]
    #[should_panic(expected = "Next operation is not supported for MathOperation")]
    fn test_transform_on_row_math_with_next_panic() {
        let i32_data_1 = create_memory_block_from_i32(10);
        let i32_data_2 = create_memory_block_from_i32(20);

        let mut transformer = ColumnTransformer::new(
            DbType::I32,
            i32_data_1.clone(),
            ColumnTransformerType::MathOperation(MathOperation::Add),
            Some(Next::And),
        );
        transformer.setup_column_2(i32_data_2.clone());

        let row = TestRow([i32_data_1.clone(), i32_data_2.clone()]);
        let empty_inter = [];
        let _ = transformer.transform_on_row(&row, &empty_inter);
    }

    #[test]
    fn test_transform_on_row_comparer_contains() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("world");

        let mut transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1.clone(),
            ColumnTransformerType::ComparerOperation(ComparerOperation::Contains),
            None,
        );
        transformer.setup_column_2(string_data_2.clone());

        let row = TestRow([string_data_1.clone(), string_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter).unwrap() {
            Either::Right(result) => {
                assert!(result.0);
            }
            _ => panic!("Expected bool result"),
        }
    }

    #[test]
    fn test_transform_on_row_comparer_not_contains() {
        let string_data_1 = create_memory_block_from_string("Hello, world!");
        let string_data_2 = create_memory_block_from_string("planet");

        let mut transformer = ColumnTransformer::new(
            DbType::STRING,
            string_data_1.clone(),
            ColumnTransformerType::ComparerOperation(ComparerOperation::Contains),
            None,
        );
        transformer.setup_column_2(string_data_2.clone());

        let row = TestRow([string_data_1.clone(), string_data_2.clone()]);
        let empty_inter = [];
        match transformer.transform_on_row(&row, &empty_inter).unwrap() {
            Either::Right(result) => assert!(!result.0),
            _ => panic!("Expected bool result"),
        }
    }

    #[test]
    fn test_processor_complex_mixed_true() {
        // Row layout: [id(i32), name(str), age(u8), email(str), department(str), bank_balance(f64)]
        let id = create_memory_block_from_i32(42);
        let name = create_memory_block_from_string("John Doe");
        let age = create_memory_block_from_u8(30);
        let email = create_memory_block_from_string("john.doe@example.com");
        let department = create_memory_block_from_string("Engineering");
        let bank_balance = create_memory_block_from_f64(1000.43);
        let row = [
            id.clone(),
            name.clone(),
            age.clone(),
            email.clone(),
            department.clone(),
            bank_balance.clone(),
        ];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        // (id - 2 = 40 AND name CONTAINS 'John') OR (bank_balance >= 1000.43 AND department STARTSWITH 'Eng' AND email ENDSWITH 'example.com')
        let t_id_minus_2 = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::DirectWithIndex(id.clone(), 0),
            create_memory_block_from_i32(2),
            MathOperation::Subtract,
        );

        proc.add_comparison(
            DbType::I32,
            ComparisonOperand::Intermediate(t_id_minus_2),
            create_memory_block_from_i32(40),
            ComparerOperation::Equals,
            Some(Next::And),
        );

        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(name.clone(), 1),
            create_memory_block_from_string("John"),
            ComparerOperation::Contains,
            Some(Next::Or),
        );

        proc.add_comparison(
            DbType::F64,
            ComparisonOperand::DirectWithIndex(bank_balance.clone(), 5),
            create_memory_block_from_f64(1000.43),
            ComparerOperation::GreaterOrEquals,
            Some(Next::And),
        );

        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(department.clone(), 4),
            create_memory_block_from_string("Eng"),
            ComparerOperation::StartsWith,
            Some(Next::And),
        );

        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(email.clone(), 3),
            create_memory_block_from_string("example.com"),
            ComparerOperation::EndsWith,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let res = proc.execute_row(&row, &mut bool_buf).unwrap();
        assert!(res);
    }

    #[test]
    fn test_processor_intermediate_math_chain_true() {
        // Compute: ((10 + 5) * 2 - 10) / 1 = 20
        let a = create_memory_block_from_i32(10);
        let b = create_memory_block_from_i32(5);
        let c = create_memory_block_from_i32(2);
        let d = create_memory_block_from_i32(1);
        let row = [a.clone(), b.clone(), c.clone(), d.clone()];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        let add_idx = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::DirectWithIndex(a.clone(), 0),
            ComparisonOperand::DirectWithIndex(b.clone(), 1),
            MathOperation::Add,
        );
        let mul_idx = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::Intermediate(add_idx),
            ComparisonOperand::DirectWithIndex(c.clone(), 2),
            MathOperation::Multiply,
        );
        let sub_idx = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::Intermediate(mul_idx),
            create_memory_block_from_i32(10),
            MathOperation::Subtract,
        );
        let div_idx = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::Intermediate(sub_idx),
            ComparisonOperand::DirectWithIndex(d.clone(), 3),
            MathOperation::Divide,
        );

        proc.add_comparison(
            DbType::I32,
            ComparisonOperand::Intermediate(div_idx),
            create_memory_block_from_i32(20),
            ComparerOperation::Equals,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let res = proc.execute_row(&row, &mut bool_buf).unwrap();
        assert!(res);
    }

    #[test]
    fn test_processor_plan_snapshot_round_trip() {
        let lhs = create_memory_block_from_i32(40);
        let rhs = create_memory_block_from_i32(2);
        let row = [lhs.clone(), rhs.clone()];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        let add_idx = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::DirectWithIndex(lhs.clone(), 0),
            ComparisonOperand::DirectWithIndex(rhs.clone(), 1),
            MathOperation::Add,
        );
        proc.add_comparison(
            DbType::I32,
            ComparisonOperand::Intermediate(add_idx),
            create_memory_block_from_i32(42),
            ComparerOperation::Equals,
            None,
        );

        let plan = proc.snapshot_plan();

        let mut cloned_transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut cloned_inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut cloned_proc =
            TransformerProcessor::new(&mut cloned_transformers, &mut cloned_inter);
        cloned_proc.load_plan(&plan);

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let res = cloned_proc.execute_row(&row, &mut bool_buf).unwrap();
        assert!(res);
    }

    #[test]
    fn test_processor_or_groups_false() {
        // (id = 0 AND name CONTAINS 'Nope') OR (bank_balance < 1000.0 AND department STARTSWITH 'Sales')
        let id = create_memory_block_from_i32(42);
        let name = create_memory_block_from_string("John Doe");
        let email = create_memory_block_from_string("john.doe@example.com");
        let department = create_memory_block_from_string("Engineering");
        let bank_balance = create_memory_block_from_f64(1000.43);
        let row = [
            id.clone(),
            name.clone(),
            email.clone(),
            department.clone(),
            bank_balance.clone(),
        ];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        proc.add_comparison(
            DbType::I32,
            ComparisonOperand::DirectWithIndex(id.clone(), 0),
            create_memory_block_from_i32(0),
            ComparerOperation::Equals,
            Some(Next::And),
        );
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(name.clone(), 1),
            create_memory_block_from_string("Nope"),
            ComparerOperation::Contains,
            Some(Next::Or),
        );
        proc.add_comparison(
            DbType::F64,
            ComparisonOperand::DirectWithIndex(bank_balance.clone(), 4),
            create_memory_block_from_f64(1000.0),
            ComparerOperation::Less,
            Some(Next::And),
        );
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(department.clone(), 3),
            create_memory_block_from_string("Sales"),
            ComparerOperation::StartsWith,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let res = proc.execute_row(&row, &mut bool_buf).unwrap();
        assert!(!res);
    }

    #[test]
    fn test_processor_precedence_without_parentheses_true() {
        // name CONTAINS 'Nope' OR age*2 = 60 AND email ENDSWITH 'example.com' -> false OR (true AND true) => true
        let age = create_memory_block_from_u8(30);
        let email = create_memory_block_from_string("john.doe@example.com");
        let name = create_memory_block_from_string("John Doe");
        let two = create_memory_block_from_u8(2);
        let row = [name.clone(), age.clone(), email.clone(), two.clone()];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        // name CONTAINS 'Nope'
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(name.clone(), 0),
            create_memory_block_from_string("Nope"),
            ComparerOperation::Contains,
            Some(Next::Or),
        );

        // age*2 = 60
        let mul_idx = proc.add_math_operation(
            DbType::U8,
            ComparisonOperand::DirectWithIndex(age.clone(), 1),
            ComparisonOperand::DirectWithIndex(two.clone(), 3),
            MathOperation::Multiply,
        );
        proc.add_comparison(
            DbType::U8,
            ComparisonOperand::Intermediate(mul_idx),
            create_memory_block_from_u8(60),
            ComparerOperation::Equals,
            Some(Next::And),
        );

        // email ENDSWITH 'example.com'
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(email.clone(), 2),
            create_memory_block_from_string("example.com"),
            ComparerOperation::EndsWith,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let res = proc.execute_row(&row, &mut bool_buf).unwrap();
        assert!(res);
    }

    #[test]
    fn test_processor_used_column_indices() {
        // Build a plan that references id(0), name(1), bank_balance(5)
        let id = create_memory_block_from_i32(42);
        let name = create_memory_block_from_string("John Doe");
        let bank_balance = create_memory_block_from_f64(1000.43);
        let forty = create_memory_block_from_i32(40);
        let row = [
            id.clone(),
            name.clone(),
            create_memory_block_from_u8(1),
            create_memory_block_from_string("x@x"),
            create_memory_block_from_string("Eng"),
            bank_balance.clone(),
        ];

        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut proc = TransformerProcessor::new(&mut transformers, &mut inter);

        // id - 2 = 40
        let t_id_minus_2 = proc.add_math_operation(
            DbType::I32,
            ComparisonOperand::DirectWithIndex(id.clone(), 0),
            create_memory_block_from_i32(2),
            MathOperation::Subtract,
        );
        proc.add_comparison(
            DbType::I32,
            ComparisonOperand::Intermediate(t_id_minus_2),
            forty,
            ComparerOperation::Equals,
            Some(Next::Or),
        );

        // name CONTAINS 'John'
        proc.add_comparison(
            DbType::STRING,
            ComparisonOperand::DirectWithIndex(name.clone(), 1),
            create_memory_block_from_string("John"),
            ComparerOperation::Contains,
            Some(Next::Or),
        );

        // bank_balance >= 1000.0
        proc.add_comparison(
            DbType::F64,
            ComparisonOperand::DirectWithIndex(bank_balance.clone(), 5),
            create_memory_block_from_f64(1000.0),
            ComparerOperation::GreaterOrEquals,
            None,
        );

        let mut bool_buf: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let _ = proc.execute_row(&row, &mut bool_buf).unwrap();

        let used = proc.get_used_column_indices();
        assert!(used.contains(&0));
        assert!(used.contains(&1));
        assert!(used.contains(&5));
        assert_eq!(used.len(), 3);
    }
}
