use ahash::{HashSet, HashSetExt};
use itertools::Either;
use smallvec::SmallVec;

use crate::{
    core::{
        db_type::DbType,
        helpers::smallvec_extensions::SmallVecExtensions,
        row::{
            error::EvalError, logical::perform_comparison_operation, math::perform_math_operation,
        },
    },
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
    /// A `::` cast applied to the left operand, as `(from, to)`.
    ///
    /// Kept beside the path rather than folded into it because a cast can apply
    /// to an ordinary column — `hardware_status::text` — where there is no
    /// document and no path. Resolved per row in the same places the path is,
    /// which is the only point at which a computed value lives long enough to
    /// be compared.
    pub column_1_cast: Option<(DbType, DbType)>,
    pub column_2_path: Option<JsonPathRef>,
    /// A `::` cast applied to the right operand — see [`Self::column_1_cast`].
    pub column_2_cast: Option<(DbType, DbType)>,

    /// The declared scale of a `NUMERIC(p, s)` column read as the left
    /// operand, when the operation needs it at the engine scale instead.
    ///
    /// Set by the planner only where the other side is at the engine scale —
    /// arithmetic, which computes there, and a comparison against an
    /// arithmetic result or a column of another scale. A plain `v > 50` keeps
    /// comparing the stored bytes against a literal encoded at `s`, which is
    /// what the filter index hashes. Lifted per row into a stack buffer.
    pub column_1_scale: Option<u32>,
    /// The same for the right operand.
    pub column_2_scale: Option<u32>,

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

    /// A hashed `IN` / `NOT IN` list standing in for a run of `col = v`
    /// comparisons ORed together (or `col <> v` ANDed). See [`InSet`]: when set,
    /// the comparison answers by one lookup of the left operand.
    pub in_set: Option<std::sync::Arc<InSet>>,

    /// The key list of a `?|` / `?&` whose right operand is a literal, split
    /// once when the predicate is planned. Without it every row re-parsed the
    /// `'{a,b}'` text and allocated a `String` per key. Never set when the
    /// list is read from a column, since that one differs per row.
    pub key_list: Option<std::sync::Arc<crate::core::pg_ops::KeyList>>,

    /// `NOT` over this comparison: true where the comparison is false.
    ///
    /// Only for an operator that has no complement to rewrite it to — `NOT
    /// (t LIKE 'p%')`, `NOT (doc ? 'k')` — since `NOT (a = 1)` is written
    /// `a != 1` before it gets here. NULL stays NULL: a path that reaches
    /// nothing answers false whether or not this is set, and a nullable
    /// column is guarded by its companion in the rewritten clause.
    pub negated: bool,
    /// `IS NULL` (`Some(true)`) or `IS NOT NULL` (`Some(false)`) over a
    /// path, which answers by whether the path reached a value rather than by
    /// comparing one. A column's NULL is its companion's, so this is never set
    /// over a column.
    pub null_test: Option<bool>,
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
            column_1_cast: self.column_1_cast.clone(),
            column_2_cast: self.column_2_cast.clone(),
            column_2_path: self.column_2_path.clone(),
            column_1_scale: self.column_1_scale,
            column_2_scale: self.column_2_scale,
            group_opens: self.group_opens,
            group_closes: self.group_closes,
            in_set: self.in_set.clone(),
            key_list: self.key_list.clone(),
            negated: self.negated,
            null_test: self.null_test,
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
    /// SQL `SIMILAR TO`, carrying its pattern as the operand.
    ///
    /// A separate operator from `Like` rather than a flag on it: the two
    /// pattern languages differ in more than syntax — `SIMILAR TO` has
    /// alternation and quantifiers — and the reduction `LIKE` gets in
    /// `predicate_rewrite`, where `'ab%'` becomes a prefix test the index can
    /// use, is not sound for a pattern that can also say `(ab|cd)%`.
    SimilarTo,
    /// A PostgreSQL structural operator — `@>`, `<@`, `&&`, `<<=`, `@@` and
    /// the rest — see [`crate::core::pg_ops`].
    ///
    /// These carry the **right operand's declared type** because, unlike every
    /// operator above, they are not symmetric in it: `range @> 100` compares a
    /// range against a bare integer, and the answer depends on knowing that the
    /// right side is an element rather than another range. The planner knows
    /// that when it builds the transformer; the evaluator would have to guess.
    Pg {
        op: crate::core::pg_ops::PgOp,
        right_type: crate::core::db_type::DbType,
    },
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
    /// `timestamp - timestamp`, which yields an `INTERVAL` rather than a
    /// number. A separate operation rather than a flag on `Subtract`, because
    /// the two produce different *widths* — eight bytes against sixteen — and
    /// the evaluator checks that before it does anything else.
    IntervalBetween,
    /// `timestamp + interval` and `timestamp - interval`, which yield a
    /// timestamp. The months and days of the interval move the calendar, so
    /// this cannot be integer addition on microseconds.
    ShiftForward,
    ShiftBackward,
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
            column_1_cast: None,
            column_2_cast: None,
            column_2_path: None,
            column_1_index: None,
            column_2_index: None,
            column_1_scale: None,
            column_2_scale: None,
            group_opens: 0,
            group_closes: 0,
            in_set: None,
            key_list: None,
            negated: false,
            null_test: None,
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
        //
        // A path that reaches nothing is SQL NULL, noted here and handed on as
        // empty bytes, which is what the arithmetic below always received.
        let mut null_operand = false;
        let left_owned;
        let input1 = match (&self.column_1_path, &self.column_1_cast) {
            (Some(path), _) => match path.extract(left_mb.into_slice()) {
                Some(found) => {
                    left_owned = found;
                    left_owned.into_slice()
                }
                None => {
                    null_operand = true;
                    &[][..]
                }
            },
            (None, Some((from, to))) => {
                left_owned = cast_block(left_mb, from, to);
                left_owned.into_slice()
            }
            (None, None) => left_mb.into_slice(),
        };
        let right_owned;
        let input2 = match (&self.column_2_path, &self.column_2_cast) {
            (Some(path), _) => match path.extract(right_mb.into_slice()) {
                Some(found) => {
                    right_owned = found;
                    right_owned.into_slice()
                }
                None => {
                    null_operand = true;
                    &[][..]
                }
            },
            (None, Some((from, to))) => {
                right_owned = cast_block(right_mb, from, to);
                right_owned.into_slice()
            }
            (None, None) => right_mb.into_slice(),
        };

        // A `NUMERIC(p, s)` operand lifted to the engine scale, where the
        // other side already is. In a stack buffer: this is per row.
        let mut lifted_1 = [0u8; 16];
        let input1 = match self.column_1_scale {
            Some(scale) => lift_to_engine_scale(input1, scale, &mut lifted_1)?,
            None => input1,
        };
        let mut lifted_2 = [0u8; 16];
        let input2 = match self.column_2_scale {
            Some(scale) => lift_to_engine_scale(input2, scale, &mut lifted_2)?,
            None => input2,
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
            // `IS [NOT] NULL` over a path: the one comparison whose answer
            // is about the NULL rather than spoiled by it.
            ColumnTransformerType::ComparerOperation(_) if self.null_test.is_some() => {
                let want_null = self.null_test.expect("guarded");
                Ok(Either::Right((null_operand == want_null, self.next.clone())))
            }
            // A comparison with SQL NULL is NULL, and a WHERE clause keeps only
            // the rows where it is true — so the row does not match, and a
            // negated comparison does not match either: NOT of NULL is NULL.
            // That one rule covers `=`, `<>`, the ordered operators, LIKE and
            // the structural operators alike. An absent key used to read as
            // the empty string, so `doc->>'a' <> 'z'` and `doc->>'a' LIKE '%'`
            // returned every row with no `a` at all.
            //
            // A cast path that cannot be read is NULL too: `(doc->>'code')::
            // bigint = 3` on a `code` that is absent, JSON `null`, or text
            // that is not a number. Answering false, rather than failing the
            // statement with an operand-size error, keeps one unreadable row
            // from failing a query every other row answered.
            ColumnTransformerType::ComparerOperation(_) if null_operand => {
                Ok(Either::Right((false, self.next.clone())))
            }
            ColumnTransformerType::ComparerOperation(_) if self.in_set.is_some() => {
                let set = self.in_set.as_ref().expect("guarded");
                Ok(Either::Right((set.matches(input1)? != self.negated, self.next.clone())))
            }
            ColumnTransformerType::ComparerOperation(ref operation) => {
                // `?|` / `?&` against a literal list split at plan time; the
                // same answer `pg_ops::evaluate` gives from the text, including
                // `None` for a malformed document becoming an error.
                let holds = if let (Some(keys), ComparerOperation::Pg { op, .. }) =
                    (&self.key_list, operation)
                {
                    crate::core::pg_ops::key_exists_in_list(*op, input1, keys)
                        .ok_or(EvalError::UnsupportedType)?
                } else {
                    perform_comparison_operation(input1, input2, &self.column_type, operation)?
                };
                // `NOT` over an operator with no complementary operator —
                // LIKE, `?`, `@>` — is applied here, after the NULL test
                // above, so it negates only a true or false answer.
                Ok(Either::Right((holds != self.negated, self.next.clone())))
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
                self.column_1 = match (&self.column_1_path, &self.column_1_cast) {
                    (Some(path), _) => path.extract(document.into_slice()).unwrap_or_default(),
                    (None, Some((from, to))) => cast_block(document, from, to),
                    (None, None) => document.clone(),
                };
            }
        }
        if let Some(idx2) = self.column_2_index {
            if self.result_index_2.is_none() {
                let document = new_columns
                    .get(idx2)
                    .unwrap_or_else(|| panic!("Column index {} out of bounds", idx2));
                let mb = match &self.column_2_path {
                    Some(path) => path.extract(document.into_slice()).unwrap_or_default(),
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
            self.column_1 = match (&self.column_1_path, &self.column_1_cast) {
                (Some(path), _) => path.extract(new_data.into_slice()).unwrap_or_default(),
                (None, Some((from, to))) => cast_block(&new_data, from, to),
                (None, None) => new_data.clone(),
            };
        }
        if self.column_2_index == Some(column_index) && self.result_index_2.is_none() {
            self.column_2 = Some(match (&self.column_2_path, &self.column_2_cast) {
                (Some(path), _) => path.extract(new_data.into_slice()).unwrap_or_default(),
                (None, Some((from, to))) => cast_block(&new_data, from, to),
                (None, None) => new_data,
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
    /// A `::` cast over the column at this index, as `(from, to)`.
    ///
    /// `a::text = 'x'` compares what the column *renders as*, which for every
    /// type but `STRING` is not its stored bytes. Without this the cast was
    /// refused outright — "a `::` cast over this expression is not supported
    /// yet" — for a comparison that is ordinary SQL.
    CastWithIndex(MemoryBlock, usize, DbType, DbType),
    Intermediate(usize),
    /// An arithmetic result converted `(from, to)` before use — an integer
    /// sub-expression meeting a `NUMERIC` one, as in `v + 1 * 2`, where the
    /// `1 * 2` is an integer and the addition runs at the engine decimal
    /// scale. Without it the pair was refused: "a I32 operand cannot be
    /// combined with a NUMERIC one here".
    IntermediateCast(usize, DbType, DbType),
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
    /// should use, or `None` for SQL NULL.
    ///
    /// NULL is a separate answer, not empty bytes. `->>` yields NULL for an
    /// absent path, for a JSON `null`, and for a step into the wrong kind of
    /// container, and every comparison with NULL is not true. Empty bytes
    /// cannot carry that: the JSON string `""` also extracts to them, and
    /// `{"a":""}` must still match `doc->>'a' = ''` while `{}` must not.
    /// `->` yields NULL only for an absent path; a JSON `null` there is the
    /// document `null`, a value like any other.
    pub fn extract(&self, document: &[u8]) -> Option<MemoryBlock> {
        let found = crate::core::jsonb::get_path(document, &self.steps)?;
        // The uncast forms run once per row of a `doc->>'k' = 'v'` scan, so
        // they copy once, from the document (or the rendered text) straight
        // into the pooled block. They used to build a `Vec` — or a `String`
        // for `->>` — and then copy that again.
        if self.cast.is_none() {
            if !self.as_text {
                return Some(pooled_copy(found));
            }
            return EXTRACT_SCRATCH.with(|scratch| {
                let mut scratch = scratch.borrow_mut();
                // JSON `null` under `->>` is SQL NULL.
                let block = crate::core::jsonb::as_text_into(found, &mut scratch).map(pooled_copy);
                // One huge rendered container must not pin its buffer to this
                // worker for the rest of the process.
                if scratch.capacity() > EXTRACT_SCRATCH_KEEP {
                    *scratch = Vec::new();
                }
                block
            });
        }
        let bytes: Vec<u8> = if self.as_text {
            crate::core::jsonb::as_text(found)?.into_bytes()
        } else {
            found.to_vec()
        };
        // Re-read as the cast's type, so the comparison happens in it.
        //
        // A value that does not read as that type is NULL as well — so
        // `(doc->>'k')::bigint = 3` on a `k` that is not a number is false
        // rather than an error. PostgreSQL raises an error there; failing a
        // whole scan over one row's value was judged the worse divergence.
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
                    _ => return None,
                }
            }
        };
        Some(pooled_copy(&bytes))
    }
}

thread_local! {
    /// Where `->>` renders a non-string value before it is copied into the
    /// comparison's block; reused across rows rather than allocated per row.
    static EXTRACT_SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// The most [`EXTRACT_SCRATCH`] keeps between rows.
const EXTRACT_SCRATCH_KEEP: usize = 64 * 1024;

#[inline]
fn pooled_copy(bytes: &[u8]) -> MemoryBlock {
    let mut mb = crate::memory_pool::MEMORY_POOL.acquire(bytes.len());
    mb.into_slice_mut().copy_from_slice(bytes);
    mb
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
            ComparisonOperand::CastWithIndex(mb, index, from, to) => {
                transformer.column_1 = mb.clone();
                transformer.set_column_1_index(*index);
                transformer.column_1_cast = Some((from.clone(), to.clone()));
            }
            ComparisonOperand::Intermediate(idx) => transformer.result_index_1 = Some(*idx),
            ComparisonOperand::IntermediateCast(idx, from, to) => {
                transformer.result_index_1 = Some(*idx);
                transformer.column_1_cast = Some((from.clone(), to.clone()));
            }
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
            ComparisonOperand::CastWithIndex(mb, index, from, to) => {
                transformer.setup_column_2(mb.clone());
                transformer.set_column_2_index(*index);
                transformer.column_2_cast = Some((from.clone(), to.clone()));
            }
            ComparisonOperand::Intermediate(idx) => transformer.result_index_2 = Some(*idx),
            ComparisonOperand::IntermediateCast(idx, from, to) => {
                transformer.result_index_2 = Some(*idx);
                transformer.column_2_cast = Some((from.clone(), to.clone()));
            }
        }
    }
}

/// Converts a column's bytes to the type a `::` cast names.
///
/// A cast the codec cannot honour yields the bytes unchanged. That is
/// deliberate: the comparison then fails on a width or a value mismatch and
/// answers `false` for that row, where returning an error would fail a whole
/// scan over one unconvertible value.
/// A `NUMERIC` stored at `scale`, moved to the engine scale as
/// `expr::to_engine_scale` moves it: exact upward, truncated downward. Bytes
/// that are not a decimal are passed through for the operation to refuse.
#[inline]
fn lift_to_engine_scale<'a>(
    input: &'a [u8],
    scale: u32,
    buf: &'a mut [u8; 16],
) -> Result<&'a [u8], EvalError> {
    let Ok(raw) = <[u8; 16]>::try_from(input) else {
        return Ok(input);
    };
    let lifted = crate::core::rql::expr::rescale_decimal(
        i128::from_le_bytes(raw),
        scale,
        crate::core::value_codec::DECIMAL_SCALE,
    )
    .ok_or(EvalError::ArithmeticOverflow)?;
    *buf = lifted.to_le_bytes();
    Ok(&buf[..])
}

fn cast_block(source: &MemoryBlock, from: &DbType, to: &DbType) -> MemoryBlock {
    if from == to {
        return source.clone();
    }
    let bytes = source.into_slice();
    // A cast to text renders the value the way the client would see it, which
    // for the types stored in PostgreSQL's own layout is not their bytes.
    let converted = if matches!(to, DbType::STRING) {
        crate::core::rql::subquery::value_to_literal(from, bytes).map(|literal| {
            crate::core::value_codec::unquote_sql(&literal)
                .to_string()
                .into_bytes()
        })
    } else {
        crate::core::value_codec::coerce_literal(bytes, from, to)
    };
    match converted {
        Some(converted) => {
            let mut mb = crate::memory_pool::MEMORY_POOL.acquire(converted.len());
            mb.into_slice_mut().copy_from_slice(&converted);
            mb
        }
        None => source.clone(),
    }
}

/// The fewest comparisons a run must hold before it becomes an [`InSet`].
///
/// Below this the chain is a handful of fixed-width compares, which a hash and
/// a probe do not beat; see `in_set_bench` for the measurement.
pub const IN_SET_MIN_VALUES: usize = 4;

/// A hashed `x IN (v1, …, vN)` — or, negated, `x NOT IN (…)`.
///
/// `IN` is desugared to `(x = v1 OR … OR x = vN)` and each literal is coerced
/// by the planner exactly as `x = literal` would be — declared NUMERIC scale,
/// text re-encoded at the column's type, UUID/INET/timestamp layouts — so the
/// values here are *already the bytes the comparison would have used*. The set
/// is built from those, after planning, which is what keeps it from ever
/// disagreeing with the chain it replaces. Evaluation then costs one hash and
/// one probe per row instead of N comparisons with no short-circuit.
///
/// Membership is equality *as the evaluator defines it*: byte equality for
/// every type admitted here, except floats, which follow PostgreSQL (`-0 = +0`,
/// `NaN = NaN`) and are canonicalised on both sides. Types whose equality is
/// not byte equality (the wire-layout types, whose ordering lives in
/// `compare_bytes_as_type`) are never collapsed.
#[derive(Debug)]
pub struct InSet {
    negated: bool,
    key: InSetKey,
    small: ahash::HashSet<u128>,
    bytes: ahash::HashSet<Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InSetKey {
    /// A fixed-width value of at most 16 bytes, keyed as a zero-extended `u128`.
    Fixed(usize),
    /// A fixed-width value wider than 16 bytes (`INET`).
    FixedBytes(usize),
    Float32,
    Float64,
    /// Variable width: the bytes themselves.
    Bytes,
}

impl InSetKey {
    fn of(db_type: &DbType) -> Option<InSetKey> {
        use crate::core::value_codec::{INET_LEN, UUID_LEN};
        Some(match db_type {
            DbType::I8 | DbType::U8 | DbType::BOOL => InSetKey::Fixed(1),
            DbType::I16 | DbType::U16 => InSetKey::Fixed(2),
            DbType::I32 | DbType::U32 | DbType::CHAR => InSetKey::Fixed(4),
            DbType::I64 | DbType::U64 | DbType::DATETIME => InSetKey::Fixed(8),
            DbType::I128 | DbType::U128 | DbType::DECIMAL => InSetKey::Fixed(16),
            DbType::UUID => InSetKey::Fixed(UUID_LEN),
            DbType::INET => InSetKey::FixedBytes(INET_LEN),
            DbType::F32 => InSetKey::Float32,
            DbType::F64 => InSetKey::Float64,
            DbType::STRING => InSetKey::Bytes,
            _ => return None,
        })
    }

    /// The `u128` key for a value, or `None` when it has the wrong width — the
    /// comparison it replaces would have failed with a size mismatch.
    #[inline(always)]
    fn small_key(&self, v: &[u8]) -> Option<u128> {
        match *self {
            InSetKey::Fixed(width) => {
                if v.len() != width {
                    return None;
                }
                let mut buf = [0u8; 16];
                buf[..width].copy_from_slice(v);
                Some(u128::from_le_bytes(buf))
            }
            InSetKey::Float32 => {
                let a: [u8; 4] = v.try_into().ok()?;
                let f = f32::from_le_bytes(a);
                let f = if f == 0.0 {
                    0.0f32
                } else if f.is_nan() {
                    f32::NAN
                } else {
                    f
                };
                Some(f.to_bits() as u128)
            }
            InSetKey::Float64 => {
                let a: [u8; 8] = v.try_into().ok()?;
                let f = f64::from_le_bytes(a);
                let f = if f == 0.0 {
                    0.0f64
                } else if f.is_nan() {
                    f64::NAN
                } else {
                    f
                };
                Some(f.to_bits() as u128)
            }
            InSetKey::FixedBytes(_) | InSetKey::Bytes => None,
        }
    }
}

impl InSet {
    /// A set over `values` encoded as `db_type`, or `None` when that type is not
    /// one whose equality this can reproduce, or a value is not well formed.
    pub fn build<'v>(
        db_type: &DbType,
        values: impl Iterator<Item = &'v [u8]>,
        negated: bool,
    ) -> Option<InSet> {
        let key = InSetKey::of(db_type)?;
        let mut small = ahash::HashSet::default();
        let mut bytes = ahash::HashSet::default();
        for v in values {
            match key {
                InSetKey::Bytes => {
                    bytes.insert(v.to_vec());
                }
                InSetKey::FixedBytes(width) => {
                    if v.len() != width {
                        return None;
                    }
                    bytes.insert(v.to_vec());
                }
                _ => {
                    small.insert(key.small_key(v)?);
                }
            }
        }
        Some(InSet {
            negated,
            key,
            small,
            bytes,
        })
    }

    /// Number of distinct values.
    pub fn len(&self) -> usize {
        self.small.len() + self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the comparison chain this replaces holds for `value`.
    #[inline]
    pub fn matches(&self, value: &[u8]) -> Result<bool, EvalError> {
        let found = match self.key {
            InSetKey::Bytes => self.bytes.contains(value),
            InSetKey::FixedBytes(width) => {
                if value.len() != width {
                    return Err(EvalError::OperandSizeMismatch);
                }
                self.bytes.contains(value)
            }
            _ => match self.key.small_key(value) {
                Some(k) => self.small.contains(&k),
                None => return Err(EvalError::OperandSizeMismatch),
            },
        };
        Ok(found != self.negated)
    }
}

/// Whether `t` is a `column = constant` (or `<>`) comparison a run can absorb,
/// and if so its operator (`false` for `=`, `true` for `<>`), column and type.
fn in_set_member(t: &ColumnTransformer) -> Option<(bool, usize, &DbType)> {
    let ColumnTransformerType::ComparerOperation(op) = &t.transformer_type else {
        return None;
    };
    let negated = match op {
        ComparerOperation::Equals => false,
        ComparerOperation::NotEquals => true,
        _ => return None,
    };
    if t.in_set.is_some()
        || t.negated
        || t.null_test.is_some()
        || t.result_index_1.is_some()
        || t.column_1_path.is_some()
        || t.column_1_cast.is_some()
        || t.column_1_scale.is_some()
        || t.column_2_index.is_some()
        || t.result_index_2.is_some()
        || t.column_2_path.is_some()
        || t.column_2_cast.is_some()
        || t.column_2_scale.is_some()
        || t.column_2.is_none()
    {
        return None;
    }
    let column = t.column_1_index?;
    InSetKey::of(&t.column_type)?;
    Some((negated, column, &t.column_type))
}

/// Replaces every parenthesised run of `col = v1 OR … OR col = vN` (or `col <>
/// v1 AND …`) over one column with a single [`InSet`] comparison.
///
/// Only a run that is *exactly* one parenthesised group is taken: its first
/// comparison opens a group, its last closes one, and nothing inside opens or
/// closes another — so the group's parentheses are the run's own and can be
/// dropped with it. Everything about the comparisons except the constant must
/// be identical (column, type, operator, no path, cast or scale lift), and the
/// connective between them must be `OR` for `=` and `AND` for `<>`; anything
/// else is left as it was. Returns whether anything changed.
pub fn collapse_in_sets(transformers: &mut SmallVec<[ColumnTransformer; 36]>) -> bool {
    let mut changed = false;
    let mut i = 0usize;
    while i < transformers.len() {
        let Some((negated, column, db_type)) = in_set_member(&transformers[i]) else {
            i += 1;
            continue;
        };
        if transformers[i].group_opens == 0 || transformers[i].group_closes != 0 {
            i += 1;
            continue;
        }
        let db_type = db_type.clone();
        let link = if negated { Next::And } else { Next::Or };
        let mut end = None;
        let mut j = i;
        loop {
            let t = &transformers[j];
            if j > i {
                match in_set_member(t) {
                    Some((n, c, ty)) if n == negated && c == column && *ty == db_type => {}
                    _ => break,
                }
                if t.group_opens != 0 {
                    break;
                }
            }
            if t.group_closes != 0 {
                end = Some(j);
                break;
            }
            if t.next.as_ref() != Some(&link) {
                break;
            }
            j += 1;
            if j >= transformers.len() {
                break;
            }
        }
        let Some(end) = end else {
            i += 1;
            continue;
        };
        if end + 1 - i < IN_SET_MIN_VALUES {
            i = end + 1;
            continue;
        }
        let set = InSet::build(
            &db_type,
            transformers[i..=end]
                .iter()
                .map(|t| t.column_2.as_ref().expect("checked").into_slice()),
            negated,
        );
        let Some(set) = set else {
            i = end + 1;
            continue;
        };
        let mut merged = transformers[i].clone();
        merged.in_set = Some(std::sync::Arc::new(set));
        merged.group_opens -= 1;
        merged.group_closes = transformers[end].group_closes - 1;
        merged.next = transformers[end].next.clone();
        transformers.drain(i + 1..=end);
        transformers[i] = merged;
        changed = true;
        i += 1;
    }
    changed
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
            ComparisonOperand::CastWithIndex(mb, _, _, _) => mb,
            ComparisonOperand::Intermediate(_) | ComparisonOperand::IntermediateCast(..) => {
                panic!("Intermediate has no direct memory block")
            }
        }
    }
    pub fn get_column_index(&self) -> Option<usize> {
        match self {
            ComparisonOperand::DirectWithIndex(_, i) => Some(*i),
            ComparisonOperand::PathWithIndex(_, i, _) => Some(*i),
            ComparisonOperand::CastWithIndex(_, i, _, _) => Some(*i),
            _ => None,
        }
    }
    pub fn is_column(&self) -> bool {
        matches!(self, ComparisonOperand::DirectWithIndex(_, _))
    }
    pub fn is_intermediate(&self) -> bool {
        matches!(
            self,
            ComparisonOperand::Intermediate(_) | ComparisonOperand::IntermediateCast(..)
        )
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
    /// The boolean program could not be represented, so no evaluation using it
    /// may be trusted. Turned into an error at the next `execute_row` rather
    /// than allowed to fall back to the flat walk, which ignores parentheses
    /// and would be a second silently wrong answer.
    program_overflowed: bool,
    /// The program no longer matches the transformers, and has to be rebuilt
    /// before the next evaluation.
    ///
    /// Building it is O(comparisons), and it used to be rebuilt by every
    /// `add_comparison` — so parsing a clause with n comparisons cost O(n²) and
    /// threw away n-1 of the programs it built. On 2026-09-02 that wedged the
    /// 192.168.1.7 node: one runtime worker spent 13 minutes of CPU inside
    /// `refresh_comparison_program` while the accept loop starved, so the port
    /// listened and answered nobody. Measured before the fix, on a clause of
    /// `(a = i AND b = 'xi')` groups ORed together:
    ///
    /// ```text
    ///    800 comparisons    ~100 ms
    ///   1600 comparisons    ~500 ms
    ///   3200 comparisons   ~2100 ms      quadruples as n doubles
    /// ```
    ///
    /// Deferring the build to the first evaluation makes it O(n) once. The flag
    /// is checked per row rather than per comparison, which is one predictable
    /// branch against work that is already per-row.
    program_dirty: bool,
    /// Whether runs of `col = v` are folded into hashed [`InSet`]s. Always on
    /// outside the differential tests, which turn it off to evaluate the
    /// comparison chain itself as the reference.
    in_sets_enabled: bool,
}

/// One step of the compiled boolean program, in postfix order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvalOp {
    /// Push the comparison result at this index.
    ///
    /// `u32`, not `u16`. As a `u16` the index was produced by `index as u16`,
    /// which truncates: a clause with more than 65,535 comparisons wrapped to
    /// 0, 1, 2 … and built a *valid-looking* program over the wrong
    /// comparisons. Nothing detected it — every wrapped index resolves to a
    /// result that exists, so the "plan and results disagree" fallback below
    /// never fired — and the predicate silently stopped discriminating.
    ///
    /// It was reachable from ordinary SQL: a correlated `EXISTS` is rewritten
    /// to `IN` and the inner query is materialised one literal per inner row,
    /// so `EXISTS (SELECT 1 FROM t WHERE t.k = outer.k)` over a table of more
    /// than 65,535 rows returned *every* outer row, and so did `NOT EXISTS`.
    /// Measured boundary: 65,535 correct, 65,536 wrong.
    Value(u32),
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
            program_dirty: false,
            program_overflowed: false,
            in_sets_enabled: true,
        }
    }

    /// Turns the [`InSet`] folding off, so the comparison chain is evaluated as
    /// written. For differential tests and benchmarks only.
    pub fn set_in_sets_enabled(&mut self, enabled: bool) {
        self.in_sets_enabled = enabled;
    }

    #[inline]
    fn update_max_result_index(&mut self, idx: usize) {
        if idx > self.max_result_store_index {
            self.max_result_store_index = idx;
        }
    }

    pub fn snapshot_plan(&self) -> TransformerPlan {
        let mut transformers = self.transformers.clone();
        // Folded once here, in the plan every worker loads, rather than once
        // per worker: a 50,000-value list is that many hash inserts.
        if self.in_sets_enabled {
            collapse_in_sets(&mut transformers);
        }
        TransformerPlan {
            transformers,
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
        self.program_dirty = false;
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

    /// Says that the operands of the transformer just added are `NUMERIC`
    /// columns stored at these declared scales, to be lifted to the engine
    /// scale before use. `None` leaves an operand as it is.
    /// Attaches a pre-split `?|` / `?&` key list to the comparison just added.
    pub fn set_last_key_list(&mut self, keys: crate::core::pg_ops::KeyList) {
        if let Some(t) = self.transformers.last_mut() {
            t.key_list = Some(std::sync::Arc::new(keys));
        }
    }

    /// Negates the comparison just added; see [`ColumnTransformer::negated`].
    pub fn set_last_negated(&mut self) {
        if let Some(t) = self.transformers.last_mut() {
            t.negated = true;
        }
    }

    /// Makes the comparison just added an `IS [NOT] NULL` test of its left
    /// operand; see [`ColumnTransformer::null_test`].
    pub fn set_last_null_test(&mut self, want_null: bool) {
        if let Some(t) = self.transformers.last_mut() {
            t.null_test = Some(want_null);
        }
    }

    pub fn set_last_operand_scales(&mut self, left: Option<u32>, right: Option<u32>) {
        if let Some(t) = self.transformers.last_mut() {
            t.column_1_scale = left;
            t.column_2_scale = right;
        }
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
            if matches!(
                t.transformer_type,
                ColumnTransformerType::ComparerOperation(_)
            ) {
                t.group_closes = t.group_closes.saturating_add(1);
                break;
            }
        }
        self.program_dirty = true;
    }

    /// Compiles the boolean structure to postfix by shunting-yard, so the
    /// per-row evaluation is a stack walk rather than a re-parse.
    ///
    /// Rebuilt whenever the plan changes, so it can never fall out of step with
    /// the results it indexes.
    fn refresh_comparison_program(&mut self) {
        self.comparison_program.clear();
        // Before the program is built from the comparisons, so it indexes the
        // folded list. A no-op on a plan that was folded when snapshotted.
        if self.in_sets_enabled {
            collapse_in_sets(self.transformers);
        }

        let comparisons: SmallVec<[(u8, u8, Option<Next>); 24]> = self
            .transformers
            .iter()
            .filter(|t| {
                matches!(
                    t.transformer_type,
                    ColumnTransformerType::ComparerOperation(_)
                )
            })
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

            let Ok(slot) = u32::try_from(index) else {
                // Unreachable in practice — it would take four billion
                // comparisons, and the transformers themselves would exhaust
                // memory long first. Handled anyway, because the last time
                // this width was assumed sufficient the overflow was silent
                // and wrong for two years' worth of large `EXISTS` queries.
                self.program_overflowed = true;
                self.comparison_program.clear();
                return;
            };
            out.push(EvalOp::Value(slot));

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
        // Marked, not rebuilt: see `program_dirty`. Rebuilding here made
        // parsing quadratic in the number of comparisons.
        self.program_dirty = true;
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
        // The one place the program is guaranteed to be current before it is
        // read. `evaluate_comparison_results` takes `&self` and cannot rebuild,
        // and every other entry point would have to remember to; this cannot be
        // forgotten, because nothing evaluates without coming through here.
        if self.program_dirty {
            self.refresh_comparison_program();
            self.program_dirty = false;
        }
        // Failing the statement is the whole point: the alternative is the flat
        // walk, which cannot see the parentheses the program existed to honour.
        if self.program_overflowed {
            return Err(EvalError::PredicateTooComplex);
        }
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

/// The hashed `IN` set against the comparison chain it replaces, over generated
/// lists, types and NULLs. The chain is evaluated with folding switched off —
/// the reference — and the two must agree on every row, errors included.
#[cfg(test)]
mod in_set_differential {
    use super::*;
    use crate::core::row::schema::SchemaField;
    use crate::core::tokenizer::query_parser::QueryParser;
    use crate::core::tokenizer::query_tokenizer::tokenize;
    use crate::memory_pool::MEMORY_POOL;
    use std::borrow::Cow;
    use std::cell::UnsafeCell;

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }
        fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
            &xs[(self.next() as usize) % xs.len()]
        }
    }

    fn mb(bytes: &[u8]) -> MemoryBlock {
        let mut m = MEMORY_POOL.acquire(bytes.len());
        m.into_slice_mut().copy_from_slice(bytes);
        m
    }

    fn schema() -> SmallVec<[SchemaField; 20]> {
        let mut d = SchemaField::new("d".to_string(), DbType::DECIMAL, 16, 0, 4, false);
        d.scale = Some(2);
        smallvec::smallvec![
            SchemaField::new("a".to_string(), DbType::I32, 4, 0, 0, false),
            SchemaField::new("b".to_string(), DbType::I64, 8, 0, 1, false),
            SchemaField::new("s".to_string(), DbType::STRING, 12, 0, 2, false),
            SchemaField::new("f".to_string(), DbType::F64, 8, 0, 3, false),
            d,
            SchemaField::new("u".to_string(), DbType::UUID, 16, 0, 5, false),
            SchemaField::new("n".to_string(), DbType::I32, 4, 0, 6, false),
            SchemaField::new("__null_n".to_string(), DbType::BOOL, 1, 0, 7, false),
            SchemaField::new("r".to_string(), DbType::F32, 4, 0, 8, false),
        ]
    }

    const INTS: &[&str] = &["-5", "0", "1", "2", "3", "7", "11", "12", "300", "-1"];
    const STRS: &[&str] = &["'x'", "'y'", "'zz'", "''", "'it''s'", "'É'", "'X'"];
    const FLOATS: &[&str] = &["0", "'-0'", "1.5", "'NaN'", "'Infinity'", "2", "-3.25"];
    const DECS: &[&str] = &["1", "1.5", "2.25", "100", "0.01", "-7"];
    const UUIDS: &[&str] = &[
        "'00000000-0000-0000-0000-000000000001'",
        "'00000000-0000-0000-0000-000000000002'",
        "'ffffffff-ffff-ffff-ffff-ffffffffffff'",
        "'12345678-1234-1234-1234-123456789abc'",
    ];

    fn domain(col: &str) -> &'static [&'static str] {
        match col {
            "a" | "b" | "n" => INTS,
            "s" => STRS,
            "f" | "r" => FLOATS,
            "d" => DECS,
            _ => UUIDS,
        }
    }

    fn unquote(v: &str) -> String {
        crate::core::value_codec::unquote_sql(v).to_string()
    }

    fn row(rng: &mut Lcg) -> SmallVec<[(Cow<'static, str>, MemoryBlock); 20]> {
        let int = |rng: &mut Lcg| rng.pick(INTS).parse::<i64>().unwrap();
        let fl = |rng: &mut Lcg| -> f64 {
            match unquote(rng.pick(FLOATS)).as_str() {
                "NaN" => {
                    // A NaN with another payload is still NaN.
                    if rng.next() % 2 == 0 {
                        f64::NAN
                    } else {
                        f64::from_bits(0xfff8_0000_0000_0001)
                    }
                }
                "Infinity" => f64::INFINITY,
                t => t.parse().unwrap(),
            }
        };
        let null = rng.next() % 4 == 0;
        let mut cols: SmallVec<[(Cow<'static, str>, MemoryBlock); 20]> = SmallVec::new();
        cols.push(("a".into(), mb(&(int(rng) as i32).to_le_bytes())));
        cols.push(("b".into(), mb(&int(rng).to_le_bytes())));
        cols.push(("s".into(), mb(unquote(rng.pick(STRS)).as_bytes())));
        cols.push(("f".into(), mb(&fl(rng).to_le_bytes())));
        let d = crate::core::value_codec::parse_decimal_scaled(&unquote(rng.pick(DECS)), 2).unwrap();
        cols.push(("d".into(), mb(&d.to_le_bytes())));
        let u = crate::core::value_codec::encode_literal(&DbType::UUID, &unquote(rng.pick(UUIDS)))
            .unwrap();
        cols.push(("u".into(), mb(&u)));
        let n = if null { 0 } else { int(rng) as i32 };
        cols.push(("n".into(), mb(&n.to_le_bytes())));
        cols.push(("__null_n".into(), mb(&[u8::from(null)])));
        cols.push(("r".into(), mb(&(fl(rng) as f32).to_le_bytes())));
        cols
    }

    fn evaluate(
        tokens: &SmallVec<[crate::core::tokenizer::query_tokenizer::Token; 36]>,
        rows: &[SmallVec<[(Cow<'static, str>, MemoryBlock); 20]>],
        folded: bool,
    ) -> (Vec<Result<bool, String>>, usize) {
        let schema = schema();
        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut results: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let processor = UnsafeCell::new(TransformerProcessor::new(&mut transformers, &mut inter));
        unsafe { &mut *processor.get() }.set_in_sets_enabled(folded);
        let mut parser =
            QueryParser::new(tokens, unsafe { &mut *processor.get() }).with_scales(&schema);
        let out = rows
            .iter()
            .map(|r| parser.evaluate_row(r, &mut results))
            .collect();
        let comparisons = unsafe { &*processor.get() }.transformers.len();
        (out, comparisons)
    }

    #[test]
    fn the_hashed_set_agrees_with_the_chain() {
        let schema = schema();
        let mut rng = Lcg(0x5eed);
        let rows: Vec<_> = (0..300).map(|_| row(&mut rng)).collect();
        let mut folded_runs = 0;
        let mut answered = 0;
        for trial in 0..400 {
            let col = *rng.pick(&["a", "b", "s", "f", "d", "u", "n", "r"]);
            let len = *rng.pick(&[0usize, 1, 3, 4, 5, 9, 40, 200]);
            let mut values: Vec<String> = (0..len).map(|_| rng.pick(domain(col)).to_string()).collect();
            if rng.next() % 7 == 0 && !values.is_empty() {
                values.push("NULL".to_string());
            }
            let negated = rng.next() % 3 == 0;
            let list = format!(
                "{} {}IN ({})",
                col,
                if negated { "NOT " } else { "" },
                values.join(", ")
            );
            let clause = match trial % 3 {
                0 => list,
                1 => format!("(a > 5 OR {}) AND b <> 3", list),
                _ => format!("{} AND s <> 'zz' OR a = 2", list),
            };
            let rewritten = crate::core::tokenizer::predicate_rewrite::rewrite_where(&clause, &schema)
                .unwrap_or_else(|e| panic!("{clause}: {e}"));
            let tokens = tokenize(&rewritten, &schema);
            let (reference, chain_len) = evaluate(&tokens, &rows, false);
            let (hashed, folded_len) = evaluate(&tokens, &rows, true);
            if folded_len < chain_len {
                folded_runs += 1;
            }
            if reference.iter().all(|r| r.is_ok()) {
                answered += 1;
            } else {
                // An error is compared too, but a trial that only errors
                // proves nothing about the set, so it must be rare.
                eprintln!("{clause}: {:?}", reference.iter().find(|r| r.is_err()));
            }
            for (i, (r, h)) in reference.iter().zip(hashed.iter()).enumerate() {
                assert_eq!(r, h, "{clause} disagrees on row {i}");
            }
        }
        // The test is only worth something if the fold actually happened.
        assert!(folded_runs > 100, "only {folded_runs} trials folded");
        assert!(answered > 380, "only {answered} of 400 trials planned and ran");
    }

    #[test]
    fn a_short_or_mixed_run_is_left_alone() {
        let schema = schema();
        for (clause, folds) in [
            ("a IN (1, 2, 3)", false),
            ("a IN (1, 2, 3, 4)", true),
            ("a NOT IN (1, 2, 3, 4)", true),
            ("(a = 1 OR a = 2 OR b = 3 OR a = 4)", false),
            ("(a = 1 OR a = 2 OR a > 3 OR a = 4)", false),
            ("(a = 1 OR a = 2 AND a = 3 OR a = 4)", false),
            ("(a = 1 OR (a = 2 OR a = 3) OR a = 4)", false),
            ("s IN ('x', 'y', 'zz', '')", true),
            ("u IN ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'ffffffff-ffff-ffff-ffff-ffffffffffff', '12345678-1234-1234-1234-123456789abc')", true),
        ] {
            let rewritten =
                crate::core::tokenizer::predicate_rewrite::rewrite_where(clause, &schema).unwrap();
            let tokens = tokenize(&rewritten, &schema);
            let rows = vec![row(&mut Lcg(1))];
            let (_, chain) = evaluate(&tokens, &rows, false);
            let (_, folded) = evaluate(&tokens, &rows, true);
            assert_eq!(folded < chain, folds, "{clause}");
        }
    }
}

/// `x IN (…)` per-row cost, the comparison chain against the hashed set.
///
/// `cargo test --release -p rasterizeddb_core --lib -- --ignored in_set_bench --nocapture`
#[cfg(test)]
mod in_set_bench {
    use super::*;
    use crate::core::row::schema::SchemaField;
    use crate::core::tokenizer::query_parser::QueryParser;
    use crate::core::tokenizer::query_tokenizer::tokenize;
    use crate::memory_pool::MEMORY_POOL;
    use std::borrow::Cow;
    use std::cell::UnsafeCell;

    fn run(n_values: usize, rows: usize, folded: bool) -> (f64, usize) {
        let schema: SmallVec<[SchemaField; 20]> = smallvec::smallvec![
            SchemaField::new("a".to_string(), DbType::I64, 8, 0, 0, false),
            SchemaField::new("s".to_string(), DbType::STRING, 12, 0, 1, false),
        ];
        let list: Vec<String> = (0..n_values).map(|i| (i * 7).to_string()).collect();
        let clause = format!("a IN ({})", list.join(", "));
        let rewritten =
            crate::core::tokenizer::predicate_rewrite::rewrite_where(&clause, &schema).unwrap();
        let tokens = tokenize(&rewritten, &schema);
        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let processor = UnsafeCell::new(TransformerProcessor::new(&mut transformers, &mut inter));
        unsafe { &mut *processor.get() }.set_in_sets_enabled(folded);
        let mb = |b: &[u8]| {
            let mut m = MEMORY_POOL.acquire(b.len());
            m.into_slice_mut().copy_from_slice(b);
            m
        };
        let mut cols: SmallVec<[(Cow<'static, str>, MemoryBlock); 20]> = SmallVec::new();
        cols.push(("a".into(), mb(&0i64.to_le_bytes())));
        cols.push(("s".into(), mb(b"x")));
        {
            let mut parser = QueryParser::new(&tokens, unsafe { &mut *processor.get() });
            parser.execute(&cols).unwrap();
        }
        let p = unsafe { &mut *processor.get() };
        // A pool of row images cycled through, so the measurement is the
        // predicate and not the allocator.
        let pool: Vec<[MemoryBlock; 2]> = (0..4096u64)
            .map(|i| {
                let v = ((i.wrapping_mul(2654435761)) % (n_values as u64 * 14 + 1)) as i64;
                [mb(&v.to_le_bytes()), mb(b"x")]
            })
            .collect();
        let mut results: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let mut hits = 0usize;
        let start = std::time::Instant::now();
        for i in 0..rows {
            if p.execute_row(&pool[i % pool.len()][..], &mut results).unwrap() {
                hits += 1;
            }
        }
        let ns = start.elapsed().as_nanos() as f64 / rows as f64;
        (ns, hits)
    }

    #[test]
    #[ignore]
    fn in_set_bench() {
        for (n, rows) in [
            (4usize, 1_000_000usize),
            (10, 1_000_000),
            (1_000, 1_000_000),
            (50_000, 20_000),
        ] {
            // Interleaved A/B, three rounds: the box drifts.
            for round in 0..3 {
                let (chain, h1) = run(n, rows, false);
                let (set, h2) = run(n, rows, true);
                assert_eq!(h1, h2, "the two paths disagree");
                println!(
                    "IN of {n:>6} values, {rows} rows, round {round}: chain {chain:>10.1} ns/row, set {set:>6.1} ns/row ({:.0}x), {h1} matches",
                    chain / set
                );
            }
        }
    }
}

/// Per-row JSON predicates through the real planner: NULL documents, missing
/// paths, the `?|` / `?&` key list split at plan time, and the `->` / `->>`
/// extraction that runs once per row.
#[cfg(test)]
mod json_row_path {
    use super::*;
    use crate::core::row::schema::SchemaField;
    use crate::core::tokenizer::query_parser::QueryParser;
    use crate::core::tokenizer::query_tokenizer::tokenize;
    use crate::memory_pool::MEMORY_POOL;
    use std::borrow::Cow;
    use std::cell::UnsafeCell;

    fn mb(b: &[u8]) -> MemoryBlock {
        let mut m = MEMORY_POOL.acquire(b.len());
        m.into_slice_mut().copy_from_slice(b);
        m
    }

    fn doc(text: &str) -> Vec<u8> {
        crate::core::jsonb::encode_text(text).expect("test document")
    }

    /// `id`, a nullable `doc` and its `__null_doc` companion, in that order.
    fn schema() -> SmallVec<[SchemaField; 20]> {
        smallvec::smallvec![
            SchemaField::new("id".to_string(), DbType::I32, 4, 0, 0, false),
            SchemaField::new("doc".to_string(), DbType::JSONB, 12, 4, 1, false),
            SchemaField::new("__null_doc".to_string(), DbType::U8, 1, 16, 2, false),
        ]
    }

    /// A row: `None` is a NULL document, stored as nothing with the companion
    /// set, which is how an `INSERT … NULL` lays it out.
    fn row(id: i32, document: Option<&str>) -> [MemoryBlock; 3] {
        match document {
            Some(text) => [mb(&id.to_le_bytes()), mb(&doc(text)), mb(&[0])],
            None => [mb(&id.to_le_bytes()), MemoryBlock::default(), mb(&[1])],
        }
    }

    /// Plans `clause` against [`schema`], with `sample` as the row the
    /// planner sees.
    fn plan<'a>(
        clause: &str,
        sample: &[MemoryBlock; 3],
        transformers: &'a mut SmallVec<[ColumnTransformer; 36]>,
        inter: &'a mut SmallVec<[MemoryBlock; 20]>,
    ) -> Result<TransformerProcessor<'a>, String> {
        let schema = schema();
        let rewritten = crate::core::tokenizer::predicate_rewrite::rewrite_where(clause, &schema)?;
        let tokens = tokenize(&rewritten, &schema);
        let processor = UnsafeCell::new(TransformerProcessor::new(transformers, inter));
        let mut cols: SmallVec<[(Cow<'static, str>, MemoryBlock); 20]> = SmallVec::new();
        for (field, value) in schema.iter().zip(sample.iter()) {
            cols.push((field.name.clone().into(), value.clone()));
        }
        {
            let mut parser = QueryParser::new(&tokens, unsafe { &mut *processor.get() });
            parser.execute(&cols)?;
        }
        Ok(processor.into_inner())
    }

    /// The ids of the rows `clause` matches, or the plan or row error.
    fn select(clause: &str, rows: &[[MemoryBlock; 3]]) -> Result<Vec<i32>, String> {
        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let mut p = plan(clause, &rows[0], &mut transformers, &mut inter)?;
        let mut results: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
        let mut out = Vec::new();
        for r in rows {
            if p.execute_row(&r[..], &mut results).map_err(|e| format!("{:?}", e))? {
                out.push(i32::from_le_bytes(r[0].into_slice().try_into().unwrap()));
            }
        }
        Ok(out)
    }

    fn table() -> Vec<[MemoryBlock; 3]> {
        vec![
            row(1, Some(r#"{"a":1,"tags":["x","y"],"s":"str","n":5,"z":null,"o":{"k":1}}"#)),
            row(2, None),
            row(3, Some(r#"{"a":2}"#)),
            row(4, Some(r#"[1,2]"#)),
            row(5, Some(r#"{"tags":["y"],"s":""}"#)),
        ]
    }

    /// PostgreSQL: NULL `@>` anything is NULL, and a `->` that reaches nothing
    /// is NULL too — neither row matches, and neither fails the statement.
    /// The containment cases were an "operation is not supported" error
    /// before, because the NULL row's empty bytes reached the blob walk.
    #[test]
    fn a_null_document_or_missing_path_does_not_match_and_does_not_fail() {
        let t = table();
        let mut wrong = Vec::new();
        for (clause, want) in [
            (r#"doc @> '{"a":1}'"#, vec![1]),
            (r#"doc @> '{"a":1}' OR id = 3"#, vec![1, 3]),
            (r#"id = 2 OR doc @> '{"a":2}'"#, vec![2, 3]),
            (r#"doc <@ '{"a":2,"b":3}'"#, vec![3]),
            (r#"doc->'tags' @> '["x"]'"#, vec![1]),
            (r#"doc->'tags' @> '["y"]' OR id = 3"#, vec![1, 3, 5]),
            (r#"doc->'tags' <@ '["x","y","q"]'"#, vec![1, 5]),
            (r#"doc ? 'a'"#, vec![1, 3]),
            (r#"doc ?| '{a,tags}'"#, vec![1, 3, 5]),
            (r#"doc ?& '{a,tags}'"#, vec![1]),
            (r#"doc ?& '{}'"#, vec![1, 3, 4, 5]),
            (r#"doc->'o' ?| '{k}'"#, vec![1]),
        ] {
            let got = select(clause, &t);
            if got != Ok(want.clone()) {
                wrong.push(format!("{}: got {:?}, want {:?}", clause, got, want));
            }
        }
        assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
    }

    /// `NOT` over a structural operator negates only a true or false answer.
    /// It used to be refused, because answering `false` for NULL is sound only
    /// while nothing complements that answer: a NULL row would otherwise start
    /// matching `NOT (doc @> x)`, where PostgreSQL returns no row. It plans
    /// now, with the NULL document guarded outside the negation and a path
    /// that reaches nothing left NULL by the evaluator. PostgreSQL 17 returns
    /// 3, 4, 5 and 5 for these over the same rows.
    #[test]
    fn not_over_a_structural_operator_leaves_null_rows_out() {
        let t = table();
        for (clause, want) in [
            (r#"NOT (doc @> '{"a":1}')"#, vec![3, 4, 5]),
            (r#"NOT (doc->'tags' @> '["x"]')"#, vec![5]),
        ] {
            assert_eq!(select(clause, &t), Ok(want), "{}", clause);
        }
    }

    /// PostgreSQL refuses a right operand that is not JSON with 22P02 before
    /// it reads a row. It used to be kept as raw text, so the answer depended
    /// on the row: no match against an object, an error against an array.
    #[test]
    fn an_invalid_json_literal_is_refused_at_plan_time() {
        let only_objects = vec![row(1, Some(r#"{"a":1}"#)), row(3, Some(r#"{"b":1}"#))];
        for clause in [r#"doc @> '{bad'"#, r#"doc <@ 'nope'"#, r#"doc->'tags' @> '[1,'"#] {
            let got = select(clause, &only_objects);
            assert!(
                got.as_ref().is_err_and(|e| e.contains("invalid input syntax for type json")),
                "{}: {:?}",
                clause,
                got
            );
        }
    }

    /// Refused before a row is read — by the rewrite, which validates the
    /// list; the planner's own check behind it is a second line, for a plan
    /// that does not come through the rewrite.
    #[test]
    fn a_malformed_key_list_is_refused_at_plan_time() {
        let got = select("doc ?| 'a,b'", &[row(2, None)]);
        assert!(got.as_ref().is_err_and(|e| e.contains("array of keys")), "{:?}", got);
    }

    /// The list is split once, when the predicate is planned.
    #[test]
    fn a_literal_key_list_is_split_when_planned() {
        let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
        let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
        let sample = row(1, Some("{}"));
        drop(plan("doc ?| '{a,\"b c\",NULL}'", &sample, &mut transformers, &mut inter).unwrap());
        let lists: Vec<_> = transformers.iter().filter_map(|t| t.key_list.clone()).collect();
        assert_eq!(lists.len(), 1, "{:?}", transformers);
        assert_eq!(*lists[0], vec![Some("a".to_string()), Some("b c".to_string()), None]);
    }

    /// What `extract` returned before it stopped copying twice: the owned
    /// `as_text` / `to_vec` forms.
    fn reference_extract(path: &JsonPathRef, document: &[u8]) -> Option<Vec<u8>> {
        let found = crate::core::jsonb::get_path(document, &path.steps)?;
        if path.as_text {
            crate::core::jsonb::as_text(found).map(String::into_bytes)
        } else {
            Some(found.to_vec())
        }
    }

    fn text_path(key: &str) -> JsonPathRef {
        JsonPathRef {
            steps: std::sync::Arc::new(vec![crate::core::jsonb::Step::Key(key.to_string())]),
            as_text: true,
            cast: None,
        }
    }

    #[test]
    fn extract_returns_what_the_owned_forms_did() {
        use crate::core::jsonb::Step;
        let documents = [
            r#"{"s":"str","e":"","u":"ünï","n":5,"f":1.50,"big":18446744073709551615,"t":true,"z":null,"o":{"k":1,"j":[1,"a"]},"a":[1,"two",null,{"x":"y"}],"q":"say \"hi\"\n"}"#,
            r#"[1,"a",null]"#,
            r#""bare""#,
            r#"null"#,
            r#"7"#,
        ];
        let paths: Vec<Vec<Step>> = ["s", "e", "u", "n", "f", "big", "t", "z", "o", "a", "q", "missing"]
            .iter()
            .map(|k| vec![Step::Key(k.to_string())])
            .chain([
                vec![Step::Key("o".into()), Step::Key("j".into())],
                vec![Step::Key("a".into()), Step::Index(1)],
                vec![Step::Key("a".into()), Step::Index(-1)],
                vec![Step::Key("a".into()), Step::Index(2)],
                vec![Step::Index(0)],
                vec![Step::Index(1)],
                vec![Step::Index(9)],
                vec![],
            ])
            .collect();
        let mut checked = 0;
        for d in documents {
            let encoded = doc(d);
            for steps in &paths {
                for as_text in [false, true] {
                    let path = JsonPathRef {
                        steps: std::sync::Arc::new(steps.clone()),
                        as_text,
                        cast: None,
                    };
                    let got = path.extract(&encoded).map(|b| b.into_slice().to_vec());
                    let want = reference_extract(&path, &encoded);
                    assert_eq!(got, want, "{} {:?} as_text={}", d, steps, as_text);
                    checked += 1;
                }
            }
        }
        assert!(checked > 100);

        // `->>` against PostgreSQL 17, case by case: a number keeps the
        // scale it was written with, and a container prints as `jsonb_out`
        // prints it, with a space after each `:` and `,`.
        let d = doc(documents[0]);
        let text = |k: &str| text_path(k).extract(&d).map(|b| b.into_slice().to_vec()).unwrap_or_default();
        assert_eq!(text("s"), b"str");
        assert_eq!(text("u"), "ünï".as_bytes());
        assert_eq!(text("q"), b"say \"hi\"\n");
        assert_eq!(text("n"), b"5");
        assert_eq!(text("f"), b"1.50");
        assert_eq!(text("t"), b"true");
        // JSON null is SQL NULL, and so is an absent key: no value at all,
        // which is not the empty string `"e"` holds.
        assert!(text_path("z").extract(&d).is_none());
        assert!(text_path("missing").extract(&d).is_none());
        assert_eq!(text_path("e").extract(&d).map(|b| b.into_slice().to_vec()), Some(Vec::new()));
        assert_eq!(text("o"), br#"{"j": [1, "a"], "k": 1}"#);

        // A rendered container larger than the scratch keeps must still come
        // out whole, and the next row must not see its leftovers.
        let huge = format!(r#"{{"o":[{}]}}"#, vec!["\"xxxxxxxxxxxxxxxx\""; 10_000].join(","));
        let h = doc(&huge);
        let path = text_path("o");
        assert_eq!(path.extract(&h).unwrap().into_slice(), &reference_extract(&path, &h).unwrap()[..]);
        assert_eq!(text("n"), b"5");
        assert_eq!(text("o"), br#"{"j": [1, "a"], "k": 1}"#);
    }


    /// The bench's documents: four kinds of leaf, one nested object.
    fn bench_rows() -> Vec<[MemoryBlock; 3]> {
        (0..4096)
            .map(|i| {
                row(
                    i,
                    Some(&format!(
                        r#"{{"id":{i},"status":"{}","tags":["a","t{}"],"meta":{{"owner":"u{}","score":{}}}}}"#,
                        ["open", "closed", "pending"][i as usize % 3],
                        i % 7,
                        i % 13,
                        i % 100
                    )),
                )
            })
            .collect()
    }

    /// Per-row cost of JSON predicates, the new form against the old one in
    /// the same process, interleaved round by round — this box is shared and
    /// drifts by 2x between runs, so separate before/after runs said little.
    ///
    /// `extract`: one `->` / `->>` extraction, against `reference_extract`
    /// plus the pool copy the old code made after it. `execute_row`: a whole
    /// predicate, with `?|` run once with its plan-time key list and once with
    /// the list removed, which is the old per-row parse.
    ///
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored json_text_path_bench --nocapture`
    #[test]
    #[ignore]
    fn json_text_path_bench() {
        use crate::core::jsonb::Step;
        use std::hint::black_box;
        let rows = bench_rows();
        let docs: Vec<&[u8]> = rows.iter().map(|r| r[1].into_slice()).collect();
        let n = 2_000_000usize;

        for (label, key, as_text) in [
            ("->> string", "status", true),
            ("->> number", "id", true),
            ("->> object", "meta", true),
            ("->  object", "meta", false),
        ] {
            let path = JsonPathRef {
                steps: std::sync::Arc::new(vec![Step::Key(key.to_string())]),
                as_text,
                cast: None,
            };
            for round in 0..4 {
                let start = std::time::Instant::now();
                let mut len = 0usize;
                for i in 0..n {
                    let old = reference_extract(&path, black_box(docs[i % docs.len()]))
                        .unwrap_or_default();
                    len += pooled_copy(&old).into_slice().len();
                }
                let old_ns = start.elapsed().as_nanos() as f64 / n as f64;
                let start = std::time::Instant::now();
                let mut len2 = 0usize;
                for i in 0..n {
                    len2 += path.extract(black_box(docs[i % docs.len()])).map_or(0, |b| b.into_slice().len());
                }
                let new_ns = start.elapsed().as_nanos() as f64 / n as f64;
                assert_eq!(len, len2);
                println!(
                    "extract {label:<11} round {round}: old {old_ns:>6.1} ns, new {new_ns:>6.1} ns ({:.2}x)",
                    old_ns / new_ns
                );
            }
        }

        for (label, clause) in [
            // A control: no key list, so the two columns must agree, and the
            // gap between them is the box's noise.
            ("control: doc->>'status' = 'open'", "doc->>'status' = 'open'"),
            ("doc ?| '{missing,status}'", "doc ?| '{missing,status}'"),
            ("doc ?& '{id,meta,status,tags}'", "doc ?& '{id,meta,status,tags}'"),
        ] {
            let mut transformers: SmallVec<[ColumnTransformer; 36]> = SmallVec::new();
            let mut inter: SmallVec<[MemoryBlock; 20]> = SmallVec::new();
            let mut p = plan(clause, &rows[0], &mut transformers, &mut inter).unwrap();
            let lists: Vec<_> = p.transformers.iter().map(|t| t.key_list.clone()).collect();
            let mut results: SmallVec<[(bool, Option<Next>); 20]> = SmallVec::new();
            for round in 0..4 {
                let mut timed = [0f64; 2];
                let mut hits = [0usize; 2];
                // 0: the key list removed (the old per-row parse); 1: as planned.
                for (slot, keep) in [(0usize, false), (1, true)] {
                    for (t, list) in p.transformers.iter_mut().zip(&lists) {
                        t.key_list = if keep { list.clone() } else { None };
                    }
                    let start = std::time::Instant::now();
                    for i in 0..n {
                        hits[slot] += p
                            .execute_row(&rows[i % rows.len()][..], &mut results)
                            .unwrap() as usize;
                    }
                    timed[slot] = start.elapsed().as_nanos() as f64 / n as f64;
                }
                assert_eq!(hits[0], hits[1]);
                println!(
                    "row {label:<34} round {round}: per-row parse {:>6.1} ns, planned {:>6.1} ns ({:.2}x), {} matches",
                    timed[0],
                    timed[1],
                    timed[0] / timed[1],
                    hits[1]
                );
            }
        }
    }
}
