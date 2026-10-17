//! Combining the branches of a `UNION` into one result.
//!
//! The executor runs each branch and hands its serialized rows here. This
//! removes duplicates from the branches that need it, then applies the
//! trailing `ORDER BY`, `OFFSET` and `LIMIT` to the combined rows.
//!
//! Two rows are duplicates when PostgreSQL's `=` calls every pair of their
//! values equal, and NULL equals NULL. That is not the same as their bytes
//! being equal. Each serialized cell carries the position of the column it was
//! read from, so `open` from the third column of one table and `open` from the
//! second of another were two rows. And a cell carries its own type, so an
//! `int` 1 and a `bigint` 1 were two rows, where PostgreSQL makes the column a
//! `bigint` and keeps one.
//!
//! That widening is also a matter of what the client is told. The column is
//! described by the first branch's type, and the wire encoder renders each
//! value by that description: a `bigint` value under an `int` description
//! did not fit and went out as NULL. So a column whose branches declare
//! integers of different widths is widened, in the rows and in the
//! description alike, by [`widened_integers`].

use std::cmp::Ordering;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::row::row::{serialized_cells, serialized_rows};
use crate::core::rql::join::UnionParts;

/// For each output column of a `UNION`, the integer type its values are
/// widened to, when its branches declare integers of different widths, as
/// PostgreSQL widens `int` and `bigint` to `bigint`. `None` where nothing
/// changes, or where any branch's type is not known: an expression's type is
/// not resolved here, and widening by a guess could disagree with the
/// description.
///
/// The executor widens the rows by this and the wire layer the description,
/// so both read it from here.
pub fn widened_integers(parts: &UnionParts, database: &Arc<Database>) -> Vec<Option<DbType>> {
    widened_branch_integers(&parts.branches, database, &[])
}

/// [`widened_integers`] over the branches alone, where a branch may also read
/// one of `relations`: the CTEs and derived tables a describe resolved. The
/// executor runs the statement with those materialised into real relations,
/// so the describe has to type them the same way to reach the same answer.
pub fn widened_branch_integers(
    branches: &[String],
    database: &Arc<Database>,
    relations: &[(String, crate::core::rql::cte::ScratchFields)],
) -> Vec<Option<DbType>> {
    let declared: Vec<Vec<Option<DbType>>> = branches
        .iter()
        .map(|branch| crate::core::rql::cte::declared_item_types_among(branch, database, relations))
        .collect();
    let width = declared.iter().map(Vec::len).min().unwrap_or(0);
    (0..width)
        .map(|at| {
            let types: Vec<&DbType> =
                declared.iter().map(|b| b[at].as_ref()).collect::<Option<Vec<_>>>()?;
            if types.windows(2).all(|w| w[0] == w[1]) {
                return None;
            }
            // The narrowest signed type each one fits in, and the widest of those.
            let widest = types
                .iter()
                .map(|t| match t {
                    DbType::I8 => Some(1u8),
                    DbType::I16 | DbType::U8 => Some(2),
                    DbType::I32 | DbType::U16 => Some(4),
                    DbType::I64 | DbType::U32 => Some(8),
                    DbType::I128 | DbType::U64 => Some(16),
                    _ => None,
                })
                .collect::<Option<Vec<u8>>>()?
                .into_iter()
                .max()?;
            Some(match widest {
                1 => DbType::I8,
                2 => DbType::I16,
                4 => DbType::I32,
                8 => DbType::I64,
                _ => DbType::I128,
            })
        })
        .collect()
}

/// The bytes of `v` as the integer type `ty`, `None` when it does not fit.
fn integer_bytes(v: i128, ty: &DbType) -> Option<Vec<u8>> {
    Some(match ty {
        DbType::I8 => i8::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::I16 => i16::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::I32 => i32::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::I64 => i64::try_from(v).ok()?.to_le_bytes().to_vec(),
        DbType::I128 => v.to_le_bytes().to_vec(),
        _ => return None,
    })
}

#[cfg(feature = "enable_long_row")]
fn push_row_length(out: &mut Vec<u8>, payload: usize) {
    out.extend_from_slice(&(payload as u64).to_le_bytes());
}
#[cfg(not(feature = "enable_long_row"))]
fn push_row_length(out: &mut Vec<u8>, payload: usize) {
    out.extend_from_slice(&(payload as u32).to_le_bytes());
}

/// Appends `row` to `out` with each integer cell at a widened position
/// rewritten as the wider type, in the serialized layout: the length header,
/// then per cell its size, schema id, data and type tag.
fn push_widened(row: &[u8], widen: &[Option<DbType>], out: &mut Vec<u8>) -> std::io::Result<()> {
    let start = out.len();
    push_row_length(out, 0);
    let header = out.len() - start;
    for (position, cell) in serialized_cells(row)?.enumerate() {
        let cell = cell?;
        let wide = widen.get(position).and_then(|t| t.as_ref()).and_then(|ty| {
            let bytes = integer_bytes(integer(&cell.column_type, cell.data)?, ty)?;
            Some((ty.clone(), bytes))
        });
        let (ty, data) = match &wide {
            Some((ty, bytes)) => (ty, bytes.as_slice()),
            None => (&cell.column_type, cell.data),
        };
        out.extend_from_slice(&(data.len() as u64).to_le_bytes());
        out.extend_from_slice(&u64::from(cell.schema_id).to_le_bytes());
        out.extend_from_slice(data);
        out.push(ty.to_byte());
    }
    let payload = out.len() - start - header;
    let mut length = Vec::with_capacity(header);
    push_row_length(&mut length, payload);
    out[start..start + header].copy_from_slice(&length);
    Ok(())
}

/// One resolved key of the trailing `ORDER BY`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SortKey {
    /// Zero-based output column.
    position: usize,
    descending: bool,
    nulls_first: bool,
}

/// The rows of a `UNION` as its branches arrive.
pub struct Combined {
    /// What the caller wants before the rows (the result marker), then each
    /// kept row's serialized bytes.
    out: Vec<u8>,
    /// Where each kept row starts in `out`.
    starts: Vec<usize>,
    /// The duplicate keys of the rows kept from distinct branches, back to
    /// back, and where each one starts. Only the distinct prefix has them,
    /// and it comes first, so key `i` belongs to row `i`.
    keys: Vec<u8>,
    key_starts: Vec<usize>,
    seen: hashbrown::HashTable<u32>,
    state: ahash::RandomState,
    /// Which output columns hold documents, compared by value.
    documents: Vec<bool>,
    /// The integer type each output column is widened to, if any.
    widen: Vec<Option<DbType>>,
    sort: Vec<SortKey>,
    limit: Option<u64>,
    offset: u64,
}

impl Combined {
    /// `documents` says which output positions hold documents in any branch,
    /// and `names` are the first branch's output names, which a trailing
    /// `ORDER BY` may sort by. `widen` is [`widened_integers`]. `out` is what
    /// the result starts with.
    pub fn new(
        parts: &UnionParts,
        documents: Vec<bool>,
        widen: Vec<Option<DbType>>,
        names: &[Option<String>],
        out: Vec<u8>,
    ) -> Result<Self, String> {
        let sort = match parts.order_by.as_deref() {
            Some(clause) => resolve_sort(clause, names)?,
            None => Vec::new(),
        };
        Ok(Combined {
            out,
            starts: Vec::new(),
            keys: Vec::new(),
            key_starts: Vec::new(),
            seen: hashbrown::HashTable::new(),
            state: ahash::RandomState::new(),
            documents,
            widen,
            sort,
            limit: parts.limit,
            offset: parts.offset,
        })
    }

    /// True when no later branch can change the answer: with no sort, the
    /// rows kept so far are the first rows of the result, and a later branch
    /// only adds rows after them.
    pub fn is_full(&self) -> bool {
        self.sort.is_empty()
            && self
                .limit
                .is_some_and(|n| self.starts.len() as u64 >= n.saturating_add(self.offset))
    }

    /// Adds one branch's serialized rows. `distinct` drops each row equal to
    /// one already kept; the branches for which it is set must come first.
    pub fn push(&mut self, rows: &[u8], distinct: bool) -> Result<(), String> {
        let mut key = Vec::new();
        for row in serialized_rows(rows) {
            if self.is_full() {
                break;
            }
            let row = row.map_err(|e| format!("a UNION branch's rows could not be read: {}", e))?;
            if distinct {
                key.clear();
                duplicate_key(row, &self.documents, &mut key)
                    .map_err(|e| format!("a UNION branch's rows could not be read: {}", e))?;
                let hash = self.state.hash_one(&key[..]);
                let (keys, key_starts) = (&self.keys, &self.key_starts);
                let key_at = |i: u32| -> &[u8] {
                    let i = i as usize;
                    let end = key_starts.get(i + 1).copied().unwrap_or(keys.len());
                    &keys[key_starts[i]..end]
                };
                if self.seen.find(hash, |i| key_at(*i) == &key[..]).is_some() {
                    continue;
                }
                let state = &self.state;
                self.seen.insert_unique(hash, self.key_starts.len() as u32, |i| {
                    state.hash_one(key_at(*i))
                });
                self.key_starts.push(self.keys.len());
                self.keys.extend_from_slice(&key);
            }
            self.starts.push(self.out.len());
            if self.widen.iter().any(Option::is_some) {
                push_widened(row, &self.widen, &mut self.out)
                    .map_err(|e| format!("a UNION branch's rows could not be read: {}", e))?;
            } else {
                self.out.extend_from_slice(row);
            }
        }
        Ok(())
    }

    /// The combined rows, sorted, offset and limited, after what `out` began
    /// with.
    pub fn finish(self) -> Result<Vec<u8>, String> {
        let base = self.starts.first().copied().unwrap_or(self.out.len());
        let row_at = |i: usize| -> &[u8] {
            let end = self.starts.get(i + 1).copied().unwrap_or(self.out.len());
            &self.out[self.starts[i]..end]
        };
        let mut order: Vec<usize> = (0..self.starts.len()).collect();
        if !self.sort.is_empty() {
            // Each row's sort cells, read once rather than once per comparison.
            let mut cells: Vec<Vec<Option<(DbType, &[u8])>>> = Vec::with_capacity(order.len());
            for i in 0..self.starts.len() {
                let mut row: Vec<Option<(DbType, &[u8])>> = Vec::new();
                for cell in serialized_cells(row_at(i)).map_err(|e| e.to_string())? {
                    let cell = cell.map_err(|e| e.to_string())?;
                    row.push(Some((cell.column_type, cell.data)));
                }
                if let Some(key) = self.sort.iter().find(|k| k.position >= row.len()) {
                    return Err(format!(
                        "ORDER BY position {} is not in the UNION's result, which has {} column(s)",
                        key.position + 1,
                        row.len()
                    ));
                }
                cells.push(row);
            }
            let documents = &self.documents;
            // A stable sort, so rows the keys call equal keep the order the
            // branches produced them in.
            order.sort_by(|&a, &b| {
                for key in &self.sort {
                    let document = documents.get(key.position).copied().unwrap_or(false);
                    let ord = compare_cells(
                        cells[a][key.position].as_ref(),
                        cells[b][key.position].as_ref(),
                        document,
                        key,
                    );
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                Ordering::Equal
            });
        }
        let skip = usize::try_from(self.offset).unwrap_or(usize::MAX);
        let take = self
            .limit
            .map_or(usize::MAX, |n| usize::try_from(n).unwrap_or(usize::MAX));
        let picked = order.iter().skip(skip).take(take);
        // Rows already in place need no copy: no sort and no offset leaves
        // the buffer as it is, cut after the last row kept.
        if self.sort.is_empty() && skip == 0 {
            let kept = order.len().min(take);
            let end = self.starts.get(kept).copied().unwrap_or(self.out.len());
            let mut out = self.out;
            out.truncate(end);
            return Ok(out);
        }
        let mut out = Vec::with_capacity(self.out.len());
        out.extend_from_slice(&self.out[..base]);
        for &i in picked {
            out.extend_from_slice(row_at(i));
        }
        Ok(out)
    }
}

/// Reads a `UNION`'s trailing `ORDER BY` into output positions.
///
/// PostgreSQL sorts a set operation by its output columns only: a position, or
/// a name the first branch gives a column. An expression is refused there, as
/// it is here, rather than sorted by something else.
fn resolve_sort(clause: &str, names: &[Option<String>]) -> Result<Vec<SortKey>, String> {
    let terms = crate::core::rql::order_by::parse(clause)?;
    let mut keys = Vec::with_capacity(terms.len());
    for term in terms {
        let token = term.token.trim();
        let position = if !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit()) {
            match token.parse::<usize>() {
                Ok(n) if n >= 1 => n - 1,
                _ => {
                    return Err(format!(
                        "ORDER BY position {} is not in the UNION's result",
                        token
                    ));
                }
            }
        } else {
            let matching: Vec<usize> = names
                .iter()
                .enumerate()
                .filter(|(_, name)| name.as_deref().is_some_and(|n| n.eq_ignore_ascii_case(token)))
                .map(|(at, _)| at)
                .collect();
            match matching.as_slice() {
                [at] => *at,
                [] => {
                    return Err(format!(
                        "ORDER BY '{}' must name a column of the UNION's result, or give its position",
                        token
                    ));
                }
                _ => return Err(format!("ORDER BY '{}' is ambiguous", token)),
            }
        };
        keys.push(SortKey {
            position,
            descending: term.descending,
            // PostgreSQL's default: NULL is the largest value.
            nulls_first: term.nulls_first.unwrap_or(term.descending),
        });
    }
    Ok(keys)
}

/// One sort key's order for two cells, NULLs placed as the key says.
fn compare_cells(
    a: Option<&(DbType, &[u8])>,
    b: Option<&(DbType, &[u8])>,
    document: bool,
    key: &SortKey,
) -> Ordering {
    let null = |c: Option<&(DbType, &[u8])>| c.is_none_or(|(t, _)| *t == DbType::NULL);
    let null_side = if key.nulls_first { Ordering::Less } else { Ordering::Greater };
    let ord = match (null(a), null(b)) {
        (true, true) => return Ordering::Equal,
        (true, false) => return null_side,
        (false, true) => return null_side.reverse(),
        (false, false) => {
            let (a, b) = (a.unwrap(), b.unwrap());
            compare_values(a, b, document)
        }
    };
    if key.descending { ord.reverse() } else { ord }
}

/// Two non-NULL values of one output column, in PostgreSQL's order.
fn compare_values(a: &(DbType, &[u8]), b: &(DbType, &[u8]), document: bool) -> Ordering {
    let ((ta, va), (tb, vb)) = (a, b);
    // A stored document arrives typed as text and a computed one as `jsonb`;
    // both are documents and order as documents.
    if document || (*ta == DbType::JSONB && *tb == DbType::JSONB) {
        return crate::core::jsonb::order(va, vb);
    }
    if ta == tb {
        if va.is_empty() || vb.is_empty() {
            // Only text has an empty value, and it sorts first, as its bytes do.
            return va.len().cmp(&vb.len());
        }
        return crate::core::processor::concurrent_processor::compare_bytes_as_type(
            ta.clone(),
            va,
            vb,
        );
    }
    // Branches of different widths or kinds of number: PostgreSQL widens the
    // column to one type, and their values compare as numbers.
    if let (Some(x), Some(y)) = (integer(ta, va), integer(tb, vb)) {
        return x.cmp(&y);
    }
    if let (Some(x), Some(y)) = (number(ta, va), number(tb, vb)) {
        return crate::core::processor::concurrent_processor::pg_float_cmp(x, y);
    }
    if text_like(ta) && text_like(tb) {
        return va.cmp(vb);
    }
    // Types PostgreSQL would not have put in one column. A fixed order keeps
    // the sort total.
    ta.to_byte().cmp(&tb.to_byte()).then_with(|| va.cmp(vb))
}

fn text_like(ty: &DbType) -> bool {
    matches!(ty, DbType::STRING | DbType::CHAR)
}

fn le<const N: usize>(b: &[u8]) -> Option<[u8; N]> {
    b.try_into().ok()
}

/// An integer cell's value, whatever its width.
fn integer(ty: &DbType, v: &[u8]) -> Option<i128> {
    Some(match ty {
        DbType::I8 => i8::from_le_bytes(le::<1>(v)?) as i128,
        DbType::I16 => i16::from_le_bytes(le::<2>(v)?) as i128,
        DbType::I32 => i32::from_le_bytes(le::<4>(v)?) as i128,
        DbType::I64 => i64::from_le_bytes(le::<8>(v)?) as i128,
        DbType::I128 => i128::from_le_bytes(le::<16>(v)?),
        DbType::U8 => u8::from_le_bytes(le::<1>(v)?) as i128,
        DbType::U16 => u16::from_le_bytes(le::<2>(v)?) as i128,
        DbType::U32 => u32::from_le_bytes(le::<4>(v)?) as i128,
        DbType::U64 => u64::from_le_bytes(le::<8>(v)?) as i128,
        DbType::U128 => i128::try_from(u128::from_le_bytes(le::<16>(v)?)).ok()?,
        _ => return None,
    })
}

/// An integer or floating-point cell's value as a float.
fn number(ty: &DbType, v: &[u8]) -> Option<f64> {
    match ty {
        DbType::F32 => Some(f32::from_le_bytes(le::<4>(v)?) as f64),
        DbType::F64 => Some(f64::from_le_bytes(le::<8>(v)?)),
        _ => integer(ty, v).map(|i| i as f64),
    }
}

/// Appends what one serialized row is deduplicated by: equal for two rows
/// exactly when every pair of their values is equal, NULL equal to NULL, and
/// independent of which column position each value was read from.
///
/// Each value is a marker and then its bytes, with variable-width ones
/// length-prefixed, so the values of a row cannot run into each other:
///
/// - NULL is its marker alone.
/// - A document is keyed by its value (`jsonb::push_group_key`), so
///   `{"a":1}` and `{"a":1.0}` are one, as `=` says.
/// - An integer of any width is its value. A float is too when it holds a
///   whole number, as `int` and `float8` unify to `float8` and 1 = 1.0; any
///   other float is its value as an `f64`, zero and NaN each spelled once.
/// - Text and a single character are their bytes, so `text` meets `varchar`.
/// - Anything else keeps its type tag with its bytes, as before.
fn duplicate_key(row: &[u8], documents: &[bool], out: &mut Vec<u8>) -> std::io::Result<()> {
    for (position, cell) in serialized_cells(row)?.enumerate() {
        let cell = cell?;
        let (ty, data) = (&cell.column_type, cell.data);
        if *ty == DbType::NULL {
            out.push(0);
            continue;
        }
        if *ty == DbType::JSONB || documents.get(position).copied().unwrap_or(false) {
            out.push(1);
            crate::core::jsonb::push_group_key(data, out);
            continue;
        }
        if let Some(v) = integer(ty, data) {
            out.push(2);
            out.extend_from_slice(&v.to_le_bytes());
            continue;
        }
        if let (DbType::F32 | DbType::F64, Some(v)) = (ty, number(ty, data)) {
            // Below 2^127 a whole float is exactly an `i128`.
            if v.is_finite() && v.fract() == 0.0 && v.abs() < 1.7e38 {
                out.push(2);
                out.extend_from_slice(&(v as i128).to_le_bytes());
            } else if v.is_nan() {
                out.push(3);
            } else {
                out.push(4);
                out.extend_from_slice(&v.to_bits().to_le_bytes());
            }
            continue;
        }
        let text = match ty {
            DbType::STRING => Some(std::borrow::Cow::Borrowed(data)),
            DbType::CHAR => le::<4>(data)
                .and_then(|c| char::from_u32(u32::from_le_bytes(c)))
                .map(|c| std::borrow::Cow::Owned(c.to_string().into_bytes())),
            _ => None,
        };
        match text {
            Some(text) => {
                out.push(5);
                out.extend_from_slice(&(text.len() as u32).to_le_bytes());
                out.extend_from_slice(&text);
            }
            None => {
                out.push(6);
                out.push(ty.to_byte());
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                out.extend_from_slice(data);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::row::row::{Column, Row};

    fn cell(schema_id: u32, ty: DbType, data: &[u8]) -> Column {
        let mut block = crate::memory_pool::MEMORY_POOL.acquire(data.len());
        if !data.is_empty() {
            block.into_slice_mut().copy_from_slice(data);
        }
        Column { schema_id, data: block, column_type: ty }
    }

    fn serialized(columns: Vec<Column>) -> Vec<u8> {
        let row = Row { columns: columns.into_iter().collect(), ..Default::default() };
        let mut out = Vec::new();
        row.extend_serialized(&mut out);
        out
    }

    fn key(columns: Vec<Column>, documents: &[bool]) -> Vec<u8> {
        let mut out = Vec::new();
        duplicate_key(&serialized(columns), documents, &mut out).unwrap();
        out
    }

    #[test]
    fn the_key_ignores_the_source_position() {
        let a = key(vec![cell(2, DbType::STRING, b"open")], &[]);
        let b = key(vec![cell(1, DbType::STRING, b"open")], &[]);
        assert_eq!(a, b);
    }

    #[test]
    fn integers_of_different_widths_are_one_value() {
        let a = key(vec![cell(3, DbType::I32, &1i32.to_le_bytes())], &[]);
        let b = key(vec![cell(2, DbType::I64, &1i64.to_le_bytes())], &[]);
        let c = key(vec![cell(2, DbType::F64, &1f64.to_le_bytes())], &[]);
        assert_eq!(a, b);
        assert_eq!(a, c);
        let d = key(vec![cell(2, DbType::I64, &2i64.to_le_bytes())], &[]);
        assert_ne!(a, d);
    }

    #[test]
    fn null_meets_null_and_never_an_empty_string() {
        let a = key(vec![cell(2, DbType::NULL, b"")], &[]);
        let b = key(vec![cell(1, DbType::NULL, b"")], &[]);
        let c = key(vec![cell(1, DbType::STRING, b"")], &[]);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    fn split(sql: &str) -> UnionParts {
        crate::core::rql::join::split_union(sql).unwrap().unwrap()
    }

    #[test]
    fn the_distinct_part_is_the_prefix_up_to_the_last_bare_union() {
        assert_eq!(split("SELECT a FROM x UNION ALL SELECT a FROM y").distinct_prefix, 0);
        assert_eq!(split("SELECT a FROM x UNION SELECT a FROM y").distinct_prefix, 2);
        let p = split("SELECT a FROM x UNION SELECT a FROM y UNION ALL SELECT a FROM z");
        assert_eq!(p.distinct_prefix, 2);
        let p = split("SELECT a FROM x UNION ALL SELECT a FROM y UNION SELECT a FROM z");
        assert_eq!(p.distinct_prefix, 3);
    }

    #[test]
    fn trailing_clauses_belong_to_the_union() {
        let p = split("SELECT a FROM x UNION SELECT a FROM y WHERE a > 1 ORDER BY 1 DESC, a LIMIT 3 OFFSET 2");
        assert_eq!(p.branches, ["SELECT a FROM x", "SELECT a FROM y WHERE a > 1"]);
        assert_eq!(p.order_by.as_deref(), Some("1 DESC, a"));
        assert_eq!((p.limit, p.offset), (Some(3), 2));
        let p = split("SELECT a FROM x UNION SELECT a FROM y OFFSET 4");
        assert_eq!((p.order_by, p.limit, p.offset), (None, None, 4));
        let p = split("SELECT a FROM x UNION SELECT a FROM y LIMIT ALL");
        assert_eq!(p.limit, None);
        // The standard's spelling of the same page.
        let p = split("SELECT a FROM x UNION SELECT a FROM y ORDER BY 1 OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY");
        assert_eq!(p.branches, ["SELECT a FROM x", "SELECT a FROM y"]);
        assert_eq!(p.order_by.as_deref(), Some("1"));
        assert_eq!((p.limit, p.offset), (Some(3), 2));
        let p = split("SELECT a FROM x UNION ALL SELECT a FROM y FETCH FIRST ROW ONLY");
        assert_eq!(p.branches, ["SELECT a FROM x", "SELECT a FROM y"]);
        assert_eq!((p.order_by, p.limit, p.offset), (None, Some(1), 0));
    }

    #[test]
    fn a_parenthesised_branch_keeps_its_own_clauses() {
        let p = split("(SELECT a FROM x ORDER BY a LIMIT 2) UNION (SELECT a FROM y LIMIT 1) ORDER BY 1");
        assert_eq!(p.branches, ["SELECT a FROM x ORDER BY a LIMIT 2", "SELECT a FROM y LIMIT 1"]);
        assert_eq!(p.order_by.as_deref(), Some("1"));
        assert_eq!(p.limit, None);
    }

    #[test]
    fn an_unreadable_tail_is_refused_rather_than_left_on_the_last_branch() {
        for sql in [
            "SELECT a FROM x UNION SELECT a FROM y LIMIT $1",
            "SELECT a FROM x UNION SELECT a FROM y LIMIT 2 ORDER BY a",
            "SELECT a FROM x UNION SELECT a FROM y LIMIT 2 FOR UPDATE",
            "SELECT a FROM x UNION SELECT a FROM y ORDER BY 1 FETCH FIRST 2 ROWS WITH TIES",
            "SELECT a FROM x UNION SELECT a FROM y FETCH FIRST 2 ROWS",
        ] {
            assert!(crate::core::rql::join::split_union(sql).unwrap().is_err(), "{sql}");
        }
        assert!(crate::core::rql::join::split_union("SELECT a FROM x").is_none());
    }

    #[test]
    fn a_widened_row_reads_back_as_the_wider_type() {
        let row = serialized(vec![
            cell(3, DbType::I32, &(-7i32).to_le_bytes()),
            cell(1, DbType::STRING, b"open"),
            cell(2, DbType::NULL, b""),
        ]);
        let mut out = vec![2u8];
        push_widened(&row, &[Some(DbType::I64), None, Some(DbType::I64)], &mut out).unwrap();
        let back = Row::decode(&out[1..]).unwrap();
        assert_eq!(back.columns.len(), 3);
        assert_eq!(back.columns[0].column_type, DbType::I64);
        assert_eq!(back.columns[0].data.into_slice(), (-7i64).to_le_bytes());
        assert_eq!(back.columns[0].schema_id, 3);
        assert_eq!(back.columns[1].data.into_slice(), b"open");
        assert_eq!(back.columns[2].column_type, DbType::NULL);
        // Nothing is left over after the row.
        assert_eq!(serialized_rows(&out[1..]).count(), 1);
    }

    #[test]
    fn values_cannot_run_into_each_other() {
        let a = key(vec![cell(0, DbType::STRING, b"ab"), cell(1, DbType::STRING, b"c")], &[]);
        let b = key(vec![cell(0, DbType::STRING, b"a"), cell(1, DbType::STRING, b"bc")], &[]);
        assert_ne!(a, b);
    }
}
