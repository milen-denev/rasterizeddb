//! The native protocol's result format, read without the engine.
//!
//! A result is rows back to back. Each row is a little-endian `u32` payload
//! length, then per column: a `u64` data size, a `u64` schema id (the
//! column's write order, unused here), the data, and a one-byte type tag.
//! This mirrors `core/row/row.rs` (`Row::extend_serialized`,
//! `serialized_cells`) for a server built without `enable_long_row`; the
//! integration tests read real answers through it.

use crate::from_sql::{FromSql, FromSqlOwned};
use crate::value::Value;
use crate::{Error, Result};

/// The type tags a cell can carry (`DbType::to_byte` in the server).
pub mod tag {
    /// `TINYINT`: an `i8`.
    pub const I8: u8 = 1;
    /// `SMALLINT`: an `i16`, little-endian.
    pub const I16: u8 = 2;
    /// `INTEGER`: an `i32`, little-endian.
    pub const I32: u8 = 3;
    /// `BIGINT`: an `i64`, little-endian (also `count(*)`).
    pub const I64: u8 = 4;
    /// `HUGEINT`: an `i128`, little-endian.
    pub const I128: u8 = 5;
    /// `UTINYINT`: a `u8`.
    pub const U8: u8 = 6;
    /// `USMALLINT`: a `u16`, little-endian.
    pub const U16: u8 = 7;
    /// `UINT`: a `u32`, little-endian.
    pub const U32: u8 = 8;
    /// `UBIGINT`: a `u64`, little-endian.
    pub const U64: u8 = 9;
    /// `UHUGEINT`: a `u128`, little-endian.
    pub const U128: u8 = 10;
    /// `REAL`: an `f32`, little-endian.
    pub const F32: u8 = 11;
    /// `DOUBLE PRECISION`: an `f64`, little-endian.
    pub const F64: u8 = 12;
    /// A legacy fixed-width character column.
    pub const CHAR: u8 = 13;
    /// Text, and also `bytea`, `char(n)`, `jsonb` and arrays, which the native protocol sends under this tag.
    pub const STRING: u8 = 14;
    /// `timestamptz` and `timestamp` alike: `i64` µs since 1970, little-endian.
    pub const DATETIME: u8 = 15;
    /// `BOOLEAN`: one byte, 0 or 1.
    pub const BOOL: u8 = 16;
    /// `UUID`: 16 octets.
    pub const UUID: u8 = 17;
    /// `INET`: family, 16 address bytes (big-endian), prefix length.
    pub const INET: u8 = 18;
    /// `NUMERIC`: an `i128`, little-endian, then a scale byte when the wire
    /// carries it (see [`Numeric`](crate::Numeric)).
    pub const DECIMAL: u8 = 19;
    /// `bytea` from an expression (a stored `bytea` column arrives as [`STRING`]).
    pub const BYTEA: u8 = 20;
    /// An array in PostgreSQL's binary form (a stored array column arrives as [`STRING`]).
    pub const ARRAY: u8 = 21;
    /// A JSONB document in the engine's binary form.
    pub const JSONB: u8 = 22;
    /// JSON text from an expression.
    pub const JSON: u8 = 23;
    /// `INTERVAL`: µs, days, months, big-endian.
    pub const INTERVAL: u8 = 24;
    /// `DATE`: days since 2000-01-01, big-endian `i32`.
    pub const DATE: u8 = 29;
    /// `TIME`: µs since midnight, big-endian `i64`.
    pub const TIME: u8 = 30;
    /// `timestamp` from an expression; stored ones arrive as [`DATETIME`].
    pub const TIMESTAMP: u8 = 46;
    /// A `json` column's text.
    pub const JSONTEXT: u8 = 47;
    /// SQL NULL.
    pub const NULL: u8 = 253;
}

const ROW_LEN_HEADER: usize = 4;
const COLUMN_OVERHEAD: usize = 8 + 8 + 1;

#[derive(Debug, Clone, Copy)]
struct Cell {
    tag: u8,
    start: usize,
    len: usize,
}

/// The answer to a statement: its rows, if it returned any, and the row count
/// the server reported, if it reported one.
#[derive(Debug, Default)]
pub struct Rows {
    data: Vec<u8>,
    cells: Vec<Cell>,
    /// Per row: where its cells start in `cells`, and how many it has.
    rows: Vec<(usize, usize)>,
    affected: Option<u64>,
}

impl Rows {
    pub(crate) fn affected(count: Option<u64>) -> Self {
        Rows {
            affected: count,
            ..Rows::default()
        }
    }

    /// Splits a serialized result into rows and cells. Every length is
    /// checked against the buffer; a result that ends partway through a row is
    /// an error, never a shorter answer.
    pub(crate) fn decode(data: Vec<u8>) -> Result<Self> {
        let truncated =
            || Error::Protocol("a row in the answer is truncated or its lengths disagree".into());
        let mut cells = Vec::new();
        let mut rows = Vec::new();
        let mut at = 0usize;
        while at < data.len() {
            let header = data.get(at..at + ROW_LEN_HEADER).ok_or_else(truncated)?;
            let payload = u32::from_le_bytes(header.try_into().unwrap()) as usize;
            let end = (at + ROW_LEN_HEADER)
                .checked_add(payload)
                .filter(|end| *end <= data.len())
                .ok_or_else(truncated)?;
            let first = cells.len();
            let mut p = at + ROW_LEN_HEADER;
            while p < end {
                if end - p < COLUMN_OVERHEAD {
                    return Err(truncated());
                }
                let size = u64::from_le_bytes(data[p..p + 8].try_into().unwrap());
                let size = usize::try_from(size)
                    .ok()
                    .filter(|s| *s <= end - p - COLUMN_OVERHEAD)
                    .ok_or_else(truncated)?;
                let start = p + 16;
                cells.push(Cell {
                    tag: data[start + size],
                    start,
                    len: size,
                });
                p = start + size + 1;
            }
            rows.push((first, cells.len() - first));
            // Sized once from the first row, the answer's rows being alike:
            // pushing from empty regrew both vectors two or three times on
            // every answer, a measurable share of the client's allocations.
            if rows.len() == 1 && end < data.len() {
                let estimate = data.len().div_ceil(end.max(1));
                rows.reserve(estimate);
                cells.reserve(estimate.saturating_mul(cells.len()));
            }
            at = end;
        }
        Ok(Rows {
            data,
            cells,
            rows,
            affected: None,
        })
    }

    /// How many rows the answer holds.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether the answer holds no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Row `i`, if there is one.
    pub fn get(&self, i: usize) -> Option<Row<'_>> {
        let &(first, count) = self.rows.get(i)?;
        Some(Row {
            data: &self.data,
            cells: &self.cells[first..first + count],
        })
    }

    /// The rows, in order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Row<'_>> + '_ {
        (0..self.rows.len()).map(|i| self.get(i).unwrap())
    }

    /// The row count a write reported (`INSERT`, `UPDATE`, `DELETE` without
    /// `RETURNING`). `None` for an answer that carried rows instead, or none.
    pub fn rows_affected(&self) -> Option<u64> {
        self.affected
    }

    /// Every row as `T` (see [`FromRow`]).
    pub fn decode_all<T: FromRow>(&self) -> Result<Vec<T>> {
        self.iter().map(|row| T::from_row(&row)).collect()
    }

    /// The first row as `T`, if there is one.
    pub fn decode_first<T: FromRow>(&self) -> Result<Option<T>> {
        self.get(0).map(|row| T::from_row(&row)).transpose()
    }
}

impl<'a> IntoIterator for &'a Rows {
    type Item = Row<'a>;
    type IntoIter = Box<dyn ExactSizeIterator<Item = Row<'a>> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// One row of an answer. Columns are positional: column `i` is item `i` of
/// the statement's select or `RETURNING` list.
#[derive(Clone, Copy)]
pub struct Row<'a> {
    data: &'a [u8],
    cells: &'a [Cell],
}

impl std::fmt::Debug for Row<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        for i in 0..self.len() {
            list.entry(&(self.tag(i), self.raw(i)));
        }
        list.finish()
    }
}

impl<'a> Row<'a> {
    /// How many columns the row has.
    pub fn len(&self) -> usize {
        self.cells.len()
    }

    /// Whether the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    fn cell(&self, i: usize) -> Result<Cell> {
        self.cells.get(i).copied().ok_or_else(|| {
            Error::column(
                i,
                format!("the row has only {} column(s)", self.cells.len()),
            )
        })
    }

    /// The cell's type tag ([`tag`]), or `None` past the end of the row.
    pub fn tag(&self, i: usize) -> Option<u8> {
        self.cells.get(i).map(|c| c.tag)
    }

    /// The cell's bytes as the server sent them, `None` past the end of the
    /// row. A NULL is an empty slice under [`tag::NULL`].
    pub fn raw(&self, i: usize) -> Option<&'a [u8]> {
        self.cells
            .get(i)
            .map(|c| &self.data[c.start..c.start + c.len])
    }

    /// Whether column `i` is NULL. Past the end of the row is an error, not
    /// a NULL.
    pub fn is_null(&self, i: usize) -> Result<bool> {
        Ok(self.cell(i)?.tag == tag::NULL)
    }

    /// Column `i` as `T`: any [`FromSql`] type, `Option<T>` for a column that
    /// may be NULL.
    ///
    /// ```no_run
    /// # fn demo(row: rasterizeddb_client::Row<'_>) -> rasterizeddb_client::Result<()> {
    /// let id: i64 = row.get(0)?;
    /// let name: &str = row.get(1)?;
    /// let score: Option<f64> = row.get(2)?;
    /// # Ok(()) }
    /// ```
    pub fn get<T: FromSql<'a>>(&self, i: usize) -> Result<T> {
        let cell = self.cell(i)?;
        if cell.tag == tag::NULL {
            return T::from_null().ok_or_else(|| Error::column(i, "is NULL"));
        }
        T::from_sql(cell.tag, &self.data[cell.start..cell.start + cell.len])
            .map_err(|message| Error::column(i, message))
    }

    /// Every column as a [`Value`].
    pub fn values(&self) -> Result<Vec<Value>> {
        (0..self.len()).map(|i| self.get(i)).collect()
    }

    /// The row as `T`: a tuple of [`FromSql`] types, or a struct deriving
    /// [`FromRow`].
    pub fn decode<T: FromRow>(&self) -> Result<T> {
        T::from_row(self)
    }

    /// An integer column of any width, if it fits `i64`.
    pub fn get_opt_i64(&self, i: usize) -> Result<Option<i64>> {
        self.get(i)
    }

    /// An integer column, if it fits `i32`.
    pub fn get_opt_i32(&self, i: usize) -> Result<Option<i32>> {
        self.get(i)
    }

    /// An integer column, if it fits `i16`.
    pub fn get_opt_i16(&self, i: usize) -> Result<Option<i16>> {
        self.get(i)
    }

    /// An integer column, if it fits `i64`; NULL is an error.
    pub fn get_i64(&self, i: usize) -> Result<i64> {
        self.get(i)
    }

    /// An integer column, if it fits `i32`; NULL is an error.
    pub fn get_i32(&self, i: usize) -> Result<i32> {
        self.get(i)
    }

    /// An integer column, if it fits `i16`; NULL is an error.
    pub fn get_i16(&self, i: usize) -> Result<i16> {
        self.get(i)
    }

    /// A `double precision` or `real` column (or an integer, or a NUMERIC
    /// that carries its scale).
    pub fn get_opt_f64(&self, i: usize) -> Result<Option<f64>> {
        self.get(i)
    }

    /// [`get_opt_f64`](Self::get_opt_f64); NULL is an error.
    pub fn get_f64(&self, i: usize) -> Result<f64> {
        self.get(i)
    }

    /// A `real` column, or a `double precision` one whose value is exactly
    /// an `f32`.
    pub fn get_opt_f32(&self, i: usize) -> Result<Option<f32>> {
        self.get(i)
    }

    /// [`get_opt_f32`](Self::get_opt_f32); NULL is an error.
    pub fn get_f32(&self, i: usize) -> Result<f32> {
        self.get(i)
    }

    /// A `BOOLEAN` column.
    pub fn get_opt_bool(&self, i: usize) -> Result<Option<bool>> {
        self.get(i)
    }

    /// A `BOOLEAN` column; NULL is an error.
    pub fn get_bool(&self, i: usize) -> Result<bool> {
        self.get(i)
    }

    /// A text column (`text`, `varchar`, a JSON document's text), checked to
    /// be UTF-8.
    pub fn get_opt_text(&self, i: usize) -> Result<Option<&'a str>> {
        self.get(i)
    }

    /// [`get_opt_text`](Self::get_opt_text); NULL is an error.
    pub fn get_text(&self, i: usize) -> Result<&'a str> {
        self.get(i)
    }

    /// A `bytea` column's bytes. The engine stores `bytea` as a string of
    /// bytes, so a stored one may arrive tagged [`tag::STRING`] as well as
    /// [`tag::BYTEA`]; both are accepted, other tags are not.
    pub fn get_opt_bytes(&self, i: usize) -> Result<Option<&'a [u8]>> {
        self.get(i)
    }

    /// [`get_opt_bytes`](Self::get_opt_bytes); NULL is an error.
    pub fn get_bytes(&self, i: usize) -> Result<&'a [u8]> {
        self.get(i)
    }

    /// A `NUMERIC` as exact decimal text. Reads one that carries its scale
    /// (what the server sends), an integer, or a column selected as
    /// `col::text`; a scaled `NUMERIC` with no scale, from a server that
    /// predates sending it, is an error rather than a guess.
    pub fn get_numeric(&self, i: usize) -> Result<crate::Numeric> {
        self.get(i)
    }

    /// [`get_numeric`](Self::get_numeric), NULL as `None`.
    pub fn get_opt_numeric(&self, i: usize) -> Result<Option<crate::Numeric>> {
        self.get(i)
    }
}

/// A type a whole row can be read as: a tuple of [`FromSql`] types (up to
/// twelve), a [`Vec<Value>`], or a struct with `#[derive(FromRow)]` or
/// `#[derive(Model)]`.
///
/// Columns are matched by position: field or element `i` is column `i`.
pub trait FromRow: Sized {
    /// The row as `Self`.
    fn from_row(row: &Row<'_>) -> Result<Self>;
}

impl FromRow for Vec<Value> {
    fn from_row(row: &Row<'_>) -> Result<Self> {
        row.values()
    }
}

/// Fails when a row has more or fewer columns than the tuple, so a select list
/// that drifted from the code is found at once.
pub fn check_width(row: &Row<'_>, width: usize) -> Result<()> {
    if row.len() == width {
        Ok(())
    } else {
        Err(Error::Column {
            index: width.min(row.len()),
            message: format!("the row has {} column(s), {width} were expected", row.len()),
        })
    }
}

macro_rules! tuple_from_row {
    ($width:literal: $($t:ident $i:tt),+) => {
        impl<$($t: FromSqlOwned),+> FromRow for ($($t,)+) {
            fn from_row(row: &Row<'_>) -> Result<Self> {
                check_width(row, $width)?;
                Ok(($(row.get::<$t>($i)?,)+))
            }
        }
    };
}
tuple_from_row!(1: A 0);
tuple_from_row!(2: A 0, B 1);
tuple_from_row!(3: A 0, B 1, C 2);
tuple_from_row!(4: A 0, B 1, C 2, D 3);
tuple_from_row!(5: A 0, B 1, C 2, D 3, E 4);
tuple_from_row!(6: A 0, B 1, C 2, D 3, E 4, F 5);
tuple_from_row!(7: A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuple_from_row!(8: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuple_from_row!(9: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuple_from_row!(10: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
tuple_from_row!(11: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuple_from_row!(12: A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(out: &mut Vec<u8>, tag: u8, data: &[u8]) {
        out.extend_from_slice(&(data.len() as u64).to_le_bytes());
        out.extend_from_slice(&7u64.to_le_bytes());
        out.extend_from_slice(data);
        out.push(tag);
    }

    fn row(cells: &[(u8, &[u8])]) -> Vec<u8> {
        let mut payload = Vec::new();
        for (t, d) in cells {
            cell(&mut payload, *t, d);
        }
        let mut out = (payload.len() as u32).to_le_bytes().to_vec();
        out.extend(payload);
        out
    }

    #[test]
    fn decodes_typed_cells_positionally() {
        let mut data = row(&[
            (tag::I64, &(-5i64).to_le_bytes()),
            (tag::I16, &3i16.to_le_bytes()),
            (tag::STRING, b"a'b"),
            (tag::NULL, b""),
            (tag::BOOL, &[1]),
            (tag::STRING, &[0, 255]),
        ]);
        data.extend(row(&[(tag::I32, &9i32.to_le_bytes())]));
        let rows = Rows::decode(data).unwrap();
        assert_eq!(rows.len(), 2);
        let r = rows.get(0).unwrap();
        assert_eq!(r.get_i64(0).unwrap(), -5);
        assert_eq!(r.get_i16(1).unwrap(), 3);
        assert_eq!(r.get_i64(1).unwrap(), 3);
        assert_eq!(r.get_text(2).unwrap(), "a'b");
        assert!(r.is_null(3).unwrap());
        assert_eq!(r.get_opt_text(3).unwrap(), None);
        assert!(r.get_text(3).is_err());
        assert!(r.get_bool(4).unwrap());
        assert_eq!(r.get_bytes(5).unwrap(), &[0, 255]);
        assert!(r.get_text(5).is_err(), "not UTF-8");
        assert!(r.is_null(6).is_err(), "past the end is not NULL");
        assert_eq!(rows.get(1).unwrap().get_i32(0).unwrap(), 9);
    }

    /// The vectors are sized from the first row. A first row narrower than
    /// the rest must still give every later row all of its cells.
    #[test]
    fn rows_wider_than_the_first_are_whole() {
        let mut data = row(&[(tag::I32, &1i32.to_le_bytes())]);
        let long = vec![b'x'; 300];
        for i in 0..50i64 {
            data.extend(row(&[
                (tag::I64, &i.to_le_bytes()),
                (tag::STRING, &long),
                (tag::I16, &7i16.to_le_bytes()),
            ]));
        }
        let rows = Rows::decode(data).unwrap();
        assert_eq!(rows.len(), 51);
        assert_eq!(rows.get(0).unwrap().get_i32(0).unwrap(), 1);
        for i in 0..50 {
            let r = rows.get(i + 1).unwrap();
            assert_eq!(r.get_i64(0).unwrap(), i as i64);
            assert_eq!(r.get_text(1).unwrap().len(), 300);
            assert_eq!(r.get_i16(2).unwrap(), 7);
            assert!(r.is_null(3).is_err(), "row {i} has a fourth cell");
        }
    }

    #[test]
    fn a_null_cell_is_a_null_value() {
        let rows = Rows::decode(row(&[
            (tag::I64, &1i64.to_le_bytes()),
            (tag::NULL, &[0, 0, 0, 0]),
        ]))
        .unwrap();
        let r = rows.get(0).unwrap();
        assert_eq!(r.values().unwrap(), vec![Value::I64(1), Value::Null]);
        assert_eq!(r.decode::<Vec<Value>>().unwrap()[1], Value::Null);
    }

    #[test]
    fn narrowing_is_checked() {
        let rows = Rows::decode(row(&[(tag::I64, &70000i64.to_le_bytes())])).unwrap();
        let r = rows.get(0).unwrap();
        assert!(r.get_i16(0).is_err());
        assert_eq!(r.get_i32(0).unwrap(), 70000);
    }

    #[test]
    fn a_truncated_answer_is_an_error() {
        let mut data = row(&[(tag::STRING, b"hello")]);
        data.pop();
        assert!(Rows::decode(data).is_err());
        let mut lying = row(&[(tag::STRING, b"hello")]);
        lying[4] = 200; // the cell claims 200 bytes
        assert!(Rows::decode(lying).is_err());
    }

    #[test]
    fn a_wrong_width_is_an_error() {
        let rows = Rows::decode(row(&[(tag::I64, &[1, 2, 3])])).unwrap();
        assert!(rows.get(0).unwrap().get_i64(0).is_err());
    }
}
