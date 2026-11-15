//! The native protocol's result format, read without the engine.
//!
//! A result is rows back to back. Each row is a little-endian `u32` payload
//! length, then per column: a `u64` data size, a `u64` schema id (the
//! column's write order, unused here), the data, and a one-byte type tag.
//! This mirrors `core/row/row.rs` (`Row::extend_serialized`,
//! `serialized_cells`) for a server built without `enable_long_row`; the
//! integration tests read real answers through it.

use crate::{Error, Result};

/// The type tags a cell can carry (`DbType::to_byte` in the server).
pub mod tag {
    pub const I8: u8 = 1;
    pub const I16: u8 = 2;
    pub const I32: u8 = 3;
    pub const I64: u8 = 4;
    pub const I128: u8 = 5;
    pub const U8: u8 = 6;
    pub const U16: u8 = 7;
    pub const U32: u8 = 8;
    pub const U64: u8 = 9;
    pub const U128: u8 = 10;
    pub const F32: u8 = 11;
    pub const F64: u8 = 12;
    pub const CHAR: u8 = 13;
    pub const STRING: u8 = 14;
    pub const BOOL: u8 = 16;
    pub const BYTEA: u8 = 20;
    pub const JSON: u8 = 23;
    pub const JSONTEXT: u8 = 47;
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

    pub fn iter(&self) -> impl ExactSizeIterator<Item = Row<'_>> + '_ {
        (0..self.rows.len()).map(|i| self.get(i).unwrap())
    }

    /// The row count a write reported (`INSERT`, `UPDATE`, `DELETE` without
    /// `RETURNING`). `None` for an answer that carried rows instead, or none.
    pub fn rows_affected(&self) -> Option<u64> {
        self.affected
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

    fn non_null(&self, i: usize) -> Result<Option<(u8, &'a [u8])>> {
        let cell = self.cell(i)?;
        if cell.tag == tag::NULL {
            return Ok(None);
        }
        Ok(Some((
            cell.tag,
            &self.data[cell.start..cell.start + cell.len],
        )))
    }

    fn required<T>(i: usize, value: Option<T>) -> Result<T> {
        value.ok_or_else(|| Error::column(i, "is NULL"))
    }

    /// Any integer cell, as `i128`, checked against its width.
    fn integer(&self, i: usize) -> Result<Option<i128>> {
        let Some((tag, bytes)) = self.non_null(i)? else {
            return Ok(None);
        };
        fn fixed<const N: usize>(i: usize, tag: u8, bytes: &[u8]) -> Result<[u8; N]> {
            bytes.try_into().map_err(|_| {
                Error::column(
                    i,
                    format!(
                        "type tag {tag} needs {N} bytes, the cell has {}",
                        bytes.len()
                    ),
                )
            })
        }
        let value = match tag {
            tag::I8 => i8::from_le_bytes(fixed(i, tag, bytes)?) as i128,
            tag::I16 => i16::from_le_bytes(fixed(i, tag, bytes)?) as i128,
            tag::I32 => i32::from_le_bytes(fixed(i, tag, bytes)?) as i128,
            tag::I64 => i64::from_le_bytes(fixed(i, tag, bytes)?) as i128,
            tag::I128 => i128::from_le_bytes(fixed(i, tag, bytes)?),
            tag::U8 => u8::from_le_bytes(fixed(i, tag, bytes)?) as i128,
            tag::U16 => u16::from_le_bytes(fixed(i, tag, bytes)?) as i128,
            tag::U32 => u32::from_le_bytes(fixed(i, tag, bytes)?) as i128,
            tag::U64 => u64::from_le_bytes(fixed(i, tag, bytes)?) as i128,
            other => {
                return Err(Error::column(
                    i,
                    format!("type tag {other} is not an integer"),
                ));
            }
        };
        Ok(Some(value))
    }

    fn narrowed<T: TryFrom<i128>>(&self, i: usize, name: &str) -> Result<Option<T>> {
        match self.integer(i)? {
            None => Ok(None),
            Some(v) => T::try_from(v)
                .map(Some)
                .map_err(|_| Error::column(i, format!("{v} does not fit {name}"))),
        }
    }

    /// An integer column of any width, if it fits `i64`.
    pub fn get_opt_i64(&self, i: usize) -> Result<Option<i64>> {
        self.narrowed(i, "i64")
    }

    pub fn get_opt_i32(&self, i: usize) -> Result<Option<i32>> {
        self.narrowed(i, "i32")
    }

    pub fn get_opt_i16(&self, i: usize) -> Result<Option<i16>> {
        self.narrowed(i, "i16")
    }

    pub fn get_i64(&self, i: usize) -> Result<i64> {
        Self::required(i, self.get_opt_i64(i)?)
    }

    pub fn get_i32(&self, i: usize) -> Result<i32> {
        Self::required(i, self.get_opt_i32(i)?)
    }

    pub fn get_i16(&self, i: usize) -> Result<i16> {
        Self::required(i, self.get_opt_i16(i)?)
    }

    pub fn get_opt_bool(&self, i: usize) -> Result<Option<bool>> {
        match self.non_null(i)? {
            None => Ok(None),
            Some((tag::BOOL, [b])) => Ok(Some(*b != 0)),
            Some((tag, bytes)) => Err(Error::column(
                i,
                format!(
                    "type tag {tag} with {} byte(s) is not a boolean",
                    bytes.len()
                ),
            )),
        }
    }

    pub fn get_bool(&self, i: usize) -> Result<bool> {
        Self::required(i, self.get_opt_bool(i)?)
    }

    /// A text column (`text`, `varchar`, a JSON document's text), checked to
    /// be UTF-8.
    pub fn get_opt_text(&self, i: usize) -> Result<Option<&'a str>> {
        match self.non_null(i)? {
            None => Ok(None),
            Some((tag::STRING | tag::JSON | tag::JSONTEXT, bytes)) => std::str::from_utf8(bytes)
                .map(Some)
                .map_err(|_| Error::column(i, "text is not valid UTF-8")),
            Some((tag, _)) => Err(Error::column(i, format!("type tag {tag} is not text"))),
        }
    }

    pub fn get_text(&self, i: usize) -> Result<&'a str> {
        Self::required(i, self.get_opt_text(i)?)
    }

    /// A `bytea` column's bytes. The engine stores `bytea` as a string of
    /// bytes, so a stored one may arrive tagged [`tag::STRING`] as well as
    /// [`tag::BYTEA`]; both are accepted, other tags are not.
    pub fn get_opt_bytes(&self, i: usize) -> Result<Option<&'a [u8]>> {
        match self.non_null(i)? {
            None => Ok(None),
            Some((tag::BYTEA | tag::STRING, bytes)) => Ok(Some(bytes)),
            Some((tag, _)) => Err(Error::column(i, format!("type tag {tag} is not bytea"))),
        }
    }

    pub fn get_bytes(&self, i: usize) -> Result<&'a [u8]> {
        Self::required(i, self.get_opt_bytes(i)?)
    }
}

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
