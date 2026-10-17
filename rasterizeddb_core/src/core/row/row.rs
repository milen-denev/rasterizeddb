use std::{borrow::Cow, io::Read};

use byteorder::ReadBytesExt;
use smallvec::SmallVec;

use crate::{
    core::db_type::DbType,
    memory_pool::{MEMORY_POOL, MemoryBlock},
};

use super::schema::SchemaField;

/// `Clone` because a scan may be retried under a wider slot ceiling, and this
/// is a small descriptor of *which* columns to read — offsets, types and sizes,
/// no row data — so copying it costs nothing next to the scan it configures.
#[derive(Debug, Clone)]
pub struct RowFetch {
    pub columns_fetching_data: SmallVec<[ColumnFetchingData; 32]>,
}

#[derive(Debug, Clone)]
pub struct ColumnFetchingData {
    pub column_offset: u32,
    pub column_type: DbType,
    pub size: u32,
    pub schema_id: u64,
}

pub struct RowWrite {
    pub columns_writing_data: SmallVec<[ColumnWritePayload; 32]>,
}

pub struct ColumnWritePayload {
    pub data: MemoryBlock,
    pub write_order: u32,
    pub column_type: DbType,
    pub size: u32,
}

/// How many columns a `Row` holds before spilling to the heap.
///
/// Was 32, which made every `Row` ~1.5 KB whatever it held: a scan of a
/// five-column table materialised 1,560 bytes per row. 12 was chosen from
/// measurement (`examples/row_inline_capacity_bench.rs`, 1M rows): against 32
/// it is 2x faster to materialise and fold rows of 4–12 columns, ties at 20,
/// and still wins at 40; against 8 it gives up ~25% at 4–8 columns and wins
/// 15–60% at 12. The application schemas fetch 2–22 columns (companions
/// included), so 12 has the smallest worst-case loss.
pub const ROW_INLINE_COLUMNS: usize = 12;

/// A row's columns. Spell the type through this alias so the inline capacity
/// is changed in one place.
pub type RowColumns = SmallVec<[Column; ROW_INLINE_COLUMNS]>;

#[derive(Debug, Default, Clone)]
pub struct Row {
    pub position: u64,
    pub columns: RowColumns,
    #[cfg(feature = "enable_long_row")]
    pub length: u64,
    #[cfg(not(feature = "enable_long_row"))]
    pub length: u32,
}

impl Row {
    pub fn clone_row(row: &Row) -> Row {
        let columns = row.columns.iter().map(|col| col.deep_clone()).collect();
        Row {
            position: row.position.clone(),
            columns: columns,
            length: row.length.clone(),
        }
    }

    pub fn get_column(
        &self,
        column_name: &str,
        schema: &SmallVec<[SchemaField; 20]>,
    ) -> Option<MemoryBlock> {
        let result = self.columns.iter().find_map(|col| {
            // `schema_id` is a `write_order`, not a position: `calculate_schema_offset`
            // returns `field.write_order`. Indexing the slice with it holds only
            // while the two happen to coincide, and they stop coinciding as soon
            // as a caller passes a *projection* rather than the whole field list —
            // which is what a SELECT of some columns does. The same raw index in
            // pgwire panicked a worker with "len is 22 but the index is 22" and
            // dropped the connection.
            //
            // Positional lookup stays as a fallback so a caller that really did
            // pass indices keeps working.
            let field = schema
                .iter()
                .find(|f| f.write_order == u64::from(col.schema_id))
                .or_else(|| schema.get(col.schema_id as usize))?;
            if field.name == *column_name {
                Some(col.data.clone())
            } else {
                None
            }
        });

        result
    }

    pub fn into_vec(self) -> Vec<u8> {
        let mut vec = Vec::new();
        self.extend_serialized(&mut vec);
        vec
    }

    /// Append this row's wire-format bytes to `out` without allocating a fresh
    /// `Vec` per row. Equivalent to `out.extend_from_slice(&self.into_vec())`,
    /// but avoids the intermediate allocation and a redundant memcpy. Used by
    /// the bytes-mode query path so workers can serialize directly into a
    /// per-worker output buffer.
    pub fn extend_serialized(&self, out: &mut Vec<u8>) {
        #[cfg(feature = "enable_long_row")]
        const LEN_HEADER: usize = 8;
        #[cfg(not(feature = "enable_long_row"))]
        const LEN_HEADER: usize = 4;

        // Per column: an 8-byte size, an 8-byte schema id, the data, and a
        // 1-byte type tag.
        const COLUMN_OVERHEAD: usize = 8 + 8 + 1;

        // Size the whole row up front. The previous version issued four
        // separately capacity-checked writes per column and then patched the
        // header afterwards; one `reserve` plus straight-line stores replaces
        // roughly 4N bounds/growth checks with one.
        let payload: usize = self.columns.len() * COLUMN_OVERHEAD
            + self
                .columns
                .iter()
                .map(|column| column.data.len())
                .sum::<usize>();

        let start = out.len();
        out.reserve(LEN_HEADER + payload);

        // SAFETY: `reserve` guarantees `LEN_HEADER + payload` writable bytes
        // past `start`, and the loop below writes exactly that many before
        // `set_len` makes them visible.
        unsafe {
            let mut p = out.as_mut_ptr().add(start);

            #[cfg(feature = "enable_long_row")]
            let header = (payload as u64).to_le_bytes();
            #[cfg(not(feature = "enable_long_row"))]
            let header = (payload as u32).to_le_bytes();

            std::ptr::copy_nonoverlapping(header.as_ptr(), p, LEN_HEADER);
            p = p.add(LEN_HEADER);

            for column in &self.columns {
                let column_data = column.data.into_slice();
                let column_size = column_data.len();

                std::ptr::copy_nonoverlapping((column_size as u64).to_le_bytes().as_ptr(), p, 8);
                p = p.add(8);

                // Still 8 bytes on the wire: widened, never a 4-byte read
                // stretched over 8.
                std::ptr::copy_nonoverlapping(u64::from(column.schema_id).to_le_bytes().as_ptr(), p, 8);
                p = p.add(8);

                if column_size > 0 {
                    std::ptr::copy_nonoverlapping(column_data.as_ptr(), p, column_size);
                    p = p.add(column_size);
                }

                *p = column.column_type.to_byte();
                p = p.add(1);
            }

            debug_assert_eq!(
                p as usize - out.as_mut_ptr().add(start) as usize,
                LEN_HEADER + payload,
                "serialized byte count must match the reserved size"
            );

            out.set_len(start + LEN_HEADER + payload);
        }
    }

    pub fn from_vec(bytes: &[u8]) -> std::io::Result<Self> {
        let mut cursor = std::io::Cursor::new(bytes);

        #[cfg(feature = "enable_long_row")]
        let length = cursor.read_u64::<byteorder::LittleEndian>()?;

        #[cfg(not(feature = "enable_long_row"))]
        let length = cursor.read_u32::<byteorder::LittleEndian>()?;

        let mut columns = SmallVec::new();
        while cursor.position() < bytes.len() as u64 {
            let column_size = cursor.read_u64::<byteorder::LittleEndian>()?;
            let schema_id = cursor.read_u64::<byteorder::LittleEndian>()?;

            let mut column_data_mb = MEMORY_POOL.acquire(column_size as usize);
            let mut column_data = column_data_mb.into_slice_mut();
            cursor.read_exact(&mut column_data)?;

            let column_type = DbType::from_byte(cursor.read_u8()?);
            // A column id past 32 bits is not one DDL can have handed out, so
            // the buffer is damaged; refused rather than truncated.
            let schema_id = u32::try_from(schema_id).map_err(|_| truncated_row())?;
            columns.push(Column {
                schema_id,
                data: column_data_mb,
                column_type,
            });
        }
        Ok(Row {
            position: 0,
            columns,
            length,
        })
    }

    pub fn from_cursor(
        cursor: &mut std::io::Cursor<&[u8]>,
        global_position: &mut u64,
    ) -> std::io::Result<Self> {
        let bytes: &[u8] = cursor.get_ref();
        let start = usize::try_from(*global_position).map_err(|_| truncated_row())?;
        let rest = bytes.get(start..).ok_or_else(truncated_row)?;
        let len = serialized_row_len(rest)?;
        let row = Row::decode(&rest[..len])?;
        let end = start + len;
        cursor.set_position(end as u64);
        *global_position = end as u64;
        Ok(row)
    }

    /// Decodes exactly one serialized row: its length header and that many
    /// payload bytes. `bytes` may run on past the row; the rest is ignored.
    ///
    /// Every length is checked against the buffer before it is used. A column
    /// size read from a damaged buffer used to be handed straight to the
    /// allocator and then to an `unwrap`ped read, so corruption was a panic —
    /// or, with a large enough size, an allocation failure.
    pub fn decode(bytes: &[u8]) -> std::io::Result<Self> {
        let cells = serialized_cells(bytes)?;
        let length = cells.length;
        let mut columns: RowColumns = SmallVec::new();
        for cell in cells {
            let cell = cell?;
            let mut data = MEMORY_POOL.acquire(cell.data.len());
            if !cell.data.is_empty() {
                data.into_slice_mut().copy_from_slice(cell.data);
            }
            columns.push(Column {
                schema_id: cell.schema_id,
                data,
                column_type: cell.column_type,
            });
        }

        Ok(Row {
            position: 0,
            columns,
            length,
        })
    }
}

/// One column of a serialized row, borrowed from the buffer.
pub struct SerializedCell<'a> {
    pub schema_id: u32,
    pub column_type: DbType,
    pub data: &'a [u8],
}

/// The columns of one serialized row, read in place — nothing is copied or
/// allocated. What a consumer that only reads the values (the wire encoder)
/// uses instead of decoding the row into a `Row`.
pub struct SerializedCells<'a> {
    bytes: &'a [u8],
    position: usize,
    end: usize,
    /// The row's length header, as `Row::length` carries it.
    #[cfg(feature = "enable_long_row")]
    pub length: u64,
    #[cfg(not(feature = "enable_long_row"))]
    pub length: u32,
}

/// Reads the columns of the serialized row at the front of `bytes` (its
/// length header included; anything after the row is ignored).
///
/// Every length is checked against the buffer before it is used. A column
/// size read from a damaged buffer used to be handed straight to the
/// allocator and then to an `unwrap`ped read, so corruption was a panic —
/// or, with a large enough size, an allocation failure.
pub fn serialized_cells(bytes: &[u8]) -> std::io::Result<SerializedCells<'_>> {
    let header = bytes.get(..ROW_LEN_HEADER).ok_or_else(truncated_row)?;
    #[cfg(feature = "enable_long_row")]
    let length = u64::from_le_bytes(header.try_into().unwrap());
    #[cfg(not(feature = "enable_long_row"))]
    let length = u32::from_le_bytes(header.try_into().unwrap());

    let end = usize::try_from(length)
        .ok()
        .and_then(|l| l.checked_add(ROW_LEN_HEADER))
        .filter(|end| *end <= bytes.len())
        .ok_or_else(truncated_row)?;
    Ok(SerializedCells {
        bytes,
        position: ROW_LEN_HEADER,
        end,
        length,
    })
}

impl<'a> Iterator for SerializedCells<'a> {
    type Item = std::io::Result<SerializedCell<'a>>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let p = self.position;
        if p >= self.end {
            return None;
        }
        let bytes = self.bytes;
        let end = self.end;
        // A failure ends the iteration: nothing after a bad length is
        // trustworthy.
        self.position = end;
        if end - p < COLUMN_OVERHEAD {
            return Some(Err(truncated_row()));
        }
        let size = u64::from_le_bytes(bytes[p..p + 8].try_into().unwrap());
        let schema_id = u64::from_le_bytes(bytes[p + 8..p + 16].try_into().unwrap());
        // Serialized as 8 bytes for format stability; a value past 32 bits
        // cannot be a column id, so the buffer is damaged.
        let Ok(schema_id) = u32::try_from(schema_id) else {
            return Some(Err(truncated_row()));
        };
        let Some(size) = usize::try_from(size)
            .ok()
            .filter(|s| *s <= end - p - COLUMN_OVERHEAD)
        else {
            return Some(Err(truncated_row()));
        };
        let data_at = p + 16;
        let column_type = DbType::from_byte(bytes[data_at + size]);
        self.position = data_at + size + 1;
        Some(Ok(SerializedCell {
            schema_id,
            column_type,
            data: &bytes[data_at..data_at + size],
        }))
    }
}

#[cfg(feature = "enable_long_row")]
const ROW_LEN_HEADER: usize = 8;
#[cfg(not(feature = "enable_long_row"))]
const ROW_LEN_HEADER: usize = 4;

/// Per column: an 8-byte size, an 8-byte schema id and a 1-byte type tag,
/// around the data itself.
const COLUMN_OVERHEAD: usize = 8 + 8 + 1;

fn truncated_row() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "a serialized row is truncated or its lengths are inconsistent",
    )
}

/// The byte length of the serialized row at the front of `bytes`, header
/// included, checked against the buffer.
#[inline]
fn serialized_row_len(bytes: &[u8]) -> std::io::Result<usize> {
    let header = bytes.get(..ROW_LEN_HEADER).ok_or_else(truncated_row)?;
    #[cfg(feature = "enable_long_row")]
    let payload = u64::from_le_bytes(header.try_into().unwrap());
    #[cfg(not(feature = "enable_long_row"))]
    let payload = u32::from_le_bytes(header.try_into().unwrap()) as u64;
    usize::try_from(payload)
        .ok()
        .and_then(|p| p.checked_add(ROW_LEN_HEADER))
        .filter(|len| *len <= bytes.len())
        .ok_or_else(truncated_row)
}

/// The rows of a serialized result, one slice each (header included), found by
/// walking the length headers — nothing is decoded.
///
/// Unlike [`count_serialized_rows`], a buffer that ends partway through a row
/// is an error rather than the end: this feeds rows to a client, and a short
/// answer must not pass for a complete one.
pub struct SerializedRows<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Iterator for SerializedRows<'a> {
    type Item = std::io::Result<&'a [u8]>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.bytes.len() {
            return None;
        }
        let rest = &self.bytes[self.position..];
        match serialized_row_len(rest) {
            Ok(len) => {
                self.position += len;
                Some(Ok(&rest[..len]))
            }
            Err(e) => {
                // One error, then done: there is no next row to find after a
                // header that cannot be trusted.
                self.position = self.bytes.len();
                Some(Err(e))
            }
        }
    }
}

pub fn serialized_rows(bytes: &[u8]) -> SerializedRows<'_> {
    SerializedRows { bytes, position: 0 }
}

/// What a serialized row is deduplicated by when some of its columns hold
/// documents: each cell's id, type and bytes, as comparing the rows' bytes
/// would see them, except that a document cell — one typed `jsonb`, or one
/// `document(position)` names — is keyed by its value, so `{"a":1}` and
/// `{"a":1.0}` are one row to `DISTINCT` and `UNION`, as `=` says they are. A
/// document's type tag is left out of its key: a scan reports a stored
/// document as text and a computed one as `jsonb`, and the two are the same
/// value. A NULL keeps its tag, so it never meets a value.
///
/// `None` when the row cannot be read; the caller then compares its bytes.
pub fn value_key(row: &[u8], document: impl Fn(usize) -> bool) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(row.len() + 16);
    for (position, cell) in serialized_cells(row).ok()?.enumerate() {
        let cell = cell.ok()?;
        out.extend_from_slice(&cell.schema_id.to_le_bytes());
        if cell.column_type == DbType::JSONB
            || (cell.column_type != DbType::NULL && document(position))
        {
            out.push(u8::MAX);
            crate::core::jsonb::push_group_key(cell.data, &mut out);
        } else {
            out.push(cell.column_type.to_byte());
            out.extend_from_slice(&(cell.data.len() as u32).to_le_bytes());
            out.extend_from_slice(cell.data);
        }
    }
    Some(out)
}

/// Everything after the first `n` rows of a serialized result, as a subslice.
///
/// What `OFFSET` needs when it is the only reason to touch the rows: the rows
/// it drops are walked by their length headers and never decoded, and the
/// page that remains is already in the wire format. Fewer than `n` rows leaves
/// nothing.
pub fn skip_serialized_rows(bytes: &[u8], n: u64) -> std::io::Result<&[u8]> {
    let mut position = 0usize;
    let mut skipped = 0u64;
    while skipped < n && position < bytes.len() {
        position += serialized_row_len(&bytes[position..])?;
        skipped += 1;
    }
    Ok(&bytes[position..])
}

pub fn rows_into_vec(rows: Vec<Row>) -> Vec<u8> {
    // Rough pre-size: avoids most reallocs for the common SELECT-many case.
    // Header (4 or 8 bytes) + per-column metadata (~17 bytes) + payload bytes.
    let approx = rows.len() * 64;
    let mut vec = Vec::with_capacity(approx);
    for row in &rows {
        row.extend_serialized(&mut vec);
    }
    vec
}

/// How many rows a serialized result holds, without decoding any of them.
///
/// Each row is a length header followed by that many bytes, so counting is a
/// walk of the headers. The statement log needs the figure for every result the
/// native protocol returns, and decoding a million-row answer to count it would
/// cost more than producing it did.
///
/// A truncated or malformed buffer stops the walk rather than failing: this
/// answers a statistic, and a wrong count is not worth turning into an error
/// the caller would have to handle on the way out of a statement that worked.
pub fn count_serialized_rows(bytes: &[u8]) -> u64 {
    #[cfg(feature = "enable_long_row")]
    const LEN_HEADER: usize = 8;
    #[cfg(not(feature = "enable_long_row"))]
    const LEN_HEADER: usize = 4;

    let mut position = 0usize;
    let mut rows = 0u64;
    while position + LEN_HEADER <= bytes.len() {
        #[cfg(feature = "enable_long_row")]
        let payload =
            u64::from_le_bytes(bytes[position..position + LEN_HEADER].try_into().unwrap()) as usize;
        #[cfg(not(feature = "enable_long_row"))]
        let payload =
            u32::from_le_bytes(bytes[position..position + LEN_HEADER].try_into().unwrap()) as usize;

        let next = position + LEN_HEADER + payload;
        if next > bytes.len() {
            break;
        }
        position = next;
        rows += 1;
    }
    rows
}

pub fn vec_into_rows(bytes: &[u8]) -> std::io::Result<Vec<Row>> {
    // Not pre-sized, deliberately. Counting the rows first so the vector is
    // allocated once looks like a win for a 1.5 KB element and measured as
    // none (1M rows, 2 and 6 columns: 374 vs 370 ms, 421 vs 420 ms — see
    // `examples/result_pipeline_bench.rs decode`): a buffer that size is
    // mmap'ed, and glibc grows it with `mremap`, which moves no bytes.
    let mut rows = Vec::new();
    for row in serialized_rows(bytes) {
        rows.push(Row::decode(row?)?);
    }
    Ok(rows)
}

/// One value of a row.
///
/// 32 bytes, from 48: a 24-byte `MemoryBlock`, the type tag and a 32-bit
/// `schema_id`. `schema_id` is the column's `SchemaField::write_order`, which
/// is a `u64` in the schema; DDL refuses to hand out a write order that does
/// not fit 32 bits (`check_write_order`), and every conversion goes through
/// [`schema_id_of`], which refuses rather than truncates. Compare the other way
/// by widening (`u64::from(column.schema_id) == field.write_order`).
#[derive(Debug, Clone)]
pub struct Column {
    pub schema_id: u32,
    pub data: MemoryBlock,
    pub column_type: DbType,
}

const _: () = assert!(std::mem::size_of::<Column>() == 32);

/// The largest write order a column can have: `Column::schema_id` is 32 bits.
pub const MAX_WRITE_ORDER: u64 = u32::MAX as u64;

/// A schema write order as a `Column::schema_id`.
///
/// Panics past `u32::MAX` instead of truncating: a truncated id names a
/// different column, and every lookup by it would then read the wrong value
/// without any error. DDL refuses such write orders before any row can carry
/// one (see `check_write_order`), so reaching the panic is a bug.
#[inline(always)]
pub fn schema_id_of(write_order: u64) -> u32 {
    match u32::try_from(write_order) {
        Ok(id) => id,
        Err(_) => panic!(
            "write order {} does not fit a column id (max {})",
            write_order, MAX_WRITE_ORDER
        ),
    }
}

/// Refuses a write order a `Column` cannot carry. Called where DDL assigns one.
pub fn check_write_order(write_order: u64) -> Result<(), String> {
    if write_order > MAX_WRITE_ORDER {
        return Err(format!(
            "the table has used up its column ids: write order {} exceeds {}",
            write_order, MAX_WRITE_ORDER
        ));
    }
    Ok(())
}

impl Column {
    pub fn deep_clone(&self) -> Self {
        Column {
            schema_id: self.schema_id,
            data: self.data.deep_clone(),
            column_type: self.column_type.clone(),
        }
    }
}

pub fn column_vec_into_vec<'a>(
    row_mut: &mut SmallVec<[(Cow<'a, str>, MemoryBlock); 20]>,
    columns: &Vec<Column>,
    schema: &'a SmallVec<[SchemaField; 20]>,
) {
    for column in columns {
        // Same `write_order`-is-not-an-index hazard as `Row::get_column`, but it
        // fails differently here: this names the output columns, so a mismatch
        // that lands in range returns the right values under the *wrong names*
        // rather than panicking — wrong answers with nothing reporting it.
        //
        // A column that matches nothing still has to be emitted: dropping it
        // would change the result's arity, which is worse than an honest
        // placeholder and much harder to trace back to here.
        let name = schema
            .iter()
            .find(|f| f.write_order == u64::from(column.schema_id))
            .or_else(|| schema.get(column.schema_id as usize))
            .map(|f| Cow::Borrowed(f.name.as_str()))
            .unwrap_or_else(|| Cow::Owned(format!("column_{}", column.schema_id)));
        row_mut.push((name, column.data.clone()));
    }
}

pub enum ReturnResult {
    Rows(Vec<Row>),
    HtmlView(String),
}

#[cfg(test)]
mod row_tests {
    use super::*;
    use crate::core::{
        db_type::DbType,
        tokenizer::query_tokenizer::{NumericValue, numeric_to_mb, str_to_mb},
    };

    fn make_numeric_column(schema_id: u64, value: NumericValue, column_type: DbType) -> Column {
        let mb = numeric_to_mb(&value);
        Column {
            schema_id: crate::core::row::row::schema_id_of(schema_id),
            data: mb,
            column_type,
        }
    }

    fn make_string_column(schema_id: u64, value: &str, column_type: DbType) -> Column {
        let mb = str_to_mb(value);
        Column {
            schema_id: crate::core::row::row::schema_id_of(schema_id),
            data: mb,
            column_type,
        }
    }

    /// Width of `Row::length`, which is the only thing `enable_long_row` changes
    /// about a row. `columns` is a `SmallVec` either way — the long-row variant
    /// of this helper used to ask for a `Vec`, which is why every caller failed
    /// to compile under that feature.
    #[cfg(feature = "enable_long_row")]
    type TestRowLen = u64;
    #[cfg(not(feature = "enable_long_row"))]
    type TestRowLen = u32;

    fn make_test_row(position: u64, columns: RowColumns, length: TestRowLen) -> Row {
        Row {
            position,
            columns,
            length,
        }
    }

    #[test]
    fn test_row_into_vec_and_from_vec_roundtrip() {
        let col1 = make_numeric_column(0, NumericValue::I32(0x05040301), DbType::I32);
        let col2 = make_string_column(1, "hello", DbType::STRING);
        let row = make_test_row(42, smallvec::smallvec![col1, col2], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns.len(), 2);
        assert_eq!(
            row2.columns[0].data.into_slice(),
            &0x05040301i32.to_le_bytes()
        );
        assert_eq!(row2.columns[1].data.into_slice(), b"hello");
        assert_eq!(row2.columns[0].column_type, DbType::I32);
        assert_eq!(row2.columns[1].column_type, DbType::STRING);
    }

    #[test]
    fn test_row_into_vec_and_from_vec_empty_row() {
        let row = make_test_row(0, smallvec::smallvec![], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns.len(), 0);
    }

    #[test]
    fn test_rows_into_vec_and_vec_into_rows_multiple_rows() {
        let col1 = make_numeric_column(0, NumericValue::I32(2020), DbType::I32);
        let col2 = make_string_column(1, "world", DbType::STRING);
        let row1 = make_test_row(1, smallvec::smallvec![col1.clone()], 0);
        let row2 = make_test_row(2, smallvec::smallvec![col2.clone()], 0);
        let rows = vec![row1.clone(), row2.clone()];
        let vec = rows_into_vec(rows.clone());
        let rows2 = vec_into_rows(&vec).unwrap();
        assert_eq!(rows2.len(), 2);
        assert_eq!(
            rows2[0].columns[0].data.into_slice(),
            &2020i32.to_le_bytes()
        );
        assert_eq!(rows2[1].columns[0].data.into_slice(), b"world");
    }

    #[test]
    fn test_vec_into_rows_empty() {
        let vec: Vec<u8> = Vec::new();
        let rows = vec_into_rows(&vec).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_row_from_cursor_and_from_vec_equivalence() {
        let col1 = make_numeric_column(0, NumericValue::I32(789), DbType::I32);
        let row = make_test_row(0, smallvec::smallvec![col1.clone()], 0);
        let vec = row.clone().into_vec();
        let mut cursor = std::io::Cursor::new(vec.as_slice());
        let mut global_position = 0u64;
        let row2 = Row::from_cursor(&mut cursor, &mut global_position).unwrap();
        assert_eq!(row2.columns.len(), 1);
        assert_eq!(row2.columns[0].data.into_slice(), &789i32.to_le_bytes());
        assert_eq!(row2.columns[0].column_type, DbType::I32);
    }

    #[test]
    fn test_row_from_cursor_multiple_rows() {
        let col1 = make_numeric_column(0, NumericValue::I32(1), DbType::I32);
        let col2 = make_string_column(1, "2", DbType::STRING);
        let row1 = make_test_row(0, smallvec::smallvec![col1.clone()], 0);
        let row2 = make_test_row(0, smallvec::smallvec![col2.clone()], 0);
        let vec = rows_into_vec(vec![row1.clone(), row2.clone()]);
        let mut cursor = std::io::Cursor::new(vec.as_slice());
        let mut global_position = 0u64;
        let r1 = Row::from_cursor(&mut cursor, &mut global_position).unwrap();
        let r2 = Row::from_cursor(&mut cursor, &mut global_position).unwrap();
        assert_eq!(r1.columns[0].data.into_slice(), &1i32.to_le_bytes());
        assert_eq!(r2.columns[0].data.into_slice(), b"2");
    }

    #[test]
    fn test_row_into_vec_column_types_and_schema_ids() {
        let col1 = make_numeric_column(5, NumericValue::I32(100), DbType::I32);
        let col2 = make_string_column(7, "abc", DbType::STRING);
        let row = make_test_row(0, smallvec::smallvec![col1.clone(), col2.clone()], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns[0].schema_id, 5);
        assert_eq!(row2.columns[1].schema_id, 7);
        assert_eq!(row2.columns[0].column_type, DbType::I32);
        assert_eq!(row2.columns[1].column_type, DbType::STRING);
    }

    // Running Indefinitely in CI
    // #[test]
    // fn test_row_into_vec_and_from_vec_with_large_data() {
    //     let long_str = "a".repeat(1000);
    //     let col = make_string_column(0, &long_str, DbType::STRING);
    //     let row = make_test_row(0, smallvec::smallvec![col.clone()], 0);
    //     let vec = row.clone().into_vec();
    //     let row2 = Row::from_vec(&vec).unwrap();
    //     assert_eq!(row2.columns[0].data.into_slice(), long_str.as_bytes());
    // }

    #[test]
    fn test_row_into_vec_and_from_vec_with_no_columns() {
        let row = make_test_row(0, smallvec::smallvec![], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns.len(), 0);
    }

    // Running Indefinitely in CI
    // #[test]
    // fn test_row_with_very_large_numeric_column() {
    //     // Create a column with 100 i32 values
    //     let large_vec: Vec<i32> = (0..100).collect();
    //     let mut bytes = Vec::with_capacity(large_vec.len() * 4);
    //     for v in &large_vec {
    //         bytes.extend_from_slice(&v.to_le_bytes());
    //     }
    //     // Store raw bytes directly (simulate as a string/byte column for simplicity)
    //     let col = Column {
    //         schema_id: 0,
    //         data: MemoryBlock::from_vec(bytes.clone()),
    //         column_type: DbType::STRING,
    //     };
    //     let row = make_test_row(0, smallvec::smallvec![col.clone()], 0);
    //     let vec = row.clone().into_vec();
    //     let row2 = Row::from_vec(&vec).unwrap();
    //     assert_eq!(row2.columns.len(), 1);
    //     assert_eq!(row2.columns[0].data.into_slice().len(), bytes.len());
    //     assert_eq!(row2.columns[0].data.into_slice(), bytes.as_slice());
    // }

    #[test]
    fn test_row_with_very_large_string_column() {
        // Create a string with 100 characters
        let large_str = "x".repeat(100);
        let col = make_string_column(0, &large_str, DbType::STRING);
        let row = make_test_row(0, smallvec::smallvec![col.clone()], 0);
        let vec = row.clone().into_vec();
        let row2 = Row::from_vec(&vec).unwrap();
        assert_eq!(row2.columns.len(), 1);
        assert_eq!(row2.columns[0].data.into_slice(), large_str.as_bytes());
    }

    /// The statement log counts a native-protocol result rather than decoding
    /// it, so the walk of the length headers has to agree with the decoder on
    /// every shape — including rows of different widths in one buffer, which is
    /// what a `RETURNING` over a mixed schema produces.
    #[test]
    fn counting_rows_agrees_with_decoding_them() {
        for widths in [vec![1usize], vec![1, 3, 2], vec![4, 4, 4, 4], vec![]] {
            let mut rows = Vec::new();
            for (i, width) in widths.iter().enumerate() {
                let columns: RowColumns = (0..*width)
                    .map(|c| {
                        make_string_column(
                            c as u64,
                            &format!("r{}c{}{}", i, c, "x".repeat(c * 7)),
                            DbType::STRING,
                        )
                    })
                    .collect();
                rows.push(make_test_row(i as u64, columns, 0));
            }
            let bytes = rows_into_vec(rows);
            assert_eq!(
                count_serialized_rows(&bytes),
                vec_into_rows(&bytes).unwrap().len() as u64,
                "widths {:?}",
                widths
            );
        }

        // A buffer cut short stops the walk rather than counting a row that is
        // not all there.
        let bytes = rows_into_vec(vec![make_test_row(
            0,
            smallvec::smallvec![make_string_column(0, "whole", DbType::STRING)],
            0,
        )]);
        assert_eq!(count_serialized_rows(&bytes), 1);
        assert_eq!(count_serialized_rows(&bytes[..bytes.len() - 1]), 0);
    }

    #[test]
    fn test_rows_into_vec_and_back_with_large_rows() {
        // Create multiple rows with large columns
        let large_str1 = "A".repeat(50);
        let large_str2 = "B".repeat(50);
        let col1 = make_string_column(0, &large_str1, DbType::STRING);
        let col2 = make_string_column(1, &large_str2, DbType::STRING);
        let row1 = make_test_row(0, smallvec::smallvec![col1.clone()], 0);
        let row2 = make_test_row(1, smallvec::smallvec![col2.clone()], 0);
        let rows = vec![row1.clone(), row2.clone()];
        let vec = rows_into_vec(rows.clone());
        let rows2 = vec_into_rows(&vec).unwrap();
        assert_eq!(rows2.len(), 2);
        assert_eq!(rows2[0].columns[0].data.into_slice(), large_str1.as_bytes());
        assert_eq!(rows2[1].columns[0].data.into_slice(), large_str2.as_bytes());
    }

    #[test]
    fn test_combined_roundtrip_large_rows_and_columns() {
        // Create 10 rows, each with a large numeric and string column
        let mut rows = Vec::new();
        for i in 0..10 {
            let num_bytes: Vec<u8> = (0..10)
                .flat_map(|v| (v as i32 + i as i32).to_le_bytes().to_vec())
                .collect();
            let str_val = format!("row{}_{}", i, "Y".repeat(10));
            let col_num = make_string_column(
                0,
                std::str::from_utf8(&num_bytes).unwrap_or(""),
                DbType::STRING,
            );
            let col_str = make_string_column(1, &str_val, DbType::STRING);
            let row = make_test_row(i, smallvec::smallvec![col_num, col_str], 0);
            rows.push(row);
        }
        let vec = rows_into_vec(rows.clone());
        let rows2 = vec_into_rows(&vec).unwrap();
        assert_eq!(rows2.len(), 10);
        for (i, row) in rows2.iter().enumerate() {
            assert_eq!(row.columns.len(), 2);
            let prefix = &row.columns[1].data.into_slice()[..format!("row{}", i).len()];
            assert_eq!(prefix, format!("row{}", i).as_bytes());
        }
    }
}
