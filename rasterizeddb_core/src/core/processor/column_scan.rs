//! A scan that returns cells, not rows.
//!
//! An aggregate over fixed-width columns reads a few bytes of each matched
//! row: the grouping key, the aggregates' arguments and their NULL flags. The
//! ordinary scan still builds a `Row` for each (one pooled buffer per
//! column), serializes it, sends the bytes to the statement, which decodes
//! each row again (`Row::decode`), retypes its NULLs and only then folds it.
//! For `SELECT COUNT(*), AVG(salary) FROM t WHERE age > 44` over a million
//! rows that was most of the statement.
//!
//! Here a worker notes where each row it would have emitted starts, and at
//! the end of the batch reads those rows' cells, column by fixed-width
//! column, into one buffer per column ([`ColumnChunk`]). Nothing is built per
//! row, and the fold reads the cells where they lie
//! (`aggregate::Folder::push_chunk`).
//!
//! **Which rows.** Exactly the rows the bytes-mode scan emits, decided by the
//! same code: the note is taken at the point that scan would fetch the row
//! to emit it, after the `WHERE` (interpreted, or decided by the prefilter's
//! mask), the reclaimable test and the snapshot's visibility test. Only what
//! is read of such a row differs.
//!
//! **Which bytes.** For a column that is not a `STRING`, `fetch_row` reads
//! `size` bytes at `row position + column offset`; so does this, from the
//! same `RowFetch` entry. A `STRING`'s cell is a header saying where in the
//! row its bytes are and how many: `fetch_row` reads the headers with the
//! fixed cells, checks each (`row_pointer::string_extent`) and reads the
//! bytes in a second pass. So does this, a batch of rows at a time, with the
//! same check: every row's value of the column lies one after another in
//! the chunk, with where each ends beside it.
//!
//! **A row that cannot be read.** The row scan leaves out a matched row
//! whose fetch fails, a string header that does not check among the reasons.
//! So does this: when a batch's read fails, its rows are read again one at a
//! time, and only the rows that still cannot be read are logged and left out
//! ([`Gather::flush_readable`]). The two scans therefore answer alike even
//! then. (Whether a visible row that cannot be read should instead fail the
//! statement is a question for both scans, not for this one alone.)

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use crate::core::db_type::DbType;
use crate::core::row::row::RowFetch;
use crate::core::row::row_pointer::{string_extent, STRING_HEADER_BYTES};
use crate::core::storage_providers::traits::StorageIO;

/// 0 = not read yet, 1 = on, 2 = off.
static ENABLED: AtomicU8 = AtomicU8::new(0);

/// Whether an aggregate that can fold from cells does. On unless
/// `RDB_FOLD_CELLS=0`, which keeps the fold over serialized rows, for A/B
/// measurement and for the tests that hold the two to the same answers.
pub fn enabled() -> bool {
    match ENABLED.load(Ordering::Relaxed) {
        1 => true,
        2 => false,
        _ => {
            let on = std::env::var_os("RDB_FOLD_CELLS").is_none_or(|v| v != "0");
            ENABLED.store(if on { 1 } else { 2 }, Ordering::Relaxed);
            on
        }
    }
}

/// Turns the cell fold on or off for the whole process.
pub fn set_enabled(on: bool) {
    ENABLED.store(if on { 1 } else { 2 }, Ordering::Relaxed);
}

#[cfg(test)]
tokio::task_local! {
    /// How many folds from cells the statement running on this task made.
    /// Per task, so a test counts its own statement's and no other test's.
    pub(crate) static FOLDS_FROM_CELLS: std::cell::Cell<u32>;
}

/// Counts a fold from cells for the test whose statement this is.
#[cfg(test)]
pub(crate) fn note_fold_from_cells() {
    let _ = FOLDS_FROM_CELLS.try_with(|n| n.set(n.get() + 1));
}

/// One column of a scan's rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellColumn {
    /// The column's `write_order`.
    pub schema_id: u64,
    /// Where its cell sits in a row.
    pub offset: u32,
    /// How many bytes the cell is, in every row: the value itself, or for
    /// a string the header saying where the value is.
    pub width: u32,
    /// A `STRING`: the cell is a header and the value lies elsewhere in the
    /// row, as long as the header says.
    pub text: bool,
}

/// The columns a cell scan reads, in the order its chunks hold them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellLayout {
    pub columns: Vec<CellColumn>,
}

impl CellLayout {
    /// The layout of `fetch`'s columns, or `None` when one of them is a
    /// fixed-width column of no width. An empty fetch has a layout of no
    /// columns, whose chunks only count rows.
    pub fn of(fetch: &RowFetch) -> Option<CellLayout> {
        let mut columns = Vec::with_capacity(fetch.columns_fetching_data.len());
        for c in fetch.columns_fetching_data.iter() {
            // What `fetch_row` asks of a column, and all it asks: a `STRING`
            // is read through its header, anything else as `size` bytes.
            let text = c.column_type == DbType::STRING;
            if !text && c.size == 0 {
                return None;
            }
            columns.push(CellColumn {
                schema_id: c.schema_id,
                offset: c.column_offset,
                width: if text { STRING_HEADER_BYTES as u32 } else { c.size },
                text,
            });
        }
        Some(CellLayout { columns })
    }

    /// The column holding `schema_id`, the first if several do, as a row's
    /// reader would find it.
    pub fn position(&self, schema_id: u64) -> Option<usize> {
        self.columns.iter().position(|c| c.schema_id == schema_id)
    }
}

/// The cells of some rows of one scan, a buffer per column of the layout.
#[derive(Debug, Default)]
pub struct ColumnChunk {
    pub rows: usize,
    /// A fixed-width column's cells, `width` bytes each, row after row; a
    /// string column's values, one after another.
    pub cols: Vec<Vec<u8>>,
    /// For a string column, where each row's value ends in its buffer (the
    /// next row's starts there). Empty for a fixed-width column.
    pub ends: Vec<Vec<usize>>,
}

impl ColumnChunk {
    /// Row `row`'s cell of the fixed-width column `col`, `width` bytes wide.
    #[inline(always)]
    pub fn cell(&self, col: usize, width: usize, row: usize) -> &[u8] {
        &self.cols[col][row * width..(row + 1) * width]
    }

    /// Row `row`'s value of the string column `col`.
    #[inline(always)]
    pub fn text(&self, col: usize, row: usize) -> &[u8] {
        let ends = &self.ends[col];
        let start = if row == 0 { 0 } else { ends[row - 1] };
        &self.cols[col][start..ends[row]]
    }
}

/// How many rows' cells a cached match set is read in at a time: about a
/// scan's batch, so one statement's read list stays a few thousand entries
/// however many rows its entry names.
pub(crate) const CACHED_READ_ROWS: usize = 4096;

/// Where a cell scan's workers send what they read.
#[derive(Clone)]
pub(crate) struct CellSink {
    pub(crate) layout: Arc<CellLayout>,
    pub(crate) chunks: kanal::AsyncSender<ColumnChunk>,
}

/// One worker's cells: the rows noted since the last [`Gather::flush`], and
/// everything read so far.
pub(crate) struct Gather<'l> {
    layout: &'l CellLayout,
    /// Where each noted row starts, and how long it is.
    noted: Vec<(u64, u64)>,
    chunk: ColumnChunk,
    /// The string columns' headers of the rows being read, a buffer per
    /// column of the layout (empty for a fixed-width one). Kept for its
    /// allocation.
    headers: Vec<Vec<u8>>,
}

impl<'l> Gather<'l> {
    pub(crate) fn new(layout: &'l CellLayout) -> Self {
        let n = layout.columns.len();
        Gather {
            layout,
            noted: Vec::new(),
            chunk: ColumnChunk { rows: 0, cols: vec![Vec::new(); n], ends: vec![Vec::new(); n] },
            headers: vec![Vec::new(); n],
        }
    }

    /// A row the scan would have emitted: where it starts and its length,
    /// as its pointer has them.
    #[inline(always)]
    pub(crate) fn note(&mut self, position: u64, length: u64) {
        self.noted.push((position, length));
    }

    /// How many rows are noted and not yet read.
    #[inline(always)]
    pub(crate) fn pending(&self) -> usize {
        self.noted.len()
    }

    /// Reads the noted rows' cells onto the chunk, all of them or none: on
    /// an error the chunk is as it was before the call. Either way the noted
    /// rows are forgotten.
    pub(crate) async fn flush<S: StorageIO>(&mut self, io: &S) -> std::io::Result<()> {
        let noted = std::mem::take(&mut self.noted);
        let read = self.read(io, &noted).await;
        // Kept for its allocation.
        self.noted = noted;
        self.noted.clear();
        read
    }

    /// [`Self::flush`] for a scan, which leaves out a row it cannot read as
    /// the row scan does: when the batch's read fails, each row is read by
    /// itself, and the ones that fail are logged and skipped.
    pub(crate) async fn flush_readable<S: StorageIO>(&mut self, io: &S) {
        let noted = std::mem::take(&mut self.noted);
        if let Err(e) = self.read(io, &noted).await {
            log::error!("Failed to read the cells of {} rows together: {}", noted.len(), e);
            for row in &noted {
                if let Err(e) = self.read(io, std::slice::from_ref(row)).await {
                    log::error!("Failed to read the cells of the row at {}: {}", row.0, e);
                }
            }
        }
        self.noted = noted;
        self.noted.clear();
    }

    /// Appends the cells of `rows` (start and length each), or nothing.
    async fn read<S: StorageIO>(&mut self, io: &S, rows: &[(u64, u64)]) -> std::io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let cols_before: Vec<usize> = self.chunk.cols.iter().map(Vec::len).collect();
        let ends_before: Vec<usize> = self.chunk.ends.iter().map(Vec::len).collect();
        let read = self.read_onto(io, rows).await;
        match read {
            Ok(()) => self.chunk.rows += rows.len(),
            Err(_) => {
                for (buf, len) in self.chunk.cols.iter_mut().zip(cols_before) {
                    buf.truncate(len);
                }
                for (ends, len) in self.chunk.ends.iter_mut().zip(ends_before) {
                    ends.truncate(len);
                }
            }
        }
        read
    }

    /// [`Self::read`] without its undoing: on an error the chunk's buffers
    /// hold part of `rows`.
    async fn read_onto<S: StorageIO>(&mut self, io: &S, rows: &[(u64, u64)]) -> std::io::Result<()> {
        let n = rows.len();
        let layout = self.layout;
        {
            // One read per cell, a row's cells together: they are within one
            // row of each other, so the copy walks the file once. A fixed
            // cell goes where it stays; a string's header to the side.
            let mut cells: Vec<std::slice::ChunksExactMut<'_, u8>> = self
                .chunk
                .cols
                .iter_mut()
                .zip(self.headers.iter_mut())
                .zip(layout.columns.iter())
                .map(|((buf, headers), col)| {
                    let width = col.width as usize;
                    if col.text {
                        headers.clear();
                        headers.resize(n * width, 0);
                        headers.chunks_exact_mut(width)
                    } else {
                        let at = buf.len();
                        buf.resize(at + n * width, 0);
                        buf[at..].chunks_exact_mut(width)
                    }
                })
                .collect();
            let mut reads: Vec<(u64, &mut [u8])> = Vec::with_capacity(n * cells.len());
            for &(position, _) in rows {
                for (cell, col) in cells.iter_mut().zip(layout.columns.iter()) {
                    // `n` cells were added to every column just above.
                    let Some(cell) = cell.next() else { unreachable!("a cell per row") };
                    reads.push((position + col.offset as u64, cell));
                }
            }
            if !reads.is_empty() && !io.try_read_vectored_sync(&mut reads) {
                io.read_vectored(&mut reads).await?;
            }
        }
        if !layout.columns.iter().any(|c| c.text) {
            return Ok(());
        }
        // The strings: every header checked as `fetch_row` checks it, then
        // every value read to where it stays, in one read.
        let mut extents: Vec<(u64, usize)> = Vec::new();
        let mut reads: Vec<(u64, &mut [u8])> = Vec::new();
        let mut string_columns = 0;
        for (((buf, ends), headers), col) in self
            .chunk
            .cols
            .iter_mut()
            .zip(self.chunk.ends.iter_mut())
            .zip(self.headers.iter())
            .zip(layout.columns.iter())
        {
            if !col.text {
                continue;
            }
            string_columns += 1;
            extents.clear();
            let at = buf.len();
            let mut end = at;
            for (header, &(position, length)) in headers.chunks_exact(STRING_HEADER_BYTES).zip(rows) {
                let extent = string_extent(position, length, header)?;
                end += extent.1;
                ends.push(end);
                extents.push(extent);
            }
            buf.resize(end, 0);
            let mut rest = &mut buf[at..];
            for &(from, size) in &extents {
                let (value, after) = rest.split_at_mut(size);
                rest = after;
                if size != 0 {
                    reads.push((from, value));
                }
            }
        }
        if string_columns > 1 {
            // Column by column above; by where they lie for the read.
            reads.sort_unstable_by_key(|read| read.0);
        }
        if !reads.is_empty() && !io.try_read_vectored_sync(&mut reads) {
            io.read_vectored(&mut reads).await?;
        }
        Ok(())
    }

    /// Everything read. Rows noted and not flushed are not in it.
    pub(crate) fn finish(self) -> ColumnChunk {
        self.chunk
    }
}
