use std::{io::Cursor, sync::atomic::AtomicU64};

use rclite::Arc;

use super::{error::Result, row::RowWrite};
use byteorder::{LittleEndian, WriteBytesExt};

#[cfg(feature = "enable_long_row")]
use crate::core::storage_providers::CRC;

use itertools::Itertools;
use smallvec::SmallVec;

use crate::core::mvcc::transaction::{FROZEN_TX, NO_TX, TxId};
use crate::{
    core::{
        db_type::DbType,
        filter::CandidateBitmap,
        index::gallop_to,
        row::row::{Column, Row, RowFetch},
        storage_providers::traits::{BorrowedBytes, StorageIO},
        vacuum::FreeSpace,
    },
    memory_pool::{MEMORY_POOL, MemoryBlock},
};
use branches::unlikely;
use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};

#[cfg(feature = "enable_long_row")]
const TOTAL_LENGTH : usize = 
    16 + // id (u128)
    8 + // length (u64)
    8 + // position (u64)
    1 + // deleted (bool)
    4 + // checksum (u32)
    4 + // cluster (u32)
    16 + // deleted_at (u128)
    16 + // created_at (u128)
    16 + // updated_at (u128)
    2 + // version (u16)
    1 + // is_active (bool)
    8 + // xmin (u64)
    8   // xmax (u64)
    ;

#[cfg(not(feature = "enable_long_row"))]
const TOTAL_LENGTH : usize = 
    8 + // id (u64)
    4 + // length (u32)
    8 + // position (u64)
    1 + // deleted (bool)
    // The MVCC stamps are appended rather than inserted, so every field above
    // keeps the offset it had before versioning existed.
    8 + // xmin (u64)
    8   // xmax (u64)
    ;

/// Fixed serialized byte length of a `RowPointer` record in the row-pointer file.
///
/// This depends on the `enable_long_row` feature flag.
pub const ROW_POINTER_RECORD_LEN: usize = TOTAL_LENGTH;

#[cfg(feature = "enable_long_row")]
#[cfg(not(debug_assertions))]
const CHUNK_SIZE: usize = TOTAL_LENGTH * 3047; // ~320KB chunks

#[cfg(feature = "enable_long_row")]
#[cfg(debug_assertions)]
const CHUNK_SIZE: usize = TOTAL_LENGTH * 2; // 92 * 2 bytes chunks for debugging

#[cfg(not(feature = "enable_long_row"))]
const CHUNK_SIZE: usize = TOTAL_LENGTH * 3047; // ~64KB chunks


/// Iterates over RowPointers from a StorageIO in 64KB chunks
pub struct RowPointerIterator<S: StorageIO> {
    /// Reference to the storage provider
    io: Arc<S>,
    /// Current position in the storage
    position: u64,
    /// Buffer to store 64KB chunks
    buffer: MemoryBlock,
    /// Current index in the buffer
    buffer_index: usize,
    /// Valid data length in the buffer
    buffer_valid_length: usize,
    /// Total length of the storage
    total_length: usize,
    /// End of data flag
    end_of_data: bool,

    /// Start byte position of the currently loaded chunk in the underlying storage.
    chunk_start_position: u64,

    /// When the storage can hand out the chunk directly (a memory map with the
    /// range resident), records are parsed from here instead of being copied
    /// into `buffer` first. A full scan is otherwise partly memcpy-bound on a
    /// copy whose only purpose is to be read once and discarded.
    borrowed_chunk: Option<BorrowedBytes>,

    /// Optional per-slot candidate mask from the filter index. A cleared bit
    /// means the row cannot satisfy the query, so its pointer record is never
    /// decoded and its row bytes are never fetched. The mask is always a
    /// *superset* of true matches, so ignoring it costs only performance.
    candidates: Option<Arc<CandidateBitmap>>,
    /// Pointers handed out per batch, when not `batch_size()`. A slot list
    /// of a few thousand candidates is cut finer so several workers share it;
    /// see `ConcurrentProcessor::spawn_batch_tasks`.
    batch_limit: Option<usize>,
}

/// The pointer-record length before MVCC stamps were added.
///
/// Kept for the upgrade note in `docs/mvcc.md`, which is where the format break
/// is handled: an existing table has to be rebuilt.
///
/// It is deliberately **not** used to detect the old format at runtime. The
/// obvious test — a length that divides by the old stride but not the new one —
/// cannot be told apart from a current file observed mid-append, because a
/// reader may see a partially written record and the trailing bytes are then
/// legitimately ignored. Roughly one torn append in 21 would land on a multiple
/// of the old stride and get a valid table refused. Rejecting good data to catch
/// bad data is the wrong trade, so the check was removed.
pub const PRE_MVCC_RECORD_LEN: usize = TOTAL_LENGTH - 16;

/// Implementation for RowPointerIterator providing methods to create and iterate over RowPointers
impl<S: StorageIO> RowPointerIterator<S> {
    /// Create a new RowPointerIterator for the given StorageIO
    pub async fn new(io: Arc<S>) -> Result<Self> {
        // Only consider fully written pointers; callers may append concurrently.
        let total_length = {
            let len = io.get_len().await as usize;
            len - (len % TOTAL_LENGTH)
        };

        let buffer_size = if total_length > CHUNK_SIZE {
            CHUNK_SIZE
        } else {
            total_length
        };

        let memory_block = MEMORY_POOL.acquire(buffer_size);

        let mut iterator = RowPointerIterator {
            io,
            position: 0,
            buffer: memory_block,
            buffer_index: 0,
            buffer_valid_length: 0,
            total_length,
            end_of_data: false,
            chunk_start_position: 0,
            borrowed_chunk: None,
            candidates: None,
            batch_limit: None,
        };

        // Load the first chunk of data
        iterator.load_next_chunk().await?;

        Ok(iterator)
    }

    /// Like [`Self::new`], but reads nothing until the first record is asked
    /// for.
    ///
    /// For the query paths, which attach a candidate mask and then `reset`
    /// before scanning: the chunk `new` loads up front is thrown away by
    /// that reset and read again, and with a slot list from an index the
    /// first read should be the chunk holding a candidate, not slot 0. A
    /// point lookup paid for two whole chunks to read one record.
    pub async fn new_deferred(io: Arc<S>) -> Result<Self> {
        let total_length = {
            let len = io.get_len().await as usize;
            len - (len % TOTAL_LENGTH)
        };
        Ok(RowPointerIterator {
            io,
            position: 0,
            buffer: MemoryBlock::default(),
            buffer_index: 0,
            buffer_valid_length: 0,
            total_length,
            end_of_data: false,
            chunk_start_position: 0,
            borrowed_chunk: None,
            candidates: None,
            batch_limit: None,
        })
    }

    /// Attach (or clear) a candidate mask for the scan about to run.
    ///
    /// Deliberately survives [`Self::reset`], which the processor calls before
    /// driving a scan — resetting it there would silently discard the mask.
    #[inline]
    pub fn set_candidates(&mut self, candidates: Option<Arc<CandidateBitmap>>) {
        self.candidates = candidates;
        self.end_if_nothing();
    }

    /// Hands out at most `limit` pointers per batch.
    pub fn set_batch_limit(&mut self, limit: usize) {
        self.batch_limit = Some(limit.max(1));
    }

    /// Whether the candidates are a slot list rather than a mask.
    pub fn has_sparse_candidates(&self) -> bool {
        self.candidates.as_ref().is_some_and(|c| c.is_sparse())
    }

    /// A mask admitting nothing (a constant-false `WHERE`) ends the scan before
    /// a single pointer is read.
    #[inline]
    fn end_if_nothing(&mut self) {
        if self.candidates.as_ref().is_some_and(|c| c.is_nothing()) {
            self.end_of_data = true;
        }
    }

    #[inline]
    pub fn has_candidates(&self) -> bool {
        self.candidates.is_some()
    }

    /// Slot index of the record at the current buffer position.
    ///
    /// Pure arithmetic because the pointer file is a dense fixed-stride array —
    /// the structural property the entire filter design rests on.
    #[inline(always)]
    fn current_slot(&self) -> u64 {
        (self.chunk_start_position + self.buffer_index as u64) / TOTAL_LENGTH as u64
    }

    /// Records this iterator is bounded to, fixed when it was constructed.
    #[inline]
    pub fn total_records(&self) -> u64 {
        (self.total_length / TOTAL_LENGTH) as u64
    }

    /// Records the scan will actually hand out at most: the whole table, or
    /// with a slot list from an index, its candidates plus whatever was
    /// appended after the list was made. What the dispatcher sizes its
    /// workers by, so a point lookup does not spawn a worker per core to
    /// read one row.
    pub fn expected_records(&self) -> u64 {
        let total = self.total_records();
        match self.candidates.as_ref() {
            Some(c) if c.is_nothing() => 0,
            Some(c) if c.is_sparse() => c.count() + total.saturating_sub(c.n_slots()),
            _ => total,
        }
    }

    pub fn reset(&mut self) {
        self.position = 0;
        self.buffer_index = 0;
        self.buffer_valid_length = 0;
        self.end_of_data = false;
        self.chunk_start_position = 0;
        self.borrowed_chunk = None;
        self.end_if_nothing();
    }

    /// Bytes of the currently loaded chunk, from the borrowed mapping when the
    /// storage could provide one and from the owned buffer otherwise.
    #[inline(always)]
    fn chunk_bytes(&self) -> &[u8] {
        match self.borrowed_chunk {
            Some(ref b) => b.as_slice(),
            None => self.buffer.into_slice(),
        }
    }

    #[inline]
    pub fn io(&self) -> Arc<S> {
        Arc::clone(&self.io)
    }

    /// Refreshes the underlying IO length.
    ///
    /// This is important for long-running scans where new pointers may be appended
    /// while the iterator is reading.
    pub async fn refresh_total_length(&mut self) {
        let new_len = self.io.get_len().await as usize;
        let aligned = new_len - (new_len % TOTAL_LENGTH);
        if aligned > self.total_length {
            self.total_length = aligned;
            // If we previously marked end_of_data but there is more data now, continue.
            if self.position < self.total_length as u64 {
                self.end_of_data = false;
            }
        }
    }

    /// Seeks the iterator to an absolute byte offset within the pointers IO.
    ///
    /// The offset must be aligned to `TOTAL_LENGTH`.
    pub async fn seek_to_byte_position(&mut self, byte_pos: u64) -> Result<()> {
        if (byte_pos as usize) % TOTAL_LENGTH != 0 {
            return Err(super::error::RowError::Other(format!(
                "RowPointerIterator seek must be aligned to TOTAL_LENGTH={}, got {}",
                TOTAL_LENGTH, byte_pos
            )));
        }
        self.position = byte_pos;
        self.buffer_index = 0;
        self.buffer_valid_length = 0;
        self.end_of_data = false;
        self.chunk_start_position = byte_pos;
        self.borrowed_chunk = None;
        self.refresh_total_length().await;
        self.load_next_chunk().await
    }

    /// The largest number of whole records, up to `max_records`, that can be
    /// read at `self.position` right now.
    ///
    /// Used only when a read has already failed short. Reads the candidate
    /// prefix rather than asking for a length, because the length is the thing
    /// that turned out to be wrong.
    ///
    /// Only a *short* read (`UnexpectedEof`) narrows the search. Any other
    /// failure is an I/O error and is returned: counting it as "not readable"
    /// shrank the chunk, and with it the scan, without anything reporting it.
    async fn readable_record_prefix(&mut self, max_records: u64) -> std::io::Result<u64> {
        if max_records == 0 {
            return Ok(0);
        }
        // Invariant: `lo` records are readable, `hi` records are not.
        let mut lo = 0u64;
        let mut hi = max_records;
        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            let bytes = (mid * TOTAL_LENGTH as u64) as usize;
            let mut probe_at = self.position;
            let slice = &mut self.buffer.into_slice_mut()[..bytes];
            match self.io.read_data_into_buffer(&mut probe_at, slice).await {
                Ok(()) => lo = mid,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => hi = mid,
                Err(e) => return Err(e),
            }
        }
        // `lo` was proven readable by the probe that set it; a `lo` still at 0
        // has not been proven, so check it before claiming it — unless `hi` has
        // narrowed to 1, which is a proof that one record is *not* readable.
        if lo == 0 && hi > 1 {
            let bytes = TOTAL_LENGTH;
            let mut probe_at = self.position;
            let slice = &mut self.buffer.into_slice_mut()[..bytes];
            match self.io.read_data_into_buffer(&mut probe_at, slice).await {
                Ok(()) => lo = 1,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
                Err(e) => return Err(e),
            }
        }
        Ok(lo)
    }

    /// Load the next chunk of data into the buffer
    async fn load_next_chunk(&mut self) -> Result<()> {
        // Reset buffer index. Drop any previous borrow up front so an early
        // return can never leave a stale mapping paired with a fresh position.
        self.buffer_index = 0;
        self.buffer_valid_length = 0;
        self.borrowed_chunk = None;

        // A slot list from a secondary index names the few slots worth
        // reading. Jump to the chunk holding the next one rather than loading
        // every chunk in between only to skip all of its records: a point
        // lookup over a million rows reads one chunk, not three hundred.
        if let Some(c) = self.candidates.as_ref().filter(|c| c.is_sparse()) {
            let slot = self.position / TOTAL_LENGTH as u64;
            let target = c.next_candidate(slot).saturating_mul(TOTAL_LENGTH as u64);
            if target > self.position {
                self.position = target.min(self.total_length as u64);
            }
        }

        // Check if we've reached the end of the storage
        if self.position >= self.total_length as u64 {
            self.end_of_data = true;
            return Ok(());
        }

        // Calculate how many bytes to read (may be less than CHUNK_SIZE at the end).
        // Always read whole pointer records.
        let bytes_remaining = self.total_length as u64 - self.position;
        if bytes_remaining < TOTAL_LENGTH as u64 {
            self.end_of_data = true;
            return Ok(());
        }

        let mut bytes_to_read_raw = std::cmp::min(bytes_remaining, CHUNK_SIZE as u64);
        // With a slot list, read from this candidate to the last one this
        // chunk would reach, and stop there: the records past it would only
        // be skipped, and the next load jumps straight to the next candidate.
        if let Some(c) = self.candidates.as_ref().filter(|c| c.is_sparse()) {
            let first = self.position / TOTAL_LENGTH as u64;
            let reach = first + (bytes_to_read_raw / TOTAL_LENGTH as u64);
            if let Some(last) = c.last_candidate_below(reach).filter(|l| *l >= first) {
                bytes_to_read_raw = (last - first + 1) * TOTAL_LENGTH as u64;
            }
        }
        let bytes_to_read = bytes_to_read_raw - (bytes_to_read_raw % (TOTAL_LENGTH as u64));
        if bytes_to_read == 0 {
            self.end_of_data = true;
            return Ok(());
        }

        // Track the absolute byte position of this chunk.
        self.chunk_start_position = self.position;

        // Fast path: parse straight out of the storage's mapping. Falls through
        // to the copying read whenever the provider declines (staged writes, a
        // range past the mapping, or a provider with nothing to borrow).
        if let Some(borrowed) = self
            .io
            .try_borrow_range(self.position, bytes_to_read as usize)
        {
            self.borrowed_chunk = Some(borrowed);
            self.buffer_valid_length = bytes_to_read as usize;
            self.position += bytes_to_read;
            return Ok(());
        }

        self.borrowed_chunk = None;

        // Clear the buffer and ensure capacity. Reuse existing allocation when sizes match
        // to avoid pool churn on every chunk.
        if self.buffer.len() != bytes_to_read as usize {
            self.buffer = MEMORY_POOL.acquire(bytes_to_read as usize);
        }

        // Read data into the buffer
        let mut read_position = self.position;
        match self
            .io
            .read_data_into_buffer(&mut read_position, &mut self.buffer.into_slice_mut())
            .await
        {
            Ok(()) => {
                self.buffer_valid_length = bytes_to_read as usize;
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // The declared length ran ahead of the bytes actually there.
                // That happens when a concurrent append has reserved its record
                // but not yet written it, and it is not always caught by the
                // `len % TOTAL_LENGTH` guard above — a reservation is a *whole*
                // record, so the over-reported length stays a clean multiple.
                //
                // Discarding the chunk here would throw away every record in it,
                // not just the missing tail. A chunk holds 3047 records and a
                // small table is one chunk, so a single unwritten record at the
                // end made the whole table read as empty:
                // `ext_cov_concurrent_inserts_keep_row_offsets` saw 0 of 320
                // rows against a pointer file that held all 320.
                //
                // So find the largest whole-record prefix that can actually be
                // read, rather than trusting any length. Binary search costs at
                // most ~12 reads and only on this path.
                let max_records = bytes_to_read / TOTAL_LENGTH as u64;
                let readable = self.readable_record_prefix(max_records).await?;
                if readable == 0 {
                    self.end_of_data = true;
                    self.buffer_valid_length = 0;
                    return Ok(());
                }

                // Re-read the prefix we proved readable. The probe that ended the
                // search may have been a *failure*, which can leave the buffer
                // holding part of a read that did not complete.
                //
                // A failure here is an error, not the end of the table. These
                // records were read successfully a moment ago, so nothing benign
                // explains losing them — and reporting the end of data here
                // returned every record before this chunk as the whole answer.
                let usable = readable * TOTAL_LENGTH as u64;
                let mut read_at = self.position;
                let slice = &mut self.buffer.into_slice_mut()[..usable as usize];
                if let Err(e) = self.io.read_data_into_buffer(&mut read_at, slice).await {
                    self.buffer_valid_length = 0;
                    return Err(e.into());
                }

                self.buffer_valid_length = usable as usize;
                self.position += usable;

                // Deliberately *not* `end_of_data = true`. The caller checks that
                // flag immediately after this returns and gives up if it is set —
                // so setting it here would throw away the chunk this call just
                // filled, which is the bug being fixed. Clamping the declared
                // length instead lets the next call end the iteration cleanly,
                // after these records have been handed out.
                self.total_length = self.position as usize;
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        // Update position for next read
        self.position += bytes_to_read;

        Ok(())
    }

    /// Returns the next non-deleted RowPointer along with its byte offset in the pointers IO.
    ///
    /// The returned offset is the location of the pointer record (aligned to TOTAL_LENGTH)
    /// inside the pointers storage.
    pub async fn next_row_pointer_with_offset(&mut self) -> Result<Option<(u64, RowPointer)>> {
        loop {
            // Both guards fire at most once per *chunk* — 3047 records — so the
            // hint costs nothing and keeps the record-decoding path below as the
            // fall-through the branch predictor and the code layout both assume.
            if unlikely(self.end_of_data) {
                return Ok(None);
            }

            if unlikely(self.buffer_index + TOTAL_LENGTH > self.buffer_valid_length) {
                self.load_next_chunk().await?;
                if unlikely(self.end_of_data) {
                    return Ok(None);
                }
            }

            // Decided before `buffer_index` is touched, so the borrow of
            // `self.candidates` ends with the match rather than having to be
            // dodged by cloning. The clone was an atomic refcount pair *per
            // record examined*, paid on the one path where the mask is supposed
            // to be cheaper than the fetch it replaces — the batch path at
            // `next_row_pointers_into` already hoists it for the same reason.
            //
            // The answer is how many records to pass over before the next one
            // worth decoding. A slot list answers it with one search that
            // lands on the next listed slot, so every record between two
            // candidates is passed in one step instead of a search each, and a
            // candidate costs one search instead of a `get` and then another.
            // `next_candidate` never returns less than the slot it was given,
            // and returns that slot itself past `n_slots`, so an appended row
            // is still decoded.
            let skip = match self.candidates.as_ref() {
                Some(bm) if bm.is_sparse() => {
                    let slot = self.current_slot();
                    bm.next_candidate(slot) - slot
                }
                Some(bm) => u64::from(!bm.get(self.current_slot())),
                None => 0,
            };
            if skip > 0 {
                // Clamped to the chunk: the next load jumps to the candidate.
                self.buffer_index = (self.buffer_index as u64)
                    .saturating_add(skip.saturating_mul(TOTAL_LENGTH as u64))
                    .min(self.buffer_valid_length as u64) as usize;
                continue;
            }

            let record_offset = self.chunk_start_position + self.buffer_index as u64;

            // Cast directly from the buffer slice; from_slice only reads through raw pointers
            // so we can avoid an intermediate stack copy of TOTAL_LENGTH bytes per pointer.
            let buf_slice = self.chunk_bytes();
            let arr_ref: &[u8; TOTAL_LENGTH] = unsafe {
                &*(buf_slice.as_ptr().add(self.buffer_index) as *const [u8; TOTAL_LENGTH])
            };
            let row_pointer = RowPointer::from_slice(arr_ref);

            self.buffer_index += TOTAL_LENGTH;

            if unlikely(row_pointer.deleted) {
                continue;
            }

            return Ok(Some((record_offset, row_pointer)));
        }
    }

    /// Get the next RowPointer from the buffer, loading a new chunk if necessary
    pub async fn next_row_pointer(&mut self) -> Result<Option<RowPointer>> {
        Ok(self.next_row_pointer_with_offset().await?.map(|(_, p)| p))
    }

    /// Get multiple RowPointers at once, up to BATCH_SIZE
    pub async fn next_row_pointers(&mut self) -> Result<Vec<RowPointer>> {
        let mut pointers: Vec<RowPointer> = Vec::new();
        self.next_row_pointers_into(&mut pointers).await?;
        Ok(pointers)
    }

    /// Fills `out` with up to BATCH_SIZE pointers, reusing its allocation.
    pub async fn next_row_pointers_into(&mut self, out: &mut Vec<RowPointer>) -> Result<()> {
        out.clear();

        let batch_size = self.batch_limit.unwrap_or_else(crate::batch_size);
        out.reserve(batch_size.saturating_sub(out.capacity()));

        // Inline the synchronous fast-path of `next_row_pointer_with_offset` so we avoid
        // creating/polling a fresh async future for every pointer. We only `.await` when
        // we actually need to load the next chunk from storage.
        let mut produced = 0usize;
        while produced < batch_size {
            if unlikely(self.end_of_data) {
                break;
            }

            if unlikely(self.buffer_index + TOTAL_LENGTH > self.buffer_valid_length) {
                self.load_next_chunk().await?;
                if unlikely(self.end_of_data) {
                    break;
                }
            }

            // Drain as many records as we can from the currently loaded chunk without awaiting.
            let buf_ptr = self.chunk_bytes().as_ptr();
            // Cloned once per chunk: holding a borrow of `self.candidates` would
            // conflict with mutating `buffer_index` inside the loop.
            let candidates = self.candidates.clone();

            // A slot list is walked with a cursor into it. Asking the list
            // about each record cost about three binary searches per candidate
            // (`get` on the candidate, `get` missing on the record after it,
            // then `next_candidate`), all in the one dispatcher task, which at
            // 40k candidates is milliseconds of serial work per query.
            //
            // The cursor lives only for this drain of this chunk, so nothing
            // has to be kept in step with `reset`, `seek_to_byte_position`,
            // `load_next_chunk`, a batch limit or a call to the single-record
            // path: each drain finds its place again with one search. Within
            // it the slot only moves forward, so every listed slot before the
            // cursor is below the current slot, which is what `gallop_to`
            // needs to land where a fresh search would.
            if let Some((slots, n_slots)) = candidates
                .as_deref()
                .and_then(|bm| bm.sparse_slots().map(|s| (s, bm.n_slots())))
            {
                let current = self.current_slot();
                let mut at = slots.partition_point(|s| *s < current);
                while produced < batch_size
                    && self.buffer_index + TOTAL_LENGTH <= self.buffer_valid_length
                {
                    let slot = self.current_slot();
                    // Past `n_slots` every record is a candidate: a row
                    // appended after the list was made was never indexed.
                    if slot < n_slots {
                        at = gallop_to(slots, at, slot);
                        let next = slots.get(at).copied().unwrap_or(n_slots);
                        if next != slot {
                            let skip = (next - slot).saturating_mul(TOTAL_LENGTH as u64);
                            self.buffer_index = (self.buffer_index as u64)
                                .saturating_add(skip)
                                .min(self.buffer_valid_length as u64) as usize;
                            continue;
                        }
                    }
                    let arr_ref: &[u8; TOTAL_LENGTH] = unsafe {
                        &*(buf_ptr.add(self.buffer_index) as *const [u8; TOTAL_LENGTH])
                    };
                    let row_pointer = RowPointer::from_slice(arr_ref);
                    self.buffer_index += TOTAL_LENGTH;
                    if unlikely(row_pointer.deleted) {
                        continue;
                    }
                    out.push(row_pointer);
                    produced += 1;
                }
                continue;
            }

            while produced < batch_size
                && self.buffer_index + TOTAL_LENGTH <= self.buffer_valid_length
            {
                if let Some(ref bm) = candidates {
                    let slot = self.current_slot();
                    if !bm.get(slot) {
                        // A slot list never gets here: it was walked above.
                        //
                        // Whole-word skip: when no slot in this 64-row group can
                        // match, 64 pointer records are dismissed without
                        // decoding any of them. Both operands are multiples of
                        // TOTAL_LENGTH, so buffer alignment is preserved.
                        if bm.word((slot / 64) as usize) == 0 {
                            let next_word_slot = (slot / 64 + 1) * 64;
                            let skip = (next_word_slot - slot) as usize * TOTAL_LENGTH;
                            self.buffer_index =
                                (self.buffer_index + skip).min(self.buffer_valid_length);
                            // The one place in this loop where the access stride
                            // breaks. Everywhere else the walk is +37 bytes and
                            // the hardware prefetcher tracks it perfectly; a
                            // whole-word skip jumps ~2.3 KB and leaves it to
                            // re-learn the stream from a cold miss. Naming the
                            // landing address covers that gap. It is a hint, so
                            // an index clamped to the end of the chunk is safe:
                            // the read never happens if nothing follows.
                            //
                            // `_mm_prefetch` directly rather than
                            // `branches::prefetch_read_data`, which looks like
                            // the natural choice and is a trap. That crate
                            // numbers LOCALITY backwards from LLVM on stable
                            // (`0 => prefetcht0 .. _ => prefetchnta`, so the 3
                            // that means "keep this in L1" everywhere else asks
                            // for non-temporal), and on nightly it forwards the
                            // constant to `core::intrinsics`, which uses LLVM's
                            // numbering — the same source silently emitting the
                            // opposite instruction depending on the toolchain,
                            // with no diagnostic either way. `_MM_HINT_T0` has
                            // one meaning on both. This crate already depends on
                            // `std::arch::x86_64` unconditionally in
                            // `memory_pool.rs`, so nothing portable is given up.
                            //
                            // T0 is the hint we want and non-temporal is the one
                            // we must not give: the landing address begins the
                            // ~2.3 KB this loop walks immediately and in order.
                            if self.buffer_index + TOTAL_LENGTH <= self.buffer_valid_length {
                                let landing = unsafe { buf_ptr.add(self.buffer_index) };
                                unsafe { _mm_prefetch::<_MM_HINT_T0>(landing as *const i8) };
                            }
                        } else {
                            self.buffer_index += TOTAL_LENGTH;
                        }
                        continue;
                    }
                }

                let arr_ref: &[u8; TOTAL_LENGTH] =
                    unsafe { &*(buf_ptr.add(self.buffer_index) as *const [u8; TOTAL_LENGTH]) };
                let row_pointer = RowPointer::from_slice(arr_ref);
                self.buffer_index += TOTAL_LENGTH;

                if unlikely(row_pointer.deleted) {
                    continue;
                }
                out.push(row_pointer);
                produced += 1;
            }
        }

        Ok(())
    }

    pub async fn next(&mut self) -> Result<Option<RowPointer>> {
        let result = self.next_row_pointer().await;
        result
    }

    pub async fn read_last(&mut self) -> Option<RowPointer> {
        let mut position = self.io.get_len().await;
        let mut slice: [u8; TOTAL_LENGTH] = [0; TOTAL_LENGTH];

        if position < TOTAL_LENGTH as u64 {
            return None;
        }

        position = position - TOTAL_LENGTH as u64;

        _ = self
            .io
            .read_data_into_buffer(&mut position, &mut slice)
            .await;

        let row_pointer = RowPointer::from_slice(&slice);

        Some(row_pointer)
    }
}

/// Where a string column's bytes start: the row's position plus the offset
/// its 12-byte header holds.
///
/// The header is read from disk, and not always from a row: the open-time
/// filter backfill and index build read rows by pointer without the
/// compaction gate (their epoch checks discard what they read while rows
/// move), and a dead slot's extent may hold another row's bytes. An offset
/// that does not fit is a misread header, the error the range check after
/// this reports, not an overflow: `+` panicked in debug builds, killing the
/// backfill and leaving the table unfiltered (`partition_twin` hit it in
/// most runs). In release it wrapped, and the range check refused it.
#[inline]
fn string_position(row_position: u64, header: &[u8]) -> std::io::Result<u64> {
    let offset = u64::from_le_bytes(header[0..8].try_into().unwrap());
    row_position.checked_add(offset).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Invalid string offset {offset} in a row at {row_position} (likely misread header)"),
        )
    })
}

#[derive(Debug, Clone)]
pub struct RowPointer {
    #[cfg(feature = "enable_long_row")]
    pub id: u128,
    #[cfg(feature = "enable_long_row")]
    pub length: u64,

    #[cfg(not(feature = "enable_long_row"))]
    pub id: u64,
    #[cfg(not(feature = "enable_long_row"))]
    pub length: u32,

    pub position: u64,
    pub deleted: bool,

    #[cfg(feature = "enable_long_row")]
    pub checksum: u32,
    #[cfg(feature = "enable_long_row")]
    pub cluster: u32,
    #[cfg(feature = "enable_long_row")]
    pub deleted_at: u128,
    #[cfg(feature = "enable_long_row")]
    pub created_at: u128,
    #[cfg(feature = "enable_long_row")]
    pub updated_at: u128,
    #[cfg(feature = "enable_long_row")]
    pub version: u16,
    #[cfg(feature = "enable_long_row")]
    pub is_active: bool,

    /// The transaction that created this row version.
    ///
    /// Never zero: a version always has a creator. Rows written before
    /// versioning read as [`FROZEN_TX`], which every snapshot sees.
    pub xmin: TxId,

    /// The transaction that removed this row version, or [`NO_TX`] if none has.
    ///
    /// Set rather than cleared: a delete does not erase the version, because a
    /// snapshot older than the delete must still be able to read it.
    pub xmax: TxId,

    pub writing_data: WritingData,
}

#[derive(Debug, Clone)]
pub struct WritingData {
    #[cfg(feature = "enable_long_row")]
    pub total_columns_size: u64,

    #[cfg(not(feature = "enable_long_row"))]
    pub total_columns_size: u32,

    pub total_strings_size: u32,
}

/// Reads the MVCC stamps from the tail of a pointer record.
///
/// A record shorter than a full one has no stamps to read; it reads as frozen
/// and undeleted, which is the same reading an unversioned record gets.
#[inline]
fn read_stamps(buffer: &[u8]) -> (TxId, TxId) {
    if buffer.len() < TOTAL_LENGTH {
        return (FROZEN_TX, NO_TX);
    }
    let base = TOTAL_LENGTH - 16;
    let xmin = u64::from_le_bytes(buffer[base..base + 8].try_into().expect("8 bytes"));
    let xmax = u64::from_le_bytes(buffer[base + 8..base + 16].try_into().expect("8 bytes"));
    (xmin, xmax)
}

impl RowPointer {
    /// Refuses a row position no column offset can be added to without
    /// overflowing. A column's offset is a `u32`, so below this bound
    /// `position + column_offset` cannot wrap; the position comes from a
    /// pointer record whose checksum is not verified on read, so it is input
    /// like a string header's offset (`string_position`). One compare per
    /// fetch rather than one checked add per column.
    #[inline]
    fn check_column_range(&self) -> std::io::Result<()> {
        if unlikely(self.position > u64::MAX - u32::MAX as u64) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid row position {} (likely a misread pointer)", self.position),
            ));
        }
        Ok(())
    }

    /// Gets the current timestamp in milliseconds since UNIX epoch
    pub fn get_timestamp() -> u128 {
        // Get the current timestamp in milliseconds
        let now = std::time::SystemTime::now();
        let duration = now.duration_since(std::time::UNIX_EPOCH).unwrap();
        let timestamp = duration.as_millis();
        timestamp
    }

    /// Gets the current timestamp in milliseconds since UNIX epoch
    pub fn get_timestamp_seconds() -> u64 {
        // Get the current timestamp in milliseconds
        let now = std::time::SystemTime::now();
        let duration = now.duration_since(std::time::UNIX_EPOCH).unwrap();
        let timestamp = duration.as_secs();
        timestamp
    }

    /// Updates the updated_at
    #[cfg(feature = "enable_long_row")]
    pub fn update_timestamp(&mut self) {
        // Update the created_at and updated_at timestamps
        let timestamp = Self::get_timestamp();
        self.updated_at = timestamp as u128;
    }

    /// The row's identity, narrowed to `u64`.
    ///
    /// `id` widens to `u128` under `enable_long_row`, but it is always allocated
    /// from a `u64` counter, so the narrowing cannot lose anything. Callers use
    /// this rather than casting so the cast lives in one place and is explained
    /// once.
    #[inline]
    #[allow(clippy::unnecessary_cast)]
    pub fn row_id(&self) -> u64 {
        self.id as u64
    }

    /// Carries a row's identity and history across a relocation.
    ///
    /// An `UPDATE` that outgrows its extent tombstones the old pointer and
    /// appends a fresh one. A fresh pointer is born with a new `id`, with
    /// `created_at` set to now and `version` back at zero — so left alone, a
    /// relocation reports the row as a *different row* that was created by the
    /// update which modified it. Both halves of that are wrong: it is the same
    /// row, and it is not new.
    ///
    /// Keeping `id` is what makes a row's identity outlive its address. Two
    /// things depend on it:
    ///
    /// * A concurrent statement that finds its pointer tombstoned can tell
    ///   whether the row was *deleted* or merely *moved*, by looking for a live
    ///   pointer carrying the same id. Without that it cannot distinguish the
    ///   two and has to drop the row — a lost update.
    /// * Compaction renumbers every slot, so a slot address is not an identity
    ///   that survives maintenance. The id is.
    ///
    /// The cost is that ids no longer ascend with file order, so the counter
    /// cannot be recovered from the last record. `Table` persists it instead.
    #[cfg(feature = "enable_long_row")]
    pub fn inherit_metadata(&mut self, previous: &RowPointer) {
        // `xmin`/`xmax` are deliberately not inherited: a relocation writes a
        // new version, and dating it to the original insert would make it
        // visible to snapshots that predate the update.
        self.id = previous.id;
        self.created_at = previous.created_at;
        self.cluster = previous.cluster;
        self.version = previous.version.saturating_add(1);
        self.update_timestamp();
    }

    /// The short-row layout stores no history, but it does store the id.
    #[cfg(not(feature = "enable_long_row"))]
    #[inline]
    pub fn inherit_metadata(&mut self, previous: &RowPointer) {
        self.id = previous.id;
    }

    /// Creates a new RowPointer with the given parameters.
    ///
    /// `position` is left at 0: the row's byte offset is not knowable here.
    /// It has to be the offset the row data actually lands at, which only the
    /// append itself can report, so the caller must assign `position` from
    /// `append_data_returning_offset` before persisting this pointer. See
    /// `write_row_with_pointer_record_pos`.
    pub fn new(
        //TODO Replace with AtomicU128 then stabilized
        #[cfg(feature = "enable_long_row")] last_id: &AtomicU64,

        #[cfg(not(feature = "enable_long_row"))] last_id: &AtomicU64,

        #[cfg(feature = "enable_long_row")] cluster: u32,

        row_write: &RowWrite,

        // The version this one replaces, or `None` for a genuine insert. A
        // replacement keeps the row's existing id, so it must not draw one from
        // the sequence: `inherit_metadata` would overwrite it, and the number
        // would be spent on nothing. That burn made `last_row_id` — reported as
        // "highest id handed out" — count pointer records rather than rows, and
        // it read 472 401 against ~1 000 live rows on the first deployment it
        // was measured on, which is not a figure anything can use.
        previous: Option<&RowPointer>,

        // The transaction this version belongs to. `FROZEN_TX` for a write
        // outside any transaction, which is visible to everyone immediately.
        written_by: TxId,
    ) -> Self {
        #[cfg(feature = "enable_long_row")]
        let id = match previous {
            Some(previous) => previous.id,
            None => last_id.fetch_add(1, std::sync::atomic::Ordering::Acquire) as u128,
        };

        #[cfg(not(feature = "enable_long_row"))]
        let id = match previous {
            Some(previous) => previous.id,
            None => last_id.fetch_add(1, std::sync::atomic::Ordering::Acquire),
        };

        #[cfg(feature = "enable_long_row")]
        let (total_bytes, total_string_size) = {
            let mut total_bytes = row_write
                .columns_writing_data
                .iter()
                .map(|col| col.size as u64)
                .sum::<u64>();

            let total_string_size = row_write
                .columns_writing_data
                .iter()
                .filter(|col| col.column_type == DbType::STRING)
                .map(|col| col.data.into_slice().len() as u32)
                .sum::<u32>();

            total_bytes = total_bytes + total_string_size as u64;

            (total_bytes, total_string_size)
        };

        #[cfg(not(feature = "enable_long_row"))]
        let (total_bytes, total_string_size) = {
            let mut total_bytes = row_write
                .columns_writing_data
                .iter()
                .map(|col| col.size)
                .sum::<u32>();

            let total_string_size = row_write
                .columns_writing_data
                .iter()
                .filter(|col| col.column_type == DbType::STRING)
                .map(|col| col.data.into_slice().len() as u32)
                .sum::<u32>();

            total_bytes = total_bytes + total_string_size;

            (total_bytes, total_string_size)
        };

        // Assigned by the caller once the row data has been appended.
        let position = 0u64;

        #[cfg(feature = "enable_long_row")]
        let timestamp = Self::get_timestamp();

        #[cfg(feature = "enable_long_row")]
        let checksum: u32 = {
            let all_data_size = row_write
                .columns_writing_data
                .iter()
                .map(|col| col.data.into_slice().len() as u64)
                .sum::<u64>();

            let mut block = MEMORY_POOL.acquire(all_data_size as usize);
            let slice = block.into_slice_mut();

            let mut position: usize = 0;

            for row_write in &row_write.columns_writing_data {
                let column_slice = row_write.data.into_slice();
                let size = column_slice.len();

                slice[position..position + size].copy_from_slice(column_slice);
                position += size;
            }

            CRC.checksum(slice)
        };

        RowPointer {
            id: id + 1,
            length: total_bytes,
            position,
            deleted: false,

            #[cfg(feature = "enable_long_row")]
            checksum,
            #[cfg(feature = "enable_long_row")]
            cluster,
            #[cfg(feature = "enable_long_row")]
            deleted_at: 0,
            #[cfg(feature = "enable_long_row")]
            created_at: timestamp,
            #[cfg(feature = "enable_long_row")]
            updated_at: 0,
            #[cfg(feature = "enable_long_row")]
            version: 0,
            #[cfg(feature = "enable_long_row")]
            is_active: true,

            xmin: written_by,
            // Nothing has deleted a row that is being created.
            xmax: NO_TX,
            writing_data: WritingData {
                total_columns_size: total_bytes,
                total_strings_size: total_string_size,
            },
        }
    }

    /// Appends this RowPointer as part of `txn`.
    ///
    /// Returns the record's byte offset and a durability token for
    /// `StorageIO::commit_durable`. The offset comes from the append itself, so
    /// it identifies *this* record even when other tasks are appending
    /// concurrently — which is what makes it safe to hand to `save_at` later.
    pub async fn save_in_txn<S: StorageIO>(&self, io: &S, txn: u64) -> Result<(u64, u64)> {
        let block = self.into_memory_block();
        let slice = block.into_slice();
        Ok(io.append_data_txn(txn, slice).await)
    }

    /// Appends this RowPointer as a transaction of its own and makes it durable.
    pub async fn save<S: StorageIO>(&self, io: Arc<S>, _immediate: bool) -> Result<u64> {
        let txn = io.begin_txn();
        let (pos, token) = self.save_in_txn(&*io, txn).await?;
        io.commit_durable(token).await?;
        Ok(pos)
    }

    /// Persists this RowPointer into the pointers IO at a specific byte position.
    ///
    /// This is used for in-place updates (e.g., tombstoning / updating length).
    /// In-place counterpart of `save_in_txn`, for rewriting an existing record.
    pub async fn save_at_in_txn<S: StorageIO>(
        &self,
        io: &S,
        pointer_record_pos: u64,
        txn: u64,
    ) -> Result<u64> {
        if (pointer_record_pos as usize) % TOTAL_LENGTH != 0 {
            return Err(super::error::RowError::Other(format!(
                "RowPointer save_at must be aligned to TOTAL_LENGTH={}, got {}",
                TOTAL_LENGTH, pointer_record_pos
            )));
        }

        let block = self.into_memory_block();
        let slice = block.into_slice();
        Ok(io.write_data_txn(txn, pointer_record_pos, slice).await)
    }

    pub async fn save_at<S: StorageIO>(&self, io: Arc<S>, pointer_record_pos: u64) -> Result<()> {
        if (pointer_record_pos as usize) % TOTAL_LENGTH != 0 {
            return Err(super::error::RowError::Other(format!(
                "RowPointer save_at must be aligned to TOTAL_LENGTH={}, got {}",
                TOTAL_LENGTH, pointer_record_pos
            )));
        }

        let block = self.into_memory_block();
        let slice = block.into_slice();
        io.write_data(pointer_record_pos, slice).await;
        Ok(())
    }

    /// Marks this row pointer as deleted (tombstone) and writes the updated pointer record
    /// back to the pointers IO at `pointer_record_pos`.
    pub async fn delete<S: StorageIO>(
        &mut self,
        pointers_io: Arc<S>,
        pointer_record_pos: u64,
        // A transaction to join, or `None` to open and commit one here.
        txn: Option<u64>,

        // The transaction removing this row. `FROZEN_TX` means the table is not
        // versioned, and the row is tombstoned outright as it always was.
        deleted_by: TxId,
    ) -> Result<()> {
        // A tombstoned slot holds nothing to retire, and retiring twice for the
        // same transaction is a no-op.
        //
        // An `xmax` belonging to some *other* transaction is deliberately
        // overwritten rather than treated as already-retired. It can only be
        // there if that transaction aborted — a live or committed one would have
        // been refused by `Table::check_write_conflict` before reaching here —
        // and an aborted retirement never happened. Leaving the stale stamp in
        // place would keep the old version visible alongside the replacement,
        // which is the row appearing twice.
        if self.deleted || (self.xmax != NO_TX && self.xmax == deleted_by) {
            return Ok(());
        }

        if deleted_by == FROZEN_TX {
            // Unversioned: no snapshot can exist to disagree, so the flag is the
            // whole truth and reclamation may act on it immediately.
            self.deleted = true;

            #[cfg(feature = "enable_long_row")]
            {
                self.is_active = false;
                self.deleted_at = Self::get_timestamp();
                self.update_timestamp();
            }
        } else {
            // Versioned: stamp, do not tombstone. A snapshot older than this
            // transaction must still be able to read the row, and reclamation
            // decides when that is no longer possible. Setting `deleted` here
            // would tell it the row predates versioning and is free to reuse.
            self.xmax = deleted_by;

            #[cfg(feature = "enable_long_row")]
            self.update_timestamp();
        }

        let owns_txn = txn.is_none();
        let txn = txn.unwrap_or_else(|| pointers_io.begin_txn());
        // A transaction opened here is ended here on every path: one left open
        // with a logged in-place stamp holds the log's pending count up, and
        // the log cannot be checkpointed again for the life of the process.
        let token = match self
            .save_at_in_txn(&*pointers_io, pointer_record_pos, txn)
            .await
        {
            Ok(token) => token,
            Err(e) => {
                if owns_txn {
                    pointers_io.abort_txn(txn).await;
                }
                return Err(e);
            }
        };
        if owns_txn {
            // The tombstone is not a tombstone until it is durable. Reporting a
            // row deleted whose deletion may not survive a restart is how a
            // deleted row comes back.
            if let Err(e) = pointers_io.commit_durable(token).await {
                pointers_io.abort_txn(txn).await;
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Largest row this layout can describe.
    ///
    /// The short-row layout stores a row's length in a `u32`, so a row past that
    /// wraps the field rather than failing. Nothing reachable through SQL gets
    /// near it, but `RowWrite` is a public API and the failure mode — a pointer
    /// claiming a length far smaller than the bytes actually written — is
    /// silent corruption rather than an error.
    #[cfg(not(feature = "enable_long_row"))]
    pub const MAX_ROW_BYTES: u64 = u32::MAX as u64;

    /// The long-row layout stores lengths in a `u64`. The cap here is a sanity
    /// bound, not a format limit: a single row of this size is a bug in the
    /// caller, and accepting it would mean a multi-gigabyte allocation.
    #[cfg(feature = "enable_long_row")]
    pub const MAX_ROW_BYTES: u64 = 1 << 40;

    /// Checks that a `RowWrite` describes a row this layout can actually store.
    ///
    /// Called before anything is allocated or written. Every check here
    /// corresponds to an assumption the encoder makes and would otherwise
    /// discover by panicking part-way through building the row — after the
    /// pointer had already been assigned an id.
    pub fn validate(row_write: &RowWrite) -> Result<()> {
        let mut total: u64 = 0;

        for column in row_write.columns_writing_data.iter() {
            let declared = column.size as u64;
            let actual = column.data.into_slice().len() as u64;

            if column.column_type == DbType::STRING {
                // A string contributes a fixed header plus its bytes, which live
                // in the variable-length region after the fixed columns.
                let header = column.column_type.get_size() as u64;
                if declared != header {
                    return Err(super::error::RowError::InvalidLength(format!(
                        "STRING column {} declares size {}, but its header is always {} bytes",
                        column.write_order, declared, header
                    )));
                }
                total = total
                    .checked_add(declared)
                    .and_then(|t| t.checked_add(actual))
                    .ok_or_else(|| {
                        super::error::RowError::InvalidLength(
                            "row length overflows u64".to_string(),
                        )
                    })?;
            } else {
                // A fixed-width column is copied into a slot of exactly its
                // declared size; a mismatch is a panic in `copy_from_slice`.
                if declared != actual {
                    return Err(super::error::RowError::InvalidLength(format!(
                        "column {} declares size {} but carries {} bytes",
                        column.write_order, declared, actual
                    )));
                }
                total = total.checked_add(declared).ok_or_else(|| {
                    super::error::RowError::InvalidLength("row length overflows u64".to_string())
                })?;
            }
        }

        if total > Self::MAX_ROW_BYTES {
            return Err(super::error::RowError::InvalidLength(format!(
                "row of {} bytes exceeds the {}-byte limit of this row layout",
                total,
                Self::MAX_ROW_BYTES
            )));
        }

        Ok(())
    }

    fn calculate_total_bytes(row_write: &RowWrite) -> (u64, u32) {
        #[cfg(feature = "enable_long_row")]
        {
            let mut total_bytes = row_write
                .columns_writing_data
                .iter()
                .map(|col| col.size as u64)
                .sum::<u64>();

            let total_string_size = row_write
                .columns_writing_data
                .iter()
                .filter(|col| col.column_type == DbType::STRING)
                .map(|col| col.data.into_slice().len() as u32)
                .sum::<u32>();

            total_bytes += total_string_size as u64;
            (total_bytes, total_string_size)
        }

        #[cfg(not(feature = "enable_long_row"))]
        {
            let mut total_bytes = row_write
                .columns_writing_data
                .iter()
                .map(|col| col.size as u64)
                .sum::<u64>();

            let total_string_size = row_write
                .columns_writing_data
                .iter()
                .filter(|col| col.column_type == DbType::STRING)
                .map(|col| col.data.into_slice().len() as u32)
                .sum::<u32>();

            total_bytes += total_string_size as u64;
            (total_bytes, total_string_size)
        }
    }

    fn build_row_data(
        row_write: &RowWrite,
        total_bytes: usize,
        total_string_size: usize,
    ) -> MemoryBlock {
        let mut block = MEMORY_POOL.acquire(total_bytes);
        let slice = block.into_slice_mut();

        let mut row_position: u64 = 0;

        let mut string_block = if total_string_size > 0 {
            MEMORY_POOL.acquire(total_string_size)
        } else {
            MemoryBlock::default()
        };
        let string_slice = string_block.into_slice_mut();

        let mut end_of_row: u64 = row_write
            .columns_writing_data
            .iter()
            .map(|col| col.size as u64)
            .sum();

        let mut string_data_position: u64 = 0;

        for column in row_write
            .columns_writing_data
            .iter()
            .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
        {
            let end_of_column = row_position + column.size as u64;

            if column.column_type == DbType::STRING {
                let string_column_slice = column.data.into_slice();
                let string_size = string_column_slice.len() as u32;

                if string_size > 0 {
                    string_slice[string_data_position as usize
                        ..string_data_position as usize + string_size as usize]
                        .copy_from_slice(string_column_slice);
                    string_data_position += string_size as u64;
                }

                let end_of_row_bytes = end_of_row.to_le_bytes();

                // Write STRING_POINTER (offset from row start)
                slice[row_position as usize..row_position as usize + 8]
                    .copy_from_slice(&end_of_row_bytes);

                // Write STRING_SIZE
                slice[row_position as usize + 8..row_position as usize + 4 + 8]
                    .copy_from_slice(&string_size.to_le_bytes());

                end_of_row += string_column_slice.len() as u64;
                row_position = end_of_column;
            } else {
                let column_slice = column.data.into_slice();
                slice[row_position as usize..end_of_column as usize].copy_from_slice(column_slice);
                row_position = end_of_column;
            }
        }

        if total_string_size > 0 {
            slice[row_position as usize..row_position as usize + total_string_size]
                .copy_from_slice(string_slice);
        }

        block
    }

    /// Updates an existing row.
    ///
    /// Rules:
    /// 1) If the new row total length is bigger than the old row length: tombstone this pointer
    ///    and append the updated row at the end, returning the new pointer.
    /// 2) If equal or smaller: overwrite the row data in-place and update this pointer's length
    ///    (and metadata), returning the updated pointer.
    pub async fn update<S: StorageIO>(
        &mut self,
        pointers_io: Arc<S>,
        rows_io: Arc<S>,
        pointer_record_pos: u64,

        #[cfg(feature = "enable_long_row")] last_id: &AtomicU64,
        #[cfg(not(feature = "enable_long_row"))] last_id: &AtomicU64,

        table_length: &AtomicU64,

        #[cfg(feature = "enable_long_row")] cluster: u32,

        row_write: &RowWrite,
    ) -> Result<RowPointer> {
        let (p, new_pos) = self
            .update_with_pointer_record_pos(
                pointers_io,
                rows_io,
                pointer_record_pos,
                last_id,
                table_length,
                #[cfg(feature = "enable_long_row")]
                cluster,
                row_write,
                None,
                None,
                // No transaction: this wrapper has always meant an immediately
                // visible write.
                FROZEN_TX,
            )
            .await?;
        let _ = new_pos;
        Ok(p)
    }

    /// Same as `update`, but returns the newly appended pointer-record position when the update
    /// requires tombstoning and appending a fresh pointer record.
    pub async fn update_with_pointer_record_pos<S: StorageIO>(
        &mut self,
        pointers_io: Arc<S>,
        rows_io: Arc<S>,
        pointer_record_pos: u64,

        #[cfg(feature = "enable_long_row")] last_id: &AtomicU64,
        #[cfg(not(feature = "enable_long_row"))] last_id: &AtomicU64,

        table_length: &AtomicU64,

        #[cfg(feature = "enable_long_row")] cluster: u32,

        row_write: &RowWrite,

        // A transaction to join, or `None` to open and commit one here.
        txn: Option<u64>,

        // Reclaimed extents for the relocation path to draw from.
        free_space: Option<&FreeSpace>,

        // The transaction making the change. The relocation path stamps the
        // replacement version with it.
        written_by: TxId,
    ) -> Result<(RowPointer, Option<u64>)> {
        Self::validate(row_write)?;

        // A transaction opened here is ended here, on every path. The body
        // below has half a dozen ways out, and any of them taken with an
        // in-place write already logged left the transaction open — holding
        // the log's pending count up so it could never be checkpointed again.
        // So the body runs inside the transaction as if a caller had supplied
        // it, and this level commits or aborts.
        if txn.is_none() {
            let txn = rows_io.begin_txn();
            let result = Box::pin(self.update_with_pointer_record_pos(
                pointers_io.clone(),
                rows_io.clone(),
                pointer_record_pos,
                last_id,
                table_length,
                #[cfg(feature = "enable_long_row")]
                cluster,
                row_write,
                Some(txn),
                free_space,
                written_by,
            ))
            .await;
            return match result {
                Err(e) => {
                    pointers_io.abort_txn(txn).await;
                    Err(e)
                }
                Ok((pointer, moved_to)) => {
                    if let Err(e) = pointers_io.commit_durable(txn).await {
                        // A relocation that is not durable owns no extent that
                        // survives a restart; leaving the loan out would shield
                        // those bytes from every future sweep.
                        if moved_to.is_some() {
                            if let Some(free_space) = free_space {
                                free_space.release_failed_loan(pointer.position);
                            }
                        }
                        pointers_io.abort_txn(txn).await;
                        return Err(e.into());
                    }
                    Ok((pointer, moved_to))
                }
            };
        }

        let (new_total_bytes_u64, new_total_string_size) = Self::calculate_total_bytes(row_write);
        let old_len_u64 = self.length as u64;

        // One transaction for whichever path we take. Both of them touch two
        // files, and neither half is meaningful on its own.
        let owns_txn = txn.is_none();
        let txn = txn.unwrap_or_else(|| rows_io.begin_txn());
        let pointers_io_for_commit = pointers_io.clone();

        // A row that outgrew its extent has to move. So does *any* row being
        // changed by a transaction: overwriting the bytes in place would leave
        // nothing to roll back to, and a snapshot older than the update would
        // read the new value through a version it is entitled to see unchanged.
        // Only a write outside any transaction — where there is no rollback and
        // no other snapshot — may overwrite.
        let must_relocate = new_total_bytes_u64 > old_len_u64 || written_by != FROZEN_TX;

        if must_relocate {
            // Snapshot before `delete` stamps the tombstone fields over it.
            let previous = self.clone();

            // Retire the old version and append the replacement. Under
            // versioning that is a stamp rather than a tombstone, so a snapshot
            // older than this update still reads the row it was looking at.
            self.delete(
                pointers_io.clone(),
                pointer_record_pos,
                Some(txn),
                written_by,
            )
            .await?;

            let (new_pointer_record_pos, new_pointer) =
                RowPointer::write_row_with_pointer_record_pos(
                    pointers_io,
                    rows_io,
                    last_id,
                    table_length,
                    #[cfg(feature = "enable_long_row")]
                    cluster,
                    row_write,
                    // Same transaction: tombstoning the old pointer and appending
                    // the replacement must not be separable by a crash.
                    Some(txn),
                    free_space,
                    // The replacement is the same row, so it keeps the row's
                    // history rather than presenting itself as newly created.
                    Some(&previous),
                    // ...but it is a new *version*, so it is stamped with the
                    // transaction making the change, not the one that inserted it.
                    written_by,
                )
                .await?;

            if owns_txn {
                if let Err(e) = pointers_io_for_commit.commit_durable(txn).await {
                    // The relocation is not durable, so the extent it was
                    // placed in is not owned by anything that survives a
                    // restart. Leaving the loan outstanding would shield those
                    // bytes from every future sweep.
                    if let (Some(free_space), Some(position)) =
                        (free_space, Some(new_pointer.position))
                    {
                        free_space.release_failed_loan(position);
                    }
                    return Err(e.into());
                }
            }

            return Ok((new_pointer, Some(new_pointer_record_pos)));
        }

        // Overwrite in-place.
        let total_bytes_usize = new_total_bytes_u64 as usize;
        let string_size_usize = new_total_string_size as usize;
        let block = Self::build_row_data(row_write, total_bytes_usize, string_size_usize);
        let slice = block.into_slice();

        // Row bytes and the pointer's new length are one transaction: a crash
        // between them would otherwise leave the row rewritten while the pointer
        // still describes its old length.
        let _ = rows_io.write_data_txn(txn, self.position, slice).await;

        #[cfg(feature = "enable_long_row")]
        {
            // Recompute checksum and bump updated_at.
            self.checksum = CRC.checksum(slice);
            self.update_timestamp();
            self.version = self.version.saturating_add(1);
        }

        #[cfg(feature = "enable_long_row")]
        {
            self.length = new_total_bytes_u64;
        }

        #[cfg(not(feature = "enable_long_row"))]
        {
            self.length = u32::try_from(new_total_bytes_u64).map_err(|_| {
                super::error::RowError::Other(format!(
                    "Updated row length {} overflows u32 length field",
                    new_total_bytes_u64
                ))
            })?;
        }

        let token = self
            .save_at_in_txn(&*pointers_io, pointer_record_pos, txn)
            .await?;
        if owns_txn {
            pointers_io.commit_durable(token).await?;
        }
        Ok((self.clone(), None))
    }

    /// Serializes the RowPointer into a Vec<u8>
    pub fn into_memory_block(&self) -> MemoryBlock {
        let mut block = MEMORY_POOL.acquire(TOTAL_LENGTH as usize);
        let slice = block.into_slice_mut();

        debug_assert!(slice.len() == TOTAL_LENGTH, "Buffer size mismatch");

        let mut cursor = Cursor::new(slice);
        cursor.set_position(0);

        // Write all fields in order
        #[cfg(feature = "enable_long_row")]
        cursor.write_u128::<LittleEndian>(self.id).unwrap();
        #[cfg(feature = "enable_long_row")]
        cursor.write_u64::<LittleEndian>(self.length).unwrap();

        #[cfg(not(feature = "enable_long_row"))]
        cursor.write_u64::<LittleEndian>(self.id).unwrap();

        #[cfg(not(feature = "enable_long_row"))]
        cursor.write_u32::<LittleEndian>(self.length).unwrap();

        cursor.write_u64::<LittleEndian>(self.position).unwrap();
        cursor.write_u8(self.deleted as u8).unwrap();

        #[cfg(feature = "enable_long_row")]
        {
            cursor.write_u32::<LittleEndian>(self.checksum).unwrap();
            cursor.write_u32::<LittleEndian>(self.cluster).unwrap();
            cursor.write_u128::<LittleEndian>(self.deleted_at).unwrap();
            cursor.write_u128::<LittleEndian>(self.created_at).unwrap();
            cursor.write_u128::<LittleEndian>(self.updated_at).unwrap();
            cursor.write_u16::<LittleEndian>(self.version).unwrap();
            cursor.write_u8(self.is_active as u8).unwrap();
        }

        cursor.write_u64::<LittleEndian>(self.xmin).unwrap();
        cursor.write_u64::<LittleEndian>(self.xmax).unwrap();

        block
    }

    /// Deserializes a slice of u8 into a RowPointer
    pub fn from_slice(buffer: &[u8; TOTAL_LENGTH]) -> Self {
        // ID field
        #[cfg(feature = "enable_long_row")]
        let id = unsafe { u128::from_le_bytes(*buffer.as_ptr().cast::<[u8; 16]>()) };

        #[cfg(not(feature = "enable_long_row"))]
        let id = unsafe { u64::from_le_bytes(*buffer.as_ptr().cast::<[u8; 8]>()) };

        // Length field
        #[cfg(feature = "enable_long_row")]
        let length = unsafe { u64::from_le_bytes(*buffer.as_ptr().add(16).cast::<[u8; 8]>()) };

        #[cfg(not(feature = "enable_long_row"))]
        let length = unsafe { u32::from_le_bytes(*buffer.as_ptr().add(8).cast::<[u8; 4]>()) };

        // Position field (u64 in both cases)
        #[cfg(feature = "enable_long_row")]
        let position = unsafe { u64::from_le_bytes(*buffer.as_ptr().add(24).cast::<[u8; 8]>()) };

        #[cfg(not(feature = "enable_long_row"))]
        let position = unsafe { u64::from_le_bytes(*buffer.as_ptr().add(12).cast::<[u8; 8]>()) };

        // Deleted field (bool - 1 byte)
        #[cfg(feature = "enable_long_row")]
        let deleted = unsafe { u8::from_le_bytes(*buffer.as_ptr().add(32).cast::<[u8; 1]>()) } != 0;

        #[cfg(not(feature = "enable_long_row"))]
        let deleted = unsafe { u8::from_le_bytes(*buffer.as_ptr().add(20).cast::<[u8; 1]>()) } != 0;

        // The following fields only exist in the long row format
        #[cfg(feature = "enable_long_row")]
        let checksum = unsafe { u32::from_le_bytes(*buffer.as_ptr().add(33).cast::<[u8; 4]>()) };

        #[cfg(feature = "enable_long_row")]
        let cluster = unsafe { u32::from_le_bytes(*buffer.as_ptr().add(37).cast::<[u8; 4]>()) };

        #[cfg(feature = "enable_long_row")]
        let deleted_at =
            unsafe { u128::from_le_bytes(*buffer.as_ptr().add(41).cast::<[u8; 16]>()) };

        #[cfg(feature = "enable_long_row")]
        let created_at =
            unsafe { u128::from_le_bytes(*buffer.as_ptr().add(57).cast::<[u8; 16]>()) };

        #[cfg(feature = "enable_long_row")]
        let updated_at =
            unsafe { u128::from_le_bytes(*buffer.as_ptr().add(73).cast::<[u8; 16]>()) };

        #[cfg(feature = "enable_long_row")]
        let version = unsafe { u16::from_le_bytes(*buffer.as_ptr().add(89).cast::<[u8; 2]>()) };

        #[cfg(feature = "enable_long_row")]
        let is_active =
            unsafe { u8::from_le_bytes(*buffer.as_ptr().add(91).cast::<[u8; 1]>()) } != 0;

        // The stamps sit at the end of the record, so their offsets follow from
        // the record length rather than being another constant to keep in step.
        let (xmin, xmax) = read_stamps(buffer);

        // Create the RowPointer struct from the read values
        let row_pointer = RowPointer {
            xmin,
            xmax,
            id,
            length,
            position,
            deleted,
            #[cfg(feature = "enable_long_row")]
            checksum,
            #[cfg(feature = "enable_long_row")]
            cluster,
            #[cfg(feature = "enable_long_row")]
            deleted_at,
            #[cfg(feature = "enable_long_row")]
            created_at,
            #[cfg(feature = "enable_long_row")]
            updated_at,
            #[cfg(feature = "enable_long_row")]
            version,
            #[cfg(feature = "enable_long_row")]
            is_active,
            writing_data: WritingData {
                total_columns_size: 0, // Placeholder, will be set later
                total_strings_size: 0, // Placeholder, will be set later
            },
        };

        row_pointer
    }

    /// Deserializes a slice of u8 into a RowPointer
    pub fn from_slice_unknown(buffer: &[u8]) -> Self {
        // ID field
        #[cfg(feature = "enable_long_row")]
        let id = unsafe { u128::from_le_bytes(*buffer.as_ptr().cast::<[u8; 16]>()) };

        #[cfg(not(feature = "enable_long_row"))]
        let id = unsafe { u64::from_le_bytes(*buffer.as_ptr().cast::<[u8; 8]>()) };

        // Length field
        #[cfg(feature = "enable_long_row")]
        let length = unsafe { u64::from_le_bytes(*buffer.as_ptr().add(16).cast::<[u8; 8]>()) };

        #[cfg(not(feature = "enable_long_row"))]
        let length = unsafe { u32::from_le_bytes(*buffer.as_ptr().add(8).cast::<[u8; 4]>()) };

        // Position field (u64 in both cases)
        #[cfg(feature = "enable_long_row")]
        let position = unsafe { u64::from_le_bytes(*buffer.as_ptr().add(24).cast::<[u8; 8]>()) };

        #[cfg(not(feature = "enable_long_row"))]
        let position = unsafe { u64::from_le_bytes(*buffer.as_ptr().add(12).cast::<[u8; 8]>()) };

        // Deleted field (bool - 1 byte)
        #[cfg(feature = "enable_long_row")]
        let deleted = unsafe { u8::from_le_bytes(*buffer.as_ptr().add(32).cast::<[u8; 1]>()) } != 0;

        #[cfg(not(feature = "enable_long_row"))]
        let deleted = unsafe { u8::from_le_bytes(*buffer.as_ptr().add(20).cast::<[u8; 1]>()) } != 0;

        // The following fields only exist in the long row format
        #[cfg(feature = "enable_long_row")]
        let checksum = unsafe { u32::from_le_bytes(*buffer.as_ptr().add(33).cast::<[u8; 4]>()) };

        #[cfg(feature = "enable_long_row")]
        let cluster = unsafe { u32::from_le_bytes(*buffer.as_ptr().add(37).cast::<[u8; 4]>()) };

        #[cfg(feature = "enable_long_row")]
        let deleted_at =
            unsafe { u128::from_le_bytes(*buffer.as_ptr().add(41).cast::<[u8; 16]>()) };

        #[cfg(feature = "enable_long_row")]
        let created_at =
            unsafe { u128::from_le_bytes(*buffer.as_ptr().add(57).cast::<[u8; 16]>()) };

        #[cfg(feature = "enable_long_row")]
        let updated_at =
            unsafe { u128::from_le_bytes(*buffer.as_ptr().add(73).cast::<[u8; 16]>()) };

        #[cfg(feature = "enable_long_row")]
        let version = unsafe { u16::from_le_bytes(*buffer.as_ptr().add(89).cast::<[u8; 2]>()) };

        #[cfg(feature = "enable_long_row")]
        let is_active =
            unsafe { u8::from_le_bytes(*buffer.as_ptr().add(91).cast::<[u8; 1]>()) } != 0;

        // The stamps sit at the end of the record, so their offsets follow from
        // the record length rather than being another constant to keep in step.
        let (xmin, xmax) = read_stamps(buffer);

        // Create the RowPointer struct from the read values
        let row_pointer = RowPointer {
            xmin,
            xmax,
            id,
            length,
            position,
            deleted,
            #[cfg(feature = "enable_long_row")]
            checksum,
            #[cfg(feature = "enable_long_row")]
            cluster,
            #[cfg(feature = "enable_long_row")]
            deleted_at,
            #[cfg(feature = "enable_long_row")]
            created_at,
            #[cfg(feature = "enable_long_row")]
            updated_at,
            #[cfg(feature = "enable_long_row")]
            version,
            #[cfg(feature = "enable_long_row")]
            is_active,
            writing_data: WritingData {
                total_columns_size: 0, // Placeholder, will be set later
                total_strings_size: 0, // Placeholder, will be set later
            },
        };

        row_pointer
    }

    /// Reads a RowPointer from the given StorageIO at the specified position
    pub async fn read_from_io<S: StorageIO>(io: &mut S, position: &mut u64) -> Result<Self> {
        let mut slice: [u8; TOTAL_LENGTH] = [0; TOTAL_LENGTH];

        // A failed read leaves `slice` zeroed, and a zeroed record decodes as a
        // live pointer at offset 0 — so the error is returned, not discarded.
        io.read_data_into_buffer(position, &mut slice)
            .await
            .map_err(super::error::RowError::Io)?;

        let row_pointer = Self::from_slice(&slice);

        Ok(row_pointer)
    }

    /// Fetches specific columns of a row based on the RowFetch specification
    ///
    /// This method reads only the columns specified in the RowFetch rather than the entire row,
    /// which can be more efficient for queries that only need specific columns.
    ///
    /// # Arguments
    ///
    /// * `io` - The storage I/O provider to read from
    /// * `row_fetch` - Specification of which columns to fetch and their positions/sizes
    ///
    /// # Returns
    ///
    /// A Result containing the constructed Row with only the requested columns
    pub async fn fetch_row<S: StorageIO>(&self, io: Arc<S>, row_fetch: &RowFetch) -> Result<Row> {
        // Skip fetching if the row is marked as deleted
        if self.deleted {
            return Err(super::error::RowError::NotFound(
                "Row is marked as deleted".into(),
            ));
        }
        if let Err(e) = self.check_column_range() {
            return Err(super::error::RowError::InvalidData(e.to_string()));
        }

        // Create a vector to hold all the columns
        let mut columns: crate::core::row::row::RowColumns =
            SmallVec::with_capacity(row_fetch.columns_fetching_data.len());

        // Pre-allocate columns with placeholders
        for _ in 0..row_fetch.columns_fetching_data.len() {
            columns.push(Column {
                schema_id: 0,
                data: MemoryBlock::default(),
                column_type: DbType::U8,
            });
        }

        let mut string_headers_buffer: SmallVec<[u8; 128]> = SmallVec::new();
        let mut first_pass_reads: SmallVec<[(u64, &mut [u8]); 32]> = SmallVec::new();
        let mut string_col_indices: SmallVec<[usize; 8]> = SmallVec::new();

        for (i, column_data) in row_fetch.columns_fetching_data.iter().enumerate() {
            if column_data.column_type == DbType::STRING {
                string_col_indices.push(i);
            } else {
                let position = self.position + column_data.column_offset as u64;
                let c_size = column_data.size as usize;

                let block = MEMORY_POOL.acquire(c_size);
                let col = &mut columns[i];
                col.data = block;
                col.schema_id = crate::core::row::row::schema_id_of(column_data.schema_id);
                col.column_type = column_data.column_type.clone();

                let ptr = col.data.as_mut_ptr();
                let len = col.data.len();
                let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
                first_pass_reads.push((position, buf));
            }
        }

        if !string_col_indices.is_empty() {
            string_headers_buffer.resize(string_col_indices.len() * 12, 0);

            for (chunk, &col_idx) in string_headers_buffer
                .chunks_exact_mut(12)
                .zip(string_col_indices.iter())
            {
                let column_data = &row_fetch.columns_fetching_data[col_idx];
                let position = self.position + column_data.column_offset as u64;
                first_pass_reads.push((position, chunk));
            }
        }

        // Not discarded: the column buffers come from the pool unzeroed, so a
        // read that failed or fell short would hand back whatever the buffer
        // last held — another row's bytes — as this row, with no error.
        if !first_pass_reads.is_empty() {
            io.read_vectored(&mut first_pass_reads)
                .await
                .map_err(super::error::RowError::Io)?;
        }

        drop(first_pass_reads);

        if !string_col_indices.is_empty() {
            let mut second_pass_reads: SmallVec<[(u64, &mut [u8]); 8]> = SmallVec::new();
            let row_start = self.position;
            let row_len = self.length as u64;
            let row_end = row_start.saturating_add(row_len);
            const MAX_REASONABLE_STRING_BYTES: usize = 64 * 1024 * 1024;

            for (chunk, &col_idx) in string_headers_buffer
                .chunks_exact(12)
                .zip(string_col_indices.iter())
            {
                let string_row_position = string_position(self.position, chunk)?;
                let string_size = u32::from_le_bytes(chunk[8..12].try_into().unwrap()) as usize;

                let col = &mut columns[col_idx];
                col.schema_id = crate::core::row::row::schema_id_of(row_fetch.columns_fetching_data[col_idx].schema_id);
                col.column_type = DbType::STRING;

                if string_size == 0 {
                    col.data = MemoryBlock::default();
                    continue;
                }

                if string_size > MAX_REASONABLE_STRING_BYTES {
                    return Err(super::error::RowError::InvalidData(format!(
                        "Invalid string size {} (too large)",
                        string_size
                    )));
                }

                // Ensure the string data pointer + size stays within the row bounds.
                if string_row_position < row_start
                    || string_row_position > row_end
                    || string_row_position.saturating_add(string_size as u64) > row_end
                {
                    return Err(super::error::RowError::InvalidData(format!(
                        "Invalid string range: pos={} size={} row_start={} row_end={} (likely misread header)",
                        string_row_position, string_size, row_start, row_end
                    )));
                }

                col.data = MEMORY_POOL.acquire(string_size);
                let ptr = col.data.as_mut_ptr();
                let len = col.data.len();
                let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
                second_pass_reads.push((string_row_position, buf));
            }

            if !second_pass_reads.is_empty() {
                io.read_vectored(&mut second_pass_reads)
                    .await
                    .map_err(super::error::RowError::Io)?;
            }
        }

        // Create and return the Row with just the fetched columns
        Ok(Row {
            position: self.position,
            columns,
            length: self.length,
        })
    }

    pub async fn fetch_row_reuse_async<S: StorageIO>(
        &self,
        io: &S,
        row_fetch: &RowFetch,
        row_reuse: &mut Row,
    ) -> std::io::Result<()> {
        // Every scan path filters deleted pointers out before it gets here, so
        // this is a guard against a caller bug, not a case that occurs.
        if unlikely(self.deleted) {
            return Err(std::io::Error::other("row is marked as deleted"));
        }
        self.check_column_range()?;

        let columns = &mut row_reuse.columns;

        // Ensure we have exactly the needed number of column slots without dropping reusable ones unnecessarily.
        let needed = row_fetch.columns_fetching_data.len();
        let current_len = columns.len();

        // Both arms are the resize case. `row_reuse` is threaded through a whole
        // scan against one `RowFetch`, so `needed` is fixed for the entire loop
        // and only the very first row of a scan takes either branch.
        if unlikely(current_len < needed) {
            // Grow columns with cheap placeholders; these will be overwritten below.
            columns.reserve(needed - current_len);
            for _ in 0..(needed - current_len) {
                columns.push(Column {
                    schema_id: 0,
                    data: MemoryBlock::default(),
                    column_type: DbType::U8, // placeholder, will be set properly
                });
            }
        } else if unlikely(current_len > needed) {
            // Drop only the excess; common case keeps length stable so this won't happen often.
            columns.truncate(needed);
        }

        // Reuse buffers in-place when sizes match; otherwise, replace the MemoryBlock.
        // This avoids frequent drop+alloc cycles and SmallVec push/clear overhead.
        let mut string_headers_buffer: SmallVec<[u8; 128]> = SmallVec::new();
        let mut first_pass_reads: SmallVec<[(u64, &mut [u8]); 32]> = SmallVec::new();
        let mut string_col_indices: SmallVec<[usize; 8]> = SmallVec::new();

        for (i, column_data) in row_fetch.columns_fetching_data.iter().enumerate() {
            if column_data.column_type == DbType::STRING {
                string_col_indices.push(i);
            } else {
                let position = self.position + column_data.column_offset as u64;
                let col = &mut columns[i];
                let c_size = column_data.size as usize;

                // The reuse hit. For a fixed-width column `c_size` is the same
                // on every row of the scan, so after the first row this block is
                // dead — which is exactly what the reuse buffer exists for, and
                // the pool acquire it guards is the expensive half of the loop.
                if unlikely(col.data.into_slice().len() != c_size) {
                    col.data = MEMORY_POOL.acquire(c_size);
                }

                col.schema_id = crate::core::row::row::schema_id_of(column_data.schema_id);
                col.column_type = column_data.column_type.clone();

                let ptr = col.data.as_mut_ptr();
                let len = col.data.len();
                let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
                first_pass_reads.push((position, buf));
            }
        }

        if !string_col_indices.is_empty() {
            string_headers_buffer.resize(string_col_indices.len() * 12, 0);

            for (chunk, &col_idx) in string_headers_buffer
                .chunks_exact_mut(12)
                .zip(string_col_indices.iter())
            {
                let column_data = &row_fetch.columns_fetching_data[col_idx];
                let position = self.position + column_data.column_offset as u64;
                first_pass_reads.push((position, chunk));
            }
        }

        if !first_pass_reads.is_empty() {
            if !io.try_read_vectored_sync(&mut first_pass_reads) {
                io.read_vectored(&mut first_pass_reads).await?;
            }
        }

        drop(first_pass_reads);

        if !string_col_indices.is_empty() {
            let mut second_pass_reads: SmallVec<[(u64, &mut [u8]); 8]> = SmallVec::new();
            let row_start = self.position;
            let row_len = self.length as u64;
            let row_end = row_start.saturating_add(row_len);
            const MAX_REASONABLE_STRING_BYTES: usize = 64 * 1024 * 1024;

            for (chunk, &col_idx) in string_headers_buffer
                .chunks_exact(12)
                .zip(string_col_indices.iter())
            {
                let string_row_position = string_position(self.position, chunk)?;
                let string_size = u32::from_le_bytes(chunk[8..12].try_into().unwrap()) as usize;

                let col = &mut columns[col_idx];

                if string_size == 0 {
                    col.data = MemoryBlock::default();
                    col.schema_id = crate::core::row::row::schema_id_of(row_fetch.columns_fetching_data[col_idx].schema_id);
                    col.column_type = DbType::STRING;
                    continue;
                }

                if string_size > MAX_REASONABLE_STRING_BYTES {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid string size {} (too large)", string_size),
                    ));
                }

                if string_row_position < row_start
                    || string_row_position > row_end
                    || string_row_position.saturating_add(string_size as u64) > row_end
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Invalid string range: pos={} size={} row_start={} row_end={} (likely misread header)",
                            string_row_position, string_size, row_start, row_end
                        ),
                    ));
                }

                if col.data.into_slice().len() != string_size {
                    col.data = MEMORY_POOL.acquire(string_size);
                }

                col.schema_id = crate::core::row::row::schema_id_of(row_fetch.columns_fetching_data[col_idx].schema_id);
                col.column_type = DbType::STRING;

                let ptr = col.data.as_mut_ptr();
                let len = col.data.len();
                let buf = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
                second_pass_reads.push((string_row_position, buf));
            }

            if !second_pass_reads.is_empty() {
                if !io.try_read_vectored_sync(&mut second_pass_reads) {
                    io.read_vectored(&mut second_pass_reads).await?;
                }
            }
        }

        row_reuse.position = self.position;
        row_reuse.length = self.length;

        Ok(())
    }

    /// Writes a row to storage using the given RowWrite payload
    ///
    /// This function writes row data to the rows_io storage and creates a row pointer in pointers_io.
    ///
    /// # Arguments
    ///
    /// * `pointers_io` - The storage IO for row pointers
    /// * `rows_io` - The storage IO for row data
    /// * `row_write` - The row write payload containing column data to write
    ///
    /// # Returns
    ///
    /// A Result containing the created RowPointer on success
    pub async fn write_row<S: StorageIO>(
        pointers_io: Arc<S>,
        rows_io: Arc<S>,

        #[cfg(feature = "enable_long_row")] last_id: &AtomicU64,

        #[cfg(not(feature = "enable_long_row"))] last_id: &AtomicU64,

        table_length: &AtomicU64,

        #[cfg(feature = "enable_long_row")] cluster: u32,

        row_write: &RowWrite,
    ) -> Result<RowPointer> {
        let (_pointer_record_pos, p) = RowPointer::write_row_with_pointer_record_pos(
            pointers_io,
            rows_io,
            last_id,
            table_length,
            #[cfg(feature = "enable_long_row")]
            cluster,
            row_write,
            None,
            None,
            None,
            // No transaction: the row is visible as soon as it is written,
            // which is what this wrapper has always meant.
            FROZEN_TX,
        )
        .await?;
        Ok(p)
    }

    /// Writes a row and returns the pointer-record byte offset where the RowPointer was appended.
    pub async fn write_row_with_pointer_record_pos<S: StorageIO>(
        pointers_io: Arc<S>,
        rows_io: Arc<S>,

        #[cfg(feature = "enable_long_row")] last_id: &AtomicU64,

        #[cfg(not(feature = "enable_long_row"))] last_id: &AtomicU64,

        table_length: &AtomicU64,

        #[cfg(feature = "enable_long_row")] cluster: u32,

        row_write: &RowWrite,

        // A transaction to join, or `None` to open and commit one here. A
        // statement writing many rows passes the same id for all of them so
        // they land together or not at all.
        txn: Option<u64>,

        // Reclaimed extents to place the row in before extending the file, or
        // `None` to always append.
        free_space: Option<&FreeSpace>,

        // The pointer this row is replacing, when the write is a relocation
        // rather than a fresh insert. Its history is carried over.
        previous: Option<&RowPointer>,

        // The transaction this version belongs to, stamped as its `xmin`.
        // `FROZEN_TX` for a write outside any transaction.
        written_by: TxId,
    ) -> Result<(u64, RowPointer)> {
        // Before an id is allocated or a byte is written: every check inside is
        // an assumption the encoder below would otherwise discover by panicking
        // part-way through.
        Self::validate(row_write)?;

        let mut row_pointer = RowPointer::new(
            last_id,
            #[cfg(feature = "enable_long_row")]
            cluster,
            row_write,
            previous,
            written_by,
        );

        // `new` has already taken the id from `previous`; this carries the rest
        // of the metadata across. Left as a separate step so `inherit_metadata`
        // stays correct on its own terms rather than depending on `new` having
        // done half of it.
        if let Some(previous) = previous {
            row_pointer.inherit_metadata(previous);
        }

        let total_bytes = row_pointer.writing_data.total_columns_size;
        let total_string_size = row_pointer.writing_data.total_strings_size;

        // Allocate memory to store the row data
        let mut block = MEMORY_POOL.acquire(total_bytes as usize);
        let slice = block.into_slice_mut();

        let mut row_position = 0;

        let mut string_block = MEMORY_POOL.acquire(total_string_size as usize);
        let string_slice = string_block.into_slice_mut();

        let mut end_of_row: u64 = row_write
            .columns_writing_data
            .iter()
            .map(|col| col.size as u64)
            .sum();

        // Write each column data into the buffer

        let mut string_data_position: u64 = 0;

        for column in row_write
            .columns_writing_data
            .iter()
            .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
        {
            let end_of_column = row_position + column.size as u64;

            // Data Layout Description:
            //
            // For fixed-size data types (integers, floats, booleans, etc.),
            // the value is placed directly into the main data buffer.
            //
            // For variable-length data types, such as strings, a 64-bit pointer
            // and a 32-bit size field are used. The pointer references the
            // location where the string's binary data is stored, which is
            // typically at the end of the buffer, after the fixed-size fields.
            //
            // Example:
            //              U64 POINTER | U32 SIZE             U64 POINTER | U32 SIZE     BINARY DATA | BINARY DATA
            //    4 bytes | 8 bytes | 4 bytes | 4 bytes | 16 bytes |  8 bytes  |  4 bytes  |   X bytes   |   X bytes
            // ROW: [I32|STRING_POINTER_1|STRING_SIZE|I32|U128|STRING_POINTER_2|STRING_SIZE|STRING_DATA_1|STRING_DATA_2]
            //
            if column.column_type == DbType::STRING {
                let string_column_slice = column.data.into_slice();

                let string_size = string_column_slice.len() as u32;

                string_slice[string_data_position as usize
                    ..string_data_position as usize + string_size as usize]
                    .copy_from_slice(string_column_slice);

                string_data_position = string_data_position + string_size as u64;

                let end_of_row_bytes = end_of_row.to_le_bytes();

                // Write STRING_POINTER
                slice[row_position as usize..row_position as usize + 8]
                    .copy_from_slice(&end_of_row_bytes);

                // Write STRING_SIZE
                slice[row_position as usize + 8..row_position as usize + 4 + 8]
                    .copy_from_slice(&string_size.to_le_bytes());

                end_of_row = end_of_row + string_column_slice.len() as u64;

                row_position = end_of_column;
            } else {
                let column_slice = column.data.into_slice();

                slice[row_position as usize..end_of_column as usize].copy_from_slice(column_slice);

                row_position = end_of_column;
            }
        }

        slice[row_position as usize..].copy_from_slice(&string_slice);

        // Write the row data to the rows_io.
        //
        // The row's `position` is whatever offset the append reports, never a
        // separately reserved counter value. Reserving from a counter and then
        // appending are two steps with no lock between them, so two concurrent
        // inserts could reserve in one order and append in the other, leaving
        // each pointer describing the other row's bytes. Nothing inside `slice`
        // depends on the absolute position — string pointers are row-relative —
        // so the buffer can be built first and placed afterwards.
        // Deferred: the row bytes and the pointer record below are made durable
        // together. An INSERT is only meaningful once both are on stable
        // storage, so flushing after each one costs two flushes for one
        // logically atomic write.
        // One transaction spans both files. Replay applies a transaction only if
        // it reached its commit record, so a crash between the row bytes and the
        // pointer leaves neither — the row cannot become visible half-written.
        let owns_txn = txn.is_none();
        let txn = txn.unwrap_or_else(|| rows_io.begin_txn());

        // A reclaimed extent is preferred over the append frontier: without it
        // a table that deletes and reinserts the same working set grows its
        // rows file by the whole working set every cycle. The extent is only on
        // the free list because no live pointer refers to it and every query
        // that could have held one has finished, so writing into it cannot
        // disturb a read in progress.
        let reclaimed = free_space.and_then(|f| f.alloc(total_bytes as u64));

        let io_position = match reclaimed {
            Some(position) => {
                let _ = rows_io.write_data_txn(txn, position, slice).await;
                position
            }
            None => rows_io.append_data_txn(txn, slice).await.0,
        };
        row_pointer.position = io_position;

        // Keep the table's length as a high-water mark of the rows file. It is
        // only bookkeeping now that positions come from the append.
        table_length.fetch_max(
            io_position.saturating_add(total_bytes as u64),
            std::sync::atomic::Ordering::AcqRel,
        );

        // Write the pointer last, taking its record offset from the same append
        // so later `save_at` / `delete` calls target this record and no other.
        //
        // A failure here leaves a reclaimed extent lent out with no pointer that
        // will ever own it. The loan shields those bytes from every future scan,
        // so without giving it back the space is lost for the life of the
        // process.
        let (pointer_record_pos, token) = match row_pointer.save_in_txn(&*pointers_io, txn).await {
            Ok(result) => result,
            Err(e) => {
                if let (Some(free_space), Some(position)) = (free_space, reclaimed) {
                    free_space.release_failed_loan(position);
                }
                // A reclaimed extent was an in-place write: the transaction
                // holds a pending payload until it ends, and pins the log.
                if owns_txn {
                    rows_io.abort_txn(txn).await;
                }
                return Err(e);
            }
        };

        // Only end the transaction when we opened it. A caller that supplied one
        // is batching several rows and will commit them together.
        if owns_txn {
            if let Err(e) = pointers_io.commit_durable(token).await {
                // Same reasoning as a failed `save_in_txn`: nothing will ever
                // own this extent, so the loan has to go back or those bytes
                // are shielded from every future sweep.
                if let (Some(free_space), Some(position)) = (free_space, reclaimed) {
                    free_space.release_failed_loan(position);
                }
                pointers_io.abort_txn(txn).await;
                return Err(e.into());
            }
        }

        #[cfg(feature = "enable_data_verification")]
        // The bytes are durable via the log by this point, so a read-back
        // comparison is all that is left to check.
        let write_result = rows_io.verify_data(io_position, slice).await;

        #[cfg(feature = "enable_data_verification")]
        if !write_result {
            if let (Some(free_space), Some(position)) = (free_space, reclaimed) {
                free_space.release_failed_loan(position);
            }
            // Rollback the transaction if verification fails
            // TODO
            return Err(super::error::RowError::SavingFailed(
                "Failed to save data, rolling back the transaction.".into(),
            ));
        }

        Ok((pointer_record_pos, row_pointer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        row::row::{ColumnFetchingData, ColumnWritePayload},
        storage_providers::mock_file_sync::MockStorageProvider,
    };

    #[derive(Clone)]
    pub(super) struct InMemoryIo {
        pub(super) data: std::sync::Arc<Vec<u8>>,
        /// A length to report instead of `data.len()`.
        ///
        /// Real providers can report a length ahead of the bytes actually
        /// written — a concurrent append reserves its record before filling it —
        /// and the iterator has to cope rather than trusting it.
        pub(super) declared_len: Option<u64>,
    }

    impl crate::core::storage_providers::traits::StorageIO for InMemoryIo {
        async fn verify_data(&self, _position: u64, _buffer: &[u8]) -> bool {
            true
        }

        async fn write_data(&self, _position: u64, _buffer: &[u8]) {}

        async fn write_data_seek(&self, _seek: std::io::SeekFrom, _buffer: &[u8]) {}

        async fn read_data_into_buffer(
            &self,
            position: &mut u64,
            buffer: &mut [u8],
        ) -> std::result::Result<(), std::io::Error> {
            let start = *position as usize;
            let end = start.saturating_add(buffer.len());
            if end > self.data.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "InMemoryIo EOF",
                ));
            }
            buffer.copy_from_slice(&self.data[start..end]);
            *position += buffer.len() as u64;
            Ok(())
        }

        async fn read_vectored(
            &self,
            reads: &mut [(u64, &mut [u8])],
        ) -> std::result::Result<(), std::io::Error> {
            for (pos, buf) in reads {
                let mut p = *pos;
                self.read_data_into_buffer(&mut p, buf).await?;
            }
            Ok(())
        }

        async fn append_data(&self, _buffer: &[u8], _immediate: bool) {}

        async fn append_data_returning_offset(&self, _buffer: &[u8], _immediate: bool) -> u64 {
            self.data.len() as u64
        }

        async fn get_len(&self) -> u64 {
            self.declared_len.unwrap_or(self.data.len() as u64)
        }

        fn exists(_location: &str, _table_name: &str) -> bool {
            false
        }

        async fn create_temp(&self) -> Self {
            self.clone()
        }

        async fn swap_temp(&self, _temp_io_sync: &mut Self) {}

        fn get_location(&self) -> Option<String> {
            None
        }

        async fn create_new(&self, _name: String) -> Self {
            self.clone()
        }

        fn drop_io(&self) {}

        fn get_hash(&self) -> u32 {
            0
        }

        async fn start_service(&self) {}

        fn get_name(&self) -> String {
            "InMemoryIo".to_string()
        }
    }

    #[tokio::test]
    async fn test_row_pointer_iterator_ignores_partial_trailing_bytes() {
        // One valid pointer record, then some trailing bytes that don't make a full record.
        let pointer = create_test_row_pointer(1, 100, 50, false);
        let block = pointer.into_memory_block();
        let mut bytes = block.into_slice().to_vec();
        bytes.extend_from_slice(&[1, 2, 3, 4, 5]);

        let io = Arc::new(InMemoryIo {
            declared_len: None,
            data: std::sync::Arc::new(bytes),
        });
        let mut iterator = RowPointerIterator::new(io).await.unwrap();

        let p1 = iterator.next_row_pointer().await.unwrap().unwrap();
        assert_eq!(p1.id, 1);
        assert_eq!(p1.position, 100);
        assert_eq!(p1.deleted, false);

        let p2 = iterator.next_row_pointer().await.unwrap();
        assert!(p2.is_none());
    }

    #[tokio::test]
    async fn test_row_pointer_iterator_stops_on_unexpected_eof_without_garbage() {
        #[derive(Clone)]
        struct AlwaysEofIo;

        impl crate::core::storage_providers::traits::StorageIO for AlwaysEofIo {
            async fn verify_data(&self, _position: u64, _buffer: &[u8]) -> bool {
                true
            }
            async fn write_data(&self, _position: u64, _buffer: &[u8]) {}
            async fn write_data_seek(&self, _seek: std::io::SeekFrom, _buffer: &[u8]) {}
            async fn read_data_into_buffer(
                &self,
                _position: &mut u64,
                _buffer: &mut [u8],
            ) -> std::result::Result<(), std::io::Error> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "AlwaysEofIo",
                ))
            }
            async fn read_vectored(
                &self,
                _reads: &mut [(u64, &mut [u8])],
            ) -> std::result::Result<(), std::io::Error> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "AlwaysEofIo",
                ))
            }
            async fn append_data(&self, _buffer: &[u8], _immediate: bool) {}
            async fn append_data_returning_offset(&self, _buffer: &[u8], _immediate: bool) -> u64 {
                0
            }
            async fn get_len(&self) -> u64 {
                TOTAL_LENGTH as u64
            }
            fn exists(_location: &str, _table_name: &str) -> bool {
                false
            }
            async fn create_temp(&self) -> Self {
                self.clone()
            }
            async fn swap_temp(&self, _temp_io_sync: &mut Self) {}
            fn get_location(&self) -> Option<String> {
                None
            }
            async fn create_new(&self, _name: String) -> Self {
                self.clone()
            }
            fn drop_io(&self) {}
            fn get_hash(&self) -> u32 {
                0
            }
            async fn start_service(&self) {}
            fn get_name(&self) -> String {
                "AlwaysEofIo".to_string()
            }
        }

        let io = Arc::new(AlwaysEofIo);
        let mut iterator = RowPointerIterator::new(io).await.unwrap();
        let next = iterator.next_row_pointer().await.unwrap();
        assert!(next.is_none());
    }

    fn create_string_column(data: &str, write_order: u32) -> ColumnWritePayload {
        let string_bytes = data.as_bytes();

        let mut string_data = MEMORY_POOL.acquire(string_bytes.len());
        let string_slice = string_data.into_slice_mut();

        string_slice[0..].copy_from_slice(string_bytes);

        ColumnWritePayload {
            data: string_data,
            write_order,
            column_type: DbType::STRING,
            size: 4 + 8,
        }
    }

    fn create_u8_column(data: u8, write_order: u32) -> ColumnWritePayload {
        let mut u8_data = MEMORY_POOL.acquire(1);
        let u8_slice = u8_data.into_slice_mut();
        u8_slice.copy_from_slice(&data.to_le_bytes());

        ColumnWritePayload {
            data: u8_data,
            write_order,
            column_type: DbType::U8,
            size: 1,
        }
    }

    fn create_u64_column(data: u64, write_order: u32) -> ColumnWritePayload {
        let mut u64_data = MEMORY_POOL.acquire(8);
        let u64_slice = u64_data.into_slice_mut();
        u64_slice.copy_from_slice(&data.to_le_bytes());

        ColumnWritePayload {
            data: u64_data,
            write_order,
            column_type: DbType::U64,
            size: 8,
        }
    }

    fn create_f32_column(data: f32, write_order: u32) -> ColumnWritePayload {
        let mut f32_data = MEMORY_POOL.acquire(4);
        let f32_slice = f32_data.into_slice_mut();
        f32_slice.copy_from_slice(&data.to_le_bytes());

        ColumnWritePayload {
            data: f32_data,
            write_order,
            column_type: DbType::F32,
            size: 4,
        }
    }

    fn create_f64_column(data: f64, write_order: u32) -> ColumnWritePayload {
        let mut f64_data = MEMORY_POOL.acquire(8);
        let f64_slice = f64_data.into_slice_mut();
        f64_slice.copy_from_slice(&data.to_le_bytes());

        ColumnWritePayload {
            data: f64_data,
            write_order,
            column_type: DbType::F64,
            size: 8,
        }
    }

    fn create_row_write(id: u64) -> RowWrite {
        RowWrite {
            columns_writing_data: smallvec::smallvec![
                create_u64_column(id, 0),
                create_string_column("John Doe", 1),
                create_u8_column(30, 2),
                create_string_column("john.doe@example.com", 3),
                create_string_column("123 Main St", 4),
                create_string_column("555-1234", 5),
                create_u8_column(1, 6),
                create_f64_column(1000.50, 7),
                create_u8_column(0, 8),
                create_string_column("1990-01-01", 9),
                create_string_column("2023-10-01", 10),
                create_string_column("Sunsung Phone Andomega 10", 11),
                create_f32_column(100.0, 12),
                create_string_column("2023-10-01", 13),
                create_string_column("New York", 14),
                create_string_column("Credit Card", 15),
                create_string_column("Electronics", 16),
                create_string_column("Smartphones", 17),
                create_string_column("Latest model", 18),
                create_string_column("No notes", 19),
            ],
        }
    }

    // Helper function to create a row pointer and return its serialized form
    fn create_test_row_pointer(id: u64, position: u64, length: u32, deleted: bool) -> RowPointer {
        #[cfg(feature = "enable_long_row")]
        {
            RowPointer {
                id: id as u128,
                length: length as u64,
                position,
                deleted,
                xmin: crate::core::mvcc::transaction::FROZEN_TX,
                xmax: crate::core::mvcc::transaction::NO_TX,
                checksum: 0,
                cluster: 0,
                deleted_at: 0,
                created_at: 0,
                updated_at: 0,
                version: 0,
                is_active: true,
                writing_data: WritingData {
                    total_columns_size: 0,
                    total_strings_size: 0,
                },
            }
        }

        #[cfg(not(feature = "enable_long_row"))]
        {
            RowPointer {
                id,
                length,
                position,
                deleted,
                xmin: crate::core::mvcc::transaction::FROZEN_TX,
                xmax: crate::core::mvcc::transaction::NO_TX,
                writing_data: WritingData {
                    total_columns_size: 0,
                    total_strings_size: 0,
                },
            }
        }
    }

    /// A string header whose offset does not fit past the row's position is
    /// a misread header, refused as one by both fetch paths, not an add that
    /// overflows (a panic in debug builds, killing the filter backfill).
    /// A pointer whose position no column offset can be added to is a
    /// misread pointer, refused by both fetch paths before any add.
    ///
    /// Mutation: `check_column_range` always `Ok` panics on the add.
    #[tokio::test]
    async fn a_row_position_past_u64_is_an_error_not_an_overflow() {
        let io = InMemoryIo { data: std::sync::Arc::new(vec![0u8; 64]), declared_len: None };
        let pointer = create_test_row_pointer(1, u64::MAX - 4, 16, false);
        let fetch = RowFetch {
            columns_fetching_data: smallvec::smallvec![ColumnFetchingData {
                column_offset: 8,
                column_type: DbType::I64,
                size: 8,
                schema_id: 0,
            }],
        };
        let mut row = Row::default();
        let reused = pointer.fetch_row_reuse_async(&io, &fetch, &mut row).await;
        assert_eq!(reused.err().map(|e| e.kind()), Some(std::io::ErrorKind::InvalidData));
        assert!(pointer.fetch_row(Arc::new(io), &fetch).await.is_err());
    }

    #[tokio::test]
    async fn a_string_header_offset_past_u64_is_an_error_not_an_overflow() {
        let position = 64u64;
        let mut data = vec![0u8; 128];
        data[64..72].copy_from_slice(&(u64::MAX - 8).to_le_bytes());
        data[72..76].copy_from_slice(&5u32.to_le_bytes());
        let io = InMemoryIo { data: std::sync::Arc::new(data), declared_len: None };
        let pointer = create_test_row_pointer(1, position, 32, false);
        let fetch = RowFetch {
            columns_fetching_data: smallvec::smallvec![ColumnFetchingData {
                column_offset: 0,
                column_type: DbType::STRING,
                size: 12,
                schema_id: 0,
            }],
        };
        let mut row = Row::default();
        let reused = pointer.fetch_row_reuse_async(&io, &fetch, &mut row).await;
        assert_eq!(reused.err().map(|e| e.kind()), Some(std::io::ErrorKind::InvalidData));
        let fetched = pointer.fetch_row(Arc::new(io), &fetch).await;
        assert!(fetched.is_err(), "fetch_row read a header past u64 as a string");
    }

    #[tokio::test]
    async fn test_row_pointer_iterator_empty() {
        let mock_io = Arc::new(MockStorageProvider::new().await);

        let mut iterator = RowPointerIterator::new(mock_io.clone()).await.unwrap();

        // Should return None for an empty storage
        assert!(iterator.next_row_pointer().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_row_pointer_iterator_single_pointer() {
        // Create a single row pointer
        let row_pointer = create_test_row_pointer(1, 100, 50, false);

        let mock_io = Arc::new(MockStorageProvider::new().await);

        row_pointer.save(mock_io.clone(), true).await.unwrap();

        let mut row_pointer_iterator = RowPointerIterator::new(mock_io.clone()).await.unwrap();

        // Should return our row pointer
        let read_pointer = row_pointer_iterator
            .next_row_pointer()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_pointer.id, 1);
        assert_eq!(read_pointer.position, 100);
        assert_eq!(read_pointer.length, 50);
        assert_eq!(read_pointer.deleted, false);

        assert!(
            row_pointer_iterator
                .next_row_pointer()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_row_pointer_delete_persists_flag() {
        let row_pointer = create_test_row_pointer(1, 100, 50, false);
        let mock_io = Arc::new(MockStorageProvider::new().await);
        row_pointer.save(mock_io.clone(), true).await.unwrap();

        let mut it = RowPointerIterator::new(mock_io.clone()).await.unwrap();
        let (pos, mut p) = it.next_row_pointer_with_offset().await.unwrap().unwrap();
        assert_eq!(p.deleted, false);

        p.delete(mock_io.clone(), pos, None, FROZEN_TX)
            .await
            .unwrap();

        let mut read_pos = pos;
        let mut slice: [u8; TOTAL_LENGTH] = [0; TOTAL_LENGTH];
        mock_io
            .read_data_into_buffer(&mut read_pos, &mut slice)
            .await
            .unwrap();

        let reread = RowPointer::from_slice(&slice);
        assert!(reread.deleted);
    }

    #[tokio::test]
    async fn test_row_pointer_update_overwrite_in_place_when_smaller() {
        let mock_io_pointers = Arc::new(MockStorageProvider::new().await);
        let mock_io_rows = Arc::new(MockStorageProvider::new().await);

        #[cfg(feature = "enable_long_row")]
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        // Initial write with a longer string.
        let initial = RowPointer::write_row(
            mock_io_pointers.clone(),
            mock_io_rows.clone(),
            &last_id,
            &table_length,
            #[cfg(feature = "enable_long_row")]
            cluster,
            &create_row_write(0),
        )
        .await
        .unwrap();

        // Find pointer record offset.
        let mut it = RowPointerIterator::new(mock_io_pointers.clone())
            .await
            .unwrap();
        let (pos, mut p) = it.next_row_pointer_with_offset().await.unwrap().unwrap();
        assert_eq!(p.id, initial.id);

        // Update with a smaller payload (shorter strings).
        let smaller = {
            let mut rw = create_row_write(0);
            // Replace the first string column with a tiny string.
            for col in rw.columns_writing_data.iter_mut() {
                if col.column_type == DbType::STRING {
                    let bytes = b"x";
                    let mut mb = MEMORY_POOL.acquire(bytes.len());
                    mb.into_slice_mut().copy_from_slice(bytes);
                    col.data = mb;
                    break;
                }
            }
            rw
        };

        let updated = p
            .update(
                mock_io_pointers.clone(),
                mock_io_rows.clone(),
                pos,
                &last_id,
                &table_length,
                #[cfg(feature = "enable_long_row")]
                cluster,
                &smaller,
            )
            .await
            .unwrap();

        assert!(!updated.deleted);
        assert!(updated.length as u64 <= initial.length as u64);
    }

    #[tokio::test]
    async fn test_row_pointer_update_appends_when_bigger() {
        let mock_io_pointers = Arc::new(MockStorageProvider::new().await);
        let mock_io_rows = Arc::new(MockStorageProvider::new().await);

        #[cfg(feature = "enable_long_row")]
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        // Initial write with a small row.
        let mut small = create_row_write(0);
        for col in small.columns_writing_data.iter_mut() {
            if col.column_type == DbType::STRING {
                let bytes = b"x";
                let mut mb = MEMORY_POOL.acquire(bytes.len());
                mb.into_slice_mut().copy_from_slice(bytes);
                col.data = mb;
            }
        }

        let _initial = RowPointer::write_row(
            mock_io_pointers.clone(),
            mock_io_rows.clone(),
            &last_id,
            &table_length,
            #[cfg(feature = "enable_long_row")]
            cluster,
            &small,
        )
        .await
        .unwrap();

        // Locate pointer record.
        let mut it = RowPointerIterator::new(mock_io_pointers.clone())
            .await
            .unwrap();
        let (pos, mut p) = it.next_row_pointer_with_offset().await.unwrap().unwrap();

        // Bigger update: expand a string.
        let mut big = create_row_write(0);
        for col in big.columns_writing_data.iter_mut() {
            if col.column_type == DbType::STRING {
                let bytes = vec![b'a'; 1024];
                let mut mb = MEMORY_POOL.acquire(bytes.len());
                mb.into_slice_mut().copy_from_slice(&bytes);
                col.data = mb;
                break;
            }
        }

        let new_pointer = p
            .update(
                mock_io_pointers.clone(),
                mock_io_rows.clone(),
                pos,
                &last_id,
                &table_length,
                #[cfg(feature = "enable_long_row")]
                cluster,
                &big,
            )
            .await
            .unwrap();

        assert!(p.deleted);
        assert!(!new_pointer.deleted);

        // Iterating should see at least one non-deleted pointer (the appended one).
        let mut it2 = RowPointerIterator::new(mock_io_pointers.clone())
            .await
            .unwrap();
        let mut count = 0usize;
        while let Some(_p) = it2.next_row_pointer().await.unwrap() {
            count += 1;
        }
        assert!(count >= 1);
    }

    #[tokio::test]
    async fn test_row_pointer_iterator_2_pointers() {
        // Create a single row pointer
        let row_pointer = create_test_row_pointer(1, 100, 50, false);

        let mock_io = Arc::new(MockStorageProvider::new().await);

        row_pointer.save(mock_io.clone(), true).await.unwrap();
        row_pointer.save(mock_io.clone(), true).await.unwrap();

        let mut row_pointer_iterator = RowPointerIterator::new(mock_io.clone()).await.unwrap();

        // Should return our row pointer
        let read_pointer = row_pointer_iterator
            .next_row_pointer()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_pointer.id, 1);
        assert_eq!(read_pointer.position, 100);
        assert_eq!(read_pointer.length, 50);
        assert_eq!(read_pointer.deleted, false);

        // Next call should return None
        assert!(
            row_pointer_iterator
                .next_row_pointer()
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            row_pointer_iterator
                .next_row_pointer()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_row_pointer_iterator_3_pointers() {
        // Create a single row pointer
        let row_pointer = create_test_row_pointer(1, 100, 50, false);
        let row_pointer2 = create_test_row_pointer(2, 150, 100, false);

        let mock_io = Arc::new(MockStorageProvider::new().await);

        row_pointer.save(mock_io.clone(), true).await.unwrap();
        row_pointer2.save(mock_io.clone(), true).await.unwrap();
        row_pointer.save(mock_io.clone(), true).await.unwrap();

        let mut row_pointer_iterator = RowPointerIterator::new(mock_io.clone()).await.unwrap();

        // Should return our row pointer
        let read_pointer = row_pointer_iterator
            .next_row_pointer()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_pointer.id, 1);
        assert_eq!(read_pointer.position, 100);
        assert_eq!(read_pointer.length, 50);
        assert_eq!(read_pointer.deleted, false);

        let read_pointer = row_pointer_iterator
            .next_row_pointer()
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read_pointer.id, 2);
        assert_eq!(read_pointer.position, 150);
        assert_eq!(read_pointer.length, 100);
        assert_eq!(read_pointer.deleted, false);

        // Next call should return None
        assert!(
            row_pointer_iterator
                .next_row_pointer()
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            row_pointer_iterator
                .next_row_pointer()
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_save_row_pointers() {
        let mock_io_pointers = Arc::new(MockStorageProvider::new().await);
        let mock_io_rows = Arc::new(MockStorageProvider::new().await);

        #[cfg(feature = "enable_long_row")]
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        let string_bytes = b"Hello, world!";

        let mut string_data = MEMORY_POOL.acquire(string_bytes.len());
        let string_slice = string_data.into_slice_mut();

        string_slice[0..].copy_from_slice(string_bytes);

        let i32_bytes = 42_i32.to_le_bytes();
        let mut i32_data = MEMORY_POOL.acquire(i32_bytes.len());
        let i32_slice = i32_data.into_slice_mut();
        i32_slice.copy_from_slice(&i32_bytes);

        let row_write = RowWrite {
            columns_writing_data: smallvec::smallvec![
                ColumnWritePayload {
                    data: string_data,
                    write_order: 0,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
                ColumnWritePayload {
                    data: i32_data,
                    write_order: 1,
                    column_type: DbType::I32,
                    size: i32_bytes.len() as u32
                },
            ],
        };

        let result = RowPointer::write_row(
            mock_io_pointers.clone(),
            mock_io_rows.clone(),
            &last_id,
            &table_length,
            #[cfg(feature = "enable_long_row")]
            cluster,
            &row_write,
        )
        .await;

        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn test_save_and_read_row_pointers() {
        let mock_io_pointers = Arc::new(MockStorageProvider::new().await);
        let mock_io_rows = Arc::new(MockStorageProvider::new().await);

        #[cfg(feature = "enable_long_row")]
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        let string_bytes = b"Hello, world!";

        let mut string_data = MEMORY_POOL.acquire(string_bytes.len());
        let string_slice = string_data.into_slice_mut();
        string_slice[0..].copy_from_slice(string_bytes);

        let i32_bytes = 42_i32.to_le_bytes();
        let mut i32_data = MEMORY_POOL.acquire(i32_bytes.len());
        let i32_slice = i32_data.into_slice_mut();
        i32_slice.copy_from_slice(&i32_bytes);

        let row_write = RowWrite {
            columns_writing_data: smallvec::smallvec![
                ColumnWritePayload {
                    data: string_data,
                    write_order: 0,
                    column_type: DbType::STRING,
                    size: 4 + 8 // String Size + String Pointer
                },
                ColumnWritePayload {
                    data: i32_data,
                    write_order: 1,
                    column_type: DbType::I32,
                    size: i32_bytes.len() as u32
                },
            ],
        };

        let _result = RowPointer::write_row(
            mock_io_pointers.clone(),
            mock_io_rows.clone(),
            &last_id,
            &table_length,
            #[cfg(feature = "enable_long_row")]
            cluster,
            &row_write,
        )
        .await;

        let result = RowPointer::write_row(
            mock_io_pointers.clone(),
            mock_io_rows.clone(),
            &last_id,
            &table_length,
            #[cfg(feature = "enable_long_row")]
            cluster,
            &row_write,
        )
        .await;

        assert!(result.is_ok());

        let mut iterator = RowPointerIterator::new(mock_io_pointers.clone())
            .await
            .unwrap();

        let next_pointer = iterator.next_row_pointer().await.unwrap().unwrap();

        let row_fetch = RowFetch {
            columns_fetching_data: smallvec::smallvec![
                ColumnFetchingData {
                    column_offset: 0,
                    column_type: DbType::STRING,
                    size: string_bytes.len() as u32 + 4,
                    schema_id: 0,
                },
                ColumnFetchingData {
                    column_offset: 8 + 4,
                    column_type: DbType::I32,
                    size: i32_bytes.len() as u32,
                    schema_id: 1,
                },
            ],
        };

        let row = next_pointer
            .fetch_row(mock_io_rows.clone(), &row_fetch)
            .await
            .unwrap();

        row.columns
            .iter()
            .for_each(|column| match column.column_type {
                DbType::STRING => {
                    let string_slice = column.data.into_slice();
                    assert_eq!(&string_slice[0..], string_bytes);
                }
                DbType::I32 => {
                    let i32_slice = column.data.into_slice();
                    assert_eq!(i32_slice, i32_bytes);
                }
                _ => {}
            });
    }

    #[tokio::test]
    async fn test_save_and_read_row_pointers_async() {
        let mock_io_pointers = Arc::new(MockStorageProvider::new().await);
        let mock_io_rows = Arc::new(MockStorageProvider::new().await);

        #[cfg(feature = "enable_long_row")]
        let cluster = 0;

        let last_id = AtomicU64::new(0);
        let table_length = AtomicU64::new(0);

        let _result = RowPointer::write_row(
            mock_io_pointers.clone(),
            mock_io_rows.clone(),
            &last_id,
            &table_length,
            #[cfg(feature = "enable_long_row")]
            cluster,
            &create_row_write(0),
        )
        .await;

        let result = RowPointer::write_row(
            mock_io_pointers.clone(),
            mock_io_rows.clone(),
            &last_id,
            &table_length,
            #[cfg(feature = "enable_long_row")]
            cluster,
            &create_row_write(1),
        )
        .await;
        assert!(result.is_ok());

        let mut iterator = RowPointerIterator::new(mock_io_pointers.clone())
            .await
            .unwrap();

        let next_pointer = iterator.next_row_pointer().await.unwrap().unwrap();

        let row_fetch = RowFetch {
            columns_fetching_data: smallvec::smallvec![
                ColumnFetchingData {
                    column_offset: 0,
                    column_type: DbType::U64,
                    size: 8,
                    schema_id: 0
                },
                ColumnFetchingData {
                    column_offset: 8,
                    column_type: DbType::STRING,
                    size: 4 + 8,
                    schema_id: 1
                },
                ColumnFetchingData {
                    column_offset: 8 + 4 + 8,
                    column_type: DbType::U8,
                    size: 1,
                    schema_id: 2
                }
            ],
        };

        let row = next_pointer
            .fetch_row(mock_io_rows.clone(), &row_fetch)
            .await
            .unwrap();

        row.columns
            .iter()
            .for_each(|column| match column.column_type {
                DbType::U8 => {
                    let u8_slice = column.data.into_slice();
                    assert_eq!(u8_slice, &[30]);
                }
                DbType::STRING => {
                    let string_slice = column.data.into_slice();
                    assert_eq!(&string_slice[0..], b"John Doe");
                }
                DbType::U64 => {
                    let u64_bytes = 0_u64.to_le_bytes();
                    let u64_slice = column.data.into_slice();
                    assert_eq!(u64_slice, u64_bytes);
                }
                _ => {}
            });
    }
}

#[cfg(test)]
mod layout_invariants {
    use super::*;

    /// The stamps must survive a round trip through the record.
    ///
    /// Pinned because the encoder writes them at the end and the decoder reads
    /// them at `TOTAL_LENGTH - 16`; if those two ever disagree, the failure is a
    /// row that reads as belonging to some other transaction — silent, and
    /// indistinguishable from data loss.
    #[test]
    fn the_version_stamps_round_trip_through_a_record() {
        // Built from a zeroed record rather than a test helper, so this test
        // depends on nothing but the encoder and the decoder it is checking.
        let mut pointer = RowPointer::from_slice(&[0u8; TOTAL_LENGTH]);
        pointer.id = 7;
        pointer.position = 1234;
        pointer.length = 56;
        pointer.deleted = false;
        pointer.xmin = 0x0102_0304_0506_0708;
        pointer.xmax = 0x1112_1314_1516_1718;

        let block = pointer.into_memory_block();
        let slice = block.into_slice();
        assert_eq!(slice.len(), TOTAL_LENGTH);

        let decoded = RowPointer::from_slice(slice.try_into().expect("record length"));
        assert_eq!(decoded.xmin, pointer.xmin);
        assert_eq!(decoded.xmax, pointer.xmax);
        // The stamps must not have landed on top of anything else.
        assert_eq!(decoded.id, pointer.id);
        assert_eq!(decoded.position, pointer.position);
        assert_eq!(decoded.length, pointer.length);
        assert_eq!(decoded.deleted, pointer.deleted);
    }

    /// The record grew by exactly two `u64`s, and the pre-MVCC length names the
    /// old size for the upgrade note.
    #[test]
    fn the_record_grew_by_exactly_the_two_stamps() {
        assert_eq!(TOTAL_LENGTH, PRE_MVCC_RECORD_LEN + 16);
        assert_eq!(ROW_POINTER_RECORD_LEN, TOTAL_LENGTH);
    }
}

#[cfg(test)]
mod over_reported_length {
    use super::tests::InMemoryIo;
    use super::*;
    use rclite::Arc;

    /// `n` valid records, with the provider claiming there are `n + extra`.
    async fn iterator_over(n: usize, extra: usize) -> Vec<RowPointer> {
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024);

        let mut bytes = Vec::with_capacity(n * TOTAL_LENGTH);
        for id in 0..n {
            let mut p = RowPointer::from_slice(&[0u8; TOTAL_LENGTH]);
            // `.into()` rather than a cast: `id` is `u128` under
            // `enable_long_row` and `u64` otherwise, and `From` holds both ways.
            p.id = (id as u64 + 1).into();
            p.position = (id * 16) as u64;
            p.length = 16;
            p.xmin = FROZEN_TX;
            bytes.extend_from_slice(p.into_memory_block().into_slice());
        }

        let io = Arc::new(InMemoryIo {
            declared_len: Some(((n + extra) * TOTAL_LENGTH) as u64),
            data: std::sync::Arc::new(bytes),
        });

        let mut it = RowPointerIterator::new(io).await.expect("iterator");
        let mut out = Vec::new();
        loop {
            let batch = it.next_row_pointers().await.expect("batch");
            if batch.is_empty() {
                break;
            }
            out.extend(batch);
        }
        out
    }

    /// A provider reporting one record more than it holds must not cost the
    /// records it *does* hold.
    ///
    /// This is what `ext_cov_concurrent_inserts_keep_row_offsets` was failing
    /// on: a concurrent append reserves a whole record before writing it, so the
    /// over-reported length stays a clean multiple of the record size and the
    /// `len % TOTAL_LENGTH` guard does not catch it. The short read then
    /// discarded the entire chunk — and a small table is one chunk, so all 320
    /// rows read back as zero against a file that held every one of them.
    #[tokio::test]
    async fn an_over_reported_length_does_not_discard_the_records_that_exist() {
        let rows = iterator_over(320, 1).await;
        assert_eq!(rows.len(), 320, "the readable records were thrown away");
        assert_eq!(rows.first().map(|p| p.id), Some(1));
        assert_eq!(rows.last().map(|p| p.id), Some(320));
    }

    /// The binary search has to land on the exact boundary, not near it.
    #[tokio::test]
    async fn the_readable_prefix_is_exact() {
        for extra in [1usize, 2, 7, 64] {
            let rows = iterator_over(100, extra).await;
            assert_eq!(rows.len(), 100, "over-reported by {} records", extra);
        }
    }

    /// An honest length still reads everything, and reads nothing extra.
    #[tokio::test]
    async fn an_honest_length_is_unaffected() {
        assert_eq!(iterator_over(50, 0).await.len(), 50);
        assert_eq!(iterator_over(0, 0).await.len(), 0);
    }

    /// A file that claims records but holds none must yield none rather than
    /// looping or panicking.
    #[tokio::test]
    async fn a_wholly_absent_file_yields_nothing() {
        assert_eq!(iterator_over(0, 4).await.len(), 0);
    }
}

#[cfg(test)]
mod failed_prefix_reads {
    //! A chunk whose declared length runs past the bytes there is recovered by
    //! probing for the readable prefix and re-reading it. A *failed* read on
    //! that path — as opposed to a short one — must fail the iteration. It used
    //! to count as "not readable" (a probe) or as the end of the table (the
    //! re-read), and either way the scan came back short and successful.
    use super::*;
    use rclite::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// `data` holds whole records and the length reported is one more record.
    /// Reads past `data` are short (`UnexpectedEof`). Once the first short read
    /// has happened, the `fail_at`-th read after it fails outright.
    #[derive(Clone)]
    struct FlakyIo {
        data: std::sync::Arc<Vec<u8>>,
        reads_since_eof: std::sync::Arc<AtomicU32>,
        fail_at: u32,
    }

    impl crate::core::storage_providers::traits::StorageIO for FlakyIo {
        async fn verify_data(&self, _position: u64, _buffer: &[u8]) -> bool {
            true
        }
        async fn write_data(&self, _position: u64, _buffer: &[u8]) {}
        async fn write_data_seek(&self, _seek: std::io::SeekFrom, _buffer: &[u8]) {}
        async fn read_data_into_buffer(
            &self,
            position: &mut u64,
            buffer: &mut [u8],
        ) -> std::result::Result<(), std::io::Error> {
            let seen = self.reads_since_eof.load(Ordering::SeqCst);
            if seen > 0 {
                self.reads_since_eof.store(seen + 1, Ordering::SeqCst);
                if seen == self.fail_at {
                    return Err(std::io::Error::other("injected read failure"));
                }
            }
            let start = *position as usize;
            let end = start.saturating_add(buffer.len());
            if end > self.data.len() {
                if seen == 0 {
                    self.reads_since_eof.store(1, Ordering::SeqCst);
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short read",
                ));
            }
            buffer.copy_from_slice(&self.data[start..end]);
            *position += buffer.len() as u64;
            Ok(())
        }
        async fn read_vectored(
            &self,
            reads: &mut [(u64, &mut [u8])],
        ) -> std::result::Result<(), std::io::Error> {
            for (pos, buf) in reads {
                let mut p = *pos;
                self.read_data_into_buffer(&mut p, buf).await?;
            }
            Ok(())
        }
        async fn append_data(&self, _buffer: &[u8], _immediate: bool) {}
        async fn append_data_returning_offset(&self, _buffer: &[u8], _immediate: bool) -> u64 {
            self.data.len() as u64
        }
        async fn get_len(&self) -> u64 {
            (self.data.len() + TOTAL_LENGTH) as u64
        }
        fn exists(_location: &str, _table_name: &str) -> bool {
            false
        }
        async fn create_temp(&self) -> Self {
            self.clone()
        }
        async fn swap_temp(&self, _temp_io_sync: &mut Self) {}
        fn get_location(&self) -> Option<String> {
            None
        }
        async fn create_new(&self, _name: String) -> Self {
            self.clone()
        }
        fn drop_io(&self) {}
        fn get_hash(&self) -> u32 {
            0
        }
        async fn start_service(&self) {}
        fn get_name(&self) -> String {
            "FlakyIo".to_string()
        }
    }

    async fn scan(n: usize, fail_at: u32) -> Result<Vec<RowPointer>> {
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024);
        let mut bytes = Vec::with_capacity(n * TOTAL_LENGTH);
        for id in 0..n {
            let mut p = RowPointer::from_slice(&[0u8; TOTAL_LENGTH]);
            p.id = (id as u64 + 1).into();
            p.position = (id * 16) as u64;
            p.length = 16;
            p.xmin = FROZEN_TX;
            bytes.extend_from_slice(p.into_memory_block().into_slice());
        }
        let io = Arc::new(FlakyIo {
            data: std::sync::Arc::new(bytes),
            reads_since_eof: std::sync::Arc::new(AtomicU32::new(0)),
            fail_at,
        });
        let mut it = RowPointerIterator::new(io).await?;
        let mut out = Vec::new();
        loop {
            let batch = it.next_row_pointers().await?;
            if batch.is_empty() {
                return Ok(out);
            }
            out.extend(batch);
        }
    }

    /// Every position a failure can land on — each probe of the search and the
    /// re-read after it — gives either the whole table or an error.
    #[tokio::test]
    async fn a_failed_read_while_recovering_a_short_chunk_is_an_error() {
        let n = 320;
        let mut errors = 0;
        for fail_at in 1..=24 {
            match scan(n, fail_at).await {
                Ok(rows) => assert_eq!(
                    rows.len(),
                    n,
                    "fail_at {fail_at}: a failed read returned {} of {n} records as success",
                    rows.len()
                ),
                Err(e) => {
                    assert!(e.to_string().contains("injected"), "fail_at {fail_at}: {e}");
                    errors += 1;
                }
            }
        }
        // Some positions must actually have hit the recovery path.
        assert!(errors >= 5, "only {errors} injected failures were reached");
    }
}

#[cfg(test)]
mod sparse_candidate_walk {
    //! The iterator walks a slot list from an index with a cursor that lives
    //! for one drain of one chunk. A cursor that lands one candidate late is a
    //! row silently dropped from a result, so this drives the iterator through
    //! everything that repositions it — `reset`, `seek_to_byte_position`, a
    //! batch limit changed mid-scan, the single-record path interleaved with
    //! batches, chunk reloads, rows appended and picked up by
    //! `refresh_total_length` — and checks every record it hands out against a
    //! model of what the candidates admit.
    use super::*;
    use rclite::Arc;
    use std::sync::atomic::Ordering;

    /// Every record of the table is present in `data`; `len` is how many
    /// bytes the provider currently reports, so rows can be "appended" by
    /// raising it. With `borrow` the chunks come from the provider's own
    /// bytes, the path a memory map takes.
    #[derive(Clone)]
    struct GrowingIo {
        data: std::sync::Arc<Vec<u8>>,
        len: std::sync::Arc<AtomicU64>,
        borrow: bool,
    }

    impl crate::core::storage_providers::traits::StorageIO for GrowingIo {
        async fn verify_data(&self, _position: u64, _buffer: &[u8]) -> bool {
            true
        }
        async fn write_data(&self, _position: u64, _buffer: &[u8]) {}
        async fn write_data_seek(&self, _seek: std::io::SeekFrom, _buffer: &[u8]) {}
        async fn read_data_into_buffer(
            &self,
            position: &mut u64,
            buffer: &mut [u8],
        ) -> std::result::Result<(), std::io::Error> {
            let start = *position as usize;
            let end = start.saturating_add(buffer.len());
            if end > self.len.load(Ordering::SeqCst) as usize {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "EOF"));
            }
            buffer.copy_from_slice(&self.data[start..end]);
            *position += buffer.len() as u64;
            Ok(())
        }
        async fn read_vectored(
            &self,
            reads: &mut [(u64, &mut [u8])],
        ) -> std::result::Result<(), std::io::Error> {
            for (pos, buf) in reads {
                let mut p = *pos;
                self.read_data_into_buffer(&mut p, buf).await?;
            }
            Ok(())
        }
        fn try_borrow_range(&self, position: u64, len: usize) -> Option<BorrowedBytes> {
            let end = (position as usize).checked_add(len)?;
            if !self.borrow || end > self.len.load(Ordering::SeqCst) as usize {
                return None;
            }
            let owner: std::sync::Arc<dyn std::any::Any + Send + Sync> = self.data.clone();
            // SAFETY: `owner` keeps the vector alive and it is never mutated.
            Some(unsafe { BorrowedBytes::new(owner, self.data.as_ptr().add(position as usize), len) })
        }
        async fn append_data(&self, _buffer: &[u8], _immediate: bool) {}
        async fn append_data_returning_offset(&self, _buffer: &[u8], _immediate: bool) -> u64 {
            self.len.load(Ordering::SeqCst)
        }
        async fn get_len(&self) -> u64 {
            self.len.load(Ordering::SeqCst)
        }
        fn exists(_location: &str, _table_name: &str) -> bool {
            false
        }
        async fn create_temp(&self) -> Self {
            self.clone()
        }
        async fn swap_temp(&self, _temp_io_sync: &mut Self) {}
        fn get_location(&self) -> Option<String> {
            None
        }
        async fn create_new(&self, _name: String) -> Self {
            self.clone()
        }
        fn drop_io(&self) {}
        fn get_hash(&self) -> u32 {
            0
        }
        async fn start_service(&self) {}
        fn get_name(&self) -> String {
            "GrowingIo".to_string()
        }
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            if n == 0 { 0 } else { self.next() % n }
        }
    }

    /// Records per chunk, so candidates can be placed on chunk edges.
    const CHUNK_RECORDS: u64 = (CHUNK_SIZE / TOTAL_LENGTH) as u64;

    /// What the candidates admit at a slot.
    enum Model {
        Sparse { listed: std::collections::HashSet<u64>, n_slots: u64 },
        Leading { upto: u64, n_slots: u64 },
        Unfiltered,
    }

    impl Model {
        fn admits(&self, slot: u64) -> bool {
            match self {
                Model::Sparse { listed, n_slots } => slot >= *n_slots || listed.contains(&slot),
                Model::Leading { upto, n_slots } => slot >= *n_slots || slot < *upto,
                Model::Unfiltered => true,
            }
        }
    }

    fn slot_of(p: &RowPointer) -> u64 {
        // `id` is `u128` under `enable_long_row`; ids here are small.
        p.id as u64 - 1
    }

    /// One random table, candidate set and script of iterator calls.
    async fn run_case(seed: u64) {
        let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);

        // Mostly within one or two chunks, sometimes across several.
        let max_records = match r.below(4) {
            0 => 1 + r.below(40),
            1 => 1 + r.below(CHUNK_RECORDS * 2),
            _ => CHUNK_RECORDS + r.below(CHUNK_RECORDS * 3),
        };
        let deleted_one_in = [0u64, 2, 7, 50][r.below(4) as usize];
        let mut deleted = vec![false; max_records as usize];
        let mut bytes = Vec::with_capacity(max_records as usize * TOTAL_LENGTH);
        for slot in 0..max_records {
            let mut p = RowPointer::from_slice(&[0u8; TOTAL_LENGTH]);
            p.id = (slot + 1).into();
            p.position = slot * 16;
            p.length = 16;
            p.xmin = FROZEN_TX;
            p.deleted = deleted_one_in != 0 && r.below(deleted_one_in) == 0;
            deleted[slot as usize] = p.deleted;
            bytes.extend_from_slice(p.into_memory_block().into_slice());
        }

        // The table as it was when the iterator was made; the rest arrives
        // later. `n_slots` is when the index was asked, which may be a little
        // before or after that.
        let mut visible = if r.below(3) == 0 { r.below(max_records + 1) } else { max_records };
        let n_slots = (visible + r.below(4)).saturating_sub(r.below(4) * r.below(visible / 4 + 1));

        let model = match r.below(6) {
            0 => Model::Unfiltered,
            1 => Model::Leading { upto: r.below(n_slots + 1), n_slots },
            _ => {
                // Densities from a handful of slots to all of them, plus the
                // slots where a chunk or the list begins and ends.
                let one_in = [1u64, 2, 3, 40, 500, 5000][r.below(6) as usize];
                let mut listed = std::collections::HashSet::new();
                for slot in 0..n_slots {
                    if r.below(one_in) == 0 {
                        listed.insert(slot);
                    }
                }
                let edges = [
                    0,
                    1,
                    CHUNK_RECORDS - 1,
                    CHUNK_RECORDS,
                    CHUNK_RECORDS + 1,
                    2 * CHUNK_RECORDS - 1,
                    2 * CHUNK_RECORDS,
                    n_slots.saturating_sub(1),
                ];
                for edge in edges {
                    if edge < n_slots && r.below(2) == 0 {
                        listed.insert(edge);
                    }
                }
                Model::Sparse { listed, n_slots }
            }
        };
        let bitmap = match &model {
            Model::Sparse { listed, n_slots } => {
                let mut slots: Vec<u64> = listed.iter().copied().collect();
                slots.sort_unstable();
                Some(Arc::new(CandidateBitmap::sparse(slots, *n_slots)))
            }
            Model::Leading { upto, n_slots } => {
                Some(Arc::new(CandidateBitmap::leading(*n_slots, *upto)))
            }
            Model::Unfiltered => None,
        };

        let io = Arc::new(GrowingIo {
            data: std::sync::Arc::new(bytes),
            len: std::sync::Arc::new(AtomicU64::new(visible * TOTAL_LENGTH as u64)),
            borrow: r.below(2) == 0,
        });
        let mut it = if r.below(2) == 0 {
            RowPointerIterator::new_deferred(Arc::clone(&io)).await.unwrap()
        } else {
            RowPointerIterator::new(Arc::clone(&io)).await.unwrap()
        };
        it.set_candidates(bitmap);
        it.reset();
        if r.below(2) == 0 {
            it.set_batch_limit(1 + r.below(64) as usize);
        }

        // Where the current pass started, and what it has handed out.
        let mut start = 0u64;
        let mut got: Vec<u64> = Vec::new();
        let mut out = Vec::new();
        let ctx = |start: u64, visible: u64| {
            format!("seed {seed}: pass from slot {start}, {visible} of {max_records} records")
        };
        // Repositionings are capped so a pass always gets to finish.
        let mut moves = 0;
        loop {
            let op = r.below(40);
            if op <= 1 {
                moves += 1;
            }
            match op {
                // Start over, as the processor does before driving a scan.
                0 if moves <= 4 => {
                    it.reset();
                    start = 0;
                    got.clear();
                }
                // Jump to a slot, as a ranged scan does.
                1 if moves <= 4 => {
                    start = r.below(visible + 1);
                    it.seek_to_byte_position(start * TOTAL_LENGTH as u64).await.unwrap();
                    got.clear();
                }
                // Rows appended since the iterator was made.
                2 => {
                    visible = (visible + 1 + r.below(CHUNK_RECORDS / 2)).min(max_records);
                    io.len.store(visible * TOTAL_LENGTH as u64, Ordering::SeqCst);
                    it.refresh_total_length().await;
                }
                3 => it.set_batch_limit(1 + r.below(300) as usize),
                0 | 1 => {}
                // The single-record path, between batches.
                4..=11 => match it.next_row_pointer_with_offset().await.unwrap() {
                    Some((offset, p)) => {
                        let slot = slot_of(&p);
                        assert_eq!(offset, slot * TOTAL_LENGTH as u64, "{}", ctx(start, visible));
                        got.push(slot);
                    }
                    // Rows may still be appended after the end was seen.
                    None if r.below(3) != 0 => break,
                    None => {}
                },
                _ => {
                    it.next_row_pointers_into(&mut out).await.unwrap();
                    if out.is_empty() && r.below(3) != 0 {
                        break;
                    }
                    got.extend(out.iter().map(slot_of));
                }
            }
        }
        // Drained: nothing more on either path.
        assert!(
            it.next_row_pointer_with_offset().await.unwrap().is_none(),
            "{}",
            ctx(start, visible)
        );
        it.next_row_pointers_into(&mut out).await.unwrap();
        assert!(out.is_empty(), "{}", ctx(start, visible));

        let want: Vec<u64> = (start..visible)
            .filter(|s| model.admits(*s) && !deleted[*s as usize])
            .collect();
        if got != want {
            let dropped: Vec<u64> =
                want.iter().filter(|s| !got.contains(s)).copied().take(10).collect();
            let extra: Vec<u64> =
                got.iter().filter(|s| !want.contains(s)).copied().take(10).collect();
            panic!(
                "{}: handed out {} records, expected {}; first dropped {dropped:?}, first unexpected {extra:?}",
                ctx(start, visible),
                got.len(),
                want.len()
            );
        }
    }

    /// The iterator hands out exactly the records the candidates admit, in
    /// slot order: every listed slot that is not deleted, and every slot at
    /// or past `n_slots`, whatever repositioned it along the way.
    #[tokio::test]
    async fn the_walk_yields_exactly_the_candidates() {
        let _ = crate::BATCH_SIZE.get_or_init(|| 1024);
        for seed in 1..=600u64 {
            run_case(seed).await;
        }
    }
}
