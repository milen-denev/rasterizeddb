use std::{
    any::Any,
    future::Future,
    io::SeekFrom,
    sync::Arc,
};

pub type IOError = std::io::Error;
pub type IOResult<T> = std::result::Result<T, IOError>;

/// Marks a commit failure as permanent rather than transient.
///
/// The write-ahead log poisons itself when a flush fails, because on Linux the
/// kernel may drop the dirty pages after reporting the error — so a retry could
/// return success while the data is gone. A caller that treats this like a
/// transient failure and retries will never make progress; one that treats a
/// transient failure as permanent gives up too early. The distinction has to
/// survive the trait boundary, and an `io::Error` has nowhere else to carry it.
pub const POISONED_LOG: &str = "wal-poisoned";

/// Whether a commit failure is the permanent kind. See [`POISONED_LOG`].
#[inline]
pub fn is_poisoned(error: &IOError) -> bool {
    error.to_string().contains(POISONED_LOG)
}

/// A read-only window onto a provider's backing storage.
///
/// Lets a caller parse records straight out of a memory map instead of copying
/// a chunk into its own buffer first. The guard owns a handle to whatever backs
/// the bytes, so the region stays mapped even if the provider re-maps the file
/// in the meantime.
pub struct BorrowedBytes {
    /// Keeps the backing store (a mapping, a buffer) alive for our lifetime.
    _owner: Arc<dyn Any + Send + Sync>,
    ptr: *const u8,
    len: usize,
}

// SAFETY: the bytes are read-only for the guard's lifetime and `_owner` keeps
// the region alive; no interior mutation is performed through this type.
unsafe impl Send for BorrowedBytes {}
unsafe impl Sync for BorrowedBytes {}

impl BorrowedBytes {
    /// # Safety
    /// `ptr..ptr+len` must remain mapped and readable for as long as `owner` is
    /// held, and must not be deallocated or unmapped while this guard lives.
    #[inline]
    pub unsafe fn new(owner: Arc<dyn Any + Send + Sync>, ptr: *const u8, len: usize) -> Self {
        Self { _owner: owner, ptr, len }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: upheld by the constructor's contract.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[allow(async_fn_in_trait)]
pub trait StorageIO: Clone + Sync + Send + 'static {
    fn verify_data(&self, position: u64, buffer: &[u8])
    -> impl Future<Output = bool> + Send + Sync;

    fn write_data(&self, position: u64, buffer: &[u8]) -> impl Future<Output = ()> + Send + Sync;

    fn write_data_seek(
        &self,
        seek: SeekFrom,
        buffer: &[u8],
    ) -> impl Future<Output = ()> + Send + Sync;

    fn read_data_into_buffer(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send + Sync;

    fn read_vectored(
        &self,
        reads: &mut [(u64, &mut [u8])],
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send + Sync;

    /// Synchronous fast path for `read_vectored`. Returns `true` if every read
    /// was fully satisfied without blocking (e.g. all data resident in mmap),
    /// in which case callers must NOT call `read_vectored` for the same reads.
    /// Default impl returns `false` (no fast path); the caller must fall back
    /// to the async `read_vectored`.
    #[inline(always)]
    fn try_read_vectored_sync(&self, _reads: &mut [(u64, &mut [u8])]) -> bool {
        false
    }

    fn append_data(&self, buffer: &[u8], immediate: bool)
    -> impl Future<Output = ()> + Send + Sync;

    /// Appends `buffer` and returns the byte offset the data was written at.
    ///
    /// Implementations MUST determine the offset atomically with the append:
    /// two concurrent callers have to receive distinct offsets that each match
    /// where their own bytes actually landed.
    ///
    /// Anything that records a location to read back later — a row pointer, an
    /// index entry — must obtain it from here. Pairing `get_len()` with
    /// `append_data()` looks equivalent but is not: another task can append
    /// between the two calls, so the offset ends up describing someone else's
    /// bytes.
    fn append_data_returning_offset(
        &self,
        buffer: &[u8],
        immediate: bool,
    ) -> impl Future<Output = u64> + Send + Sync;

    /// Starts a write transaction, or returns 0 when this provider has no log.
    ///
    /// Writes tagged with the same id are applied together or not at all: replay
    /// discards any transaction that never reached its commit.
    fn begin_txn(&self) -> u64 {
        0
    }

    /// Appends `buffer` as part of `txn`, returning its offset and a durability
    /// token.
    ///
    /// The bytes are not durable when this returns; pass the token to
    /// [`StorageIO::commit_durable`], which ends the transaction. That split is
    /// what lets an INSERT — row bytes in one file, pointer record in another —
    /// become durable with a single flush and survive a crash as a unit, and it
    /// lets a whole UPDATE statement do the same across many rows.
    fn append_data_txn(
        &self,
        _txn: u64,
        buffer: &[u8],
    ) -> impl Future<Output = (u64, u64)> + Send + Sync {
        async move { (self.append_data_returning_offset(buffer, true).await, 0) }
    }

    /// In-place counterpart of [`StorageIO::append_data_txn`].
    fn write_data_txn(
        &self,
        _txn: u64,
        position: u64,
        buffer: &[u8],
    ) -> impl Future<Output = u64> + Send + Sync {
        async move {
            self.write_data(position, buffer).await;
            0
        }
    }

    /// Makes every write up to `token` durable. A token of 0 means the writes
    /// were already made durable individually, so this is a no-op.
    ///
    /// An error means the writes are **not** durable and the caller must not
    /// report success for them. This used to return `()` and log the failure,
    /// which told a caller its data had landed when it had not — the same shape
    /// as a commit record that is written nowhere.
    ///
    /// `ErrorKind::Other` carrying [`POISONED_LOG`] is permanent: a flush has
    /// already failed, so durability cannot be promised for anything after it,
    /// and retrying spins forever. Every other error may be transient.
    fn commit_durable(&self, _token: u64) -> impl Future<Output = IOResult<()>> + Send + Sync {
        async move { Ok(()) }
    }

    /// Borrows `len` bytes at `position` directly from the backing store.
    ///
    /// Returns `None` whenever the range cannot be served as one contiguous,
    /// already-resident region — for example when writes are staged and some
    /// bytes only exist in the staging buffer. Callers must then fall back to
    /// a copying read; this is purely an optimization, never the only path.
    #[inline(always)]
    fn try_borrow_range(&self, _position: u64, _len: usize) -> Option<BorrowedBytes> {
        None
    }

    /// Shortens the backing store to `len` bytes, discarding everything past it.
    ///
    /// Only for callers that have established nothing refers to the discarded
    /// range — compaction, after it has moved every live record below `len`.
    /// Growing is not supported: a larger `len` is an error rather than a
    /// silent zero-fill, because every caller that asks for one has miscounted.
    ///
    /// Implementations that cannot truncate return `ErrorKind::Unsupported`,
    /// which callers must treat as "the space stays allocated" rather than as a
    /// failure of the operation that led here.
    fn truncate(&self, _len: u64) -> impl Future<Output = IOResult<()>> + Send + Sync {
        async move {
            Err(IOError::new(
                std::io::ErrorKind::Unsupported,
                "this storage provider cannot truncate",
            ))
        }
    }

    fn get_len(&self) -> impl Future<Output = u64> + Send + Sync;

    fn exists(location: &str, table_name: &str) -> bool;

    fn create_temp(&self) -> impl Future<Output = Self> + Send + Sync;

    fn swap_temp(&self, temp_io_sync: &mut Self) -> impl Future<Output = ()> + Send + Sync;

    fn get_location(&self) -> Option<String>;

    fn create_new(&self, name: String) -> impl Future<Output = Self> + Send + Sync;

    /// As [`StorageIO::create_new`], but rooted at `location` rather than at
    /// this provider's own directory.
    ///
    /// What lets a table's files be created somewhere other than the database
    /// directory: a table is opened from a seed provider, and the seed is this
    /// call against the directory the pool placed the table in.
    ///
    /// The default ignores `location` and defers to `create_new`, which is the
    /// right answer for any provider that has no directory to speak of — an
    /// in-memory one has a name and nothing else.
    fn create_new_at(
        &self,
        _location: &str,
        name: String,
    ) -> impl Future<Output = Self> + Send + Sync {
        self.create_new(name)
    }

    fn drop_io(&self);

    fn get_hash(&self) -> u32;

    fn start_service(&self) -> impl Future<Output = ()> + Send + Sync;

    fn get_name(&self) -> String;
}
