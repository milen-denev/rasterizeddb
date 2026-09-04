//! Tests for the key/value storage backend.
//!
//! Everything here runs against an in-process store, so the parts that are hard
//! to get right — recovery, the commit that spans two files, compaction — are
//! exercised by `cargo test` with no MinIO and no network.

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::store::{BoxFuture, KvStore, MemoryKvStore, ObjectInfo};
use super::{KvConfig, KvFamily, KvStorageProvider};
use crate::core::storage_providers::traits::StorageIO;

/// Small geometry, so a handful of kilobytes crosses chunk and page boundaries.
fn tiny_config() -> KvConfig {
    let mut config = KvConfig::new("t");
    config.chunk_size = 4096;
    config.page_size = 1024;
    config.cache_bytes = 1 << 20;
    config.compact_chunk_bytes = 512;
    config.spill_bytes = 1 << 20;
    config
}

async fn open_family(store: Arc<dyn KvStore>) -> Arc<KvFamily> {
    KvFamily::open(store, tiny_config()).await.unwrap()
}

async fn open(family: &Arc<KvFamily>, name: &str) -> KvStorageProvider {
    KvStorageProvider::open(family.clone(), name).await.unwrap()
}

async fn read_all(provider: &KvStorageProvider) -> Vec<u8> {
    let len = provider.get_len().await as usize;
    let mut out = vec![0u8; len];
    if len > 0 {
        let mut position = 0u64;
        provider
            .read_data_into_buffer(&mut position, &mut out)
            .await
            .unwrap();
    }
    out
}

// ---------------------------------------------------------------------------
// Instrumented stores
// ---------------------------------------------------------------------------

/// Counts what the provider actually asks of the store.
///
/// The request *shape* is the whole point of this backend, so it is asserted on
/// directly: a change that turns one put per commit into three is invisible to
/// every correctness test and obvious here.
struct CountingStore {
    inner: MemoryKvStore,
    puts: AtomicUsize,
    gets: AtomicUsize,
    ranges: AtomicUsize,
    deletes: AtomicUsize,
    lists: AtomicUsize,
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: MemoryKvStore::new(),
            puts: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
            ranges: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            lists: AtomicUsize::new(0),
        }
    }

    fn puts(&self) -> usize {
        self.puts.load(Ordering::Relaxed)
    }

    fn reads(&self) -> usize {
        self.gets.load(Ordering::Relaxed) + self.ranges.load(Ordering::Relaxed)
    }
}

impl KvStore for CountingStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key)
    }
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.ranges.fetch_add(1, Ordering::Relaxed);
        self.inner.get_range(key, offset, len)
    }
    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, std::io::Result<()>> {
        self.puts.fetch_add(1, Ordering::Relaxed);
        self.inner.put(key, body)
    }
    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, std::io::Result<()>> {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, std::io::Result<Vec<ObjectInfo>>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }
    fn concurrency(&self) -> usize {
        4
    }
    fn describe(&self) -> String {
        "counting".to_string()
    }
}

/// Fails every put once `armed` is set. Stands in for the store going away
/// mid-commit, which is the only moment a torn transaction could be created.
struct FailAfterStore {
    inner: MemoryKvStore,
    remaining: AtomicUsize,
    armed: std::sync::atomic::AtomicBool,
}

impl FailAfterStore {
    fn new(allow: usize) -> Self {
        Self {
            inner: MemoryKvStore::new(),
            remaining: AtomicUsize::new(allow),
            armed: std::sync::atomic::AtomicBool::new(false),
        }
    }
    fn arm(&self) {
        self.armed.store(true, Ordering::Relaxed);
    }
}

impl KvStore for FailAfterStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.inner.get(key)
    }
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.inner.get_range(key, offset, len)
    }
    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, std::io::Result<()>> {
        if self.armed.load(Ordering::Relaxed) && self.remaining.fetch_sub(1, Ordering::Relaxed) == 0
        {
            return Box::pin(async { Err(std::io::Error::other("store went away")) });
        }
        self.inner.put(key, body)
    }
    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, std::io::Result<Vec<ObjectInfo>>> {
        self.inner.list(prefix)
    }
    fn describe(&self) -> String {
        "fail-after".to_string()
    }
}

// ---------------------------------------------------------------------------
// Basic file semantics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn appends_come_back_in_order() {
    let family = open_family(Arc::new(MemoryKvStore::new())).await;
    let io = open(&family, "rows.db").await;

    let first = io.append_data_returning_offset(b"hello ", true).await;
    let second = io.append_data_returning_offset(b"world", true).await;
    assert_eq!(first, 0);
    assert_eq!(second, 6);
    assert_eq!(io.get_len().await, 11);
    assert_eq!(read_all(&io).await, b"hello world");
}

#[tokio::test]
async fn a_write_past_the_end_leaves_a_hole_of_zeros() {
    let family = open_family(Arc::new(MemoryKvStore::new())).await;
    let io = open(&family, "rows.db").await;

    io.append_data(b"abc", true).await;
    io.write_data(10, b"xyz").await;
    assert_eq!(io.get_len().await, 13);

    let bytes = read_all(&io).await;
    assert_eq!(&bytes[..3], b"abc");
    assert_eq!(&bytes[3..10], &[0u8; 7]);
    assert_eq!(&bytes[10..], b"xyz");
}

#[tokio::test]
async fn reading_past_the_end_reports_eof() {
    let family = open_family(Arc::new(MemoryKvStore::new())).await;
    let io = open(&family, "rows.db").await;
    io.append_data(b"four", true).await;

    let mut buffer = [0u8; 8];
    let mut position = 0u64;
    let error = io
        .read_data_into_buffer(&mut position, &mut buffer)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);

    let mut position = 99u64;
    let error = io
        .read_data_into_buffer(&mut position, &mut buffer)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
}

#[tokio::test]
async fn an_in_place_write_spanning_chunks_reads_back_whole() {
    let store = Arc::new(MemoryKvStore::new());
    let family = open_family(store).await;
    let io = open(&family, "rows.db").await;

    // Three chunks of the tiny geometry, then a write straddling the first
    // boundary — the case a chunked layout gets wrong.
    let original = vec![b'.'; 4096 * 3];
    io.append_data(&original, true).await;
    io.compact_now().await.unwrap();

    let patch = vec![b'#'; 200];
    io.write_data(4096 - 100, &patch).await;

    let bytes = read_all(&io).await;
    assert_eq!(&bytes[4096 - 100..4096 + 100], &patch[..]);
    assert_eq!(bytes[4096 - 101], b'.');
    assert_eq!(bytes[4096 + 100], b'.');
    assert_eq!(bytes.len(), 4096 * 3);
}

#[tokio::test]
async fn two_handles_to_one_name_are_one_file() {
    let family = open_family(Arc::new(MemoryKvStore::new())).await;
    let first = open(&family, "schema.db").await;
    // How the engine opens a sibling: through an existing provider.
    let second = first.create_new("schema.db".to_string()).await;

    second
        .append_data(b"written through the other handle", true)
        .await;
    assert_eq!(read_all(&first).await, b"written through the other handle");
}

#[tokio::test]
async fn truncate_shortens_and_refuses_to_grow() {
    let family = open_family(Arc::new(MemoryKvStore::new())).await;
    let io = open(&family, "rows.db").await;
    io.append_data(&vec![b'z'; 10_000], true).await;
    io.compact_now().await.unwrap();

    io.truncate(4_000).await.unwrap();
    assert_eq!(io.get_len().await, 4_000);
    assert_eq!(read_all(&io).await.len(), 4_000);

    let error = io.truncate(9_000).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
}

#[tokio::test]
async fn a_vectored_read_matches_reading_one_at_a_time() {
    let family = open_family(Arc::new(MemoryKvStore::new())).await;
    let io = open(&family, "rows.db").await;

    let data: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
    io.append_data(&data, true).await;
    io.compact_now().await.unwrap();

    let offsets = [0u64, 999, 4096, 8191, 15_000];
    let mut buffers: Vec<Vec<u8>> = offsets.iter().map(|_| vec![0u8; 300]).collect();
    {
        let mut reads: Vec<(u64, &mut [u8])> = offsets
            .iter()
            .copied()
            .zip(buffers.iter_mut().map(|b| b.as_mut_slice()))
            .collect();
        io.read_vectored(&mut reads).await.unwrap();
    }
    for (offset, buffer) in offsets.iter().zip(buffers.iter()) {
        let at = *offset as usize;
        assert_eq!(buffer.as_slice(), &data[at..at + 300], "at offset {offset}");
    }
}

// ---------------------------------------------------------------------------
// Durability and recovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_committed_transaction_survives_a_restart() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let family = open_family(store.clone()).await;
        let io = open(&family, "rows.db").await;
        let txn = io.begin_txn();
        let (offset, token) = io.append_data_txn(txn, b"durable").await;
        assert_eq!(offset, 0);
        io.commit_durable(token).await.unwrap();
    }
    // A new family over the same objects is what a restart looks like.
    let family = open_family(store).await;
    let io = open(&family, "rows.db").await;
    assert_eq!(read_all(&io).await, b"durable");
}

#[tokio::test]
async fn an_uncommitted_transaction_does_not_survive_a_restart() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let family = open_family(store.clone()).await;
        let io = open(&family, "rows.db").await;

        let committed = io.begin_txn();
        io.append_data_txn(committed, b"kept").await;
        io.commit_durable(committed).await.unwrap();

        // Written, spilled to the store, and never committed.
        let abandoned = io.begin_txn();
        io.append_data_txn(abandoned, b"discarded").await;
        io.flush().await.unwrap();
    }
    let family = open_family(store).await;
    let io = open(&family, "rows.db").await;
    assert_eq!(read_all(&io).await, b"kept");
}

#[tokio::test]
async fn a_transaction_spanning_two_files_commits_on_one_of_them() {
    // The engine's own pattern: begin on rows, write to rows and pointers,
    // commit on pointers alone.
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let family = open_family(store.clone()).await;
        let rows = open(&family, "t.db").await;
        let pointers = open(&family, "t_pointers.db").await;

        let txn = rows.begin_txn();
        rows.append_data_txn(txn, b"the row bytes").await;
        pointers.append_data_txn(txn, b"the pointer").await;
        pointers.commit_durable(txn).await.unwrap();
    }
    let family = open_family(store).await;
    assert_eq!(
        read_all(&open(&family, "t.db").await).await,
        b"the row bytes"
    );
    assert_eq!(
        read_all(&open(&family, "t_pointers.db").await).await,
        b"the pointer"
    );
}

#[tokio::test]
async fn a_commit_that_fails_leaves_neither_half_behind() {
    // The failure this whole arrangement exists to prevent: a stored pointer
    // naming row bytes that were never stored.
    let store = Arc::new(FailAfterStore::new(1));
    let handle: Arc<dyn KvStore> = store.clone();
    {
        let family = open_family(handle.clone()).await;
        let rows = open(&family, "t.db").await;
        let pointers = open(&family, "t_pointers.db").await;

        let txn = rows.begin_txn();
        rows.append_data_txn(txn, b"row bytes").await;
        pointers.append_data_txn(txn, b"pointer").await;

        // The rows object lands; the pointers object — the one carrying the
        // commit declaration — does not.
        store.arm();
        let outcome = pointers.commit_durable(txn).await;
        assert!(outcome.is_err(), "a failed put must not report success");
    }
    let family = open_family(handle).await;
    assert_eq!(read_all(&open(&family, "t.db").await).await, b"");
    assert_eq!(read_all(&open(&family, "t_pointers.db").await).await, b"");
}

#[tokio::test]
async fn transaction_ids_resume_past_what_the_store_has_seen() {
    // The counter restarting at 1 would let a fresh transaction take an id some
    // surviving batch already declared committed.
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let first_run_txn = {
        let family = open_family(store.clone()).await;
        let io = open(&family, "rows.db").await;
        let mut last = 0;
        for _ in 0..5 {
            let txn = io.begin_txn();
            io.append_data_txn(txn, b"x").await;
            io.commit_durable(txn).await.unwrap();
            last = txn;
        }
        last
    };
    let family = open_family(store).await;
    let io = open(&family, "rows.db").await;
    assert!(
        io.begin_txn() > first_run_txn,
        "a restart reissued transaction id {first_run_txn}"
    );
}

#[tokio::test]
async fn a_corrupt_delta_is_treated_as_uncommitted_not_as_an_outage() {
    let store = Arc::new(MemoryKvStore::new());
    let handle: Arc<dyn KvStore> = store.clone();
    {
        let family = open_family(handle.clone()).await;
        let io = open(&family, "rows.db").await;
        let txn = io.begin_txn();
        io.append_data_txn(txn, b"first").await;
        io.commit_durable(txn).await.unwrap();
        let txn = io.begin_txn();
        io.append_data_txn(txn, b"second").await;
        io.commit_durable(txn).await.unwrap();
    }
    // Damage the newest delta.
    let listed = store.list("t/rows.db/d/").await.unwrap();
    let newest = listed.last().unwrap().key.clone();
    store.put(&newest, vec![0xff; 64]).await.unwrap();

    let family = open_family(handle).await;
    let io = open(&family, "rows.db").await;
    assert_eq!(
        read_all(&io).await,
        b"first",
        "the intact prefix must still open"
    );
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compaction_folds_into_base_chunks_and_retires_the_deltas() {
    let store = Arc::new(MemoryKvStore::new());
    let handle: Arc<dyn KvStore> = store.clone();
    let family = open_family(handle).await;
    let io = open(&family, "rows.db").await;

    let mut expected = Vec::new();
    for i in 0..40u32 {
        let block = vec![(i % 251) as u8; 512];
        let txn = io.begin_txn();
        io.append_data_txn(txn, &block).await;
        io.commit_durable(txn).await.unwrap();
        expected.extend_from_slice(&block);
    }
    let deltas_before = store.list("t/rows.db/d/").await.unwrap().len();
    assert!(
        deltas_before >= 40,
        "expected a delta per commit, saw {deltas_before}"
    );

    io.compact_now().await.unwrap();

    let deltas_after = store.list("t/rows.db/d/").await.unwrap().len();
    let chunks = store.list("t/rows.db/g").await.unwrap().len();
    assert!(deltas_after < deltas_before, "compaction retired nothing");
    assert!(chunks > 0, "compaction wrote no base chunks");
    assert_eq!(
        read_all(&io).await,
        expected,
        "compaction changed what the file says"
    );
}

#[tokio::test]
async fn a_compacted_file_reopens_from_its_base_chunks() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let expected: Vec<u8> = (0..30_000u32).map(|i| (i % 251) as u8).collect();
    {
        let family = open_family(store.clone()).await;
        let io = open(&family, "rows.db").await;
        for block in expected.chunks(1000) {
            let txn = io.begin_txn();
            io.append_data_txn(txn, block).await;
            io.commit_durable(txn).await.unwrap();
        }
        io.compact_now().await.unwrap();
    }
    let family = open_family(store).await;
    let io = open(&family, "rows.db").await;
    assert_eq!(io.get_len().await, expected.len() as u64);
    assert_eq!(read_all(&io).await, expected);
}

#[tokio::test]
async fn compaction_leaves_an_open_transaction_alone() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let family = open_family(store.clone()).await;
    let io = open(&family, "rows.db").await;

    let settled = vec![b'a'; 20_000];
    let txn = io.begin_txn();
    io.append_data_txn(txn, &settled).await;
    io.commit_durable(txn).await.unwrap();

    // Still open when the compactor runs.
    let in_flight = io.begin_txn();
    io.append_data_txn(in_flight, b"not committed").await;
    io.compact_now().await.unwrap();

    // Visible to its own writer, and gone after a restart.
    let mut buffer = vec![0u8; 13];
    let mut position = 20_000u64;
    io.read_data_into_buffer(&mut position, &mut buffer)
        .await
        .unwrap();
    assert_eq!(&buffer, b"not committed");

    drop(io);
    let family = open_family(store).await;
    let io = open(&family, "rows.db").await;
    assert_eq!(
        read_all(&io).await,
        settled,
        "an open transaction became base data"
    );
}

#[tokio::test]
async fn repeated_writes_to_one_place_do_not_grow_the_store() {
    let store = Arc::new(MemoryKvStore::new());
    let handle: Arc<dyn KvStore> = store.clone();
    let family = open_family(handle).await;
    let io = open(&family, "pointers.db").await;

    io.append_data(&vec![0u8; 40_000], true).await;
    io.compact_now().await.unwrap();

    // The engine's most common write: flipping a status byte on a row pointer,
    // over and over, in the middle of a file.
    for round in 0..200u8 {
        let txn = io.begin_txn();
        io.write_data_txn(txn, 12_345, &[round]).await;
        io.commit_durable(txn).await.unwrap();
        io.compact_now().await.unwrap();
    }
    let mut buffer = [0u8; 1];
    let mut position = 12_345u64;
    io.read_data_into_buffer(&mut position, &mut buffer)
        .await
        .unwrap();
    assert_eq!(buffer[0], 199);

    let bytes = store.total_bytes();
    assert!(
        bytes < 400_000,
        "200 one-byte writes to a 40 KB file left {bytes} bytes in the store"
    );
}

// ---------------------------------------------------------------------------
// Request shape
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_commit_costs_one_put_per_file_it_touched() {
    let store = Arc::new(CountingStore::new());
    let handle: Arc<dyn KvStore> = store.clone();
    let family = open_family(handle).await;
    let rows = open(&family, "t.db").await;
    let pointers = open(&family, "t_pointers.db").await;

    let before = store.puts();
    let txn = rows.begin_txn();
    rows.append_data_txn(txn, &vec![b'r'; 2000]).await;
    pointers.append_data_txn(txn, &vec![b'p'; 64]).await;
    pointers.commit_durable(txn).await.unwrap();

    assert_eq!(
        store.puts() - before,
        2,
        "a two-file commit must be two objects, whatever it wrote"
    );
}

#[tokio::test]
async fn a_commit_spanning_many_chunks_is_still_one_put() {
    let store = Arc::new(CountingStore::new());
    let handle: Arc<dyn KvStore> = store.clone();
    let family = open_family(handle).await;
    let io = open(&family, "rows.db").await;

    io.append_data(&vec![0u8; 40_000], true).await;
    io.flush().await.unwrap();
    io.compact_now().await.unwrap();

    let before = store.puts();
    let txn = io.begin_txn();
    // One byte in each of ten different chunks — a read-modify-write layout
    // would owe ten object rewrites for this.
    for chunk in 0..10u64 {
        io.write_data_txn(txn, chunk * 4096 + 7, &[b'!']).await;
    }
    io.commit_durable(txn).await.unwrap();
    assert_eq!(store.puts() - before, 1);
}

#[tokio::test]
async fn a_scan_reads_each_chunk_once() {
    let store = Arc::new(CountingStore::new());
    let handle: Arc<dyn KvStore> = store.clone();
    let family = open_family(handle).await;
    let io = open(&family, "rows.db").await;

    // Ten chunks of the tiny geometry.
    io.append_data(&vec![7u8; 4096 * 10], true).await;
    io.flush().await.unwrap();
    io.compact_now().await.unwrap();

    // A fresh family, so nothing is cached.
    let family = open_family(store.clone() as Arc<dyn KvStore>).await;
    let io = open(&family, "rows.db").await;
    let before = store.reads();

    let mut buffer = vec![0u8; 256];
    for block in 0..(4096 * 10 / 256) {
        let mut position = block as u64 * 256;
        io.read_data_into_buffer(&mut position, &mut buffer)
            .await
            .unwrap();
        assert!(buffer.iter().all(|&b| b == 7));
    }
    let reads = store.reads() - before;
    // 160 reads over 10 chunks. Anything near 160 means the cache is not
    // working; anything near 10 means whole chunks are being fetched.
    assert!(
        reads <= 40,
        "a sequential scan of 10 chunks took {reads} requests"
    );
}

#[tokio::test]
async fn a_resident_batch_is_served_without_touching_the_store() {
    let store = Arc::new(CountingStore::new());
    let handle: Arc<dyn KvStore> = store.clone();
    let family = open_family(handle).await;
    let io = open(&family, "rows.db").await;

    let data: Vec<u8> = (0..8192u32).map(|i| (i % 251) as u8).collect();
    io.append_data(&data, true).await;
    io.flush().await.unwrap();
    io.compact_now().await.unwrap();

    // Warm the pages.
    let mut warm = vec![0u8; 8192];
    let mut position = 0u64;
    io.read_data_into_buffer(&mut position, &mut warm)
        .await
        .unwrap();

    let before = store.reads();
    let mut buffers: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; 100]).collect();
    let served = {
        let mut reads: Vec<(u64, &mut [u8])> = [10u64, 2000, 4100, 7000]
            .into_iter()
            .zip(buffers.iter_mut().map(|b| b.as_mut_slice()))
            .collect();
        io.try_read_vectored_sync(&mut reads)
    };
    assert!(
        served,
        "everything was resident and the sync path still declined"
    );
    assert_eq!(store.reads(), before);
    assert_eq!(buffers[0].as_slice(), &data[10..110]);
    assert_eq!(buffers[3].as_slice(), &data[7000..7100]);
}

#[tokio::test]
async fn a_borrowed_range_is_refused_when_an_overlay_covers_it() {
    let family = open_family(Arc::new(MemoryKvStore::new())).await;
    let io = open(&family, "rows.db").await;

    io.append_data(&vec![b'o'; 8192], true).await;
    io.flush().await.unwrap();
    io.compact_now().await.unwrap();

    let mut warm = vec![0u8; 8192];
    let mut position = 0u64;
    io.read_data_into_buffer(&mut position, &mut warm)
        .await
        .unwrap();

    let borrowed = io
        .try_borrow_range(100, 16)
        .expect("a clean, resident range should borrow");
    assert_eq!(borrowed.as_slice(), &[b'o'; 16]);
    drop(borrowed);

    // Once a write covers it the cached page is stale, and lending a pointer
    // into it would hand out the old bytes.
    io.write_data(104, b"new").await;
    assert!(io.try_borrow_range(100, 16).is_none());
}

// ---------------------------------------------------------------------------
// Scratch files
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_swapped_scratch_file_becomes_the_real_one_and_survives_a_restart() {
    // The trap: an object store has no rename, so a scratch file kept under its
    // own name can only be adopted by leaving the live data somewhere nothing
    // reopens. That reads as a table coming back empty after a restart.
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let family = open_family(store.clone()).await;
        let io = open(&family, "rows.db").await;
        io.append_data(&vec![b'o'; 12_000], true).await;
        io.flush().await.unwrap();
        io.compact_now().await.unwrap();

        // What a compaction does: build the replacement beside it, then swap.
        let mut temp = io.create_temp().await;
        temp.append_data(&vec![b'n'; 5_000], true).await;
        io.swap_temp(&mut temp).await;

        assert_eq!(io.get_len().await, 5_000);
        assert_eq!(read_all(&io).await, vec![b'n'; 5_000]);
    }

    let family = open_family(store).await;
    let io = open(&family, "rows.db").await;
    assert_eq!(
        io.get_len().await,
        5_000,
        "the swapped file did not survive"
    );
    assert_eq!(read_all(&io).await, vec![b'n'; 5_000]);
}

#[tokio::test]
async fn a_swap_leaves_the_replaced_generation_behind_in_neither_place() {
    let store = Arc::new(MemoryKvStore::new());
    let handle: Arc<dyn KvStore> = store.clone();
    let family = open_family(handle).await;
    let io = open(&family, "rows.db").await;

    io.append_data(&vec![b'o'; 30_000], true).await;
    io.flush().await.unwrap();
    io.compact_now().await.unwrap();
    let before = store.total_bytes();

    let mut temp = io.create_temp().await;
    temp.append_data(&vec![b'n'; 1_000], true).await;
    io.swap_temp(&mut temp).await;
    io.compact_now().await.unwrap();

    let after = store.total_bytes();
    assert!(
        after < before / 2,
        "the replaced generation is still in the store: {before} bytes became {after}"
    );
    assert_eq!(read_all(&io).await, vec![b'n'; 1_000]);
}

#[tokio::test]
async fn dropping_a_scratch_file_leaves_the_real_one_alone() {
    // Scratch shares the real file's key prefix, so a sweep of that prefix
    // would take the table with it.
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let family = open_family(store.clone()).await;
    let io = open(&family, "rows.db").await;
    io.append_data(b"the real contents", true).await;
    io.flush().await.unwrap();
    io.compact_now().await.unwrap();

    let temp = io.create_temp().await;
    temp.append_data(b"scratch", true).await;
    temp.flush().await.unwrap();
    temp.drop_io();
    // `drop_io` deletes in the background.
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    assert_eq!(read_all(&io).await, b"the real contents");
    let family = open_family(store).await;
    let io = open(&family, "rows.db").await;
    assert_eq!(
        read_all(&io).await,
        b"the real contents",
        "dropping scratch erased the table"
    );
}

// ---------------------------------------------------------------------------
// Differential
// ---------------------------------------------------------------------------

/// Runs the same random operations against this backend and the in-memory one,
/// and requires the bytes to agree at every step.
#[tokio::test]
async fn it_behaves_like_the_in_memory_provider() {
    use crate::core::storage_providers::memory::MemoryStorageProvider;

    for seed in 0..8u64 {
        fastrand::seed(seed);
        let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
        let family = open_family(store).await;
        let kv = open(&family, "rows.db").await;
        let reference = MemoryStorageProvider::new("rows.db");

        for step in 0..120 {
            match fastrand::u32(0..100) {
                0..=49 => {
                    let len = fastrand::usize(1..600);
                    let byte = fastrand::u8(..);
                    let block = vec![byte; len];
                    let a = kv.append_data_returning_offset(&block, true).await;
                    let b = reference.append_data_returning_offset(&block, true).await;
                    assert_eq!(a, b, "seed {seed} step {step}: append offset");
                }
                50..=79 => {
                    let len = reference.get_len().await;
                    if len == 0 {
                        continue;
                    }
                    let at = fastrand::u64(0..len);
                    let block = vec![fastrand::u8(..); fastrand::usize(1..300)];
                    kv.write_data(at, &block).await;
                    reference.write_data(at, &block).await;
                }
                80..=89 => {
                    // A committed transaction, so the delta path is exercised too.
                    let txn = kv.begin_txn();
                    let block = vec![fastrand::u8(..); fastrand::usize(1..400)];
                    kv.append_data_txn(txn, &block).await;
                    reference.append_data(&block, true).await;
                    kv.commit_durable(txn).await.unwrap();
                }
                90..=94 => {
                    let len = reference.get_len().await;
                    if len > 100 {
                        let to = fastrand::u64(0..len);
                        kv.truncate(to).await.unwrap();
                        reference.truncate(to).await.unwrap();
                    }
                }
                _ => {
                    kv.compact_now().await.unwrap();
                }
            }

            assert_eq!(
                kv.get_len().await,
                reference.get_len().await,
                "seed {seed} step {step}: length"
            );
            let len = reference.get_len().await;
            if len > 0 {
                let at = fastrand::u64(0..len);
                let want = fastrand::usize(1..500).min((len - at) as usize);
                let mut mine = vec![0u8; want];
                let mut theirs = vec![0u8; want];
                let mut pa = at;
                let mut pb = at;
                kv.read_data_into_buffer(&mut pa, &mut mine).await.unwrap();
                reference
                    .read_data_into_buffer(&mut pb, &mut theirs)
                    .await
                    .unwrap();
                assert_eq!(
                    mine,
                    theirs,
                    "seed {seed} step {step}: bytes at {at}..{}",
                    at + want as u64
                );
                assert_eq!(pa, pb, "seed {seed} step {step}: position advance");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The engine on top of it
// ---------------------------------------------------------------------------

/// The trait tests say the provider keeps its promises. These say the database
/// runs on it — which is a different claim, because the engine reaches for
/// vectored reads, borrowed ranges and cross-file transactions that a
/// hand-written test would have to think to exercise.
mod engine {
    use itertools::Itertools;
    use rclite::Arc as RcArc;
    use smallvec::SmallVec;
    use std::sync::Arc;

    use super::{KvStorageProvider, KvStore, MemoryKvStore, open_family};
    use crate::core::db_type::DbType;
    use crate::core::row::row::ColumnWritePayload;
    use crate::core::row::row::{ColumnFetchingData, Row, RowFetch, RowWrite};
    use crate::core::row::schema::{SchemaCalculator, SchemaField};
    use crate::core::row::table::Table;
    use crate::core::rql::lexer_ct::CreateColumnData;
    use crate::memory_pool::MEMORY_POOL;

    fn init() {
        let _ = crate::MAX_PERMITS_THREADS.set(4);
        let _ = crate::BATCH_SIZE.set(512);
        let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    }

    fn payload(bytes: &[u8], write_order: u32, column_type: DbType) -> ColumnWritePayload {
        let mut mb = MEMORY_POOL.acquire(bytes.len());
        mb.into_slice_mut().copy_from_slice(bytes);
        let size = column_type.get_size();
        ColumnWritePayload {
            data: mb,
            write_order,
            column_type,
            size,
        }
    }

    fn full_fetch(fields: &SmallVec<[SchemaField; 20]>) -> RowFetch {
        let calc = SchemaCalculator;
        let mut cols = SmallVec::new();
        for f in fields
            .iter()
            .sorted_by(|a, b| a.write_order.cmp(&b.write_order))
        {
            let (column_offset, schema_id) = calc.calculate_schema_offset(&f.name, fields);
            cols.push(ColumnFetchingData {
                column_offset,
                column_type: f.db_type.clone(),
                size: f.size,
                schema_id,
            });
        }
        RowFetch {
            columns_fetching_data: cols,
        }
    }

    async fn open_table(store: Arc<dyn KvStore>) -> Table<KvStorageProvider> {
        init();
        let family = open_family(store).await;
        let seed = KvStorageProvider::open(family, "seed").await.unwrap();
        Table::new(
            "people",
            RcArc::new(seed),
            vec![
                CreateColumnData {
                    name: "id".into(),
                    data_type: DbType::U64,
                },
                CreateColumnData {
                    name: "cat".into(),
                    data_type: DbType::I32,
                },
                CreateColumnData {
                    name: "label".into(),
                    data_type: DbType::STRING,
                },
            ],
        )
        .await
    }

    async fn insert(table: &Table<KvStorageProvider>, id: u64, cat: i32, label: &str) {
        let mut rw = RowWrite {
            columns_writing_data: SmallVec::new(),
        };
        rw.columns_writing_data
            .push(payload(&id.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&cat.to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data
            .push(payload(label.as_bytes(), 2, DbType::STRING));
        table.insert_row(rw).await.unwrap();
    }

    async fn query(table: &Table<KvStorageProvider>, where_clause: &str) -> Vec<Row> {
        let fields = table.schema.fields.to_vec();
        table
            .query_row(
                where_clause,
                &fields,
                full_fetch(&table.schema.fields),
                full_fetch(&table.schema.fields),
                None,
                None,
            )
            .await
            .unwrap()
    }

    fn u64_of(row: &Row, schema_id: u64) -> u64 {
        let column = row
            .columns
            .iter()
            .find(|c| c.schema_id == schema_id)
            .unwrap();
        u64::from_le_bytes(column.data.into_slice().try_into().unwrap())
    }

    fn str_of(row: &Row, schema_id: u64) -> String {
        let column = row
            .columns
            .iter()
            .find(|c| c.schema_id == schema_id)
            .unwrap();
        String::from_utf8_lossy(column.data.into_slice()).to_string()
    }

    #[tokio::test]
    async fn a_table_inserts_and_queries_over_the_object_layout() {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
        let table = open_table(store).await;

        for id in 0..200u64 {
            insert(&table, id, (id % 5) as i32, &format!("row-{id}")).await;
        }

        let rows = query(&table, "").await;
        assert_eq!(rows.len(), 200, "not every insert came back");

        let mut ids: Vec<u64> = rows.iter().map(|r| u64_of(r, 0)).collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..200u64).collect::<Vec<_>>());

        let labelled = rows.iter().find(|r| u64_of(r, 0) == 137).unwrap();
        assert_eq!(str_of(labelled, 2), "row-137");

        let filtered = query(&table, "cat = 3").await;
        assert_eq!(filtered.len(), 40);
        assert!(filtered.iter().all(|r| u64_of(r, 0) % 5 == 3));
    }

    #[tokio::test]
    async fn a_table_reopens_from_the_store_after_a_restart() {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
        {
            let table = open_table(store.clone()).await;
            for id in 0..120u64 {
                insert(&table, id, (id % 3) as i32, &format!("value-{id}")).await;
            }
            // What a clean shutdown owes the store: the writes that were never
            // part of a transaction.
            table.io_rows.family().flush_all().await.unwrap();
        }

        let table = open_table(store).await;
        let rows = query(&table, "").await;
        assert_eq!(rows.len(), 120, "rows did not survive the restart");
        let mut ids: Vec<u64> = rows.iter().map(|r| u64_of(r, 0)).collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..120u64).collect::<Vec<_>>());
        assert_eq!(
            str_of(rows.iter().find(|r| u64_of(r, 0) == 99).unwrap(), 2),
            "value-99"
        );
    }
}

#[tokio::test]
async fn a_write_outside_a_transaction_survives_a_restart_with_flush() {
    // The schema, the foreign-key sidecars and the sequence files are all
    // written through the plain `StorageIO` API, which carries no transaction.
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let family = open_family(store.clone()).await;
        let io = open(&family, "schema.db").await;
        io.append_data(b"schema bytes", true).await;
        io.flush().await.unwrap();
    }
    let family = open_family(store).await;
    let io = open(&family, "schema.db").await;
    assert_eq!(read_all(&io).await, b"schema bytes");
}

#[tokio::test]
async fn a_write_outside_a_transaction_survives_a_restart_without_flush() {
    // Same, but nothing asks for a flush — which is what a table's schema gets
    // today, since `TableSchema::save` just writes.
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let family = open_family(store.clone()).await;
        let io = open(&family, "schema.db").await;
        io.append_data(b"schema bytes", true).await;
    }
    let family = open_family(store).await;
    let io = open(&family, "schema.db").await;
    assert_eq!(read_all(&io).await, b"schema bytes");
}

#[tokio::test]
async fn two_files_written_outside_a_transaction_both_survive() {
    // A schema is two files — the header and its `_FIELDS` sidecar — and a
    // schema that recovers its header but not its fields is a table that opens
    // with zero columns.
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let family = open_family(store.clone()).await;
        let header = open(&family, "schema.db").await;
        let fields = open(&family, "schema_FIELDS.db").await;
        header.append_data(b"header", true).await;
        fields.append_data(b"fields", true).await;
        header.flush().await.unwrap();
        fields.flush().await.unwrap();
    }
    let family = open_family(store).await;
    assert_eq!(read_all(&open(&family, "schema.db").await).await, b"header");
    assert_eq!(
        read_all(&open(&family, "schema_FIELDS.db").await).await,
        b"fields"
    );
}

/// A file dropped and immediately reopened under the same name keeps the bytes
/// written after the drop.
///
/// `drop_io` cannot await — it is a synchronous trait method and deleting is a
/// round trip — so it spawns the erase. `erase` then lists and deletes the
/// file's **whole key prefix**, and the name is the prefix. Reopen that name
/// before the spawned task runs and the new file's objects are inside the set
/// the old file is about to sweep.
///
/// The result is data that reads back correctly from the overlay for as long as
/// the process lives and is simply absent after a restart, which is the worst
/// shape a storage bug can take. `drop_io`'s comment says reopening a dropped
/// name "gets a fresh file, which is what unlinking a path does" — but unlinking
/// is synchronous, and this is not.
///
/// Reached by anything that drops a name and recreates it: `DROP TABLE` then
/// `CREATE TABLE` of the same name on a key/value store, and
/// `STORAGE MOVE TABLE` moving a table off a store and back.
/// Fixed by gating `open_file` on `KvFamily::erasing`, so a reopen waits for the
/// dropped file's sweep. Looped because the failure was timing-dependent — it
/// reproduced on the first attempt, but a fix that merely narrowed the window
/// would pass a single pass.
#[tokio::test(flavor = "multi_thread")]
async fn a_name_reopened_after_a_drop_is_not_erased_by_the_old_file() {
    for attempt in 0..20 {
        let store = Arc::new(MemoryKvStore::new());
        let family = open_family(store.clone()).await;

        let first = open(&family, "rows.db").await;
        first.append_data(b"first generation", true).await;
        first.drop_io();

        // Same name, straight away — the window the spawned erase lives in.
        let second = open(&family, "rows.db").await;
        second.append_data(b"second generation", true).await;
        let token = second.begin_txn();
        second.commit_durable(token).await.unwrap();

        // Let the spawned erase run.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // What a restart sees: a fresh family over the same store.
        let reopened = KvFamily::open(store.clone() as Arc<dyn KvStore>, tiny_config())
            .await
            .unwrap();
        let after = open(&reopened, "rows.db").await;
        let bytes = read_all(&after).await;
        assert!(
            bytes.ends_with(b"second generation"),
            "attempt {attempt}: the reopened name lost its bytes to the dropped file's \
             erase — read back {:?}",
            String::from_utf8_lossy(&bytes)
        );
    }
}
