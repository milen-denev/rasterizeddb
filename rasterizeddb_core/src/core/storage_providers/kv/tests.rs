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
            .find(|c| u64::from(c.schema_id) == schema_id)
            .unwrap();
        u64::from_le_bytes(column.data.into_slice().try_into().unwrap())
    }

    fn str_of(row: &Row, schema_id: u64) -> String {
        let column = row
            .columns
            .iter()
            .find(|c| u64::from(c.schema_id) == schema_id)
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
            table.data().unwrap().io_rows.family().flush_all().await.unwrap();
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

    /// A cold scan of a key/value table, alone and with three more scans of the
    /// same table at once, counting what the store is asked for. Ignored; run
    /// with `--ignored kv_bench --nocapture` in release.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore]
    async fn kv_bench_engine_scan() {
        use super::super::KvConfig;
        use super::super::KvFamily;
        use super::SlowStore;
        use std::sync::atomic::Ordering;

        init();
        let mut config = KvConfig::new("e");
        config.chunk_size = 256 * 1024;
        config.page_size = 16 * 1024;
        config.cache_bytes = 64 * 1024 * 1024;
        let rows = 60_000u64;

        let store = Arc::new(SlowStore::new(0));
        {
            let family = KvFamily::open(store.clone() as Arc<dyn KvStore>, config.clone())
                .await
                .unwrap();
            let seed = KvStorageProvider::open(family.clone(), "seed").await.unwrap();
            let table = Table::new(
                "people",
                RcArc::new(seed),
                vec![
                    CreateColumnData { name: "id".into(), data_type: DbType::U64 },
                    CreateColumnData { name: "cat".into(), data_type: DbType::I32 },
                    CreateColumnData { name: "label".into(), data_type: DbType::STRING },
                ],
            )
            .await;
            for id in 0..rows {
                insert(&table, id, (id % 5) as i32, &format!("label-{id:08}")).await;
            }
            family.flush_all().await.unwrap();
            for io in [&table.data().unwrap().io_rows, &table.data().unwrap().io_pointers] {
                io.compact_now().await.unwrap();
            }
            family.flush_all().await.unwrap();
        }

        for round in 0..3 {
            for scans in [1usize, 4] {
                let latency = std::time::Duration::from_millis(20);
                let slow = Arc::new(SlowStore {
                    inner: MemoryKvStore::new(),
                    latency,
                    gets: Default::default(),
                    ranges: Default::default(),
                });
                // Same objects, behind a store with latency.
                for (key, bytes) in store
                    .inner
                    .list("e/")
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|o| o.key)
                    .map(|k| {
                        let v = futures::executor::block_on(store.inner.get(&k)).unwrap().unwrap();
                        (k, v)
                    })
                {
                    slow.inner.put(&key, bytes).await.unwrap();
                }
                let family = KvFamily::open(slow.clone() as Arc<dyn KvStore>, config.clone())
                    .await
                    .unwrap();
                let seed = KvStorageProvider::open(family, "seed").await.unwrap();
                let table = Arc::new(
                    Table::new("people", RcArc::new(seed), Vec::new()).await,
                );
                let g0 = slow.gets.load(Ordering::Relaxed);
                let r0 = slow.ranges.load(Ordering::Relaxed);
                let started = std::time::Instant::now();
                let mut tasks = Vec::new();
                for _ in 0..scans {
                    let table = table.clone();
                    tasks.push(tokio::spawn(async move { query(&table, "cat = 3").await.len() }));
                }
                for t in tasks {
                    assert_eq!(t.await.unwrap() as u64, rows / 5);
                }
                let ms = started.elapsed().as_secs_f64() * 1000.0;
                println!(
                    "round {round} engine_scan scans={scans} latency=20ms: gets={} ranges={} wall={ms:.1}ms",
                    slow.gets.load(Ordering::Relaxed) - g0,
                    slow.ranges.load(Ordering::Relaxed) - r0,
                );
            }
        }
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

// ---------------------------------------------------------------------------
// Single-flight fetches
// ---------------------------------------------------------------------------

/// Holds every whole-object GET until the test opens the gate, and counts what
/// it is asked for. Ranged reads pass straight through.
///
/// The gate is what makes "two readers wanted one chunk at once" deterministic:
/// the first request cannot finish before the second reader has arrived.
struct GatedStore {
    inner: MemoryKvStore,
    gets: AtomicUsize,
    ranges: AtomicUsize,
    /// GETs to fail before succeeding, for the retry test.
    fail_gets: AtomicUsize,
    open: tokio::sync::watch::Sender<bool>,
}

impl GatedStore {
    fn new() -> Self {
        Self {
            inner: MemoryKvStore::new(),
            gets: AtomicUsize::new(0),
            ranges: AtomicUsize::new(0),
            fail_gets: AtomicUsize::new(0),
            open: tokio::sync::watch::channel(true).0,
        }
    }
    fn close(&self) {
        self.open.send_replace(false);
    }
    fn release(&self) {
        self.open.send_replace(true);
    }
    fn gets(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }
    fn ranges(&self) -> usize {
        self.ranges.load(Ordering::SeqCst)
    }
}

impl KvStore for GatedStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let mut open = self.open.subscribe();
        Box::pin(async move {
            let _ = open.wait_for(|o| *o).await;
            let failing = self
                .fail_gets
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if failing {
                return Err(std::io::Error::other("store hiccup"));
            }
            self.inner.get(key).await
        })
    }
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.ranges.fetch_add(1, Ordering::SeqCst);
        self.inner.get_range(key, offset, len)
    }
    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.put(key, body)
    }
    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, std::io::Result<Vec<ObjectInfo>>> {
        self.inner.list(prefix)
    }
    fn describe(&self) -> String {
        "gated".to_string()
    }
}

/// Four tiny-geometry chunks of patterned bytes, compacted into base objects
/// and reopened behind a fresh family, so nothing is cached.
async fn cold_gated_file(store: &Arc<GatedStore>, readahead: u64) -> (KvStorageProvider, Vec<u8>) {
    let mut config = tiny_config();
    config.readahead_chunks = readahead;
    let data: Vec<u8> = (0..4096u32 * 4).map(|i| (i % 251) as u8).collect();
    {
        let family = KvFamily::open(store.clone() as Arc<dyn KvStore>, config.clone())
            .await
            .unwrap();
        let io = KvStorageProvider::open(family, "rows.db").await.unwrap();
        io.append_data(&data, true).await;
        io.flush().await.unwrap();
        io.compact_now().await.unwrap();
    }
    let family = KvFamily::open(store.clone() as Arc<dyn KvStore>, config)
        .await
        .unwrap();
    (
        KvStorageProvider::open(family, "rows.db").await.unwrap(),
        data,
    )
}

/// A vectored read of most of chunk 0 — enough pages that it fetches the whole
/// object rather than ranges.
async fn read_chunk_zero(io: &KvStorageProvider, data: &[u8]) -> std::io::Result<()> {
    let mut buffers: Vec<Vec<u8>> = (0..4).map(|_| vec![0u8; 100]).collect();
    let positions = [10u64, 1100, 2200, 3300];
    let mut reads: Vec<(u64, &mut [u8])> = positions
        .into_iter()
        .zip(buffers.iter_mut().map(|b| b.as_mut_slice()))
        .collect();
    io.read_vectored(&mut reads).await?;
    for (position, buffer) in reads.iter() {
        let at = *position as usize;
        assert_eq!(&buffer[..], &data[at..at + buffer.len()], "wrong bytes at {at}");
    }
    Ok(())
}

/// Lets every spawned reader reach its fetch. The gate holds the request, so
/// time spent here cannot let one finish early.
async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_misses_on_one_chunk_cost_one_get() {
    let store = Arc::new(GatedStore::new());
    let (io, data) = cold_gated_file(&store, 0).await;
    let data = Arc::new(data);

    store.close();
    let before = store.gets();
    let readers: Vec<_> = (0..8)
        .map(|_| {
            let io = io.clone();
            let data = data.clone();
            tokio::spawn(async move { read_chunk_zero(&io, &data).await })
        })
        .collect();
    settle().await;
    store.release();
    for reader in readers {
        reader.await.unwrap().unwrap();
    }
    assert_eq!(
        store.gets() - before,
        1,
        "eight readers missing one chunk at once each fetched it"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_ahead_in_flight_is_joined_rather_than_fetched_again() {
    let store = Arc::new(GatedStore::new());
    let (io, data) = cold_gated_file(&store, 2).await;
    let (gets_before, ranges_before) = (store.gets(), store.ranges());

    // Two sequential reads inside page 0: the first is a ranged read, the
    // second is a cache hit that looks sequential and puts the rest of chunk 0
    // and the chunks after it in flight — held at the gate.
    store.close();
    let mut buffer = vec![0u8; 512];
    for at in [0u64, 512] {
        let mut position = at;
        io.read_data_into_buffer(&mut position, &mut buffer)
            .await
            .unwrap();
        assert_eq!(&buffer[..], &data[at as usize..at as usize + 512]);
    }
    settle().await;
    assert_eq!(store.ranges() - ranges_before, 1);
    let prefetching = store.gets() - gets_before;
    assert_eq!(prefetching, 1, "read-ahead did not start on chunk 0");

    // The next page of chunk 0 is what the read-ahead is fetching. Waiting for
    // it is one request; asking for the page by range is a second.
    let reader = {
        let io = io.clone();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; 512];
            let mut position = 1024u64;
            io.read_data_into_buffer(&mut position, &mut buffer)
                .await
                .map(|_| buffer)
        })
    };
    settle().await;
    assert_eq!(
        store.ranges() - ranges_before,
        1,
        "a page of a chunk already in flight was fetched again by range"
    );
    store.release();
    let buffer = reader.await.unwrap().unwrap();
    assert_eq!(&buffer[..], &data[1024..1536]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_reader_strands_nobody_waiting_on_the_same_fetch() {
    let store = Arc::new(GatedStore::new());
    let (io, data) = cold_gated_file(&store, 0).await;
    let data = Arc::new(data);

    store.close();
    let before = store.gets();
    let first = {
        let (io, data) = (io.clone(), data.clone());
        tokio::spawn(async move { read_chunk_zero(&io, &data).await })
    };
    settle().await;
    let second = {
        let (io, data) = (io.clone(), data.clone());
        tokio::spawn(async move { read_chunk_zero(&io, &data).await })
    };
    settle().await;
    // The reader that started the request goes away before it is answered.
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    store.release();
    second.await.unwrap().unwrap();
    assert_eq!(store.gets() - before, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fetch_whose_readers_all_left_still_fills_the_cache() {
    let store = Arc::new(GatedStore::new());
    let (io, data) = cold_gated_file(&store, 0).await;
    let data = Arc::new(data);

    store.close();
    let reader = {
        let (io, data) = (io.clone(), data.clone());
        tokio::spawn(async move { read_chunk_zero(&io, &data).await })
    };
    settle().await;
    reader.abort();
    let _ = reader.await;
    let before = store.gets();
    store.release();
    settle().await;

    // Resident now, so the synchronous path serves it without the store.
    let mut buffer = vec![0u8; 100];
    let served = io.try_read_vectored_sync(&mut [(1100u64, buffer.as_mut_slice())]);
    assert!(served, "the abandoned fetch's pages never reached the cache");
    assert_eq!(&buffer[..], &data[1100..1200]);
    assert_eq!(store.gets(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_fetch_is_retried_rather_than_remembered() {
    let store = Arc::new(GatedStore::new());
    let (io, data) = cold_gated_file(&store, 0).await;
    let data = Arc::new(data);

    store.fail_gets.store(1, Ordering::SeqCst);
    store.close();
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (io, data) = (io.clone(), data.clone());
            tokio::spawn(async move { read_chunk_zero(&io, &data).await })
        })
        .collect();
    settle().await;
    store.release();
    for reader in readers {
        let err = reader.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("store hiccup"), "{err}");
    }

    // The store has recovered; the next read must ask it again.
    read_chunk_zero(&io, &data).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fetch_that_straddles_an_invalidation_is_not_cached() {
    use super::cache::{ChunkCache, ChunkId, FetchKey};

    let cache = Arc::new(ChunkCache::new(1 << 20));
    let key = FetchKey {
        file: 7,
        generation: 0,
        index: 0,
        whole: true,
    };
    let page = ChunkId {
        file: 7,
        generation: 0,
        index: 0,
    };

    // A request that left before a compaction rewrote the chunk, and answers
    // with the old bytes after the compaction invalidated it.
    let (answer, answered) = tokio::sync::oneshot::channel::<()>();
    let stale = cache.fetch(key, async move {
        let _ = answered.await;
        Ok(vec![(0u64, Arc::new(b"old bytes".to_vec()))])
    });
    cache.invalidate(page);
    assert_eq!(cache.fetches_in_flight(), 0, "an invalidated fetch stayed joinable");

    // A reader arriving now starts its own request rather than joining.
    let fresh = cache.fetch(key, async {
        Ok(vec![(0u64, Arc::new(b"new bytes".to_vec()))])
    });
    assert_eq!(fresh.await.unwrap()[0].1.as_slice(), b"new bytes");

    let _ = answer.send(());
    // Its own waiters still get what it fetched...
    assert_eq!(stale.await.unwrap()[0].1.as_slice(), b"old bytes");
    // ...but the cache keeps the new bytes, not the late old ones.
    assert_eq!(
        cache.get(page).expect("the fresh fetch was not cached").as_slice(),
        b"new bytes"
    );
    assert_eq!(cache.fetches_in_flight(), 0);
}

/// Answers a chunk request from the store's state *now*, then holds the answer
/// until released — a GET that left before a compaction rewrote the chunk and
/// arrives after it finished.
struct LateAnswerStore {
    inner: MemoryKvStore,
    hold_next: std::sync::atomic::AtomicBool,
    held: tokio::sync::Notify,
    started: tokio::sync::Notify,
}

impl KvStore for LateAnswerStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.answer(self.inner.get(key))
    }
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.answer(self.inner.get_range(key, offset, len))
    }
    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.put(key, body)
    }
    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, std::io::Result<Vec<ObjectInfo>>> {
        self.inner.list(prefix)
    }
    fn describe(&self) -> String {
        "late-answer".to_string()
    }
}

impl LateAnswerStore {
    fn answer<'a>(
        &'a self,
        read: BoxFuture<'a, std::io::Result<Option<Vec<u8>>>>,
    ) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let answer = read.await;
            if self.hold_next.swap(false, Ordering::SeqCst) {
                self.started.notify_one();
                self.held.notified().await;
            }
            answer
        })
    }
}

/// A read whose request left before a compaction folded a committed write into
/// the chunk, and whose answer — the old bytes — arrived after it, must neither
/// return those old bytes nor leave them in the cache. The overlay that covered
/// them is gone by then, so a cached copy would be served to every later read,
/// silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fetch_answered_before_a_fold_returns_and_leaves_no_stale_bytes() {
    let store = Arc::new(LateAnswerStore {
        inner: MemoryKvStore::new(),
        hold_next: std::sync::atomic::AtomicBool::new(false),
        held: tokio::sync::Notify::new(),
        started: tokio::sync::Notify::new(),
    });
    let mut config = tiny_config();
    config.readahead_chunks = 0;
    {
        let family = KvFamily::open(store.clone() as Arc<dyn KvStore>, config.clone())
            .await
            .unwrap();
        let io = KvStorageProvider::open(family, "t.db").await.unwrap();
        let a = io.begin_txn();
        io.append_data_txn(a, &[b'a'; 4096]).await;
        io.commit_durable(a).await.unwrap();
        io.compact_now().await.unwrap();
    }
    let family = KvFamily::open(store.clone() as Arc<dyn KvStore>, config)
        .await
        .unwrap();
    let io = KvStorageProvider::open(family, "t.db").await.unwrap();
    let z = io.begin_txn();
    io.write_data_txn(z, 100, b"zzzz").await;
    io.commit_durable(z).await.unwrap();

    store.hold_next.store(true, Ordering::SeqCst);
    let started = store.started.notified();
    let reader = {
        let io = io.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 200];
            let mut position = 0u64;
            io.read_data_into_buffer(&mut position, &mut buf).await.map(|_| buf)
        })
    };
    started.await;
    io.compact_now().await.unwrap();
    store.held.notify_one();
    // The straddling read itself: its base bytes predate the fold, and the
    // overlay that covered them was retired before it applied them.
    let got = reader.await.unwrap().unwrap();
    assert_eq!(&got[100..104], b"zzzz", "the read that straddled the fold");

    let mut again = vec![0u8; 200];
    let mut position = 0u64;
    io.read_data_into_buffer(&mut position, &mut again)
        .await
        .unwrap();
    assert_eq!(&again[100..104], b"zzzz", "a later read was served the pre-fold page");
}

/// Holds the first put whose key contains `pattern` once armed, until released.
struct HoldPutStore {
    inner: MemoryKvStore,
    pattern: parking_lot::Mutex<Option<&'static str>>,
    held: tokio::sync::Notify,
    started: tokio::sync::Notify,
}

impl KvStore for HoldPutStore {
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
        Box::pin(async move {
            let hold = {
                let mut pattern = self.pattern.lock();
                match *pattern {
                    Some(p) if key.contains(p) => {
                        *pattern = None;
                        true
                    }
                    _ => false,
                }
            };
            if hold {
                self.started.notify_one();
                self.held.notified().await;
            }
            self.inner.put(key, body).await
        })
    }
    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, std::io::Result<Vec<ObjectInfo>>> {
        self.inner.list(prefix)
    }
    fn describe(&self) -> String {
        "hold-put".to_string()
    }
}

/// While a compaction publishes — its leftovers going out, then its metadata —
/// every committed byte must stay readable: the ones it folded into a chunk
/// past the old end of the base, and the ones it could not fold.
///
/// The frozen overlay used to be emptied chunk by chunk as the folds landed and
/// taken out entirely before the leftovers were written, so for those round
/// trips a folded byte past the old `base_len` read as zero and a leftover read
/// as whatever it had replaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_committed_byte_stays_readable_while_a_compaction_publishes() {
    for held in ["/d/", "/META"] {
        let store = Arc::new(HoldPutStore {
            inner: MemoryKvStore::new(),
            pattern: parking_lot::Mutex::new(None),
            held: tokio::sync::Notify::new(),
            started: tokio::sync::Notify::new(),
        });
        let family = open_family(store.clone() as Arc<dyn KvStore>).await;
        let io = open(&family, "rows.db").await;

        // Committed, never compacted: chunk 0 and most of chunk 1.
        let settled = io.begin_txn();
        io.append_data_txn(settled, &[b'a'; 6000]).await;
        io.commit_durable(settled).await.unwrap();
        // Open while the compactor runs, inside chunk 1: chunk 0 folds, and
        // the committed bytes of chunk 1 below it are leftovers.
        let open_txn = io.begin_txn();
        io.write_data_txn(open_txn, 5000, b"bbbbbbbbbb").await;

        *store.pattern.lock() = Some(held);
        let started = store.started.notified();
        let compaction = {
            let io = io.clone();
            tokio::spawn(async move { io.compact_now().await })
        };
        started.await;

        for (at, what) in [(0u64, "folded past the old base"), (4500, "a leftover")] {
            let mut buffer = vec![0u8; 100];
            let mut position = at;
            io.read_data_into_buffer(&mut position, &mut buffer)
                .await
                .unwrap();
            assert!(
                buffer.iter().all(|&b| b == b'a'),
                "holding the {held} put: {what} at {at} read back as {:?}",
                &buffer[..8]
            );
        }

        store.held.notify_one();
        compaction.await.unwrap().unwrap();
        let mut expected = vec![b'a'; 6000];
        expected[5000..5010].copy_from_slice(b"bbbbbbbbbb");
        assert_eq!(read_all(&io).await, expected, "after the compaction ({held})");
    }
}

/// The synchronous batch path agrees byte for byte with the async one whenever
/// it serves a batch, over generated batches: reads spanning pages and chunks,
/// empty reads, reads past the base into appended bytes, reads past the end,
/// committed and uncommitted overlay writes, and a cache only partly warm.
#[tokio::test]
async fn the_synchronous_batch_path_agrees_with_the_async_one() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let mut config = tiny_config();
    config.readahead_chunks = 0;
    let family = KvFamily::open(store, config).await.unwrap();
    let io = KvStorageProvider::open(family, "rows.db").await.unwrap();

    let base: Vec<u8> = (0..4096u32 * 4).map(|i| (i * 7 % 251) as u8).collect();
    let txn = io.begin_txn();
    io.append_data_txn(txn, &base).await;
    io.commit_durable(txn).await.unwrap();
    io.compact_now().await.unwrap();

    // Overlay on top of the base: committed rewrites, an uncommitted one, and
    // bytes appended past the base.
    let committed = io.begin_txn();
    io.write_data_txn(committed, 1000, &[0xA1; 300]).await;
    io.write_data_txn(committed, 4090, &[0xA2; 20]).await;
    io.commit_durable(committed).await.unwrap();
    let open_txn = io.begin_txn();
    io.write_data_txn(open_txn, 9000, &[0xB1; 50]).await;
    io.append_data(&[0xC1; 700], false).await;
    let len = io.get_len().await;

    // Part of the cache warm: pages 0, 5 and 9.
    for page in [0u64, 5, 9] {
        let mut warm = vec![0u8; 10];
        io.read_vectored(&mut [(page * 1024 + 3, warm.as_mut_slice())])
            .await
            .unwrap();
    }

    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = |bound: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % bound.max(1)
    };
    let (mut served, mut declined) = (0usize, 0usize);
    for round in 0..3000 {
        let count = 1 + next(5) as usize;
        let shape: Vec<(u64, usize)> = (0..count)
            .map(|_| {
                let position = next(len + 64);
                let width = match next(4) {
                    0 => 0,
                    1 => next(16) as usize,
                    2 => next(1500) as usize,
                    _ => next(3000) as usize,
                };
                (position, width)
            })
            .collect();

        let mut sync_buffers: Vec<Vec<u8>> = shape.iter().map(|&(_, w)| vec![0xEE; w]).collect();
        let sync_served = {
            let mut reads: Vec<(u64, &mut [u8])> = shape
                .iter()
                .zip(sync_buffers.iter_mut())
                .map(|(&(p, _), b)| (p, b.as_mut_slice()))
                .collect();
            io.try_read_vectored_sync(&mut reads)
        };

        let mut async_buffers: Vec<Vec<u8>> = shape.iter().map(|&(_, w)| vec![0xDD; w]).collect();
        let async_result = {
            let mut reads: Vec<(u64, &mut [u8])> = shape
                .iter()
                .zip(async_buffers.iter_mut())
                .map(|(&(p, _), b)| (p, b.as_mut_slice()))
                .collect();
            io.read_vectored(&mut reads).await
        };

        if sync_served {
            served += 1;
            assert!(
                async_result.is_ok(),
                "round {round}: the sync path served {shape:?}, which the async path refuses"
            );
            assert_eq!(sync_buffers, async_buffers, "round {round}: {shape:?}");
        } else {
            declined += 1;
            // A decline leaves the caller's buffers untouched.
            assert!(
                sync_buffers.iter().all(|b| b.iter().all(|&x| x == 0xEE)),
                "round {round}: a declined batch wrote into the buffers"
            );
        }
    }
    assert!(served > 500 && declined > 50, "served {served}, declined {declined}");
}

/// With no Tokio runtime to spawn on, a fetch is driven by whoever awaits it
/// rather than panicking, and still fills the cache and clears its entry.
#[test]
fn a_fetch_outside_a_runtime_is_driven_by_its_waiter() {
    use super::cache::{ChunkCache, ChunkId, FetchKey};

    let cache = Arc::new(ChunkCache::new(1 << 20));
    let key = FetchKey {
        file: 3,
        generation: 1,
        index: 2,
        whole: false,
    };
    let fetch = cache.fetch(key, async { Ok(vec![(2u64, Arc::new(b"page".to_vec()))]) });
    let joined = cache.fetch(key, async { panic!("a second request was made") });
    let pages = futures::executor::block_on(fetch).unwrap();
    assert_eq!(pages[0].1.as_slice(), b"page");
    assert_eq!(futures::executor::block_on(joined).unwrap()[0].1.as_slice(), b"page");
    assert_eq!(cache.fetches_in_flight(), 0);
    let id = ChunkId {
        file: 3,
        generation: 1,
        index: 2,
    };
    assert_eq!(cache.get(id).unwrap().as_slice(), b"page");
}

/// A compaction whose publish fails leaves the file readable and compactable:
/// its frozen bytes go back to the live overlay, and the next pass folds them.
#[tokio::test]
async fn a_failed_compaction_can_be_retried() {
    // Every put in the publish, in order: the leftovers' delta, then the
    // metadata. Each run fails one of them.
    for allowed in 0..2usize {
        let store = Arc::new(FailAfterStore::new(usize::MAX));
        let family = open_family(store.clone() as Arc<dyn KvStore>).await;
        let io = open(&family, "rows.db").await;

        let settled = io.begin_txn();
        io.append_data_txn(settled, &[b'a'; 6000]).await;
        io.commit_durable(settled).await.unwrap();
        // Leaves leftovers in chunk 1, so the publish writes a delta first.
        let open_txn = io.begin_txn();
        io.write_data_txn(open_txn, 5000, b"bbbbbbbbbb").await;

        // The rewrite of chunk 0 is the first put; the publish comes after.
        store.remaining.store(1 + allowed, Ordering::Relaxed);
        store.arm();
        assert!(io.compact_now().await.is_err(), "run {allowed}: the put did not fail");
        store.armed.store(false, Ordering::Relaxed);

        let mut expected = vec![b'a'; 6000];
        expected[5000..5010].copy_from_slice(b"bbbbbbbbbb");
        assert_eq!(read_all(&io).await, expected, "run {allowed}: after the failure");

        io.commit_durable(open_txn).await.unwrap();
        let deltas_before = store.inner.list("t/rows.db/d/").await.unwrap().len();
        io.compact_now().await.unwrap();
        assert_eq!(read_all(&io).await, expected, "run {allowed}: after the retry");
        let deltas_after = store.inner.list("t/rows.db/d/").await.unwrap().len();
        assert!(
            deltas_after < deltas_before,
            "run {allowed}: the retry folded nothing ({deltas_before} deltas before, {deltas_after} after)"
        );

        drop(io);
        let family = open_family(store.clone() as Arc<dyn KvStore>).await;
        assert_eq!(read_all(&open(&family, "rows.db").await).await, expected);
    }
}

// ---------------------------------------------------------------------------
// Fetch benchmarks (ignored)
// ---------------------------------------------------------------------------

/// A memory store that answers reads after a fixed latency and counts them.
///
/// The latency is what makes duplicate fetches visible in wall time: against a
/// real object store every GET is tens of milliseconds, and two GETs of one
/// chunk are two of those. Run with
/// `cargo test --release -p rasterizeddb_core --lib -- --ignored kv_bench --nocapture`.
pub(super) struct SlowStore {
    pub(super) inner: MemoryKvStore,
    pub(super) latency: std::time::Duration,
    pub(super) gets: AtomicUsize,
    pub(super) ranges: AtomicUsize,
}

impl SlowStore {
    pub(super) fn new(latency_ms: u64) -> Self {
        Self {
            inner: MemoryKvStore::new(),
            latency: std::time::Duration::from_millis(latency_ms),
            gets: AtomicUsize::new(0),
            ranges: AtomicUsize::new(0),
        }
    }
    pub(super) fn counts(&self) -> (usize, usize) {
        (
            self.gets.load(Ordering::Relaxed),
            self.ranges.load(Ordering::Relaxed),
        )
    }
}

impl KvStore for SlowStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        let latency = self.latency;
        Box::pin(async move {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            self.inner.get(key).await
        })
    }
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.ranges.fetch_add(1, Ordering::Relaxed);
        let latency = self.latency;
        Box::pin(async move {
            if !latency.is_zero() {
                tokio::time::sleep(latency).await;
            }
            self.inner.get_range(key, offset, len).await
        })
    }
    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.put(key, body)
    }
    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, std::io::Result<Vec<ObjectInfo>>> {
        self.inner.list(prefix)
    }
    fn concurrency(&self) -> usize {
        16
    }
    fn describe(&self) -> String {
        "slow".to_string()
    }
}

mod bench {
    use super::*;
    use std::time::Instant;

    const MIB: u64 = 1024 * 1024;

    /// A file of `chunks` default-geometry chunks, compacted into base objects,
    /// behind a fresh family so nothing is cached.
    async fn cold_file(store: &Arc<SlowStore>, chunks: u64) -> (KvStorageProvider, Vec<u8>) {
        let config = KvConfig::new("b");
        let len = chunks * config.chunk_size;
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8 ^ (i >> 16) as u8).collect();
        {
            let family = KvFamily::open(store.clone() as Arc<dyn KvStore>, config.clone())
                .await
                .unwrap();
            let io = KvStorageProvider::open(family, "rows.db").await.unwrap();
            io.append_data(&data, true).await;
            io.flush().await.unwrap();
            io.compact_now().await.unwrap();
        }
        let family = KvFamily::open(store.clone() as Arc<dyn KvStore>, config)
            .await
            .unwrap();
        (
            KvStorageProvider::open(family, "rows.db").await.unwrap(),
            data,
        )
    }

    /// `clients` tasks each read the whole file in vectored batches, all at
    /// once — concurrent scans of one cold table.
    async fn concurrent_vectored(clients: usize, latency_ms: u64) -> (usize, usize, f64) {
        let store = Arc::new(SlowStore::new(latency_ms));
        let (io, data) = cold_file(&store, 8).await;
        let data = Arc::new(data);
        let before = store.counts();
        let started = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..clients {
            let io = io.clone();
            let data = data.clone();
            tasks.push(tokio::spawn(async move {
                let len = data.len() as u64;
                let batch = 256 * 1024u64;
                let mut buffers: Vec<Vec<u8>> = (0..16).map(|_| vec![0u8; 4096]).collect();
                let mut at = 0u64;
                while at < len {
                    let mut reads: Vec<(u64, &mut [u8])> = buffers
                        .iter_mut()
                        .enumerate()
                        .map(|(i, b)| (at + i as u64 * (batch / 16), b.as_mut_slice()))
                        .collect();
                    io.read_vectored(&mut reads).await.unwrap();
                    for (pos, buf) in reads.iter() {
                        let p = *pos as usize;
                        assert_eq!(&buf[..], &data[p..p + buf.len()]);
                    }
                    at += batch;
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let after = store.counts();
        (after.0 - before.0, after.1 - before.1, elapsed)
    }

    /// One sequential reader, 64 KiB at a time, so read-ahead runs ahead of it.
    async fn sequential_with_readahead(latency_ms: u64) -> (usize, usize, f64) {
        let store = Arc::new(SlowStore::new(latency_ms));
        let (io, data) = cold_file(&store, 8).await;
        let before = store.counts();
        let started = Instant::now();
        let mut buffer = vec![0u8; 64 * 1024];
        let mut position = 0u64;
        while position < data.len() as u64 {
            let at = position as usize;
            io.read_data_into_buffer(&mut position, &mut buffer)
                .await
                .unwrap();
            assert_eq!(&buffer[..], &data[at..at + buffer.len()]);
        }
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let after = store.counts();
        (after.0 - before.0, after.1 - before.1, elapsed)
    }

    /// Hot cache, no latency: the per-page lookup cost under `clients` readers.
    async fn hot_vectored(clients: usize, rounds: usize) -> f64 {
        let store = Arc::new(SlowStore::new(0));
        let (io, data) = cold_file(&store, 8).await;
        // Warm.
        let mut warm = vec![0u8; data.len()];
        io.read_vectored(&mut [(0u64, warm.as_mut_slice())]).await.unwrap();
        let started = Instant::now();
        let mut tasks = Vec::new();
        for c in 0..clients {
            let io = io.clone();
            let len = data.len() as u64;
            tasks.push(tokio::spawn(async move {
                let mut buffers: Vec<Vec<u8>> = (0..64).map(|_| vec![0u8; 128]).collect();
                let mut seed = c as u64 * 7919 + 1;
                for _ in 0..rounds {
                    let mut reads: Vec<(u64, &mut [u8])> = buffers
                        .iter_mut()
                        .map(|b| {
                            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                            ((seed >> 20) % (len - 128), b.as_mut_slice())
                        })
                        .collect();
                    io.read_vectored(&mut reads).await.unwrap();
                }
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        started.elapsed().as_secs_f64() * 1000.0
    }

    /// Hot cache, no latency, shaped like the engine's row fetch: per row, a
    /// vectored read of its columns, tried on the synchronous path first and
    /// on the async one only if that declines (`row_pointer.rs`).
    async fn hot_rows(clients: usize, rows: usize) -> f64 {
        let store = Arc::new(SlowStore::new(0));
        let (io, data) = cold_file(&store, 8).await;
        let mut warm = vec![0u8; data.len()];
        io.read_vectored(&mut [(0u64, warm.as_mut_slice())]).await.unwrap();
        let started = Instant::now();
        let mut tasks = Vec::new();
        for c in 0..clients {
            let io = io.clone();
            let len = data.len() as u64;
            tasks.push(tokio::spawn(async move {
                let mut columns: Vec<Vec<u8>> = [8usize, 4, 12, 24]
                    .into_iter()
                    .map(|n| vec![0u8; n])
                    .collect();
                let mut seed = c as u64 * 7919 + 1;
                let mut declined = 0usize;
                for _ in 0..rows {
                    seed = seed
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    let row = (seed >> 20) % (len - 64);
                    let mut at = row;
                    let mut reads: Vec<(u64, &mut [u8])> = columns
                        .iter_mut()
                        .map(|b| {
                            let read = (at, b.as_mut_slice());
                            at += read.1.len() as u64;
                            read
                        })
                        .collect();
                    if !io.try_read_vectored_sync(&mut reads) {
                        declined += 1;
                        io.read_vectored(&mut reads).await.unwrap();
                    }
                }
                declined
            }));
        }
        for t in tasks {
            std::hint::black_box(t.await.unwrap());
        }
        started.elapsed().as_secs_f64() * 1000.0
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore]
    async fn kv_bench_hot() {
        for round in 0..5 {
            for clients in [1usize, 8] {
                let ms = hot_rows(clients, 200_000).await;
                println!(
                    "round {round} hot_rows clients={clients} 200000 rows each: {ms:.1}ms"
                );
                let ms = hot_vectored(clients, 2000).await;
                println!(
                    "round {round} hot_vectored clients={clients} 2000x64 reads each: {ms:.1}ms"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore]
    async fn kv_bench_fetches() {
        for round in 0..3 {
            for clients in [1usize, 8] {
                let (gets, ranges, ms) = concurrent_vectored(clients, 20).await;
                println!(
                    "round {round} concurrent_vectored clients={clients} latency=20ms: gets={gets} ranges={ranges} wall={ms:.1}ms (8 chunks of {} MiB)",
                    KvConfig::new("x").chunk_size / MIB
                );
            }
            let (gets, ranges, ms) = sequential_with_readahead(20).await;
            println!(
                "round {round} sequential_readahead latency=20ms: gets={gets} ranges={ranges} wall={ms:.1}ms"
            );
            for clients in [1usize, 8] {
                let ms = hot_vectored(clients, 2000).await;
                println!(
                    "round {round} hot_vectored clients={clients} 2000x64 reads each: {ms:.1}ms"
                );
            }
        }
    }
}

/// A read whose base bytes were fetched from the generation `swap_temp`
/// replaced, and whose overlays are applied after the swap, must read the new
/// file — not the new file's writes patched over the old file's bytes, and not
/// the old file's length.
///
/// The swap stored the generation and lengths, then swapped the overlays, all
/// outside anything the read path checked, so the straddling read below came
/// back as the replaced rows in the new file's hole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_that_straddles_a_swap_reads_only_the_new_file() {
    let store = Arc::new(LateAnswerStore {
        inner: MemoryKvStore::new(),
        hold_next: std::sync::atomic::AtomicBool::new(false),
        held: tokio::sync::Notify::new(),
        started: tokio::sync::Notify::new(),
    });
    let mut config = tiny_config();
    config.readahead_chunks = 0;
    {
        let family = KvFamily::open(store.clone() as Arc<dyn KvStore>, config.clone())
            .await
            .unwrap();
        let io = KvStorageProvider::open(family, "t.db").await.unwrap();
        let a = io.begin_txn();
        io.append_data_txn(a, &[b'o'; 4096]).await;
        io.commit_durable(a).await.unwrap();
        io.compact_now().await.unwrap();
    }
    // Cold, so the read below has to ask the store for the old base.
    let family = KvFamily::open(store.clone() as Arc<dyn KvStore>, config)
        .await
        .unwrap();
    let io = KvStorageProvider::open(family, "t.db").await.unwrap();

    // The replacement: 300 bytes, of which 0..100 is a hole and 100..300 is
    // written. Nothing of it is in a base chunk.
    let mut temp = io.create_temp().await;
    temp.write_data(100, &[b'n'; 200]).await;
    let mut expected = vec![0u8; 300];
    expected[100..].fill(b'n');

    store.hold_next.store(true, Ordering::SeqCst);
    let started = store.started.notified();
    let reader = {
        let io = io.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 250];
            let mut position = 0u64;
            io.read_data_into_buffer(&mut position, &mut buf).await.map(|_| buf)
        })
    };
    // The reader has its old-generation bytes in hand and is held there.
    started.await;
    io.swap_temp(&mut temp).await;
    store.held.notify_one();

    let got = reader.await.unwrap().unwrap();
    let old_bytes = got[..100].iter().filter(|&&b| b == b'o').count();
    assert_eq!(
        old_bytes, 0,
        "the read that straddled the swap returned {old_bytes} bytes of the replaced file"
    );
    assert_eq!(&got[..], &expected[..250]);
    assert_eq!(read_all(&io).await, expected, "a read after the swap");
}

/// `swap_temp` while a compaction of the live file is between its freeze and
/// its publish must leave the new file live — in this process and after a
/// restart.
///
/// The compaction froze the old generation, the swap published the new one
/// and deleted the old chunks, and then the compaction published a META naming
/// the old generation (and its length) over it: the table read correctly
/// until the restart and then came back as the replaced bytes over deleted
/// chunks. In process, the compaction found the swapped-in state had no
/// frozen overlay and panicked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_swap_during_a_compaction_leaves_the_new_file_live() {
    let store = Arc::new(HoldPutStore {
        inner: MemoryKvStore::new(),
        pattern: parking_lot::Mutex::new(None),
        held: tokio::sync::Notify::new(),
        started: tokio::sync::Notify::new(),
    });
    {
        let family = open_family(store.clone() as Arc<dyn KvStore>).await;
        let io = open(&family, "rows.db").await;
        let a = io.begin_txn();
        io.append_data_txn(a, &[b'o'; 6000]).await;
        io.commit_durable(a).await.unwrap();

        // Hold the compaction at its first chunk put: frozen, not published.
        *store.pattern.lock() = Some("/g");
        let started = store.started.notified();
        let compaction = {
            let io = io.clone();
            tokio::spawn(async move { io.compact_now().await })
        };
        started.await;

        let mut temp = io.create_temp().await;
        temp.append_data(&[b'n'; 1000], true).await;
        let swap = {
            let io = io.clone();
            tokio::spawn(async move { io.swap_temp(&mut temp).await })
        };
        // Long enough for an unguarded swap to finish entirely.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        store.held.notify_one();
        compaction
            .await
            .expect("the compaction panicked")
            .unwrap();
        swap.await.unwrap();

        assert_eq!(io.get_len().await, 1000, "length in process");
        assert_eq!(read_all(&io).await, vec![b'n'; 1000], "in process");
    }
    let family = open_family(store as Arc<dyn KvStore>).await;
    let io = open(&family, "rows.db").await;
    assert_eq!(io.get_len().await, 1000, "length after a restart");
    assert_eq!(read_all(&io).await, vec![b'n'; 1000], "after a restart");
}

// ---------------------------------------------------------------------------
// Durability across compaction, swap and failure
// ---------------------------------------------------------------------------

fn hold_put_store() -> Arc<HoldPutStore> {
    Arc::new(HoldPutStore {
        inner: MemoryKvStore::new(),
        pattern: parking_lot::Mutex::new(None),
        held: tokio::sync::Notify::new(),
        started: tokio::sync::Notify::new(),
    })
}

/// A transaction is declared committed in the file it was committed on, and
/// its other half's records live in another file's delta. That file
/// compacting — twice, so the declaration has to survive being carried — or
/// being replaced by a swap must not take the declaration with it: the rows
/// half of an INSERT used to vanish on the restart after the pointers file
/// compacted, while the pointer survived.
#[tokio::test]
async fn a_declaration_another_file_needs_survives_compaction_and_swap() {
    for how in ["compact once", "compact twice", "swap"] {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
        {
            let family = open_family(store.clone()).await;
            let rows = open(&family, "t.db").await;
            let pointers = open(&family, "t_pointers.db").await;
            let txn = rows.begin_txn();
            rows.append_data_txn(txn, b"row bytes").await;
            pointers.append_data_txn(txn, b"pointer").await;
            pointers.commit_durable(txn).await.unwrap();
            match how {
                "compact once" => pointers.compact_now().await.unwrap(),
                "compact twice" => {
                    pointers.compact_now().await.unwrap();
                    pointers.write_data(0, b"P").await;
                    pointers.compact_now().await.unwrap();
                }
                _ => {
                    let mut temp = pointers.create_temp().await;
                    temp.append_data(b"pointer", true).await;
                    pointers.swap_temp(&mut temp).await;
                }
            }
        }
        let family = open_family(store).await;
        assert_eq!(
            read_all(&open(&family, "t.db").await).await,
            b"row bytes",
            "{how}: the rows half of a committed transaction is gone after a restart"
        );
    }
}

/// Once the other file has folded its half, the declaration is no longer
/// carried: the ledger forgets a delta when it is retired.
#[tokio::test]
async fn a_carried_declaration_is_dropped_once_nothing_needs_it() {
    let store = Arc::new(MemoryKvStore::new());
    let family = open_family(store.clone() as Arc<dyn KvStore>).await;
    let rows = open(&family, "t.db").await;
    let pointers = open(&family, "t_pointers.db").await;
    let txn = rows.begin_txn();
    rows.append_data_txn(txn, &[b'r'; 600]).await;
    pointers.append_data_txn(txn, &[b'p'; 600]).await;
    pointers.commit_durable(txn).await.unwrap();

    pointers.compact_now().await.unwrap();
    rows.compact_now().await.unwrap();
    pointers.write_data(0, b"P").await;
    pointers.compact_now().await.unwrap();

    let deltas: Vec<String> = store
        .list("t/t_pointers.db/d/")
        .await
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect();
    for key in &deltas {
        let bytes = store.get(key).await.unwrap().unwrap();
        let batch = super::layout::DeltaBatch::decode(&bytes).unwrap().unwrap();
        assert!(
            !batch.committed.contains(&txn),
            "{key} still carries a declaration no record depends on"
        );
    }
}

/// A transaction open at the freeze keeps its bytes — and the committed bytes
/// above it — in the live overlay, so the deltas holding them may not be
/// retired by that pass, even if the transaction commits before it publishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_during_a_compaction_does_not_let_it_retire_unfolded_deltas() {
    let store = hold_put_store();
    let mut expected = vec![b'a'; 10_000];
    expected[9_000..9_010].copy_from_slice(b"bbbbbbbbbb");
    {
        let family = open_family(store.clone() as Arc<dyn KvStore>).await;
        let io = open(&family, "t.db").await;
        let a = io.begin_txn();
        io.append_data_txn(a, &[b'a'; 10_000]).await;
        io.commit_durable(a).await.unwrap();
        let b = io.begin_txn();
        io.write_data_txn(b, 9_000, b"bbbbbbbbbb").await;

        *store.pattern.lock() = Some("/g");
        let started = store.started.notified();
        let compaction = {
            let io = io.clone();
            tokio::spawn(async move { io.compact_now().await })
        };
        started.await;
        io.commit_durable(b).await.unwrap();
        store.held.notify_one();
        compaction.await.unwrap().unwrap();
        assert_eq!(read_all(&io).await, expected, "in process");
    }
    let family = open_family(store as Arc<dyn KvStore>).await;
    let io = open(&family, "t.db").await;
    assert_eq!(io.get_len().await, 10_000, "length after a restart");
    assert_eq!(read_all(&io).await, expected, "after a restart");
}

/// The startup scan's "next sequence" is not a compaction watermark. It used
/// to be folded into one, so the first compaction after a restart that did not
/// retire anything wrote a META claiming every existing delta was folded, and
/// the restart after it skipped them.
#[tokio::test]
async fn a_compaction_after_a_restart_does_not_claim_unfolded_deltas() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let family = open_family(store.clone()).await;
        let io = open(&family, "t.db").await;
        let a = io.begin_txn();
        io.append_data_txn(a, &[b'a'; 100]).await;
        io.commit_durable(a).await.unwrap();
    }
    {
        let family = open_family(store.clone()).await;
        let io = open(&family, "t.db").await;
        // Open across the compaction, so nothing in its chunk folds and
        // nothing is retired.
        let b = io.begin_txn();
        io.write_data_txn(b, 10, b"bbbbb").await;
        io.compact_now().await.unwrap();
    }
    let family = open_family(store).await;
    let io = open(&family, "t.db").await;
    assert_eq!(io.get_len().await, 100);
    assert_eq!(read_all(&io).await, vec![b'a'; 100]);
}

/// A scratch file abandoned with deltas in the store — the process died, or
/// the handle was simply dropped — must not lend them to the next scratch file
/// of the same table. They shared a generation, so adopting the second one
/// replayed the first one's bytes into it on the next restart.
#[tokio::test]
async fn an_abandoned_scratch_file_does_not_replay_into_the_next_one() {
    for restart_between in [true, false] {
        let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
        {
            let family = open_family(store.clone()).await;
            let io = open(&family, "t.db").await;
            io.append_data(&[b'o'; 100], true).await;
            io.compact_now().await.unwrap();
            let abandoned = io.create_temp().await;
            abandoned.append_data(&[b'x'; 300], true).await;
            if !restart_between {
                let mut next = io.create_temp().await;
                next.append_data(&[b'n'; 50], true).await;
                io.swap_temp(&mut next).await;
            }
        }
        if restart_between {
            let family = open_family(store.clone()).await;
            let io = open(&family, "t.db").await;
            assert_eq!(read_all(&io).await, vec![b'o'; 100]);
            let mut next = io.create_temp().await;
            next.append_data(&[b'n'; 50], true).await;
            io.swap_temp(&mut next).await;
            assert_eq!(read_all(&io).await, vec![b'n'; 50]);
        }
        let family = open_family(store).await;
        let io = open(&family, "t.db").await;
        assert_eq!(io.get_len().await, 50, "restart_between={restart_between}");
        assert_eq!(read_all(&io).await, vec![b'n'; 50], "restart_between={restart_between}");
    }
}

/// A commit that returned an error stays uncommitted: the next flush does not
/// declare it, and no compaction folds its bytes into a base chunk. Both used
/// to make it durable after the caller had been told it was not.
#[tokio::test]
async fn a_failed_commit_is_not_made_durable_later() {
    let store = Arc::new(FailAfterStore::new(1));
    let handle: Arc<dyn KvStore> = store.clone();
    {
        let family = open_family(handle.clone()).await;
        let rows = open(&family, "t.db").await;
        let pointers = open(&family, "t_pointers.db").await;
        let txn = rows.begin_txn();
        rows.append_data_txn(txn, &[b'r'; 600]).await;
        pointers.append_data_txn(txn, &[b'p'; 600]).await;
        // The rows object lands; the declaring pointers object fails.
        store.arm();
        assert!(pointers.commit_durable(txn).await.is_err());
        // What the background service does next.
        pointers.flush().await.unwrap();
        rows.flush().await.unwrap();
        pointers.compact_now().await.unwrap();
        rows.compact_now().await.unwrap();
    }
    let family = open_family(handle).await;
    assert_eq!(
        read_all(&open(&family, "t_pointers.db").await).await,
        b"",
        "a commit that returned an error became durable"
    );
    assert_eq!(read_all(&open(&family, "t.db").await).await, b"");
}

/// A truncate while a compaction is between its freeze and its publish: the
/// publish must not restore the base length it computed before the cut, or
/// the truncated tail reads back as base bytes once the file grows past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_truncate_during_a_compaction_leaves_zeros_in_the_hole() {
    let store = hold_put_store();
    {
        let family = open_family(store.clone() as Arc<dyn KvStore>).await;
        let io = open(&family, "t.db").await;
        let a = io.begin_txn();
        io.append_data_txn(a, &[b'a'; 10_000]).await;
        io.commit_durable(a).await.unwrap();

        *store.pattern.lock() = Some("/g");
        let started = store.started.notified();
        let compaction = {
            let io = io.clone();
            tokio::spawn(async move { io.compact_now().await })
        };
        started.await;
        io.truncate(100).await.unwrap();
        store.held.notify_one();
        compaction.await.unwrap().unwrap();
        assert_eq!(io.get_len().await, 100);

        io.write_data(2_000, b"Q").await;
        let all = read_all(&io).await;
        let stale = all[100..2_000].iter().filter(|&&b| b == b'a').count();
        assert_eq!(stale, 0, "{stale} bytes of the truncated tail came back in process");
        assert_eq!(&all[..100], &[b'a'; 100][..]);
        assert_eq!(all[2_000], b'Q');
    }
    let family = open_family(store as Arc<dyn KvStore>).await;
    let io = open(&family, "t.db").await;
    assert_eq!(io.get_len().await, 2_001);
    let all = read_all(&io).await;
    let stale = all[100..2_000].iter().filter(|&&b| b == b'a').count();
    assert_eq!(stale, 0, "{stale} bytes of the truncated tail came back after a restart");
}

// ---------------------------------------------------------------------------
// Rolling back a failed commit
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum PutFault {
    /// Store the object, then report failure: the answer was lost.
    LandThenFail,
    /// Report failure without storing it, keeping the body so the test can
    /// land it later: a request the store finishes after the client gave up.
    FailAndKeep,
}

/// Applies `fault` to the first put whose key contains `pattern`, once armed.
struct PutFaultStore {
    inner: MemoryKvStore,
    armed: parking_lot::Mutex<Option<(&'static str, PutFault)>>,
    kept: parking_lot::Mutex<Option<(String, Vec<u8>)>>,
}

impl PutFaultStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryKvStore::new(),
            armed: parking_lot::Mutex::new(None),
            kept: parking_lot::Mutex::new(None),
        })
    }
    fn arm(&self, pattern: &'static str, fault: PutFault) {
        *self.armed.lock() = Some((pattern, fault));
    }
    /// The put that "failed" arrives after all.
    async fn land_late(&self) {
        let (key, body) = self.kept.lock().take().expect("a put was kept");
        self.inner.put(&key, body).await.unwrap();
    }
}

impl KvStore for PutFaultStore {
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
        Box::pin(async move {
            let fault = {
                let mut armed = self.armed.lock();
                match *armed {
                    Some((pattern, fault)) if key.contains(pattern) => {
                        *armed = None;
                        Some(fault)
                    }
                    _ => None,
                }
            };
            match fault {
                None => self.inner.put(key, body).await,
                Some(PutFault::LandThenFail) => {
                    self.inner.put(key, body).await?;
                    Err(std::io::Error::other("connection reset after the request"))
                }
                Some(PutFault::FailAndKeep) => {
                    *self.kept.lock() = Some((key.to_string(), body));
                    Err(std::io::Error::other("request timed out"))
                }
            }
        })
    }
    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, std::io::Result<Vec<ObjectInfo>>> {
        self.inner.list(prefix)
    }
    fn describe(&self) -> String {
        "put-fault".to_string()
    }
}

async fn delta_keys(store: &dyn KvStore, file: &str) -> Vec<String> {
    store
        .list(&format!("t/{file}/d/"))
        .await
        .unwrap()
        .into_iter()
        .map(|o| o.key)
        .collect()
}

/// A commit whose declaring put failed, and is not in the store, is rolled
/// back and forgotten: its bytes leave the overlay, and it no longer holds
/// off retirement — the deltas written before it are retired by the next
/// compaction rather than kept for the life of the process.
#[tokio::test]
async fn a_failed_commit_is_rolled_back_and_stops_holding_the_file() {
    let store = PutFaultStore::new();
    let handle: Arc<dyn KvStore> = store.clone();
    {
        let family = open_family(handle.clone()).await;
        let rows = open(&family, "t.db").await;
        let pointers = open(&family, "t_pointers.db").await;
        let settled = rows.begin_txn();
        rows.append_data_txn(settled, &[b's'; 100]).await;
        pointers.append_data_txn(settled, &[b'S'; 100]).await;
        pointers.commit_durable(settled).await.unwrap();

        let txn = rows.begin_txn();
        rows.append_data_txn(txn, &[b'r'; 600]).await;
        pointers.write_data_txn(txn, 10, b"XXXX").await;
        pointers.append_data_txn(txn, &[b'p'; 600]).await;
        store.arm("t_pointers.db/d/", PutFault::FailAndKeep);
        assert!(pointers.commit_durable(txn).await.is_err());
        // Taken back out of both files, the in-place write included.
        let ptrs = read_all(&pointers).await;
        assert_eq!(&ptrs[..100], &[b'S'; 100][..], "the in-place write was not undone");
        assert!(!ptrs.contains(&b'p'), "the rolled-back append is still readable");
        assert!(!read_all(&rows).await.contains(&b'r'));

        let before = delta_keys(&*handle, "t_pointers.db").await;
        pointers.write_data(0, b"Q").await;
        pointers.compact_now().await.unwrap();
        let after = delta_keys(&*handle, "t_pointers.db").await;
        let kept: Vec<&String> = before.iter().filter(|k| after.contains(k)).collect();
        assert!(kept.is_empty(), "compaction still kept {kept:?} for a rolled-back transaction");
    }
    let family = open_family(handle).await;
    let pointers = read_all(&open(&family, "t_pointers.db").await).await;
    assert_eq!(pointers[0], b'Q');
    assert_eq!(&pointers[1..100], &[b'S'; 99][..]);
    assert!(!pointers.contains(&b'p'));
    assert_eq!(read_all(&open(&family, "t.db").await).await, vec![b's'; 100]);
}

/// The put reported failure but the object is in the store: the transaction
/// committed, and saying otherwise would lose it on the next restart.
#[tokio::test]
async fn a_commit_whose_put_landed_despite_an_error_is_committed() {
    let store = PutFaultStore::new();
    let handle: Arc<dyn KvStore> = store.clone();
    {
        let family = open_family(handle.clone()).await;
        let rows = open(&family, "t.db").await;
        let pointers = open(&family, "t_pointers.db").await;
        let txn = rows.begin_txn();
        rows.append_data_txn(txn, b"row bytes").await;
        pointers.append_data_txn(txn, b"pointer").await;
        store.arm("t_pointers.db/d/", PutFault::LandThenFail);
        pointers
            .commit_durable(txn)
            .await
            .expect("the declaration is in the store, so the commit happened");
        assert_eq!(read_all(&pointers).await, b"pointer");
    }
    let family = open_family(handle).await;
    assert_eq!(read_all(&open(&family, "t_pointers.db").await).await, b"pointer");
    assert_eq!(read_all(&open(&family, "t.db").await).await, b"row bytes");
}

/// A declaring put that failed and then arrives after the rollback must not
/// commit the transaction on the next restart: the caller was told it failed,
/// and has reused its space since.
#[tokio::test]
async fn a_rolled_back_commit_stays_rolled_back_when_its_put_lands_late() {
    let store = PutFaultStore::new();
    let handle: Arc<dyn KvStore> = store.clone();
    {
        let family = open_family(handle.clone()).await;
        let rows = open(&family, "t.db").await;
        let pointers = open(&family, "t_pointers.db").await;
        let txn = rows.begin_txn();
        rows.append_data_txn(txn, b"row bytes").await;
        pointers.append_data_txn(txn, b"pointer").await;
        store.arm("t_pointers.db/d/", PutFault::FailAndKeep);
        assert!(pointers.commit_durable(txn).await.is_err());
        store.land_late().await;
    }
    let family = open_family(handle).await;
    let pointers = read_all(&open(&family, "t_pointers.db").await).await;
    assert!(
        !pointers.windows(7).any(|w| w == b"pointer"),
        "a transaction reported as failed came back after a restart"
    );
    assert!(!read_all(&open(&family, "t.db").await).await.windows(3).any(|w| w == b"row"));
}

/// `abort_txn` rolls back a transaction that was never committed, in every
/// file it wrote, and lets compaction retire what it spilled.
#[tokio::test]
async fn abort_txn_rolls_back_an_uncommitted_transaction() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let family = open_family(store.clone()).await;
    let rows = open(&family, "t.db").await;
    let pointers = open(&family, "t_pointers.db").await;
    let settled = rows.begin_txn();
    pointers.append_data_txn(settled, &[b'S'; 50]).await;
    pointers.commit_durable(settled).await.unwrap();

    let txn = rows.begin_txn();
    rows.append_data_txn(txn, &[b'r'; 600]).await;
    pointers.write_data_txn(txn, 0, b"XX").await;
    pointers.flush().await.unwrap(); // spilled, undeclared
    rows.abort_txn(txn).await;
    assert_eq!(&read_all(&pointers).await[..50], &[b'S'; 50][..]);
    assert!(!read_all(&rows).await.contains(&b'r'));

    let before = delta_keys(&*store, "t_pointers.db").await;
    pointers.write_data(49, b"Q").await;
    pointers.compact_now().await.unwrap();
    let after = delta_keys(&*store, "t_pointers.db").await;
    assert!(before.iter().all(|k| !after.contains(k)), "{before:?} survived in {after:?}");
}

// ---- export (a backup's copy) ----------------------------------------------

async fn exported(store: &Arc<dyn KvStore>, dir: &std::path::Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let files = super::export::export(store.clone(), tiny_config(), |_| true, dir).await.unwrap();
    files
        .into_iter()
        .map(|f| {
            let bytes = std::fs::read(&f.path).unwrap();
            assert_eq!(bytes.len() as u64, f.bytes, "{}", f.name);
            (f.name, bytes)
        })
        .collect()
}

/// The copy is what a restart would recover: every committed byte, nothing
/// of a transaction still open, and the untransacted writes the background
/// service had not yet stored.
#[tokio::test]
async fn an_export_holds_the_committed_bytes_and_no_open_transaction() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let family = open_family(store.clone()).await;
    super::export::register_live(&family);
    let rows = open(&family, "t.db").await;
    let pointers = open(&family, "t_pointers.db").await;
    let sequence = open(&family, "t_seq.db").await;

    let txn = rows.begin_txn();
    rows.append_data_txn(txn, &vec![b'r'; 9_000]).await;
    pointers.append_data_txn(txn, &vec![b'p'; 300]).await;
    pointers.commit_durable(txn).await.unwrap();

    let open_txn = rows.begin_txn();
    rows.append_data_txn(open_txn, b"not committed").await;
    pointers.append_data_txn(open_txn, b"nor this").await;
    // Untransacted, and only in the overlay until a flush.
    sequence.append_data(b"42", false).await;

    let dir = tempfile::tempdir().unwrap();
    let copy = exported(&store, dir.path()).await;
    assert_eq!(copy["t.db"], vec![b'r'; 9_000]);
    assert_eq!(copy["t_pointers.db"], vec![b'p'; 300]);
    assert_eq!(copy["t_seq.db"], b"42".to_vec());

    // The live family is untouched by it: the open transaction still commits.
    pointers.commit_durable(open_txn).await.unwrap();
    assert_eq!(read_all(&rows).await.len(), 9_000 + 13);
}

/// Compaction rewrites chunks the copy is reading; a pause holds it off until
/// the pause ends, and then it runs.
#[tokio::test]
async fn a_compaction_asked_for_during_a_pause_waits_for_it() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let family = open_family(store.clone()).await;
    let io = Arc::new(open(&family, "t.db").await);
    let txn = io.begin_txn();
    io.append_data_txn(txn, &vec![b'x'; 20_000]).await;
    io.commit_durable(txn).await.unwrap();

    let metas = || async {
        store.list("t/").await.unwrap().iter().filter(|o| o.key.ends_with("/META")).count()
    };
    let before = metas().await;
    let paused = family.pause_maintenance().await;
    let compaction = tokio::spawn({
        let io = io.clone();
        async move { io.compact_now().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!compaction.is_finished(), "a compaction ran during a pause");
    assert_eq!(metas().await, before);
    drop(paused);
    compaction.await.unwrap().unwrap();
    assert_eq!(read_all(&io).await, vec![b'x'; 20_000]);
}

/// A listing under `hold_commits` sees every commit whole: one arriving
/// meanwhile waits, and lands after.
#[tokio::test]
async fn a_commit_waits_while_commits_are_held() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let family = open_family(store.clone()).await;
    let io = Arc::new(open(&family, "t.db").await);
    let txn = io.begin_txn();
    io.append_data_txn(txn, b"later").await;

    let held = family.hold_commits().await;
    let commit = tokio::spawn({
        let io = io.clone();
        async move { io.commit_durable(txn).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!commit.is_finished(), "a commit landed while commits were held");
    drop(held);
    commit.await.unwrap().unwrap();

    drop(io);
    let reopened = open(&open_family(store).await, "t.db").await;
    assert_eq!(read_all(&reopened).await, b"later".to_vec());
}

/// With no server holding the store, the copy is read from the objects alone.
#[tokio::test]
async fn an_export_reads_a_store_no_family_has_open() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let family = open_family(store.clone()).await;
        let io = open(&family, "t.db").await;
        let txn = io.begin_txn();
        io.append_data_txn(txn, b"settled").await;
        io.commit_durable(txn).await.unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let copy = exported(&store, dir.path()).await;
    assert_eq!(copy["t.db"], b"settled".to_vec());
    // `keep` decides what is copied.
    let only = super::export::export(store.clone(), tiny_config(), |name| name != "t.db", dir.path()).await.unwrap();
    assert!(only.is_empty());
}

/// Holds every put whose key holds `pattern` once armed, until released.
struct HeldPutStore {
    inner: MemoryKvStore,
    pattern: &'static str,
    armed: std::sync::atomic::AtomicBool,
    release: tokio::sync::Semaphore,
}

impl HeldPutStore {
    fn holding(pattern: &'static str) -> Arc<Self> {
        Arc::new(Self {
            inner: MemoryKvStore::new(),
            pattern,
            armed: std::sync::atomic::AtomicBool::new(false),
            release: tokio::sync::Semaphore::new(0),
        })
    }
}

impl KvStore for HeldPutStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.inner.get(key)
    }
    fn get_range<'a>(&'a self, key: &'a str, offset: u64, len: usize) -> BoxFuture<'a, std::io::Result<Option<Vec<u8>>>> {
        self.inner.get_range(key, offset, len)
    }
    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, std::io::Result<()>> {
        Box::pin(async move {
            if self.armed.load(Ordering::Acquire) && key.contains(self.pattern) {
                self.release.acquire().await.unwrap().forget();
            }
            self.inner.put(key, body).await
        })
    }
    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, std::io::Result<()>> {
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, std::io::Result<Vec<ObjectInfo>>> {
        self.inner.list(prefix)
    }
    fn concurrency(&self) -> usize {
        4
    }
    fn describe(&self) -> String {
        "gated".to_string()
    }
}

/// A commit landing while another file's flush is putting the transaction's
/// records waits for that put. It used to see nothing pending there, declare
/// the transaction on its own file, and return: the declaration was stored
/// while the other half was in flight — torn after a crash in that window,
/// and torn in any copy listed then.
#[tokio::test]
async fn a_commit_waits_for_a_flush_already_putting_its_records() {
    let gated = HeldPutStore::holding("/a.db/d/");
    let store: Arc<dyn KvStore> = gated.clone();
    let family = open_family(store.clone()).await;
    let a = Arc::new(open(&family, "a.db").await);
    let b = Arc::new(open(&family, "b.db").await);

    let txn = a.begin_txn();
    a.append_data_txn(txn, b"row").await;
    b.append_data_txn(txn, b"ptr").await;

    // What the background service does: puts what is pending.
    gated.armed.store(true, Ordering::Release);
    let flush = tokio::spawn({
        let a = a.clone();
        async move { a.flush().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let commit = tokio::spawn({
        let b = b.clone();
        async move { b.commit_durable(txn).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!commit.is_finished(), "the commit declared while the other half was being put");

    gated.armed.store(false, Ordering::Release);
    gated.release.add_permits(16);
    flush.await.unwrap().unwrap();
    commit.await.unwrap().unwrap();

    drop((a, b));
    let family = open_family(store).await;
    assert_eq!(read_all(&open(&family, "a.db").await).await, b"row".to_vec());
    assert_eq!(read_all(&open(&family, "b.db").await).await, b"ptr".to_vec());
}

/// The gates belong to the prefix, not to a family: one opened after a copy
/// paused the prefix is paused too, and compacts nothing under the reader.
#[tokio::test]
async fn a_family_opened_during_a_pause_is_paused_too() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let first = open_family(store.clone()).await;
    let paused = first.pause_maintenance().await;

    let late = open_family(store.clone()).await;
    let io = Arc::new(open(&late, "t.db").await);
    let txn = io.begin_txn();
    io.append_data_txn(txn, &vec![b'z'; 20_000]).await;
    io.commit_durable(txn).await.unwrap();
    let compaction = tokio::spawn({
        let io = io.clone();
        async move { io.compact_now().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(!compaction.is_finished(), "a family opened later compacted during the pause");
    drop(paused);
    compaction.await.unwrap().unwrap();
}

/// A file dropped while a flush is putting its records leaves nothing in the
/// store. The sweep used to list and delete while the put was in flight, and
/// the put then landed: an orphan delta that a table later made under the
/// same name replayed after a restart — and an untransacted record, a schema
/// header or the id sequence, applies with no declaration at all.
#[tokio::test]
async fn a_put_in_flight_when_a_file_is_dropped_leaves_nothing_behind() {
    let held = HeldPutStore::holding("/a.db/d/");
    let store: Arc<dyn KvStore> = held.clone();
    let family = open_family(store.clone()).await;
    let a = Arc::new(open(&family, "a.db").await);
    a.append_data(b"old header", false).await;

    held.armed.store(true, Ordering::Release);
    let flush = tokio::spawn({
        let a = a.clone();
        async move { a.flush().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    a.drop_io();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    held.armed.store(false, Ordering::Release);
    held.release.add_permits(16);
    flush.await.unwrap().unwrap();

    // Reopening the name waits for the sweep.
    let again = open(&family, "a.db").await;
    assert!(read_all(&again).await.is_empty(), "the dropped file's bytes came back");
    assert!(store.list("t/a.db/").await.unwrap().is_empty(), "an object outlived the drop");
    drop(again);
    let restarted = open(&open_family(store).await, "a.db").await;
    assert!(read_all(&restarted).await.is_empty(), "a restart replayed the dropped file");
}

/// A compaction running when its file is dropped finishes before the sweep,
/// and is swept with the rest. It used to run alongside it and publish its
/// chunks and META after it: objects under the name that a table made since
/// read as its own, or a restart replayed.
#[tokio::test]
async fn a_compaction_in_flight_when_a_file_is_dropped_leaves_nothing_behind() {
    let held = HeldPutStore::holding("/a.db/g");
    let store: Arc<dyn KvStore> = held.clone();
    let family = open_family(store.clone()).await;
    let a = Arc::new(open(&family, "a.db").await);
    let txn = a.begin_txn();
    a.append_data_txn(txn, &vec![b'c'; 20_000]).await;
    a.commit_durable(txn).await.unwrap();

    held.armed.store(true, Ordering::Release);
    let compaction = tokio::spawn({
        let a = a.clone();
        async move { a.compact_now().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    a.drop_io();
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    held.armed.store(false, Ordering::Release);
    held.release.add_permits(64);
    compaction.await.unwrap().unwrap();
    // And one asked for on the stale handle afterwards writes nothing.
    a.compact_now().await.unwrap();

    let again = open(&family, "a.db").await;
    assert!(read_all(&again).await.is_empty(), "the dropped file's bytes came back");
    assert!(store.list("t/a.db/").await.unwrap().is_empty(), "an object outlived the drop");
}

/// A transaction that wrote two files and declared its commit in the one
/// then dropped keeps its records in the other. The declaration used to be
/// swept with the dropped file, and the other file's half was uncommitted at
/// the next restart: a table losing rows to another table's DROP.
#[tokio::test]
async fn dropping_the_declaring_file_keeps_the_other_files_half_committed() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    let family = open_family(store.clone()).await;
    let declaring = open(&family, "a.db").await;
    let other = open(&family, "b.db").await;
    let txn = declaring.begin_txn();
    other.append_data_txn(txn, b"kept").await;
    declaring.append_data_txn(txn, b"dropped").await;
    declaring.commit_durable(txn).await.unwrap();

    declaring.drop_io();
    // Reopening the name waits for the sweep.
    drop(open(&family, "a.db").await);
    drop(other);

    let restarted = open_family(store).await;
    assert_eq!(read_all(&open(&restarted, "b.db").await).await, b"kept".to_vec());
}

/// DROP TABLE drops a table's files one after the other, so the first other
/// holder of a transaction is often the table's own other file, still being
/// swept. The declaration goes to a holder that stays — here `y.db` — not to
/// none.
#[tokio::test]
async fn a_table_dropped_file_by_file_still_keeps_another_tables_half() {
    let held = HeldPutStore::holding("/x.db/d/");
    let store: Arc<dyn KvStore> = held.clone();
    let family = open_family(store.clone()).await;
    let x_rows = Arc::new(open(&family, "x.db").await);
    let x_pointers = open(&family, "x_pointers.db").await;
    let y = open(&family, "y.db").await;
    let txn = x_pointers.begin_txn();
    // Stored first, so the ledger names it first among the holders.
    x_rows.append_data_txn(txn, b"x row").await;
    x_rows.flush().await.unwrap();
    y.append_data_txn(txn, b"kept").await;
    x_pointers.append_data_txn(txn, b"x pointer").await;
    x_pointers.commit_durable(txn).await.unwrap();

    // A put of x.db's in flight keeps its sweep going while x_pointers' runs.
    x_rows.append_data(b"header", false).await;
    held.armed.store(true, Ordering::Release);
    let flush = tokio::spawn({
        let x_rows = x_rows.clone();
        async move { x_rows.flush().await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    x_rows.drop_io();
    x_pointers.drop_io();
    drop(open(&family, "x_pointers.db").await);
    held.armed.store(false, Ordering::Release);
    held.release.add_permits(16);
    flush.await.unwrap().unwrap();
    drop(open(&family, "x.db").await);
    drop(y);

    let restarted = open_family(store).await;
    assert_eq!(read_all(&open(&restarted, "y.db").await).await, b"kept".to_vec());
}

/// A holder dropped while the carry's declaration is being put into it: the
/// declaration goes on to another holder (and the dropped holder's own carry
/// takes it too), so `c.db` keeps its half. It used to count as landed and be
/// swept with the holder.
#[tokio::test]
async fn a_holder_dropped_during_the_carry_passes_the_declaration_on() {
    let held = HeldPutStore::holding("/y.db/d/");
    let store: Arc<dyn KvStore> = held.clone();
    let family = open_family(store.clone()).await;
    let a = open(&family, "a.db").await;
    let y = open(&family, "y.db").await;
    let c = open(&family, "c.db").await;
    let txn = a.begin_txn();
    // y's records stored first, so the carry tries y first.
    y.append_data_txn(txn, b"y half").await;
    y.flush().await.unwrap();
    c.append_data_txn(txn, b"kept").await;
    a.append_data_txn(txn, b"a half").await;
    a.commit_durable(txn).await.unwrap();

    // The carry's put into y is held; y is dropped meanwhile.
    held.armed.store(true, Ordering::Release);
    a.drop_io();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    y.drop_io();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    held.armed.store(false, Ordering::Release);
    held.release.add_permits(16);
    drop(open(&family, "a.db").await);
    drop(open(&family, "y.db").await);
    drop(c);

    let restarted = open_family(store).await;
    assert_eq!(read_all(&open(&restarted, "c.db").await).await, b"kept".to_vec());
}
