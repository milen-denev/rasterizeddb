//! TEMPORARY audit reproducers for the kv storage driver. Delete before finishing.
#![cfg(feature = "kv_storage")]

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use rasterizeddb_core::core::storage_providers::kv::store::{
    BoxFuture, KvStore, MemoryKvStore, ObjectInfo,
};
use rasterizeddb_core::core::storage_providers::kv::{KvConfig, KvFamily, KvStorageProvider};
use rasterizeddb_core::core::storage_providers::traits::StorageIO;

fn tiny() -> KvConfig {
    let mut c = KvConfig::new("t");
    c.chunk_size = 4096;
    c.page_size = 1024;
    c.cache_bytes = 1 << 20;
    c.compact_chunk_bytes = 512;
    c.spill_bytes = 1 << 20;
    c.readahead_chunks = 0;
    c
}

async fn family(store: Arc<dyn KvStore>) -> Arc<KvFamily> {
    KvFamily::open(store, tiny()).await.unwrap()
}

async fn open(f: &Arc<KvFamily>, name: &str) -> KvStorageProvider {
    KvStorageProvider::open(f.clone(), name).await.unwrap()
}

async fn read_all(p: &KvStorageProvider) -> Vec<u8> {
    let len = p.get_len().await as usize;
    let mut out = vec![0u8; len];
    if len > 0 {
        let mut pos = 0u64;
        p.read_data_into_buffer(&mut pos, &mut out).await.unwrap();
    }
    out
}

/// Counts requests, can fail one put, and can hold one chunk request open
/// (after the store has already answered it) until the test releases it.
struct Probe {
    inner: MemoryKvStore,
    puts: AtomicUsize,
    put_bytes: AtomicU64,
    gets: AtomicUsize,
    ranges: AtomicUsize,
    lists: AtomicUsize,
    deletes: AtomicUsize,
    /// Puts allowed before the next one fails; usize::MAX = never.
    fail_after: AtomicUsize,
    gate_put: AtomicBool,
    gate_get: AtomicBool,
    started: AtomicBool,
    released: AtomicBool,
}

impl Probe {
    fn new() -> Self {
        Self {
            inner: MemoryKvStore::new(),
            puts: AtomicUsize::new(0),
            put_bytes: AtomicU64::new(0),
            gets: AtomicUsize::new(0),
            ranges: AtomicUsize::new(0),
            lists: AtomicUsize::new(0),
            deletes: AtomicUsize::new(0),
            fail_after: AtomicUsize::new(usize::MAX),
            gate_put: AtomicBool::new(false),
            gate_get: AtomicBool::new(false),
            started: AtomicBool::new(false),
            released: AtomicBool::new(false),
        }
    }
    fn reset(&self) {
        self.puts.store(0, Ordering::Relaxed);
        self.put_bytes.store(0, Ordering::Relaxed);
        self.gets.store(0, Ordering::Relaxed);
        self.ranges.store(0, Ordering::Relaxed);
        self.lists.store(0, Ordering::Relaxed);
        self.deletes.store(0, Ordering::Relaxed);
    }
    fn report(&self, what: &str) {
        eprintln!(
            "[{what}] lists={} gets={} ranges={} puts={} put_bytes={} deletes={}",
            self.lists.load(Ordering::Relaxed),
            self.gets.load(Ordering::Relaxed),
            self.ranges.load(Ordering::Relaxed),
            self.puts.load(Ordering::Relaxed),
            self.put_bytes.load(Ordering::Relaxed),
            self.deletes.load(Ordering::Relaxed),
        );
    }
    async fn hold(&self) {
        self.started.store(true, Ordering::Relaxed);
        while !self.released.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    async fn wait_started(&self) {
        while !self.started.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

fn is_chunk(key: &str) -> bool {
    key.contains("/g")
}

impl KvStore for Probe {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            self.gets.fetch_add(1, Ordering::Relaxed);
            let v = self.inner.get(key).await;
            if is_chunk(key) && self.gate_get.swap(false, Ordering::Relaxed) {
                self.hold().await;
            }
            v
        })
    }
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        offset: u64,
        len: usize,
    ) -> BoxFuture<'a, io::Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            self.ranges.fetch_add(1, Ordering::Relaxed);
            let v = self.inner.get_range(key, offset, len).await;
            if is_chunk(key) && self.gate_get.swap(false, Ordering::Relaxed) {
                self.hold().await;
            }
            v
        })
    }
    fn put<'a>(&'a self, key: &'a str, body: Vec<u8>) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            self.puts.fetch_add(1, Ordering::Relaxed);
            self.put_bytes
                .fetch_add(body.len() as u64, Ordering::Relaxed);
            let n = self.fail_after.load(Ordering::Relaxed);
            if n == 0 {
                self.fail_after.store(usize::MAX, Ordering::Relaxed);
                return Err(io::Error::other("store went away"));
            }
            if n != usize::MAX {
                self.fail_after.store(n - 1, Ordering::Relaxed);
            }
            if is_chunk(key) && self.gate_put.swap(false, Ordering::Relaxed) {
                self.hold().await;
            }
            self.inner.put(key, body).await
        })
    }
    fn delete<'a>(&'a self, keys: Vec<String>) -> BoxFuture<'a, io::Result<()>> {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(keys)
    }
    fn list<'a>(&'a self, prefix: &'a str) -> BoxFuture<'a, io::Result<Vec<ObjectInfo>>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }
    fn concurrency(&self) -> usize {
        4
    }
    fn describe(&self) -> String {
        "probe".to_string()
    }
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn t1_cross_file_declaration_lost_when_declaring_file_retires_its_deltas() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let f = family(store.clone()).await;
        let rows = open(&f, "t.db").await;
        let ptrs = open(&f, "t_pointers.db").await;
        let txn = rows.begin_txn();
        rows.append_data_txn(txn, b"row bytes").await;
        ptrs.append_data_txn(txn, b"pointer").await;
        ptrs.commit_durable(txn).await.unwrap();
        // The pointers file compacts on its own; the rows file does not.
        ptrs.compact_now().await.unwrap();
        assert_eq!(read_all(&rows).await, b"row bytes");
        assert_eq!(read_all(&ptrs).await, b"pointer");
    }
    let f = family(store).await;
    assert_eq!(read_all(&open(&f, "t_pointers.db").await).await, b"pointer");
    assert_eq!(
        read_all(&open(&f, "t.db").await).await,
        b"row bytes",
        "rows half of a committed transaction vanished after the pointers file compacted"
    );
}

#[tokio::test]
async fn t2_freeze_split_then_commit_before_publish_retires_needed_deltas() {
    let probe = Arc::new(Probe::new());
    let store: Arc<dyn KvStore> = probe.clone();
    let mut expected = vec![b'a'; 10_000];
    expected[9_000..9_010].copy_from_slice(b"bbbbbbbbbb");
    {
        let f = family(store.clone()).await;
        let io = open(&f, "t.db").await;
        let a = io.begin_txn();
        io.append_data_txn(a, &vec![b'a'; 10_000]).await;
        io.commit_durable(a).await.unwrap();
        let b = io.begin_txn();
        io.write_data_txn(b, 9_000, b"bbbbbbbbbb").await;

        probe.gate_put.store(true, Ordering::Relaxed);
        let io2 = io.clone();
        let compaction = tokio::spawn(async move { io2.compact_now().await });
        probe.wait_started().await;
        // b commits while the fold is mid-rewrite.
        io.commit_durable(b).await.unwrap();
        probe.released.store(true, Ordering::Relaxed);
        compaction.await.unwrap().unwrap();
        assert_eq!(read_all(&io).await, expected, "in-process view");
    }
    let f = family(store).await;
    let io = open(&f, "t.db").await;
    assert_eq!(io.get_len().await, 10_000, "length after restart");
    assert_eq!(read_all(&io).await, expected);
}

#[tokio::test]
async fn t3_restart_then_non_retiring_compaction_poisons_the_watermark() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let f = family(store.clone()).await;
        let io = open(&f, "t.db").await;
        let a = io.begin_txn();
        io.append_data_txn(a, &vec![b'a'; 100]).await;
        io.commit_durable(a).await.unwrap();
    }
    {
        let f = family(store.clone()).await;
        let io = open(&f, "t.db").await;
        let b = io.begin_txn();
        io.write_data_txn(b, 10, b"bbbbb").await;
        io.compact_now().await.unwrap();
        // b never commits: the process dies here.
    }
    let f = family(store).await;
    let io = open(&f, "t.db").await;
    assert_eq!(io.get_len().await, 100);
    assert_eq!(read_all(&io).await, vec![b'a'; 100]);
}

#[tokio::test]
async fn t4_a_fetch_in_flight_across_a_fold_returns_and_caches_stale_bytes() {
    let probe = Arc::new(Probe::new());
    let store: Arc<dyn KvStore> = probe.clone();
    {
        let f = family(store.clone()).await;
        let io = open(&f, "t.db").await;
        let a = io.begin_txn();
        io.append_data_txn(a, &vec![b'a'; 4096]).await;
        io.commit_durable(a).await.unwrap();
        io.compact_now().await.unwrap();
    }
    let f = family(store.clone()).await; // cold cache
    let io = open(&f, "t.db").await;
    let z = io.begin_txn();
    io.write_data_txn(z, 100, b"zzzz").await;
    io.commit_durable(z).await.unwrap();

    probe.gate_get.store(true, Ordering::Relaxed);
    let reader_io = io.clone();
    let reader = tokio::spawn(async move {
        let mut buf = vec![0u8; 200];
        let mut pos = 0u64;
        reader_io
            .read_data_into_buffer(&mut pos, &mut buf)
            .await
            .unwrap();
        buf
    });
    probe.wait_started().await;
    io.compact_now().await.unwrap();
    probe.released.store(true, Ordering::Relaxed);
    let got = reader.await.unwrap();
    let mut again = vec![0u8; 200];
    let mut pos = 0u64;
    io.read_data_into_buffer(&mut pos, &mut again).await.unwrap();
    assert_eq!(&got[100..104], b"zzzz", "read that straddled the fold");
    assert_eq!(&again[100..104], b"zzzz", "later read, served from the cache");
}

#[tokio::test]
async fn t5_orphaned_scratch_deltas_replay_after_a_retried_swap() {
    let store: Arc<dyn KvStore> = Arc::new(MemoryKvStore::new());
    {
        let f = family(store.clone()).await;
        let io = open(&f, "t.db").await;
        io.append_data(&vec![b'o'; 100], true).await;
        io.compact_now().await.unwrap();
        let t1 = io.create_temp().await;
        t1.append_data(&vec![b'x'; 300], true).await;
        // process dies before swap_temp
    }
    {
        let f = family(store.clone()).await;
        let io = open(&f, "t.db").await;
        assert_eq!(read_all(&io).await, vec![b'o'; 100]);
        let mut t2 = io.create_temp().await;
        t2.append_data(&vec![b'n'; 50], true).await;
        io.swap_temp(&mut t2).await;
        assert_eq!(read_all(&io).await, vec![b'n'; 50]);
    }
    let f = family(store).await;
    let io = open(&f, "t.db").await;
    assert_eq!(io.get_len().await, 50, "length after restart");
    assert_eq!(read_all(&io).await, vec![b'n'; 50]);
}

#[tokio::test]
async fn t8_a_failed_commit_is_committed_by_the_next_flush() {
    let probe = Arc::new(Probe::new());
    let store: Arc<dyn KvStore> = probe.clone();
    {
        let f = family(store.clone()).await;
        let rows = open(&f, "t.db").await;
        let ptrs = open(&f, "t_pointers.db").await;
        let txn = rows.begin_txn();
        rows.append_data_txn(txn, b"row bytes").await;
        ptrs.append_data_txn(txn, b"pointer").await;
        probe.fail_after.store(1, Ordering::Relaxed); // rows lands, pointers fails
        assert!(ptrs.commit_durable(txn).await.is_err());
        // What the background service does within flush_interval.
        ptrs.flush().await.unwrap();
    }
    let f = family(store).await;
    assert_eq!(
        read_all(&open(&f, "t_pointers.db").await).await,
        b"",
        "a transaction whose commit returned an error became durable anyway"
    );
    assert_eq!(read_all(&open(&f, "t.db").await).await, b"");
}

#[tokio::test]
async fn t9_truncate_during_a_fold_leaves_stale_bytes_in_a_hole() {
    let probe = Arc::new(Probe::new());
    let store: Arc<dyn KvStore> = probe.clone();
    let f = family(store.clone()).await;
    let io = open(&f, "t.db").await;
    let a = io.begin_txn();
    io.append_data_txn(a, &vec![b'a'; 10_000]).await;
    io.commit_durable(a).await.unwrap();

    probe.gate_put.store(true, Ordering::Relaxed);
    let io2 = io.clone();
    let compaction = tokio::spawn(async move { io2.compact_now().await });
    probe.wait_started().await;
    io.truncate(100).await.unwrap();
    probe.released.store(true, Ordering::Relaxed);
    compaction.await.unwrap().unwrap();
    assert_eq!(io.get_len().await, 100);

    io.write_data(2_000, b"Q").await;
    let mut hole = vec![0xFFu8; 1_900];
    let mut pos = 100u64;
    io.read_data_into_buffer(&mut pos, &mut hole).await.unwrap();
    let stale = hole.iter().filter(|&&b| b == b'a').count();
    assert_eq!(stale, 0, "{stale} bytes of the truncated tail resurfaced in the hole");
}

#[tokio::test]
async fn t10_request_counts() {
    let probe = Arc::new(Probe::new());
    let store: Arc<dyn KvStore> = probe.clone();
    {
        let f = family(store.clone()).await;
        let rows = open(&f, "t.db").await;
        let ptrs = open(&f, "t_pointers.db").await;
        let txn = rows.begin_txn();
        rows.append_data_txn(txn, &vec![b'a'; 10 * 4096]).await;
        ptrs.append_data_txn(txn, &vec![b'p'; 640]).await;
        ptrs.commit_durable(txn).await.unwrap();
        rows.compact_now().await.unwrap();
        ptrs.compact_now().await.unwrap();
        // 20 small commits left un-compacted, as a running server has.
        for i in 0..20u64 {
            let txn = rows.begin_txn();
            rows.write_data_txn(txn, i * 4096, b"x").await;
            ptrs.write_data_txn(txn, i * 64, b"y").await;
            ptrs.commit_durable(txn).await.unwrap();
        }
    }
    probe.reset();
    let f = family(store.clone()).await;
    probe.report("startup: family open (2 files, 20 deltas each)");
    probe.reset();
    let rows = open(&f, "t.db").await;
    let ptrs = open(&f, "t_pointers.db").await;
    probe.report("startup: open 2 files");

    probe.reset();
    let mut buf = vec![0u8; 64];
    let mut pos = 5_000u64;
    rows.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();
    probe.report("cold point read of 64 bytes");

    probe.reset();
    let mut pos = 0u64;
    let mut buf = vec![0u8; 4096];
    while pos < 10 * 4096 {
        rows.read_data_into_buffer(&mut pos, &mut buf).await.unwrap();
    }
    probe.report("scan of 10 chunks, one cached from the point read");

    probe.reset();
    let txn = rows.begin_txn();
    rows.append_data_txn(txn, b"row").await;
    ptrs.append_data_txn(txn, b"ptr").await;
    ptrs.commit_durable(txn).await.unwrap();
    probe.report("one commit across two files");

    probe.reset();
    ptrs.write_data(0, b"s").await;
    probe.report("one untransacted write_data");

    // Forced fold: 64 tiny commits spread over 10 chunks, then compaction.
    probe.reset();
    for i in 0..64u64 {
        let txn = rows.begin_txn();
        rows.write_data_txn(txn, (i % 10) * 4096 + 7, b"z").await;
        rows.commit_durable(txn).await.unwrap();
    }
    probe.report("64 one-byte commits");
    probe.reset();
    rows.compact_now().await.unwrap();
    probe.report("compaction forced by max_deltas over 10 touched chunks (64 bytes of change)");
}

// ---------------------------------------------------------------------------
// S3 client behaviour against a fake endpoint
// ---------------------------------------------------------------------------

#[cfg(feature = "kv_storage_s3")]
mod s3 {
    use super::*;
    use rasterizeddb_core::core::backup::s3::{Credentials, Endpoint, S3Client, S3Config};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    type Handler = Arc<dyn Fn(&str, &str, &[u8]) -> (u16, String) + Send + Sync>;

    fn find(h: &[u8], n: &[u8]) -> Option<usize> {
        h.windows(n.len()).position(|w| w == n)
    }

    async fn serve(handler: Handler, requests: Arc<AtomicUsize>, hang: bool) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let handler = handler.clone();
                let requests = requests.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let head_end = loop {
                        if let Some(p) = find(&buf, b"\r\n\r\n") {
                            break p + 4;
                        }
                        let n = sock.read(&mut tmp).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let mut lines = head.split("\r\n");
                    let req = lines.next().unwrap_or("").to_string();
                    let mut parts = req.split(' ');
                    let method = parts.next().unwrap_or("").to_string();
                    let target = parts.next().unwrap_or("").to_string();
                    let content_length: usize = lines
                        .filter_map(|l| l.split_once(':'))
                        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                        .and_then(|(_, v)| v.trim().parse().ok())
                        .unwrap_or(0);
                    while buf.len() < head_end + content_length {
                        let n = sock.read(&mut tmp).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    requests.fetch_add(1, Ordering::Relaxed);
                    if hang {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        return;
                    }
                    let body = &buf[head_end..head_end + content_length];
                    let (status, resp) = handler(&method, &target, body);
                    let out = format!(
                        "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{resp}",
                        resp.len()
                    );
                    let _ = sock.write_all(out.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        port
    }

    fn client(port: u16, attempts: u32) -> S3Client {
        S3Client::new(S3Config {
            endpoint: Endpoint::parse(&format!("http://127.0.0.1:{port}")).unwrap(),
            region: "us-east-1".into(),
            bucket: "b".into(),
            credentials: Credentials {
                access_key: "k".into(),
                secret_key: "s".into(),
                session_token: None,
            },
            path_style: true,
            max_attempts: attempts,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn s3_a_hung_connection_times_out_instead_of_hanging() {
        // Was: a blackholed connection held the caller forever. The client now
        // bounds every attempt (60 s by default, sized for a real store); the
        // test shortens that so it can observe the bound, then asserts the
        // request gives up with an error rather than hanging.
        let port = serve(
            Arc::new(|_, _, _| (200, String::new())),
            Arc::new(AtomicUsize::new(0)),
            true,
        )
        .await;
        let c = client(port, 1).with_request_timeout(Duration::from_secs(1));
        let r = tokio::time::timeout(Duration::from_secs(5), c.get_object_opt("t/META")).await;
        let inner = r.expect("no client-side request timeout: a blackholed connection hangs the caller");
        assert!(inner.is_err(), "a request nobody answered reported success: {:?}", inner.map(|_| ()));
    }

    #[tokio::test]
    async fn s3_batch_delete_reports_success_over_per_key_errors() {
        let handler: Handler = Arc::new(|m, t, _| {
            if m == "POST" && t.contains("delete") {
                (
                    200,
                    "<DeleteResult><Error><Key>t/x</Key><Code>AccessDenied</Code>\
                     <Message>denied</Message></Error></DeleteResult>"
                        .to_string(),
                )
            } else {
                (404, String::new())
            }
        });
        let port = serve(handler, Arc::new(AtomicUsize::new(0)), false).await;
        let c = client(port, 1);
        let r = c.delete_objects(&["t/x".to_string()]).await;
        assert!(r.is_err(), "a per-key <Error> in a 200 DeleteResult was reported as Ok");
    }

    #[tokio::test]
    async fn s3_throttling_budget() {
        let requests = Arc::new(AtomicUsize::new(0));
        let handler: Handler = Arc::new(|_, _, _| {
            (503, "<Error><Code>SlowDown</Code></Error>".to_string())
        });
        let port = serve(handler, requests.clone(), false).await;
        let c = client(port, 4);
        let t0 = std::time::Instant::now();
        let r = c.put_object("t/d/1", b"x").await;
        let elapsed = t0.elapsed();
        eprintln!(
            "[s3 throttling] attempts={} elapsed={:?} result_err={}",
            requests.load(Ordering::Relaxed),
            elapsed,
            r.is_err()
        );
        assert_eq!(requests.load(Ordering::Relaxed), 4);
    }
}
