//! A failed `accept()` must not stop the listener.
//!
//! The accept loop used to return its first accept error, and the binary
//! discarded the result and exited — so the `EMFILE` a connection storm
//! produces shut the whole database down, running sessions included. This
//! makes `accept()` fail for real, by running the process out of descriptors
//! while a connection waits in the backlog, then gives the descriptors back and
//! expects that same connection to be served.
//!
//! In a file of its own because it lowers a process-wide limit: any other test
//! running alongside it in the same binary would fail to open anything at all.

#![cfg(target_os = "linux")]

use std::time::Duration;

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::core::database::DEFAULT_DATABASE_NAME;
use sqlx::{Connection, Executor, Row};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Puts the descriptor limit back and closes the filler files however the
/// test ends, so a failure here cannot leave the process unable to report it.
struct Exhausted {
    original: libc::rlimit,
    filler: Vec<std::fs::File>,
}

impl Exhausted {
    /// Lowers the soft limit to just above the highest descriptor in use and
    /// fills what is left, so the next descriptor anyone asks for is refused.
    fn new() -> Self {
        let highest = std::fs::read_dir("/proc/self/fd")
            .expect("list descriptors")
            .filter_map(|e| e.ok()?.file_name().to_str()?.parse::<u64>().ok())
            .max()
            .expect("at least stdin is open");

        let mut original = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
        // SAFETY: a valid out-pointer to an `rlimit`.
        assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut original) }, 0);
        let lowered = libc::rlimit { rlim_cur: (highest + 16).min(original.rlim_max), rlim_max: original.rlim_max };
        // SAFETY: a valid pointer to an `rlimit`; lowering the soft limit
        // below the hard one is always permitted.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lowered) }, 0);

        let mut exhausted = Self { original, filler: Vec::new() };
        // Bounded: the limit leaves room for at most 16 more.
        for _ in 0..64 {
            match std::fs::File::open("/dev/null") {
                Ok(f) => exhausted.filler.push(f),
                Err(e) if e.raw_os_error() == Some(libc::EMFILE) => return exhausted,
                Err(e) => panic!("unexpected error filling descriptors: {}", e),
            }
        }
        panic!("never reached the descriptor limit");
    }

    fn at_limit(&self) -> bool {
        matches!(std::fs::File::open("/dev/null"), Err(e) if e.raw_os_error() == Some(libc::EMFILE))
    }
}

impl Drop for Exhausted {
    fn drop(&mut self) {
        self.filler.clear();
        // SAFETY: a valid pointer to the limit read at construction.
        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &self.original) };
    }
}

fn startup_packet() -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&196608i32.to_be_bytes());
    for (k, v) in [("user", "rasterized"), ("database", DEFAULT_DATABASE_NAME)] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
    packet.extend_from_slice(&body);
    packet
}

#[tokio::test(flavor = "multi_thread")]
async fn running_out_of_descriptors_does_not_stop_the_listener() {
    let server = TestServer::start().await.expect("server should start");

    let mut exhausted = Exhausted::new();
    // One descriptor free, for the client's own socket. The kernel completes
    // the TCP handshake from the backlog, so the connect succeeds; the
    // server's `accept()` then needs a descriptor that does not exist.
    exhausted.filler.pop();
    let mut waiting = tokio::net::TcpStream::connect(("127.0.0.1", server.port()))
        .await
        .expect("the kernel accepts into the backlog");
    assert!(exhausted.at_limit(), "the process must be out of descriptors for the test to mean anything");

    // Long enough for several failed accepts and their backoff.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(exhausted.at_limit(), "nothing may have freed a descriptor meanwhile");
    drop(exhausted);

    // The connection that waited through the failures is served.
    waiting.write_all(&startup_packet()).await.expect("send startup");
    let mut tag = [0u8; 1];
    let read = tokio::time::timeout(Duration::from_secs(5), waiting.read_exact(&mut tag))
        .await
        .expect("the server must answer the waiting connection");
    read.expect("the waiting connection must not have been reset");
    assert_eq!(tag[0], b'R', "expected authentication to begin");

    // And so is a new one.
    let mut c = sqlx::PgConnection::connect(&server.url()).await.expect("a new connection");
    let row = c.fetch_one(sqlx::raw_sql("SELECT 1")).await.expect("served");
    assert_eq!(row.get::<i32, _>(0), 1);
}
