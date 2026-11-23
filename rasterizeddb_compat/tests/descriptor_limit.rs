//! `max_connections` must fit in the process's descriptor limit.
//!
//! Each admitted session holds two descriptors (its socket and the duplicate
//! the disconnect watch peeks on), and each refusal in flight two more. A cap
//! larger than the limit can hold runs the process out of descriptors before
//! anyone is refused with `53300`: `accept()` fails with `EMFILE` and clients
//! wait in the backlog, and the engine's own file opens fail inside sessions
//! already admitted. So the server raises the soft limit when the hard one
//! allows, and otherwise lowers the cap and reports the lowered value.
//!
//! In a file of its own because it lowers the process's *hard* limit, which an
//! unprivileged process can never raise again: every later test in the same
//! binary would run under it.

#![cfg(target_os = "linux")]

use rasterizeddb_compat::TestServer;
use rasterizeddb_core::pgwire::ConnectionLimits;
use sqlx::{Connection, PgConnection};

/// The server's reserve for its own files and its cost per connection, as
/// `pgwire` defines them. Restated here so the expected cap is computed
/// independently of the code under test.
const RESERVED: u64 = 256;
const PER_CONNECTION: u64 = 4;

fn nofile() -> libc::rlimit {
    let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: a valid out-pointer to an `rlimit`.
    assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) }, 0);
    limit
}

fn set_nofile(soft: u64, hard: u64) {
    let limit = libc::rlimit { rlim_cur: soft as libc::rlim_t, rlim_max: hard as libc::rlim_t };
    // SAFETY: a valid pointer to an `rlimit`; lowering either limit is always
    // permitted.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
}

async fn shown_max_connections(server: &TestServer) -> String {
    let mut c = PgConnection::connect(&server.url()).await.expect("connect");
    sqlx::query_scalar::<_, String>("SHOW max_connections").fetch_one(&mut c).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn max_connections_fits_the_descriptor_limit() {
    // Low enough to be short of the cap below, high enough for the test's own
    // descriptors and two in-memory servers.
    let low: u64 = 300;
    let in_use = std::fs::read_dir("/proc/self/fd").expect("list descriptors").count() as u64;
    assert!(in_use + 64 < low, "{} descriptors already open; the test needs headroom", in_use);
    let cap = 50usize;
    let needed = cap as u64 * PER_CONNECTION + RESERVED;
    let original = nofile();
    assert!(
        original.rlim_max as u64 >= needed,
        "the hard limit ({}) is too low for this test",
        original.rlim_max
    );

    // A short soft limit under a roomy hard one is raised to what the cap
    // needs, and the cap stands.
    set_nofile(low, original.rlim_max as u64);
    let roomy = TestServer::start_with_limits(ConnectionLimits { max_connections: cap, ..ConnectionLimits::default() })
        .await
        .expect("server should start");
    assert_eq!(nofile().rlim_cur as u64, needed, "the soft limit must be raised to fit the cap");
    assert_eq!(shown_max_connections(&roomy).await, cap.to_string());

    // A hard limit the cap cannot fit in lowers the cap, and SHOW reports the
    // cap actually enforced rather than the one asked for.
    set_nofile(low, low);
    let tight = TestServer::start_with_limits(ConnectionLimits { max_connections: cap, ..ConnectionLimits::default() })
        .await
        .expect("server should start");
    let fitted = (low - RESERVED) / PER_CONNECTION;
    assert_eq!(shown_max_connections(&tight).await, fitted.to_string());
}
