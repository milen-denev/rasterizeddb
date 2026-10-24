//! Test servers started at the same moment never share a port, and every
//! client reaches the server it was given.
//!
//! `TestServer::start` used to bind `127.0.0.1:0`, read the port, let the
//! socket go, and have the server bind that number again. The kernel is free to
//! hand a released port to the next `bind(0)`, so under a parallel run two
//! servers could be given one port: the second bind failed, and its test's
//! client reached the *first* test's server. Every compat file reuses table
//! names, so that showed up as a hard, convincing engine error — "Table already
//! exists", "Failed to add table" on a `CREATE TABLE` — in one test, and "pool
//! timed out while waiting for an open connection" in its neighbour, with
//! nothing wrong in the engine. The server now keeps the socket it was bound on.
//!
//! In a normal run this catches a regression only by chance: the ephemeral
//! range is wide, so 32 servers rarely draw the same port. To make a collision
//! near-certain, run it in a private network namespace with a narrow range:
//!
//! ```text
//! unshare -rn sh -c 'ip link set lo up &&
//!   echo 40000 40399 > /proc/sys/net/ipv4/ip_local_port_range &&
//!   target/debug/deps/parallel_servers-<hash>'
//! ```

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor};

const SERVERS: usize = 32;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn servers_started_at_once_each_answer_only_their_own_client() {
    let mut starting = tokio::task::JoinSet::new();
    for i in 0..SERVERS {
        starting.spawn(async move { (i, TestServer::start().await) });
    }
    let mut started: Vec<Option<TestServer>> = (0..SERVERS).map(|_| None).collect();
    while let Some(joined) = starting.join_next().await {
        let (i, server) = joined.expect("a start task panicked");
        started[i] = Some(server.unwrap_or_else(|e| panic!("server {i} did not start: {e:#}")));
    }
    let servers: Vec<TestServer> = started.into_iter().map(Option::unwrap).collect();

    let mut ports: Vec<u16> = servers.iter().map(TestServer::port).collect();
    ports.sort_unstable();
    ports.dedup();
    assert_eq!(
        ports.len(),
        SERVERS,
        "{SERVERS} servers started at once must hold {SERVERS} distinct ports"
    );

    // Every client creates the same table, as the compat files do: two clients
    // on one server collide right here, with the error that used to be blamed
    // on the engine. Each also leaves a table named for itself, so that the
    // engine behind each server can say whose client it served.
    let mut writing = tokio::task::JoinSet::new();
    for (i, server) in servers.iter().enumerate() {
        let url = server.url();
        writing.spawn(async move {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .map_err(|e| format!("client {i}: connect: {e}"))?;
            for sql in [
                "CREATE TABLE whoami (n INTEGER)".to_string(),
                format!("INSERT INTO whoami (n) VALUES ({i})"),
                format!("CREATE TABLE whoami_{i} (n INTEGER)"),
            ] {
                pool.execute(AssertSqlSafe(sql.as_str()))
                    .await
                    .map_err(|e| format!("client {i}: `{sql}`: {e}"))?;
            }
            let seen: Vec<i32> = sqlx::query_scalar("SELECT n FROM whoami")
                .fetch_all(&pool)
                .await
                .map_err(|e| format!("client {i}: reading back: {e}"))?;
            pool.close().await;
            Ok::<_, String>((i, seen))
        });
    }
    let mut failures = Vec::new();
    while let Some(joined) = writing.join_next().await {
        match joined.expect("a client task panicked") {
            Ok((i, seen)) if seen == vec![i as i32] => {}
            Ok((i, seen)) => failures.push(format!("client {i} read back {seen:?}")),
            Err(e) => failures.push(e),
        }
    }
    assert!(
        failures.is_empty(),
        "a client reached a server that was not its own:\n{}",
        failures.join("\n")
    );

    // And from the other side: each engine holds the marker of its own client
    // and of no other, which a pair of clients that swapped servers would not
    // show through the wire alone.
    for (i, server) in servers.iter().enumerate() {
        let mut markers: Vec<String> = server
            .database()
            .table_names()
            .into_iter()
            .filter(|name| name.starts_with("whoami_"))
            .collect();
        markers.sort();
        assert_eq!(
            markers,
            vec![format!("whoami_{i}")],
            "server {i} on port {} served someone else's client",
            server.port()
        );
    }
}
