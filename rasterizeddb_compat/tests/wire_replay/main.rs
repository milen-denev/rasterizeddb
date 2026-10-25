//! Raw-wire replays of the two drivers the gate cannot run here (slice B8,
//! `docs/persistent-indexes.md` §7 "Driver gate", §13 V10 case 14; the
//! owner's Q2 condition).
//!
//! Neither Java nor Npgsql is installed on this box, and the owner allowed
//! the merge gate to rest on replays of their exact frames, validated
//! against PostgreSQL 17, provided they reproduce:
//!
//! - (a) **pgjdbc's `prepareThreshold` flip** ([`pgjdbc`]): four executions
//!   on the unnamed statement, then a named `S_1` re-Parse, then binary
//!   result formats from the cached fields with no Describe, and its retry
//!   after 0A000 `cached plan must not change result type` (Close `S_1`,
//!   Parse `S_2`, Bind, Describe, Execute, Sync);
//! - (b) **npgsql's batching** ([`npgsql`]): several Parse/Bind/Describe/
//!   Execute and ONE Sync, an error mid-batch skipping the rest until the
//!   Sync, plus its auto-prepare (`_auto0`, Describe of the statement) across
//!   a shape change.
//!
//! Each replay records every reply as a token (`support/wire_frames.rs`).
//! The constants named `PG17_*` are what PostgreSQL 17 answered, captured by
//! running [`validate_against_postgresql_17`] against `postgres:17` in a
//! throwaway container:
//!
//! ```text
//! docker run --rm -d --name rdb-b8-pg17 --tmpfs /var/lib/postgresql/data \
//!     -e POSTGRES_HOST_AUTH_METHOD=trust -p 127.0.0.1:55432:5432 postgres:17
//! PG17_ADDR=127.0.0.1:55432 cargo test -p rasterizeddb_compat --test wire_replay \
//!     -- --ignored --nocapture
//! docker stop rdb-b8-pg17
//! ```
//!
//! The tests below then require this engine to give the same sequence,
//! except where a difference is named beside the constant and in §13.
//!
//! The frames were written from the drivers' source as read from memory
//! (pgjdbc 42.x `QueryExecutorImpl.sendOneQuery`, `sendBind`,
//! `PgStatement.execute`'s retry and `willHealViaReparse`; Npgsql 8
//! `NpgsqlCommand.Write*`), not captured from a running driver. A run of
//! the real drivers is a precondition for advertising persistent indexes,
//! not for merging W3 (a TODO in §7).

#[path = "../support/wire_frames.rs"]
mod wire;

mod npgsql;
mod pgjdbc;

use rasterizeddb_compat::TestServer;

/// What a replay recorded: one (step, rendered replies) per exchange.
pub type Trace = Vec<(&'static str, String)>;

/// Where a replay connects, and as whom.
pub struct Target {
    pub addr: String,
    pub user: &'static str,
}

impl Target {
    pub fn ours(server: &TestServer) -> Target {
        Target { addr: format!("127.0.0.1:{}", server.port()), user: "postgres" }
    }
}

/// The expected trace for this engine: PostgreSQL's, with each named step
/// replaced by its documented difference.
pub fn with_differences(pg: &[(&'static str, &'static str)], diff: &[(&'static str, &'static str)]) -> Vec<(&'static str, String)> {
    for (step, _) in diff {
        assert!(pg.iter().any(|(s, _)| s == step), "difference for an unknown step {step}");
    }
    pg.iter()
        .map(|(step, trace)| {
            let t = diff.iter().find(|(s, _)| s == step).map(|(_, t)| *t).unwrap_or(trace);
            (*step, t.to_string())
        })
        .collect()
}

/// Compares step by step, reporting every step that differs.
pub fn assert_trace(what: &str, got: &Trace, expected: &[(&'static str, String)]) {
    let mut bad = Vec::new();
    for (i, (step, want)) in expected.iter().enumerate() {
        match got.get(i) {
            Some((s, g)) if s == step && g == want => {}
            Some((s, g)) => bad.push(format!("step {step}:\n   want {want}\n   got  [{s}] {g}")),
            None => bad.push(format!("step {step}: missing")),
        }
    }
    if got.len() > expected.len() {
        bad.push(format!("{} extra steps: {:?}", got.len() - expected.len(), &got[expected.len()..]));
    }
    assert!(bad.is_empty(), "{what}: the replay's answers differ:\n{}", bad.join("\n"));
}

/// A server bounded so a hang fails the test (the replays' own reads are
/// bounded too).
pub const BOUND: std::time::Duration = std::time::Duration::from_secs(30);

/// Runs every replay against the PostgreSQL 17 named by `PG17_ADDR` and
/// checks it still answers exactly as the recorded `PG17_*` constants,
/// printing each trace. Ignored: it needs a PostgreSQL to talk to.
#[tokio::test]
#[ignore = "needs PG17_ADDR=<host:port> of a PostgreSQL 17 with trust auth"]
async fn validate_against_postgresql_17() {
    let addr = std::env::var("PG17_ADDR").expect("PG17_ADDR");
    let pg = Target { addr, user: "postgres" };
    let checks: Vec<(&str, Trace, Vec<(&'static str, String)>)> = vec![
        ("pgjdbc", pgjdbc::replay(&pg, Some("error")).await, with_differences(pgjdbc::PG17_FLIP, &[])),
        ("npgsql batch", npgsql::batch(&pg).await, with_differences(npgsql::PG17_BATCH, &[])),
        ("npgsql auto-prepare", npgsql::auto_prepare(&pg, Some("error")).await, with_differences(npgsql::PG17_AUTO_PREPARE, &[])),
    ];
    for (name, got, _) in &checks {
        println!("== {name} (PostgreSQL 17)");
        for (step, t) in got {
            println!("    (\"{step}\", \"{t}\"),");
        }
    }
    for (name, got, want) in &checks {
        assert_trace(name, got, want);
    }
}
