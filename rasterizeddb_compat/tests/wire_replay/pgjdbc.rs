//! pgjdbc's `prepareThreshold` flip, frame for frame (the owner's Q2
//! condition (a); §13 sequence `pgjdbc-flip`).
//!
//! pgjdbc 42.x, `prepareThreshold=5` (its default), autocommit, binary
//! transfer on (its default), `PreparedStatement.setInt(1, k)` then
//! `executeQuery()` on `SELECT * FROM jdbc_t WHERE id = ?`:
//!
//! - executions 1-4 are one-shot: Parse of the unnamed statement with the
//!   parameter declared int4 (23) and sent in binary, Bind with no result
//!   formats (text), Describe of the portal (the fields are dropped at every
//!   re-Parse), Execute, Sync, in one write;
//! - execution 5 crosses the threshold: Parse `S_1`, Bind, Describe of the
//!   portal, Execute, Sync; the RowDescription is kept as the query's fields;
//! - execution 6 on: Bind `S_1` with one result format per cached field,
//!   binary where pgjdbc receives the type in binary (int4 yes, text no:
//!   `[1,0,1]`), Execute, Sync, and no Describe (the fields are cached);
//! - another session's `ALTER TABLE … ADD COLUMN`, then execution 7, the
//!   same frames: PostgreSQL refuses the Bind with 0A000 whose routine is
//!   `RevalidateCachedQuery`, which is exactly what pgjdbc's
//!   `willHealViaReparse` tests; under autocommit `PgStatement.execute`
//!   then closes the query (its name queued for Close) and runs it once
//!   more: Close `S_1`, Parse `S_2`, Bind, Describe of the portal, Execute,
//!   Sync;
//! - execution 8: Bind `S_2` with the four new fields' formats `[1,0,1,1]`.

use super::wire::{self, Param, Reply, Wire};
use super::{Target, Trace};

const SQL: &str = "SELECT * FROM jdbc_t WHERE id = $1";

/// pgjdbc's startup packet (its defaults), plus `options` when given.
fn startup<'a>(t: &'a Target, options: Option<&'a str>) -> Vec<(&'a str, &'a str)> {
    let mut p = vec![
        ("user", t.user),
        ("database", "postgres"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO"),
        ("TimeZone", "UTC"),
        ("extra_float_digits", "2"),
        ("application_name", "PostgreSQL JDBC Driver"),
    ];
    if let Some(o) = options {
        p.push(("options", o));
    }
    p
}

/// Runs the flip against `t`, with `rdb.shape_change = <mode>` when `mode`
/// names one (sent as a startup option, which PostgreSQL accepts as a
/// placeholder setting and ignores) and the server's default otherwise.
/// What follows execution 7 is what pgjdbc does with its answer: after
/// 0A000 the retry is replayed; after rows, execution 7's frames are sent
/// again, as a driver that never re-describes keeps sending them.
pub async fn replay(t: &Target, mode: Option<&str>) -> Trace {
    let mut admin = Wire::connect(&t.addr, &[("user", t.user), ("database", "postgres")]).await;
    admin.simple("DROP TABLE IF EXISTS jdbc_t").await;
    admin.simple_ok("CREATE TABLE jdbc_t (id INT PRIMARY KEY, a TEXT, b INT)").await;
    admin.simple_ok("INSERT INTO jdbc_t (id, a, b) VALUES (1, 'x', 10)").await;
    admin.simple_ok("INSERT INTO jdbc_t (id, a, b) VALUES (2, 'y', 20)").await;

    let options = mode.map(|m| format!("-c rdb.shape_change={m}"));
    let mut j = Wire::connect(&t.addr, &startup(t, options.as_deref())).await;
    let mut trace: Trace = Vec::new();

    let one_shot = |k: i32| {
        vec![
            wire::parse("", SQL, &[23]),
            wire::bind("", "", &[Param::Int4(k)], &[]),
            wire::describe_p(""),
            wire::execute("", 0),
            wire::sync(),
        ]
    };
    for (step, k) in [("exec1", 1), ("exec2", 2), ("exec3", 1), ("exec4", 2)] {
        trace.push((step, wire::render(&j.exchange(&one_shot(k)).await)));
    }
    let r = j
        .exchange(&[
            wire::parse("S_1", SQL, &[23]),
            wire::bind("", "S_1", &[Param::Int4(1)], &[]),
            wire::describe_p(""),
            wire::execute("", 0),
            wire::sync(),
        ])
        .await;
    trace.push(("exec5 Parse S_1", wire::render(&r)));
    let named = |stmt: &str, k: i32, formats: &[i16]| {
        vec![
            wire::bind("", stmt, &[Param::Int4(k)], formats),
            wire::execute("", 0),
            wire::sync(),
        ]
    };
    trace.push(("exec6 Bind S_1", wire::render(&j.exchange(&named("S_1", 1, &[1, 0, 1])).await)));

    admin.simple_ok("ALTER TABLE jdbc_t ADD COLUMN c INT").await;

    let r = j.exchange(&named("S_1", 1, &[1, 0, 1])).await;
    let refused = r.iter().any(|m| matches!(m, Reply::Error { code, .. } if code == "0A000"));
    trace.push(("exec7 Bind S_1", wire::render(&r)));
    if refused {
        let r = j
            .exchange(&[
                wire::close_s("S_1"),
                wire::parse("S_2", SQL, &[23]),
                wire::bind("", "S_2", &[Param::Int4(1)], &[]),
                wire::describe_p(""),
                wire::execute("", 0),
                wire::sync(),
            ])
            .await;
        trace.push(("retry Close S_1 Parse S_2", wire::render(&r)));
        trace.push(("exec8 Bind S_2", wire::render(&j.exchange(&named("S_2", 1, &[1, 0, 1, 1])).await)));
    } else {
        trace.push(("exec8 Bind S_1", wire::render(&j.exchange(&named("S_1", 1, &[1, 0, 1])).await)));
    }
    admin.simple("DROP TABLE IF EXISTS jdbc_t").await;
    trace
}

/// PostgreSQL 17's answers to [`replay`] (error mode; PostgreSQL has no
/// other), captured by `validate_against_postgresql_17`.
pub const PG17_FLIP: &[(&str, &str)] = &[
    ("exec1", "1 2 T[id/23/0,a/25/0,b/23/0] D['1'|'x'|'10'] C(SELECT 1) Z(I)"),
    ("exec2", "1 2 T[id/23/0,a/25/0,b/23/0] D['2'|'y'|'20'] C(SELECT 1) Z(I)"),
    ("exec3", "1 2 T[id/23/0,a/25/0,b/23/0] D['1'|'x'|'10'] C(SELECT 1) Z(I)"),
    ("exec4", "1 2 T[id/23/0,a/25/0,b/23/0] D['2'|'y'|'20'] C(SELECT 1) Z(I)"),
    ("exec5 Parse S_1", "1 2 T[id/23/0,a/25/0,b/23/0] D['1'|'x'|'10'] C(SELECT 1) Z(I)"),
    ("exec6 Bind S_1", "2 D[00000001|'x'|0000000a] C(SELECT 1) Z(I)"),
    (
        "exec7 Bind S_1",
        "E(0A000 M=cached plan must not change result type R=RevalidateCachedQuery) Z(I)",
    ),
    (
        "retry Close S_1 Parse S_2",
        "3 1 2 T[id/23/0,a/25/0,b/23/0,c/23/0] D['1'|'x'|'10'|NULL] C(SELECT 1) Z(I)",
    ),
    ("exec8 Bind S_2", "2 D[00000001|'x'|0000000a|NULL] C(SELECT 1) Z(I)"),
];

/// `rdb.shape_change = rowdescription`: execution 7 and every
/// one after it carry a RowDescription of the new columns ahead of the rows
/// where PostgreSQL refuses. The three result formats pgjdbc sent apply to
/// the first three of the four columns, the fourth goes as text, and the
/// RowDescription states each (§7 "Bind's per-column result formats", as
/// built). pgjdbc never sent a Describe for these, so its
/// `pendingDescribePortalQueue` is empty when the RowDescription arrives
/// (from its source: not run).
pub const OURS_ROWDESCRIPTION: &[(&str, &str)] = &[
    (
        "exec7 Bind S_1",
        "2 T[id/23/1,a/25/0,b/23/1,c/23/0] D[00000001|'x'|0000000a|NULL] C(SELECT 1) Z(I)",
    ),
];

#[tokio::test]
async fn pgjdbc_flip_in_error_mode_answers_as_postgresql_17() {
    let server = TestServer::start().await.expect("server");
    let got = tokio::time::timeout(super::BOUND, replay(&Target::ours(&server), Some("error")))
        .await
        .expect("the replay hung");
    super::assert_trace("pgjdbc flip, error mode", &got, &super::with_differences(PG17_FLIP, &[]));
}

/// The default, `auto` (slice B8b, Q1): pgjdbc's stale Bind is refused at
/// Bind with `R=RevalidateCachedQuery`, its retry under a new name (a new
/// Parse) is a fresh statement, and the whole flip answers token for token
/// as PostgreSQL 17 does.
#[tokio::test]
async fn pgjdbc_flip_under_the_default_answers_as_postgresql_17() {
    let server = TestServer::start().await.expect("server");
    let got = tokio::time::timeout(super::BOUND, replay(&Target::ours(&server), None))
        .await
        .expect("the replay hung");
    super::assert_trace("pgjdbc flip, default (auto)", &got, &super::with_differences(PG17_FLIP, &[]));
}

#[tokio::test]
async fn pgjdbc_flip_in_rowdescription_mode_repairs_every_execute() {
    let server = TestServer::start().await.expect("server");
    let got = tokio::time::timeout(super::BOUND, replay(&Target::ours(&server), Some("rowdescription")))
        .await
        .expect("the replay hung");
    let mut want = super::with_differences(&PG17_FLIP[..7], OURS_ROWDESCRIPTION);
    want.push(("exec8 Bind S_1", OURS_ROWDESCRIPTION[0].1.to_string()));
    super::assert_trace("pgjdbc flip, rowdescription mode", &got, &want);
}

use rasterizeddb_compat::TestServer;
