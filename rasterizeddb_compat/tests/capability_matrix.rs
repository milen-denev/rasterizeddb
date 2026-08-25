//! Guards the compatibility surface that already works.
//!
//! The probe list in `probes.rs` describes where the engine is going; this test
//! pins where it is. A probe that passes today is recorded in [`EXPECTED_PASS`],
//! and the test fails both when one of those regresses **and** when a probe
//! outside the list starts passing — the second case is a prompt to move it in,
//! so the baseline never silently drifts behind the implementation.

use sqlx::AssertSqlSafe;
use rasterizeddb_compat::{TestServer, probes};

/// Probes known to pass. Add to this list when a feature lands.
const EXPECTED_PASS: &[&str] = &[
    // Every probe currently passes. A name is removed from this list only when
    // a feature is deliberately dropped — never to make a failure go away.
    "ALL",
];

#[tokio::test(flavor = "multi_thread")]
async fn supported_features_keep_working() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    probes::apply_schema(&pool).await.expect("schema should apply");

    let results = probes::run_all(&pool).await.expect("probes should run");

    let mut regressed = Vec::new();
    let mut newly_passing = Vec::new();

    for r in &results {
        let expected = EXPECTED_PASS.contains(&"ALL") || EXPECTED_PASS.contains(&r.probe.name);
        match (expected, r.passed()) {
            (true, false) => regressed.push(format!(
                "  {} ({}) -- {}",
                r.probe.name,
                r.probe.area,
                r.failure.as_deref().unwrap_or("")
            )),
            (false, true) => newly_passing.push(format!("  {} ({})", r.probe.name, r.probe.area)),
            _ => {}
        }
    }

    assert!(
        regressed.is_empty(),
        "these probes used to pass and no longer do:\n{}",
        regressed.join("\n")
    );
    assert!(
        newly_passing.is_empty(),
        "these probes now pass and should be added to EXPECTED_PASS:\n{}",
        newly_passing.join("\n")
    );
}

/// The schema itself is the first compatibility statement: UUID, NUMERIC,
/// TIMESTAMPTZ and INET columns all have to be accepted before anything else
/// can be measured.
#[tokio::test(flavor = "multi_thread")]
async fn the_application_schema_applies_cleanly() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    probes::apply_schema(&pool).await.expect("schema should apply");

    for table in rasterizeddb_compat::schema::TABLES {
        let sql = format!("SELECT * FROM {}", table);
        sqlx::query(AssertSqlSafe(&*sql))
            .fetch_all(&pool)
            .await
            .unwrap_or_else(|e| panic!("{} should be queryable: {}", table, e));
    }
}