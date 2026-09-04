//! Every statement the dialect layer sends, run against a real server.
//!
//! # Why this cannot be a unit test
//!
//! The unit tests in `dialect` check that the two dialects *name* their columns
//! identically. That catches a rename and nothing else. What it cannot catch is
//! the failure that actually happens: a `pg_catalog` query that does not run —
//! a column removed in a newer PostgreSQL, a `LATERAL` the planner rejects, a
//! `::regclass` cast against a name that needs quoting. Those are all syntax
//! and catalogue questions, and only a server can answer them.
//!
//! So this test asks one. It is skipped unless pointed at a database, because
//! a test suite that needs a server running is a test suite that is red on a
//! laptop:
//!
//! ```sh
//! RDB_STUDIO_TEST_PG=postgresql://user:pw@127.0.0.1:5432/db?sslmode=disable \
//! RDB_STUDIO_TEST_RDB=postgresql://user@127.0.0.1:5432/postgres?sslmode=disable \
//!   cargo test -p rdb_studio --test live_dialect -- --nocapture
//! ```
//!
//! Both are optional and independent; whichever is set is exercised.

use rdb_studio::conn::{self, Session};
use rdb_studio::dialect;
use rdb_studio::model::{Connection, Flavor, RestoreTarget, TlsMode};

/// Turns a connection string into the profile `conn::connect` wants.
///
/// Deliberately minimal: enough of a URL parser for a test's own environment
/// variable, and nothing that would tempt anyone to use it for user input.
fn profile(url: &str, flavor: Flavor) -> Connection {
    let rest = url
        .trim_start_matches("postgresql://")
        .trim_start_matches("postgres://");
    let (credentials, rest) = rest.split_once('@').unwrap_or(("", rest));
    let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
    let (host_port, rest) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "5432"));
    let database = rest.split('?').next().unwrap_or("postgres");

    Connection {
        name: "test".to_string(),
        flavor,
        host: host.to_string(),
        port: port.parse().unwrap_or(5432),
        database: database.to_string(),
        user: if user.is_empty() { "postgres".to_string() } else { user.to_string() },
        password: password.to_string(),
        save_password: false,
        tls: TlsMode::Disable,
    }
}

async fn open(variable: &str, flavor: Flavor) -> Option<Session> {
    let url = std::env::var(variable).ok()?;
    match conn::connect(profile(&url, flavor)).await {
        Ok(session) => Some(session),
        Err(e) => panic!("{} is set but the server refused: {}", variable, e),
    }
}

/// Runs every statement the console issues, and checks each came back with the
/// columns the shared parsers read.
async fn exercise(session: &Session) {
    let flavor = session.flavor;

    let databases = session
        .query(dialect::databases_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: databases: {e}"));
    let databases = dialect::parse_databases(&databases);
    assert!(!databases.is_empty(), "{flavor}: no databases reported");
    assert!(
        databases.iter().any(|d| d.is_current),
        "{flavor}: nothing marked as the current database"
    );

    let stats = session
        .query(dialect::database_stats_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: database stats: {e}"));
    let stats = dialect::parse_stats(&stats);
    assert!(!stats.is_empty(), "{flavor}: no statistics reported");
    for metric in ["name", "tables", "total_bytes"] {
        assert!(
            stats.iter().any(|s| s.metric == metric),
            "{flavor}: database stats has no `{metric}`; the overview's tiles read it by name"
        );
    }

    let tables = session
        .query(dialect::tables_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: tables: {e}"));
    let tables = dialect::parse_tables(&tables);
    assert!(!tables.is_empty(), "{flavor}: no tables reported");
    assert!(
        tables.iter().any(|t| !t.primary_key.is_empty()),
        "{flavor}: no table reported a primary key, so nothing would be editable"
    );
    // Both servers name what is holding each table, in one vocabulary. An
    // empty answer here is what the console renders as "—", so a server that
    // stopped reporting it would quietly lose the column rather than fail.
    assert!(
        tables.iter().all(|t| !t.storage.is_empty()),
        "{flavor}: a table reported no storage kind: {:?}",
        tables.iter().filter(|t| t.storage.is_empty()).map(|t| &t.name).collect::<Vec<_>>()
    );

    let edges = session
        .query(dialect::schema_map_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: schema map: {e}"));
    let edges = dialect::parse_relationships(&edges);
    assert!(
        !edges.is_empty(),
        "{flavor}: no relationships; the fixture declares foreign keys"
    );
    for edge in &edges {
        assert!(
            tables.iter().any(|t| t.name == edge.from_table),
            "{flavor}: edge from unknown table {}", edge.from_table
        );
        // Only a foreign key names a target column. A `json` edge points at
        // the shadow table a document was mapped onto, which is a table and not
        // a column of one — the map anchors it at the table's head.
        if edge.kind == "fk" {
            assert!(
                !edge.to_column.is_empty(),
                "{flavor}: foreign key {} names no target column, so it cannot be anchored",
                edge.constraint
            );
        }
    }

    let config = session
        .query(dialect::config_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: config: {e}"));
    let config = dialect::parse_config(&config);
    assert!(!config.is_empty(), "{flavor}: no configuration reported");
    assert!(
        config.iter().any(|e| e.hot),
        "{flavor}: nothing reported as changeable while running"
    );
    assert!(
        config.iter().any(|e| !e.hot),
        "{flavor}: everything reported as changeable, which is not true of either server"
    );

    // Both servers report `synchronous_commit`, and the overview reads it by
    // bare key to say whether commits are durable when they return. A rename or
    // a removal would leave that field silently blank, which reads as "nothing
    // to worry about" — the wrong way for a durability indicator to fail.
    let durability = config
        .iter()
        .find(|e| e.key.eq_ignore_ascii_case("synchronous_commit"))
        .unwrap_or_else(|| panic!("{flavor}: no synchronous_commit in the configuration"));
    assert!(
        matches!(
            durability.value.trim().to_ascii_lowercase().as_str(),
            "on" | "off" | "local" | "remote_write" | "remote_apply"
        ),
        "{flavor}: synchronous_commit reads as {:?}, which the overview cannot interpret",
        durability.value
    );
    assert!(
        durability.hot,
        "{flavor}: synchronous_commit reported as needing a restart, so the console would \
         grey out an edit that in fact works"
    );

    // What the server has been running. Both panes of the activity page, and
    // the one place a difference between the two servers is not cosmetic: the
    // recent list is a *history* on RasterizedDB and an *instant* on
    // PostgreSQL, and only the first can be asserted to hold anything.
    let recent = session
        .query(&dialect::recent_statements_sql(flavor, 20))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: recent statements: {e}"));
    assert!(
        recent.column_index("statement").is_some(),
        "{flavor}: the recent-statement result has no `statement` column, so the page would \
         render a server that has run nothing"
    );
    let recent = dialect::parse_statement_records(&recent);
    if matches!(flavor, Flavor::Rasterized) {
        // Everything above just ran through this connection, so a server that
        // keeps a history cannot report an empty one.
        assert!(
            !recent.is_empty(),
            "{flavor}: nothing recorded, though this test has just run a dozen statements"
        );
        assert!(
            recent.iter().any(|r| !r.statement.is_empty()),
            "{flavor}: statements recorded with no text"
        );
        // The column the page exists for. A server that answered the statement
        // but left this blank would render a grid of dashes.
        assert!(
            recent.iter().all(|r| r.rows.is_some() && r.rows_scanned.is_some()),
            "{flavor}: a record came back without rows/rows_scanned, which the page reads as \
             `this server cannot answer that`"
        );
    }

    // The aggregate. On PostgreSQL this needs an extension that is usually not
    // loaded, and its absence is the page's own reported state rather than a
    // failure of the console — so it is noted and skipped rather than failing
    // a test run against a stock server.
    match session.query(dialect::statement_shapes_sql(flavor)).await {
        Ok(grid) => {
            assert!(
                grid.column_index("calls").is_some(),
                "{flavor}: the statement aggregate has no `calls` column"
            );
            let shapes = dialect::parse_statement_shapes(&grid);
            if matches!(flavor, Flavor::Rasterized) {
                assert!(!shapes.is_empty(), "{flavor}: no statement shapes recorded");
                assert!(
                    shapes.iter().all(|s| s.calls > 0),
                    "{flavor}: a shape recorded with no calls"
                );
                // Normalisation is what makes this an aggregate rather than a
                // second copy of the list above: the page's own repeated
                // `SHOW TABLES` must have folded into one row.
                assert!(
                    shapes.iter().map(|s| s.calls).sum::<u64>() >= shapes.len() as u64,
                    "{flavor}: fewer calls than shapes, which cannot happen"
                );
            }
        }
        Err(e) if matches!(flavor, Flavor::Postgres) => {
            eprintln!("{flavor}: pg_stat_statements is not loaded, so the aggregate pane would \
                       show its explanation rather than rows: {e}");
        }
        Err(e) => panic!("{flavor}: statement shapes: {e}"),
    }

    for table in &tables {
        let stats = session
            .query(&dialect::table_stats_sql(flavor, &table.name))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: table stats for {}: {e}", table.name));
        assert!(
            !dialect::parse_stats(&stats).is_empty(),
            "{flavor}: no statistics for {}", table.name
        );

        let columns = session
            .query(&dialect::columns_sql(flavor, &table.name))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: columns for {}: {e}", table.name));
        let columns = dialect::parse_columns(&columns);
        assert!(!columns.is_empty(), "{flavor}: no columns for {}", table.name);
        assert!(
            columns.iter().all(|c| !c.data_type.is_empty()),
            "{flavor}: a column of {} reported no type", table.name
        );

        // The page the data browser fetches, ordered the way it orders it.
        let key = (!table.primary_key.is_empty() && !table.primary_key.contains(','))
            .then(|| table.primary_key.clone());
        let page = session
            .query(&dialect::page_sql(&table.name, key.as_deref(), None, 10, 0))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: page of {}: {e}", table.name));
        assert_eq!(
            page.columns.len(),
            columns.len(),
            "{flavor}: {} pages {} columns but reports {}",
            table.name,
            page.columns.len(),
            columns.len()
        );

        let count = session
            .query(&dialect::count_sql(&table.name))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: count of {}: {e}", table.name));
        assert!(
            count.number(0, "n").is_some(),
            "{flavor}: count of {} did not come back as a number", table.name
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_postgres_statement_runs() {
    let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await else {
        eprintln!("RDB_STUDIO_TEST_PG is unset; skipping");
        return;
    };
    exercise(&session).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_rasterizeddb_statement_runs() {
    let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await else {
        eprintln!("RDB_STUDIO_TEST_RDB is unset; skipping");
        return;
    };
    exercise(&session).await;
}

/// The console's own writes, against whichever servers are configured.
///
/// It writes to a table it creates and drops, so it is safe to point at a real
/// database — but it is still a write, which is why it is behind the same
/// opt-in environment variables.
async fn exercise_writes(session: &Session) {
    let flavor = session.flavor;
    let table = "rdb_studio_write_probe";

    // `IF EXISTS`, so a clean run does not put an error in the server's log.
    let _ = session
        .execute(&format!("DROP TABLE IF EXISTS {}", dialect::quote_ident(table)))
        .await;
    session
        .execute(&format!(
            "CREATE TABLE {} (id BIGINT PRIMARY KEY, label TEXT, note TEXT)",
            dialect::quote_ident(table)
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: create: {e}"));

    // Exactly the statements `app` builds, quoting included: the key is written
    // as a string literal even for an integer column, which is the part that
    // would fail silently if either engine refused the cast.
    session
        .execute(&format!(
            "INSERT INTO {} ({}, {}) VALUES ({}, {})",
            dialect::quote_ident(table),
            dialect::quote_ident("id"),
            dialect::quote_ident("label"),
            dialect::quote_literal("1"),
            dialect::quote_literal("it's here")
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: insert: {e}"));

    session
        .execute(&format!(
            "UPDATE {} SET {} = {} WHERE {} = {}",
            dialect::quote_ident(table),
            dialect::quote_ident("label"),
            dialect::sql_value(Some("changed")),
            dialect::quote_ident("id"),
            dialect::quote_literal("1")
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: update: {e}"));

    session
        .execute(&format!(
            "UPDATE {} SET {} = {} WHERE {} = {}",
            dialect::quote_ident(table),
            dialect::quote_ident("note"),
            dialect::sql_value(None),
            dialect::quote_ident("id"),
            dialect::quote_literal("1")
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: update to null: {e}"));

    let page = session
        .query(&dialect::page_sql(table, Some("id"), None, 10, 0))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: read back: {e}"));
    assert_eq!(page.value(0, "label"), Some("changed"), "{flavor}: the update did not land");
    assert_eq!(
        page.value(0, "note"),
        None,
        "{flavor}: NULL came back as something other than NULL"
    );

    session
        .execute(&format!(
            "DELETE FROM {} WHERE {} = {}",
            dialect::quote_ident(table),
            dialect::quote_ident("id"),
            dialect::quote_literal("1")
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: delete: {e}"));

    let page = session
        .query(&dialect::page_sql(table, Some("id"), None, 10, 0))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: read back after delete: {e}"));
    assert!(page.rows.is_empty(), "{flavor}: the delete did not land");

    session
        .execute(&format!("DROP TABLE {}", dialect::quote_ident(table)))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: drop: {e}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_accepts_the_consoles_writes() {
    let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await else {
        return;
    };
    exercise_writes(&session).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rasterizeddb_accepts_the_consoles_writes() {
    let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await else {
        return;
    };
    exercise_writes(&session).await;
}

// ---------------------------------------------------------------------------
// Backups
// ---------------------------------------------------------------------------

/// The backup status query, which both dialects answer and which the page
/// branches on. Runs wherever a server is configured at all.
async fn exercise_backup_status(session: &Session) {
    let flavor = session.flavor;
    let status = session
        .query(dialect::backup_status_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: backup status: {e}"));
    let status = dialect::parse_stats(&status);
    assert!(!status.is_empty(), "{flavor}: backup status reported nothing");

    // The page decides between three completely different screens on these two,
    // so an absent one renders as "not configured" whatever the truth is.
    for metric in ["feature_compiled", "configured"] {
        assert!(
            status.iter().any(|s| s.metric == metric),
            "{flavor}: backup status has no `{metric}`"
        );
    }

    let configured = status
        .iter()
        .find(|s| s.metric == "configured")
        .map(|s| s.value.trim() == "true")
        .unwrap_or(false);

    if !configured {
        return;
    }

    // A configured destination has to answer the health questions too.
    for metric in ["archiving_suspended", "source_directory"] {
        assert!(
            status.iter().any(|s| s.metric == metric),
            "{flavor}: a configured destination reports no `{metric}`"
        );
    }

    if let Some(sql) = dialect::backups_sql(flavor) {
        let grid = session
            .query(sql)
            .await
            .unwrap_or_else(|e| panic!("{flavor}: backup catalogue: {e}"));
        for entry in dialect::parse_backups(&grid) {
            assert!(!entry.id.is_empty(), "{flavor}: a catalogue entry has no id");
            assert!(
                entry.kind == "snapshot" || entry.kind == "wal",
                "{flavor}: unknown catalogue kind {:?}",
                entry.kind
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_backup_status_is_readable() {
    if let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await {
        exercise_backup_status(&session).await;
    }
    if let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await {
        exercise_backup_status(&session).await;
    }
}

/// A full round trip: snapshot, archive, list, restore, read the report.
///
/// Behind its own variable because it **uploads to and reads from the
/// configured bucket**. Pointed at real S3 that costs money and puts objects in
/// someone's production prefix, which is not a thing a test suite should do
/// because it happened to have credentials.
///
/// `RDB_STUDIO_TEST_RESTORE_DIR` names a directory the test may write a
/// database into; it is removed first, so give it one of its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_backup_can_be_taken_and_restored() {
    if std::env::var("RDB_STUDIO_TEST_BACKUP").is_err() {
        eprintln!("RDB_STUDIO_TEST_BACKUP is unset; skipping the round trip");
        return;
    }
    let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await else {
        panic!("RDB_STUDIO_TEST_BACKUP is set but RDB_STUDIO_TEST_RDB is not");
    };
    let target_dir = std::env::var("RDB_STUDIO_TEST_RESTORE_DIR")
        .expect("RDB_STUDIO_TEST_BACKUP needs RDB_STUDIO_TEST_RESTORE_DIR");

    session
        .execute(dialect::backup_now_sql())
        .await
        .unwrap_or_else(|e| panic!("BACKUP DATABASE: {e}"));
    session
        .execute(dialect::archive_wal_sql())
        .await
        .unwrap_or_else(|e| panic!("BACKUP WAL: {e}"));

    let catalogue = session
        .query(dialect::backups_sql(Flavor::Rasterized).unwrap())
        .await
        .unwrap_or_else(|e| panic!("SHOW BACKUPS: {e}"));
    let entries = dialect::parse_backups(&catalogue);
    let snapshot = entries
        .iter()
        .find(|entry| entry.is_snapshot())
        .expect("a snapshot was just taken and is not in the catalogue");
    assert!(snapshot.compressed_bytes > 0, "the snapshot stored no bytes");
    assert!(
        snapshot.ratio() >= 1.0,
        "a snapshot cannot compress to larger than itself: {:?}",
        snapshot
    );

    // Latest: nothing should be discarded.
    let _ = std::fs::remove_dir_all(&target_dir);
    let sql = dialect::restore_sql(&target_dir, &RestoreTarget::Latest).unwrap();
    let grid = session
        .query(&sql)
        .await
        .unwrap_or_else(|e| panic!("restore to latest: {e}"));
    let report = dialect::parse_restore_report(&grid).expect("a restore reported no rows");
    assert_eq!(report.target_dir.trim_end_matches('/'), target_dir.trim_end_matches('/'));
    assert!(report.files_restored > 0, "a restore that wrote no files: {report:?}");
    assert_eq!(
        report.records_discarded, 0,
        "restoring to latest must not discard anything: {report:?}"
    );

    // A snapshot target stops at the snapshot, which is the whole reason the
    // console warns about it — so the report has to actually say so.
    let pinned = format!("{}-pinned", target_dir.trim_end_matches('/'));
    let _ = std::fs::remove_dir_all(&pinned);
    let sql =
        dialect::restore_sql(&pinned, &RestoreTarget::Snapshot(snapshot.id.clone())).unwrap();
    let grid = session
        .query(&sql)
        .await
        .unwrap_or_else(|e| panic!("restore from snapshot: {e}"));
    let pinned_report = dialect::parse_restore_report(&grid).expect("no rows");
    assert_eq!(pinned_report.snapshot_id, snapshot.id);

    // Restoring over the live directory is refused, and that refusal is what
    // the console's "the running database is not touched" promise rests on.
    let source = session
        .query(dialect::backup_status_sql(Flavor::Rasterized))
        .await
        .unwrap();
    let source = dialect::parse_stats(&source);
    let live = source
        .iter()
        .find(|s| s.metric == "source_directory")
        .map(|s| s.value.clone())
        .expect("no source_directory reported");
    let sql = dialect::restore_sql(&live, &RestoreTarget::Latest).unwrap();
    let error = session
        .query(&sql)
        .await
        .expect_err("restoring over the live directory must be refused");
    assert!(
        error.contains("cannot write into the directory"),
        "the refusal must say why: {error}"
    );

    let _ = std::fs::remove_dir_all(&target_dir);
    let _ = std::fs::remove_dir_all(&pinned);
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

/// The two statements the replication page reads.
///
/// Both servers answer them, and a server with no replication configured is
/// not a failure case — it is the ordinary one, and the page has to render it.
async fn exercise_replication(session: &Session) {
    let flavor = session.flavor;

    let grid = session
        .query(dialect::replication_stats_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: replication stats: {e}"));
    assert!(
        grid.column_index("metric").is_some(),
        "{flavor}: replication stats did not come back as a status result — this is what an \
         older build's runtime-parameter answer looks like"
    );
    let stats = dialect::parse_stats(&grid);
    for metric in ["active", "peers", "mode"] {
        assert!(
            stats.iter().any(|s| s.metric == metric),
            "{flavor}: replication stats has no `{metric}`; the tiles read it by name"
        );
    }

    let grid = session
        .query(dialect::replication_peers_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: replication peers: {e}"));
    assert!(
        grid.column_index("peer").is_some(),
        "{flavor}: the peer list has no `peer` column"
    );
    for peer in dialect::parse_peers(&grid) {
        assert!(!peer.name.is_empty(), "{flavor}: a peer with no name");
    }

    // The count the summary shows has to agree with the list it shows beside
    // it, or the page contradicts itself.
    let claimed: usize = stats
        .iter()
        .find(|s| s.metric == "peers")
        .and_then(|s| s.value.trim().parse().ok())
        .unwrap_or(0);
    assert_eq!(
        claimed,
        dialect::parse_peers(&grid).len(),
        "{flavor}: the peer count and the peer list disagree"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_replication_status_is_readable() {
    if let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await {
        exercise_replication(&session).await;
    }
    if let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await {
        exercise_replication(&session).await;
    }
}

// ---------------------------------------------------------------------------
// Storage placement
// ---------------------------------------------------------------------------

/// The three statements the storage page reads.
///
/// A RasterizedDB server built without the `storage_pools` feature answers
/// these as runtime parameters rather than refusing them, which is exactly the
/// state the page detects and reports — so a missing `metric` column is *not*
/// asserted against for that flavour. What is asserted is that whatever comes
/// back is self-consistent: if the server does answer, every column the page
/// reads by name has to be there, and the figures have to agree with each
/// other.
async fn exercise_storage(session: &Session) {
    let flavor = session.flavor;

    let grid = session
        .query(dialect::storage_stats_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: storage stats: {e}"));

    let Some(_) = grid.column_index("metric") else {
        // The feature is off, or this is an older build. The page renders a
        // page about that rather than a page of blanks, so there is nothing
        // further to check.
        eprintln!("{flavor}: storage pools are not enabled on this server; skipping");
        return;
    };

    let stats = dialect::parse_stats(&grid);
    for metric in ["policy", "min_free_bytes", "locations", "tables", "next_placement"] {
        assert!(
            stats.iter().any(|s| s.metric == metric),
            "{flavor}: storage stats has no `{metric}`; the tiles read it by name"
        );
    }

    let grid = session
        .query(dialect::storage_locations_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: storage locations: {e}"));
    assert!(
        grid.column_index("location").is_some(),
        "{flavor}: the location list has no `location` column"
    );
    let locations = dialect::parse_storage_locations(&grid);
    assert!(!locations.is_empty(), "{flavor}: a database always has at least one directory");
    for location in &locations {
        assert!(!location.path.is_empty(), "{flavor}: a location with no path");
        assert!(location.weight >= 1, "{flavor}: {} has weight 0", location.path);
        if location.space_known() {
            assert!(
                location.available_bytes <= location.total_bytes,
                "{flavor}: {} reports more free than it has",
                location.path
            );
        }
    }
    assert_eq!(
        locations.iter().filter(|l| l.is_root).count(),
        1,
        "{flavor}: exactly one directory is the database's own"
    );

    // The count in the summary has to agree with the list beside it, or the
    // page contradicts itself.
    let claimed: usize = stats
        .iter()
        .find(|s| s.metric == "locations")
        .and_then(|s| s.value.trim().parse().ok())
        .unwrap_or(0);
    assert_eq!(claimed, locations.len(), "{flavor}: the location count and the list disagree");

    let grid = session
        .query(dialect::storage_placements_sql(flavor))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: storage placements: {e}"));
    assert!(
        grid.column_index("table_name").is_some(),
        "{flavor}: the placement list has no `table_name` column"
    );
    for placement in dialect::parse_storage_placements(&grid) {
        assert!(!placement.table.is_empty(), "{flavor}: a placement with no table");
        // Every table has to resolve to a directory the pool actually lists, or
        // the page shows a table sitting somewhere that is not on screen.
        assert!(
            locations.iter().any(|l| l.path == placement.location),
            "{flavor}: {} is placed in {}, which is not a listed location",
            placement.table,
            placement.location
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_storage_layout_is_readable() {
    if let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await {
        exercise_storage(&session).await;
    }
    if let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await {
        exercise_storage(&session).await;
    }
}

// ---------------------------------------------------------------------------
// Statements the console writes across several lines
// ---------------------------------------------------------------------------

/// The console *generates* multi-line SQL — the FORMAT button puts every clause
/// on its own line, and every statement template is written that way — so it is
/// the right place to guard against a server that parses those differently from
/// the single-line form.
///
/// This is not hypothetical. A clause splitter that searched for the literal
/// `" WHERE "` matched nothing when a newline stood where the space was
/// expected, and `SELECT … FROM t\nWHERE x = 1` **returned every row**. A
/// formatted `DELETE` or `UPDATE` in the same shape is the same bug pointed at
/// a write.
async fn exercise_multiline(session: &Session) {
    let flavor = session.flavor;
    let table = "rdb_studio_multiline_probe";
    let quoted = dialect::quote_ident(table);

    let _ = session.execute(&format!("DROP TABLE IF EXISTS {quoted}")).await;
    session
        .execute(&format!("CREATE TABLE {quoted} (id BIGINT PRIMARY KEY, name TEXT)"))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: create: {e}"));
    session
        .execute(&format!(
            "INSERT INTO {quoted} (id, name) VALUES ('1', 'a'), ('2', 'b'), ('3', 'c')"
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: seed: {e}"));

    // A predicate that is dropped rather than applied returns everything, which
    // looks like a working query until you count the rows.
    let selected = session
        .query(&format!("SELECT \"id\", \"name\"\nFROM {quoted}\nWHERE \"id\" = '2'"))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: multi-line select: {e}"));
    assert_eq!(
        selected.rows.len(),
        1,
        "{flavor}: a newline before WHERE dropped the predicate — {} rows came back",
        selected.rows.len()
    );
    assert_eq!(selected.value(0, "id"), Some("2"));

    // The same shape pointed at a write. A dropped predicate here is the whole
    // table.
    session
        .execute(&format!(
            "UPDATE {quoted}\nSET \"name\" = 'B'\nWHERE \"id\" = '2'"
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: multi-line update: {e}"));
    let after = session
        .query(&format!("SELECT \"id\", \"name\" FROM {quoted} ORDER BY \"id\""))
        .await
        .unwrap();
    assert_eq!(after.value(0, "name"), Some("a"), "{flavor}: the UPDATE hit row 1 as well");
    assert_eq!(after.value(1, "name"), Some("B"));
    assert_eq!(after.value(2, "name"), Some("c"), "{flavor}: the UPDATE hit row 3 as well");

    session
        .execute(&format!("DELETE FROM {quoted}\nWHERE \"id\" = '3'"))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: multi-line delete: {e}"));
    let after = session
        .query(&format!("SELECT \"id\" FROM {quoted} ORDER BY \"id\""))
        .await
        .unwrap();
    assert_eq!(
        after.rows.len(),
        2,
        "{flavor}: a newline before WHERE turned a DELETE into a whole-table delete"
    );

    session
        .execute(&format!("DROP TABLE {quoted}"))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: drop: {e}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_line_statements_mean_the_same_thing() {
    if let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await {
        exercise_multiline(&session).await;
    }
    if let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await {
        exercise_multiline(&session).await;
    }
}
