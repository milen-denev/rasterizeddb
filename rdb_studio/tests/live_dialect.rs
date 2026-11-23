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
//! `RDB_STUDIO_TEST_RDB` may also name the server's native protocol, which
//! runs the same suite over it: `rdb://user:pw@127.0.0.1:61170?login=password`,
//! or a socket file, `rdb://user@/tmp/rdb.sock?login=trust` (`login` is
//! `password`, `peer` or `trust`; `password` by default).
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
    if let Some(rest) = url.strip_prefix("rdb://") {
        return native_profile(rest);
    }
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
        password_store: None,
        tls: TlsMode::Disable,
        transport: rdb_studio::model::Transport::Wire,
        login: rdb_studio::model::NativeLogin::Password,
        verify_server: false,
        ca_file: String::new(),
    }
}

/// A native-protocol profile from the part of an `rdb://` URL after the scheme.
fn native_profile(rest: &str) -> Connection {
    let (rest, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (credentials, place) = rest.rsplit_once('@').unwrap_or(("", rest));
    let (user, password) = credentials.split_once(':').unwrap_or((credentials, ""));
    let (host, port) = if place.starts_with('/') {
        (place.to_string(), rdb_studio::model::NATIVE_PORT)
    } else {
        let (host, port) = place.split_once(':').unwrap_or((place, "61170"));
        (
            host.to_string(),
            port.trim_end_matches('/').parse().unwrap_or(61170),
        )
    };
    let login = match query.split('&').find_map(|kv| kv.strip_prefix("login=")) {
        Some("trust") => rdb_studio::model::NativeLogin::Trust,
        Some("peer") => rdb_studio::model::NativeLogin::Peer,
        _ => rdb_studio::model::NativeLogin::Password,
    };
    Connection {
        name: "test".to_string(),
        flavor: Flavor::Rasterized,
        host,
        port,
        database: String::new(),
        user: user.to_string(),
        password: password.to_string(),
        save_password: false,
        password_store: None,
        tls: TlsMode::Disable,
        transport: rdb_studio::model::Transport::Native,
        login,
        verify_server: false,
        ca_file: String::new(),
    }
}

/// Taken by every test here for its whole length. They share one database,
/// and several create and drop tables while another lists every table and
/// counts each one: run together, the count meets a table dropped after it
/// was listed ("Table not found").
async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    LOCK.lock().await
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
    // The connection asking is one of the current database's sessions, over
    // the native protocol as over the wire one (native connections were not
    // counted: `sessions` 0 while one asked).
    let current = databases.iter().find(|d| d.is_current).expect("a current database");
    assert!(
        current.sessions >= 1,
        "{flavor} over {:?}: the asking connection is not among `{}`'s sessions",
        session.transport,
        current.name
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
    // It is a dropdown on both, offering the spelling its value is in.
    let offered: Vec<String> = durability.choices().iter().map(|c| c.to_string()).collect();
    assert!(
        offered.iter().any(|c| c.eq_ignore_ascii_case(durability.value.trim())),
        "{flavor}: synchronous_commit = {:?} is none of the choices {:?}",
        durability.value,
        offered
    );
    // Every choice-valued setting's current value is one of its choices, or
    // the dropdown opens on nothing. RasterizedDB's `vacuum.enabled` reads
    // `per table`, which is shown as it is rather than as a choice.
    for entry in config.iter().filter(|e| e.editable() && !e.choices().is_empty()) {
        if entry.qualified() == "vacuum.enabled" {
            continue;
        }
        assert!(
            rdb_studio::model::choice_of(&entry.choices(), &entry.value).is_some(),
            "{flavor}: {} = {:?} matches none of {:?}",
            entry.qualified(),
            entry.value,
            entry.choices()
        );
    }
    if matches!(flavor, Flavor::Postgres) {
        assert!(
            config.iter().any(|e| !e.hot && e.restart_settable),
            "{flavor}: no postmaster setting is offered for saving, though ALTER SYSTEM saves them"
        );
    }

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

    // The other tests here create and drop `rdb_studio_*_probe` tables on the
    // same database, in parallel with this one, so one listed above can be
    // gone by the time it is asked about. They are not the fixture.
    for table in tables.iter().filter(|t| !(t.name.starts_with("rdb_studio_") && t.name.ends_with("_probe"))) {
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
            .query(&dialect::page_sql(&table.name, None, key.as_deref(), None, 10, 0))
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
            .query(&dialect::count_sql(&table.name, None))
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
    let _serial = serial().await;
    let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await else {
        eprintln!("RDB_STUDIO_TEST_PG is unset; skipping");
        return;
    };
    exercise(&session).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_rasterizeddb_statement_runs() {
    let _serial = serial().await;
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
        .query(&dialect::page_sql(table, None, Some("id"), None, 10, 0))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: read back: {e}"));
    assert_eq!(page.value(0, "label"), Some("changed"), "{flavor}: the update did not land");

    // The data browser's filter, with a comment that must end before the
    // page's ORDER BY and LIMIT.
    let filter = dialect::RowFilter::parse("label = 'changed' -- the edited row")
        .unwrap()
        .unwrap();
    let filtered = session
        .query(&dialect::page_sql(table, Some(&filter), Some("id"), None, 10, 0))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: filtered page: {e}"));
    assert_eq!(filtered.rows.len(), 1, "{flavor}: the filter kept {:?}", filtered.rows);
    let counted = session
        .query(&dialect::count_sql(table, Some(&filter)))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: filtered count: {e}"));
    assert_eq!(counted.number(0, "n"), Some(1), "{flavor}");
    let none = dialect::RowFilter::parse("label <> label").unwrap().unwrap();
    let empty = session
        .query(&dialect::page_sql(table, Some(&none), Some("id"), None, 10, 0))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: empty filtered page: {e}"));
    assert!(empty.rows.is_empty(), "{flavor}");
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
        .query(&dialect::page_sql(table, None, Some("id"), None, 10, 0))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: read back after delete: {e}"));
    assert!(page.rows.is_empty(), "{flavor}: the delete did not land");

    // The column dialog's script: a type change, NOT NULL, and a rename, in
    // the order it writes them, then a drop. On an integer column, whose
    // widening both engines take without a USING clause and report plainly
    // (RasterizedDB lists a VARCHAR(n) as TEXT).
    session
        .execute(&format!(
            "ALTER TABLE {} ADD COLUMN {} INTEGER",
            dialect::quote_ident(table),
            dialect::quote_ident("qty")
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: add column: {e}"));
    let info = dialect::parse_columns(
        &session
            .query(&dialect::columns_sql(flavor, table))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: columns: {e}")),
    )
    .into_iter()
    .find(|c| c.name == "qty")
    .expect("the qty column");
    let mut edit = rdb_studio::app::ColumnEdit::of(&info);
    edit.name = "quantity".into();
    edit.data_type = "BIGINT".into();
    edit.nullable = false;
    session
        .execute(&edit.statements(table).unwrap())
        .await
        .unwrap_or_else(|e| panic!("{flavor}: edit column: {e}"));
    let columns = dialect::parse_columns(
        &session
            .query(&dialect::columns_sql(flavor, table))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: columns after edit: {e}")),
    );
    let quantity = columns
        .iter()
        .find(|c| c.name == "quantity")
        .unwrap_or_else(|| panic!("{flavor}: no renamed column in {columns:?}"));
    assert!(!quantity.nullable, "{flavor}: NOT NULL did not land: {quantity:?}");
    assert!(
        quantity.data_type.eq_ignore_ascii_case("BIGINT") || quantity.data_type == "int8",
        "{flavor}: the type did not land: {quantity:?}"
    );
    assert!(!columns.iter().any(|c| c.name == "qty"), "{flavor}: {columns:?}");
    session
        .execute(&format!(
            "ALTER TABLE {} DROP COLUMN {}",
            dialect::quote_ident(table),
            dialect::quote_ident("note")
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: drop column: {e}"));
    let columns = dialect::parse_columns(
        &session
            .query(&dialect::columns_sql(flavor, table))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: columns after drop: {e}")),
    );
    assert!(!columns.iter().any(|c| c.name == "note"), "{flavor}: {columns:?}");

    session
        .execute(&format!("DROP TABLE {}", dialect::quote_ident(table)))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: drop: {e}"));
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_accepts_the_consoles_writes() {
    let _serial = serial().await;
    let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await else {
        return;
    };
    exercise_writes(&session).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn rasterizeddb_accepts_the_consoles_writes() {
    let _serial = serial().await;
    let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await else {
        return;
    };
    exercise_writes(&session).await;
}

/// The index pane's statements: every kind the dialog writes, read back by
/// the shared parser, and removed the way the pane removes each.
///
/// The ownership check is the one that matters. RasterizedDB answers both
/// `DROP INDEX <pkey>` and `ALTER TABLE … DROP CONSTRAINT <pkey>` with success
/// and keeps the key, so a primary key read as anything else would put a
/// button on screen that reports doing what it did not.
async fn exercise_indexes(session: &Session) {
    use rdb_studio::app::{IndexKind, IndexMethod, NewIndex, create_index_sql};
    use rdb_studio::model::IndexOwner;

    let flavor = session.flavor;
    let table = "rdb_studio_index_probe";
    let quoted = dialect::quote_ident(table);
    let _ = session.execute(&format!("DROP TABLE IF EXISTS {quoted}")).await;
    session
        .execute(&format!(
            "CREATE TABLE {quoted} (id BIGINT PRIMARY KEY, email TEXT UNIQUE, \
             customer_id BIGINT, placed_at TIMESTAMP, doc JSONB)"
        ))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: create: {e}"));

    let forms = [
        NewIndex { name: "probe_cs".into(), columns: vec!["customer_id".into(), "placed_at".into()], ..NewIndex::new("doc".into()) },
        NewIndex { name: "probe_hash".into(), method: IndexMethod::Hash, columns: vec!["customer_id".into()], ..NewIndex::new("doc".into()) },
        NewIndex { name: "probe_gin".into(), kind: IndexKind::Document, path_ops: true, ..NewIndex::new("doc".into()) },
        NewIndex { name: "probe_path".into(), kind: IndexKind::Path, path: "user.id".into(), ..NewIndex::new("doc".into()) },
        // No name: the server derives one, and the list must still show it.
        NewIndex { columns: vec!["placed_at".into()], method: IndexMethod::Btree, ..NewIndex::new("doc".into()) },
    ];
    for form in &forms {
        let sql = create_index_sql(table, form).unwrap();
        session.execute(&sql).await.unwrap_or_else(|e| panic!("{flavor}: {sql}: {e}"));
    }

    let read = || async {
        let columns = dialect::parse_columns(
            &session.query(&dialect::columns_sql(flavor, table)).await.unwrap(),
        );
        let primary: Vec<&str> =
            columns.iter().filter(|c| c.primary_key).map(|c| c.name.as_str()).collect();
        let primary: Vec<String> = primary.iter().map(|s| s.to_string()).collect();
        let grid = session
            .query(&dialect::indexes_sql(flavor, table))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: indexes: {e}"));
        dialect::parse_indexes(&grid, &primary.iter().map(String::as_str).collect::<Vec<_>>())
    };
    let indexes = read().await;
    let find = |name: &str| {
        indexes
            .iter()
            .find(|i| i.name == name)
            .unwrap_or_else(|| panic!("{flavor}: no index {name} in {:?}", indexes))
    };

    let primary: Vec<_> = indexes.iter().filter(|i| i.owner == IndexOwner::Primary).collect();
    assert_eq!(primary.len(), 1, "{flavor}: the primary key's index, once: {:?}", indexes);
    assert_eq!(primary[0].columns, "id");
    let unique: Vec<_> = indexes.iter().filter(|i| i.owner == IndexOwner::Unique).collect();
    assert_eq!(unique.len(), 1, "{flavor}: the UNIQUE email's index, once: {:?}", indexes);
    assert_eq!(unique[0].columns, "email");

    assert_eq!(find("probe_cs").method, "btree");
    assert_eq!(find("probe_cs").columns, "customer_id, placed_at");
    assert_eq!(find("probe_hash").kind(), "hash");
    assert_eq!(find("probe_gin").kind(), "gin");
    assert!(find("probe_gin").columns.contains("jsonb_path_ops"), "{flavor}: {:?}", find("probe_gin"));
    assert_eq!(find("probe_path").kind(), "path", "{flavor}: {:?}", find("probe_path"));
    assert_eq!(indexes.len(), 7, "{flavor}: {:?}", indexes);
    for index in &indexes {
        assert_ne!(index.method, "", "{flavor}: {} read with no method: {}", index.name, index.definition);
    }

    // Removed as the pane removes each: the declared one by DROP INDEX, the
    // unique one with its constraint.
    session.execute(&dialect::drop_index_sql("probe_hash")).await.unwrap();
    session.execute(&dialect::drop_constraint_sql(table, &unique[0].name)).await.unwrap();
    let after = read().await;
    assert!(after.iter().all(|i| i.name != "probe_hash"), "{flavor}: DROP INDEX left it: {:?}", after);
    assert!(
        after.iter().all(|i| i.owner != IndexOwner::Unique),
        "{flavor}: DROP CONSTRAINT left the unique index: {:?}",
        after
    );
    assert_eq!(after.len(), 5, "{flavor}: {:?}", after);

    // The primary key goes the same way. A RasterizedDB server before fc3849c
    // answered this with success and kept the key; the pane checks for that
    // after every drop, and this pins the server that no longer does it.
    let key = primary[0].name.clone();
    assert!(
        session.execute(&dialect::drop_index_sql(&key)).await.is_err(),
        "{flavor}: DROP INDEX of the primary key's index was accepted"
    );
    session.execute(&dialect::drop_constraint_sql(table, &key)).await.unwrap();
    let after = read().await;
    assert!(
        after.iter().all(|i| i.name != key),
        "{flavor}: DROP CONSTRAINT {key} reported success and kept it: {:?}",
        after
    );

    session.execute(&format!("DROP TABLE {quoted}")).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_index_pane_reads_and_writes_both_servers() {
    let _serial = serial().await;
    if let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await {
        exercise_indexes(&session).await;
    }
    if let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await {
        exercise_indexes(&session).await;
    }
}

/// A partitioned table made by the new-table dialog, as the table pane sees
/// it. RasterizedDB only; see `views::partition_section` for why.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_table_is_created_and_listed() {
    let _serial = serial().await;
    use rdb_studio::app::{NewColumn, Partitioning, create_table_with};
    let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await else {
        return;
    };
    let table = "rdb_studio_partition_probe";
    let quoted = dialect::quote_ident(table);
    let _ = session.execute(&format!("DROP TABLE IF EXISTS {quoted}")).await;

    let mut tenant = NewColumn::key(Flavor::Rasterized);
    tenant.name = "tenant_id".into();
    let mut id = NewColumn::key(Flavor::Rasterized);
    id.name = "id".into();
    let mut body = NewColumn::new(Flavor::Rasterized);
    body.rename("body".into(), Flavor::Rasterized);
    let partitioning = Partitioning { enabled: true, key: vec![0], count: "16".into() };
    let sql = create_table_with(table, &[tenant, id, body], &partitioning).unwrap();
    session.execute(&sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    session
        .execute(&format!("INSERT INTO {quoted} (tenant_id, id, body) VALUES (42, 1, 'x'), (7, 2, 'y')"))
        .await
        .unwrap();

    let stats = dialect::parse_stats(
        &session.query(&dialect::table_stats_sql(Flavor::Rasterized, table)).await.unwrap(),
    );
    let key = stats.iter().find(|s| s.metric == "partition_key").map(|s| s.value.as_str());
    assert_eq!(key, Some("tenant_id"), "the pane finds a partitioned table by this metric");

    let partitions =
        dialect::parse_partitions(&session.query(&dialect::partitions_sql(table)).await.unwrap());
    assert_eq!(partitions.len(), 16);
    let rows: u64 = partitions.iter().map(|p| p.row_slots).sum();
    assert_eq!(rows, 2, "{:?}", partitions);
    assert!(partitions.iter().any(|p| p.materialized()));
    let share: f64 = partitions.iter().filter_map(|p| p.share()).sum();
    assert!((share - 1.0).abs() < 1e-9, "the ranges tile the hash space: {share}");

    // The pane offers ADD COLUMN and NEW INDEX on a partitioned table, because
    // the server applies both to every partition. Sent as the dialogs write
    // them, and checked by reading back as the pane does.
    use rdb_studio::app::{NewIndex, add_column_sql, create_index_sql};
    let mut note = NewColumn::new(Flavor::Rasterized);
    note.rename("note".into(), Flavor::Rasterized);
    let sql = add_column_sql(table, &note).unwrap();
    session.execute(&sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let columns = dialect::parse_columns(
        &session.query(&dialect::columns_sql(Flavor::Rasterized, table)).await.unwrap(),
    );
    assert!(columns.iter().any(|c| c.name == "note"), "ADD COLUMN on a partitioned table: {:?}", columns);

    let form = NewIndex { name: "partition_probe_body".into(), columns: vec!["body".into()], ..NewIndex::new("body".into()) };
    let sql = create_index_sql(table, &form).unwrap();
    session.execute(&sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let indexes = dialect::parse_indexes(
        &session.query(&dialect::indexes_sql(Flavor::Rasterized, table)).await.unwrap(),
        &[],
    );
    assert!(
        indexes.iter().any(|i| i.name == "partition_probe_body"),
        "CREATE INDEX on a partitioned table: {:?}",
        indexes
    );
    let both = session
        .query(&format!("SELECT count(*) FROM {quoted} WHERE body = 'x' AND note IS NULL"))
        .await
        .unwrap();
    assert_eq!(both.rows[0][0].as_deref(), Some("1"), "the new column and index read back: {:?}", both.rows);

    // The amplification note reads these two; before any sweep they say so
    // rather than claiming a count.
    let stats = dialect::parse_stats(
        &session.query(&dialect::table_stats_sql(Flavor::Rasterized, table)).await.unwrap(),
    );
    for metric in ["live_rows", "slots_per_live_row"] {
        assert!(stats.iter().any(|s| s.metric == metric), "SHOW TABLE STATS lacks {metric}");
    }

    session.execute(&format!("DROP TABLE {quoted}")).await.unwrap();
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
    let _serial = serial().await;
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
    let _serial = serial().await;
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
    let _serial = serial().await;
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
    let _serial = serial().await;
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
    let _serial = serial().await;
    if let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await {
        exercise_multiline(&session).await;
    }
    if let Some(session) = open("RDB_STUDIO_TEST_PG", Flavor::Postgres).await {
        exercise_multiline(&session).await;
    }
}

/// The native transport answers exactly what the wire transport does, cell
/// for cell: every panel is written against the wire's text, so a value the
/// native path rendered differently (a `jsonb` document, an array, a
/// `NUMERIC`'s scale, a time zone) would be a wrong value on screen, and in an
/// edit written back from it.
///
/// Needs both: `RDB_STUDIO_TEST_RDB` on the wire protocol and
/// `RDB_STUDIO_TEST_RDB_NATIVE` (an `rdb://` URL) on the same server.
#[tokio::test]
async fn the_native_and_wire_transports_answer_identically() {
    let _serial = serial().await;
    let (Ok(wire_url), Ok(native_url)) = (
        std::env::var("RDB_STUDIO_TEST_RDB"),
        std::env::var("RDB_STUDIO_TEST_RDB_NATIVE"),
    ) else {
        eprintln!("RDB_STUDIO_TEST_RDB and RDB_STUDIO_TEST_RDB_NATIVE are not both set; skipping");
        return;
    };
    assert!(
        !wire_url.starts_with("rdb://"),
        "RDB_STUDIO_TEST_RDB must be the wire protocol here"
    );
    let wire = conn::connect(profile(&wire_url, Flavor::Rasterized))
        .await
        .expect("wire");
    let native = conn::connect(profile(&native_url, Flavor::Rasterized))
        .await
        .expect("native");
    assert_eq!(native.transport, rdb_studio::model::Transport::Native);
    assert_eq!(
        wire.database, native.database,
        "the two name different databases"
    );

    let table = "studio_transport_parity";
    let setup = format!(
        "DROP TABLE IF EXISTS {table}; \
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, t TEXT, j JSONB, a TEXT[], n NUMERIC(12,3), \
           ts TIMESTAMPTZ, d DATE, b BYTEA, u UUID, f DOUBLE PRECISION, ok BOOLEAN, i INTEGER); \
         INSERT INTO {table} VALUES \
           (1, 'plain', '{{\"k\": [1, 2, {{\"x\": null}}], \"s\": \"q\\\"uote\"}}', '{{a,\"b c\",NULL}}', 1234.5, \
            '2026-11-19 10:15:30.123456+00', '2026-02-28', '\\x00ff10', \
            '0f8fad5b-d9cb-469f-a165-70867728950e', 0.1, true, -7), \
           (2, '', NULL, '{{}}', -0.001, NULL, NULL, '\\x', NULL, 1e300, false, NULL), \
           (3, NULL, '[]', NULL, 0, '1999-12-31 23:59:59+00', '1970-01-01', NULL, NULL, -2.5, NULL, 2147483647)"
    );
    wire.run(&setup).await.expect("setup");

    let statements = [
        format!("SELECT * FROM {table} ORDER BY id"),
        format!(
            "SELECT id, j->'k' AS k, j->>'s' AS s, a[2] AS second, n * 2 AS twice FROM {table} ORDER BY id"
        ),
        format!(
            "SELECT count(*) AS rows, sum(n) AS total, avg(i) AS mean, max(ts) AS latest FROM {table}"
        ),
        format!(
            "SELECT id, '1 day 02:03:04'::interval AS gap, ts::date AS day FROM {table} WHERE id = 1"
        ),
        format!("SHOW COLUMNS FROM {table}"),
        // Not `SHOW DATABASES`: its `sessions` counts pgwire sessions, so the
        // wire connection asking counts itself and a native one does not.
    ];
    for sql in &statements {
        let w = wire
            .run(sql)
            .await
            .unwrap_or_else(|e| panic!("wire: {sql}: {e}"));
        let n = native
            .run(sql)
            .await
            .unwrap_or_else(|e| panic!("native: {sql}: {e}"));
        assert_eq!(n, w, "the transports disagree on {sql}");
    }
    // Several statements at once, and a failure in the middle, report alike.
    let script = format!(
        "SELECT 1 AS one; UPDATE {table} SET t = t WHERE id = 2; SELECT * FROM missing_table"
    );
    let w = wire.run(&script).await;
    let n = native.run(&script).await;
    assert!(w.is_err() && n.is_err(), "{w:?} / {n:?}");
    assert_eq!(n.unwrap_err(), w.unwrap_err());

    wire.run(&format!("DROP TABLE {table}"))
        .await
        .expect("cleanup");
}

/// The editor's run: a row limit that keeps the first rows and counts the
/// rest, a block that stays open from one run to the next (on the editor's
/// own connection over native, so the panels do not see into it), notices,
/// and a statement stopped by its cancel while it waits for a lock.
async fn exercise_editor(variable: &str) {
    use rasterizeddb_client::TransactionStatus;
    use rdb_studio::model::{Outcome, Transport};

    let Some(a) = open(variable, Flavor::Rasterized).await else {
        return;
    };
    let b = open(variable, Flavor::Rasterized).await.expect("a second session");
    let native = a.transport == Transport::Native;
    let rows = |answer: &conn::EditorAnswer, i: usize| match &answer.result {
        Ok(outcomes) => match &outcomes[i] {
            Outcome::Rows(grid) => grid.clone(),
            other => panic!("{variable}: result {i} is not rows: {other:?}"),
        },
        Err(e) => panic!("{variable}: {e}"),
    };
    let run = |session: &Session, sql: &'static str, cap: Option<u64>| {
        let session = session.clone();
        async move {
            let canceller = session.canceller();
            session.run_editor(sql, cap, &canceller).await
        }
    };

    let _ = a.run("DROP TABLE IF EXISTS studio_editor_t").await;
    a.run("CREATE TABLE studio_editor_t (id BIGINT, label TEXT)")
        .await
        .unwrap();
    a.run(
        "INSERT INTO studio_editor_t (id, label) VALUES \
         (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i'),(10,'j')",
    )
    .await
    .unwrap();

    // The row limit keeps the first rows of each result and counts them all.
    let answer = run(
        &a,
        "SELECT id FROM studio_editor_t ORDER BY id; SELECT 1 AS one",
        Some(3),
    )
    .await;
    let first = rows(&answer, 0);
    assert_eq!(first.rows.len(), 3, "{variable}");
    assert_eq!(first.rows[2][0].as_deref(), Some("3"), "{variable}");
    assert_eq!(first.total, Some(10), "{variable}");
    let second = rows(&answer, 1);
    assert_eq!((second.rows.len(), second.total), (1, None), "{variable}");
    let whole = rows(&run(&a, "SELECT id FROM studio_editor_t", None).await, 0);
    assert_eq!((whole.rows.len(), whole.total), (10, None), "{variable}");

    // A block outlives its run, and a BEGIN inside it is a notice.
    let answer = run(
        &a,
        "BEGIN; INSERT INTO studio_editor_t (id, label) VALUES (11, 'k')",
        None,
    )
    .await;
    assert!(answer.result.is_ok(), "{variable}: {:?}", answer.result);
    if native {
        assert_eq!(answer.transaction, Some(TransactionStatus::InBlock), "{variable}");
        assert_eq!(a.editor_transaction(), Some(TransactionStatus::InBlock));
        // The panels' connection does not see the uncommitted row.
        let outside = a
            .query("SELECT count(*) AS n FROM studio_editor_t")
            .await
            .unwrap();
        assert_eq!(outside.number(0, "n"), Some(10), "{variable}");
    }
    let inside = rows(&run(&a, "SELECT count(*) AS n FROM studio_editor_t", None).await, 0);
    assert_eq!(inside.number(0, "n"), Some(11), "{variable}");
    let answer = run(&a, "BEGIN", None).await;
    assert!(answer.result.is_ok(), "{variable}: {:?}", answer.result);
    assert_eq!(answer.notices.len(), 1, "{variable}: {:?}", answer.notices);

    // The block takes the table; the other session's read waits for it until
    // its cancel stops it.
    let answer = run(&a, "LOCK TABLE studio_editor_t IN ACCESS EXCLUSIVE MODE", None).await;
    assert!(answer.result.is_ok(), "{variable}: {:?}", answer.result);
    if native {
        a.keep_editor_alive().await.expect("a ping inside the block");
    }
    let canceller = b.canceller();
    let waiting = {
        let (b, canceller) = (b.clone(), canceller.clone());
        tokio::spawn(async move {
            b.run_editor("SELECT id FROM studio_editor_t", None, &canceller)
                .await
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert!(!waiting.is_finished(), "{variable}: the read did not wait for the lock");
    let said = canceller.cancel().await.expect("the cancel is delivered");
    assert_eq!(said, "cancel requested", "{variable}");
    let stopped = tokio::time::timeout(std::time::Duration::from_secs(10), waiting)
        .await
        .unwrap_or_else(|_| panic!("{variable}: the cancelled read kept waiting"))
        .unwrap();
    let error = stopped.result.expect_err("the cancelled read succeeded");
    assert!(error.contains("cancel"), "{variable}: {error}");

    let answer = run(&a, "ROLLBACK", None).await;
    assert!(answer.result.is_ok(), "{variable}: {:?}", answer.result);
    if native {
        assert_eq!(answer.transaction, Some(TransactionStatus::Idle), "{variable}");
    }
    let after = a
        .query("SELECT count(*) AS n FROM studio_editor_t")
        .await
        .unwrap();
    assert_eq!(after.number(0, "n"), Some(10), "{variable}");

    // A cancel with nothing running says so.
    if native {
        let said = a.canceller().cancel().await.unwrap();
        assert!(said.starts_with("nothing was running"), "{said}");
    }
    let _ = a.run("DROP TABLE studio_editor_t").await;
}

#[tokio::test]
async fn the_editor_limits_cancels_and_keeps_its_block() {
    let _serial = serial().await;
    exercise_editor("RDB_STUDIO_TEST_RDB").await;
    exercise_editor("RDB_STUDIO_TEST_RDB_NATIVE").await;
}

/// The ROLES tab's statements: a role is created, listed without its hash,
/// logs in with its password, logs in with a new one after the change, and
/// is dropped.
#[tokio::test(flavor = "multi_thread")]
async fn a_role_is_created_repassworded_and_dropped() {
    let _serial = serial().await;
    let Ok(url) = std::env::var("RDB_STUDIO_TEST_RDB") else { return };
    let session = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await.unwrap();
    let flavor = session.flavor;
    let name = "studio_probe_role";
    let _ = session.execute(&format!("DROP ROLE IF EXISTS {}", dialect::quote_ident(name))).await;
    let _ = session.execute(&format!("DROP ROLE {}", dialect::quote_ident(name))).await;

    session
        .execute(&dialect::create_role_sql(name, "first'pw", true, false).unwrap())
        .await
        .unwrap_or_else(|e| panic!("{flavor}: create role: {e}"));
    let listed = session.query(dialect::roles_sql(flavor)).await.unwrap();
    assert!(
        listed.columns.iter().all(|c| !c.contains("password")),
        "{flavor}: the role list carries a hash: {:?}",
        listed.columns
    );
    let roles = dialect::parse_roles(&listed);
    let role = roles
        .iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("{flavor}: no new role in {roles:?}"));
    assert!(role.can_login && !role.superuser, "{role:?}");

    // Logging in as the role needs a listener that asks for a password: a
    // trust socket lets anyone in, and a password login refuses it.
    let logs_in_by_password = session.transport == rdb_studio::model::Transport::Wire
        || profile(&url, flavor).login == rdb_studio::model::NativeLogin::Password;
    let as_role = |password: &str| {
        let mut p = profile(&url, flavor);
        p.user = name.to_string();
        p.password = password.to_string();
        p.login = rdb_studio::model::NativeLogin::Password;
        p
    };
    if logs_in_by_password {
        conn::connect(as_role("first'pw"))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: the new role could not log in: {e}"));
    }
    session
        .execute(&dialect::role_password_sql(name, "second").unwrap())
        .await
        .unwrap_or_else(|e| panic!("{flavor}: alter role: {e}"));
    if logs_in_by_password {
        assert!(
            conn::connect(as_role("first'pw")).await.is_err(),
            "{flavor}: the old password still works"
        );
        conn::connect(as_role("second"))
            .await
            .unwrap_or_else(|e| panic!("{flavor}: the new password does not work: {e}"));
    }

    session
        .execute(&format!("DROP ROLE {}", dialect::quote_ident(name)))
        .await
        .unwrap_or_else(|e| panic!("{flavor}: drop role: {e}"));
    let roles = dialect::parse_roles(&session.query(dialect::roles_sql(flavor)).await.unwrap());
    assert!(!roles.iter().any(|r| r.name == name), "{flavor}: {roles:?}");
}

/// The data tab's CSV import, against a real server: values converted to
/// the column types, NULL and the empty string kept apart, and a file with a
/// value that does not convert refused whole.
#[tokio::test(flavor = "multi_thread")]
async fn a_csv_file_is_imported_whole_or_not_at_all() {
    let _serial = serial().await;
    let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await else { return };
    let flavor = session.flavor;
    let table = "studio_import_probe";
    let _ = session.execute(&format!("DROP TABLE IF EXISTS {}", dialect::quote_ident(table))).await;
    session
        .execute(&format!(
            "CREATE TABLE {} (id BIGINT PRIMARY KEY, label TEXT, amount NUMERIC(10,2))",
            dialect::quote_ident(table)
        ))
        .await
        .unwrap();
    let columns = dialect::parse_columns(
        &session.query(&dialect::columns_sql(flavor, table)).await.unwrap(),
    );
    let count = |session: Session| async move {
        session
            .query(&format!("SELECT count(*) AS n FROM {}", dialect::quote_ident(table)))
            .await
            .unwrap()
            .number(0, "n")
    };

    let bad = rdb_studio::import::parse("id,amount\n1,2.5\n2,not a number\n").unwrap();
    for script in rdb_studio::import::scripts(table, &bad, &columns).unwrap() {
        assert!(session.execute(&script.sql).await.is_err(), "{flavor}: a bad value went in");
    }
    assert_eq!(count(session.clone()).await, Some(0), "{flavor}: a refused file left rows");

    let good = rdb_studio::import::parse(
        "ID,label,amount\n1,\"a, \"\"quoted\"\" one\",12.50\n2,,\n3,\"\",0.01\n",
    )
    .unwrap();
    for script in rdb_studio::import::scripts(table, &good, &columns).unwrap() {
        session
            .execute(&script.sql)
            .await
            .unwrap_or_else(|e| panic!("{flavor}: import: {e}"));
    }
    let page = session
        .query(&dialect::page_sql(table, None, Some("id"), None, 10, 0))
        .await
        .unwrap();
    assert_eq!(page.rows.len(), 3, "{flavor}");
    assert_eq!(page.value(0, "label"), Some("a, \"quoted\" one"), "{flavor}");
    assert_eq!(page.value(0, "amount"), Some("12.50"), "{flavor}");
    assert_eq!(page.value(1, "label"), None, "{flavor}: an unquoted empty field is NULL");
    assert_eq!(page.value(1, "amount"), None, "{flavor}");
    assert_eq!(page.value(2, "label"), Some(""), "{flavor}: \"\" is the empty string");
    let _ = session.execute(&format!("DROP TABLE {}", dialect::quote_ident(table))).await;
}

/// The KEYS tab's statements: a paged scan that reaches every matching key
/// through its cursor, and each type read by its own command.
#[tokio::test(flavor = "multi_thread")]
async fn the_key_value_store_is_scanned_and_read() {
    let _serial = serial().await;
    let Some(session) = open("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await else { return };
    let keys: Vec<String> = (0..1200).map(|i| format!("studio:kv:{i}")).collect();
    // What a run that failed part-way left: RPUSH would add to it.
    session.execute("KV DEL 'studio:kv h' studio:kvl studio:kvz").await.unwrap();
    let mut mset = String::from("KV MSET");
    for key in &keys {
        mset.push_str(&format!(" {} v", dialect::quote_literal(key)));
    }
    session.execute(&mset).await.unwrap();
    session.execute("KV HSET 'studio:kv h' f1 'one value' f2 v2").await.unwrap();
    session.execute("KV RPUSH studio:kvl a b c").await.unwrap();
    session.execute("KV ZADD studio:kvz 2 b 1 a").await.unwrap();
    session.execute("KV EXPIRE studio:kvl 600").await.unwrap();

    let mut found = std::collections::HashSet::new();
    let mut cursor = "0".to_string();
    let mut pages = 0;
    loop {
        let page = session
            .query(&dialect::kv_scan_sql(&cursor, "studio:kv:*", 100))
            .await
            .unwrap();
        let (next, page_keys) = dialect::parse_kv_scan(&page);
        found.extend(page_keys);
        pages += 1;
        assert!(pages < 1000, "the scan never ended");
        match next {
            Some(next) => cursor = next,
            None => break,
        }
    }
    assert_eq!(found.len(), keys.len(), "the scan missed keys");
    assert!(pages > 1, "a scan of 1,200 keys by 100 was one page");

    let described = session.run(&dialect::kv_describe_sql("studio:kv h")).await.unwrap();
    let rdb_studio::model::Outcome::Rows(kind) = &described[0] else { panic!("{described:?}") };
    assert_eq!(kind.rows[0][0].as_deref(), Some("hash"));
    // A hash's fields come back in no fixed order.
    let hash = session.query(&dialect::kv_value_sql("hash", "studio:kv h").unwrap()).await.unwrap();
    let mut fields: Vec<(Option<&str>, Option<&str>)> =
        (0..hash.rows.len()).map(|r| (hash.value(r, "field"), hash.value(r, "value"))).collect();
    fields.sort();
    assert_eq!(fields, [(Some("f1"), Some("one value")), (Some("f2"), Some("v2"))]);
    let described = session.run(&dialect::kv_describe_sql("studio:kvl")).await.unwrap();
    let rdb_studio::model::Outcome::Rows(ttl) = &described[1] else { panic!("{described:?}") };
    let ttl: i64 = ttl.rows[0][0].as_deref().unwrap().parse().unwrap();
    assert!((1..=600).contains(&ttl), "{ttl}");
    let list = session.query(&dialect::kv_value_sql("list", "studio:kvl").unwrap()).await.unwrap();
    assert_eq!(list.rows.len(), 3);
    let zset = session.query(&dialect::kv_value_sql("zset", "studio:kvz").unwrap()).await.unwrap();
    assert_eq!(zset.value(0, "member"), Some("a"));

    let mut del = String::from("KV DEL 'studio:kv h' studio:kvl studio:kvz");
    for key in &keys {
        del.push_str(&format!(" {}", dialect::quote_literal(key)));
    }
    session.execute(&del).await.unwrap();
}

/// A profile set to verify the server connects when the server's certificate
/// is signed by the profile's CA, and is refused (never falling back to
/// cleartext) when it is not. Needs a server started with a certificate the
/// CA signed (`--pg-cert`/`--pg-key`, `--rdb-cert`/`--rdb-key`):
/// `RDB_STUDIO_TEST_TLS_CA` is that CA, `RDB_STUDIO_TEST_TLS_OTHER_CA` one
/// that signed nothing the server holds.
#[tokio::test(flavor = "multi_thread")]
async fn a_verifying_profile_accepts_only_its_ca() {
    let _serial = serial().await;
    let (Ok(url), Ok(ca), Ok(other)) = (
        std::env::var("RDB_STUDIO_TEST_RDB"),
        std::env::var("RDB_STUDIO_TEST_TLS_CA"),
        std::env::var("RDB_STUDIO_TEST_TLS_OTHER_CA"),
    ) else {
        return;
    };
    let mut base = profile(&url, Flavor::Rasterized);
    if conn::is_socket_dir(&base.host) {
        return;
    }
    base.verify_server = true;
    base.tls = TlsMode::Require;

    let mut good = base.clone();
    good.ca_file = ca;
    let session = conn::connect(good)
        .await
        .unwrap_or_else(|e| panic!("{url}: refused with the right CA: {e}"));
    assert!(session.verified && session.encrypted, "{url}");
    assert_eq!(session.link(), "TLS VERIFIED");
    session.query("SELECT 1 AS one").await.unwrap();

    let mut bad = base.clone();
    bad.ca_file = other;
    let refused = conn::connect(bad).await.expect_err("a server the CA did not sign was believed");
    assert!(refused.contains("not accepted"), "{url}: {refused}");

    // Unverified, the same server is reached and said to be unverified.
    let mut plain = base;
    plain.verify_server = false;
    let session = conn::connect(plain).await.unwrap();
    assert!(!session.verified, "{url}");
}

/// Pages the data browser reads after the previous page's last key hold the
/// same rows, in the same order, as the same pages read at an offset: by an
/// integer key and by a text key holding quotes, ascending and descending,
/// with and without a filter. Driven through the browser's own state, so
/// the statements are the ones it sends.
async fn exercise_keyset(session: &Session) {
    use rdb_studio::app::{GridSort, Message, Studio};
    let flavor = session.flavor;
    let table = "studio_keyset_probe";
    let quoted = dialect::quote_ident(table);
    let _ = session.execute(&format!("DROP TABLE IF EXISTS {quoted}")).await;
    session
        .execute(&format!(
            "CREATE TABLE {quoted} (id BIGINT PRIMARY KEY, n INTEGER NOT NULL)"
        ))
        .await
        .unwrap();
    let values = (1..=25)
        .map(|i| format!("({}, {})", (i * 37) % 101, i % 4))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .execute(&format!("INSERT INTO {quoted} (id, n) VALUES {values}"))
        .await
        .unwrap();
    let text_table = "studio_keyset_text_probe";
    let text_quoted = dialect::quote_ident(text_table);
    let _ = session.execute(&format!("DROP TABLE IF EXISTS {text_quoted}")).await;
    session
        .execute(&format!(
            "CREATE TABLE {text_quoted} (name TEXT PRIMARY KEY, n INTEGER NOT NULL)"
        ))
        .await
        .unwrap();
    let values = (1..=23)
        .map(|i| format!("('o''k {:02}', {})", (i * 7) % 29, i % 3))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .execute(&format!("INSERT INTO {text_quoted} (name, n) VALUES {values}"))
        .await
        .unwrap();

    for (table, key) in [(table, "id"), (text_table, "name")] {
        for descending in [false, true] {
            for filter in [None, Some("n <> 2")] {
                let (mut studio, _) = Studio::new(None);
                studio.selected_table = Some(table.to_string());
                studio.columns = dialect::parse_columns(
                    &session.query(&dialect::columns_sql(flavor, table)).await.unwrap(),
                );
                assert!(
                    studio.columns.iter().any(|c| c.name == key && c.primary_key),
                    "{flavor}: {table} reports no key {key}"
                );
                studio.page_size = 4;
                studio.data_filter_applied =
                    filter.map(|f| dialect::RowFilter::parse(f).unwrap().unwrap());
                if descending {
                    studio.sort.insert(
                        format!("{}{}", rdb_studio::views::DATA_SCOPE, table),
                        GridSort { column: key.to_string(), descending: true },
                    );
                }
                let mut keyset_pages = 0;
                for page in 0..10 {
                    studio.data_page = page;
                    let (sql, signature) = studio.page_query().unwrap();
                    keyset_pages += usize::from(sql.contains(" > ") || sql.contains(" < "));
                    let got = session.query(&sql).await.unwrap_or_else(|e| panic!("{flavor}: {sql}: {e}"));
                    let offset = dialect::page_sql(
                        table,
                        studio.data_filter_applied.as_ref(),
                        Some(key),
                        descending.then_some((key, true)),
                        4,
                        page * 4,
                    );
                    let want = session.query(&offset).await.unwrap();
                    assert_eq!(got.rows, want.rows, "{flavor}: {sql}\n  vs {offset}");
                    let empty = got.rows.is_empty();
                    let _ = studio.update(Message::DataLoaded(page, signature, Ok((got, None))));
                    if empty {
                        break;
                    }
                }
                assert!(
                    keyset_pages >= 4,
                    "{flavor}: {table} desc={descending} filter={filter:?} paged by key {keyset_pages} times"
                );
            }
        }
    }
    let _ = session.execute(&format!("DROP TABLE {quoted}")).await;
    let _ = session.execute(&format!("DROP TABLE {text_quoted}")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn keyset_pages_hold_the_offset_pages_rows() {
    let _serial = serial().await;
    for (variable, flavor) in [
        ("RDB_STUDIO_TEST_PG", Flavor::Postgres),
        ("RDB_STUDIO_TEST_RDB", Flavor::Rasterized),
    ] {
        if let Some(session) = open(variable, flavor).await {
            exercise_keyset(&session).await;
        }
    }
}

/// Forwards connections to `target` until [`Proxy::cut`] closes every one
/// open, as a server restart or a firewall's idle cut would; new ones are
/// still accepted after.
struct Proxy {
    port: u16,
    open: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>>,
}

impl Proxy {
    async fn start(target: String) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("a port");
        let port = listener.local_addr().expect("an address").port();
        let open = std::sync::Arc::new(std::sync::Mutex::new(Vec::<tokio::task::AbortHandle>::new()));
        let kept = open.clone();
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let target = target.clone();
                let task = tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(&target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                kept.lock().unwrap().push(task.abort_handle());
            }
        });
        Self { port, open }
    }

    fn cut(&self) {
        for task in self.open.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

/// A wire session whose connection the server closed opens a new one on its
/// next call, for the panels and the editor alike; the editor is told that a
/// transaction open on the old one is gone. Before, every call failed with
/// "connection closed" until the profile was opened again.
async fn exercise_reopen(variable: &str, flavor: Flavor) {
    let Ok(url) = std::env::var(variable) else {
        eprintln!("{variable} is unset; skipping");
        return;
    };
    let mut profile = profile(&url, flavor);
    if profile.transport != rdb_studio::model::Transport::Wire || conn::is_socket_dir(&profile.host) {
        eprintln!("{variable} is not a wire profile over TCP; skipping");
        return;
    }
    let proxy = Proxy::start(format!("{}:{}", profile.host, profile.port)).await;
    profile.host = "127.0.0.1".to_string();
    profile.port = proxy.port;
    profile.tls = TlsMode::Disable;
    profile.verify_server = false;
    let session = conn::connect(profile).await.unwrap_or_else(|e| panic!("{variable}: {e}"));
    let one = |grid: &rdb_studio::model::Grid| grid.rows.first().and_then(|r| r.first().cloned().flatten());

    let grid = session.query("SELECT 1 AS n").await.unwrap_or_else(|e| panic!("{variable}: {e}"));
    assert_eq!(one(&grid).as_deref(), Some("1"), "{variable}");

    for round in 0..2 {
        proxy.cut();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !session.client().expect("a wire session").is_closed() {
            assert!(std::time::Instant::now() < deadline, "{variable}: the cut connection never closed");
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        if round == 0 {
            let grid = session
                .query("SELECT 2 AS n")
                .await
                .unwrap_or_else(|e| panic!("{variable}: a panel's query after the cut: {e}"));
            assert_eq!(one(&grid).as_deref(), Some("2"), "{variable}");
        } else {
            let canceller = session.canceller();
            let answer = session.run_editor("SELECT 3 AS n", None, &canceller).await;
            let outcomes = answer.result.unwrap_or_else(|e| panic!("{variable}: the editor after the cut: {e}"));
            match &outcomes[0] {
                rdb_studio::model::Outcome::Rows(grid) => assert_eq!(one(grid).as_deref(), Some("3"), "{variable}"),
                other => panic!("{variable}: {other:?}"),
            }
            assert_eq!(answer.notices.first().map(String::as_str), Some(conn::WIRE_REOPENED_NOTICE), "{variable}");
        }
    }
    // Opened once per cut, and the next run says nothing of it.
    let canceller = session.canceller();
    let answer = session.run_editor("SELECT 4 AS n", None, &canceller).await;
    assert!(answer.result.is_ok(), "{variable}: {:?}", answer.result);
    assert!(answer.notices.is_empty(), "{variable}: {:?}", answer.notices);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wire_session_reopens_a_connection_the_server_closed() {
    exercise_reopen("RDB_STUDIO_TEST_PG", Flavor::Postgres).await;
    exercise_reopen("RDB_STUDIO_TEST_RDB", Flavor::Rasterized).await;
}

// ---------------------------------------------------------------------------
// Several databases
// ---------------------------------------------------------------------------

/// The sidebar's database list, and switching to one of its entries, as the
/// console does them: `CREATE DATABASE` from the modal, the list from
/// `databases_sql`, and a switch as a new connection naming the database.
/// Over native the list used to hold only the connection's own database, and
/// a database created there lived in memory inside that one connection.
async fn exercise_databases(variable: &str) {
    let Ok(url) = std::env::var(variable) else { return };
    let base = profile(&url, Flavor::Rasterized);
    let session = conn::connect(base.clone()).await.expect("connects");
    let home = session.database.clone();
    let made = "studio_live_other";
    let quoted = dialect::quote_ident(made);

    let listed = |session: &Session| {
        let session = session.clone();
        async move {
            let grid = session
                .query(dialect::databases_sql(Flavor::Rasterized))
                .await
                .expect("SHOW DATABASES");
            dialect::parse_databases(&grid)
        }
    };

    let _ = session.query(&format!("DROP DATABASE IF EXISTS {quoted}")).await;
    session
        .query(&format!("CREATE DATABASE {quoted}"))
        .await
        .expect("CREATE DATABASE");

    let databases = listed(&session).await;
    let names: Vec<&str> = databases.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&home.as_str()), "{variable}: {names:?}");
    assert!(names.contains(&made), "{variable}: {names:?}");
    let current: Vec<&str> =
        databases.iter().filter(|d| d.is_current).map(|d| d.name.as_str()).collect();
    assert_eq!(current, vec![home.as_str()], "{variable}");

    // A switch: a new connection naming the database, as `SelectDatabase`.
    let there = conn::connect(Connection { database: made.to_string(), ..base.clone() })
        .await
        .unwrap_or_else(|e| panic!("{variable}: switching to {made}: {e}"));
    assert_eq!(there.database, made, "{variable}");
    there
        .query("CREATE TABLE studio_live_only_here (id INT)")
        .await
        .expect("a table in the other database");
    let databases = listed(&there).await;
    let current: Vec<&str> =
        databases.iter().filter(|d| d.is_current).map(|d| d.name.as_str()).collect();
    assert_eq!(current, vec![made], "{variable}");
    assert!(databases.iter().any(|d| d.name == home), "{variable}: {databases:?}");
    // The table is the other database's, not the first one's.
    let tables = session.query("SHOW TABLES").await.expect("SHOW TABLES");
    assert!(
        !format!("{tables:?}").contains("studio_live_only_here"),
        "{variable}: the table reached {home}"
    );
    drop(there);

    // Dropped once no connection holds it; the switched one may take a
    // moment to be let go by the server.
    let mut dropped = Err(String::new());
    for _ in 0..50 {
        dropped = session.query(&format!("DROP DATABASE {quoted}")).await.map(|_| ());
        if dropped.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    dropped.unwrap_or_else(|e| panic!("{variable}: DROP DATABASE {made}: {e}"));
    assert!(!listed(&session).await.iter().any(|d| d.name == made), "{variable}");
}

#[tokio::test(flavor = "multi_thread")]
async fn databases_are_listed_created_and_switched_to() {
    let _serial = serial().await;
    exercise_databases("RDB_STUDIO_TEST_RDB").await;
}

/// `ANALYZE SELECT …` in the editor: refused, pointing at `EXPLAIN ANALYZE`,
/// where it used to answer "0 rows affected" having run nothing. And
/// `EXPLAIN ANALYZE` answers its plan as rows.
async fn exercise_analyze(variable: &str) {
    let Ok(url) = std::env::var(variable) else { return };
    let session = conn::connect(profile(&url, Flavor::Rasterized)).await.expect("connects");
    let tables = session.query(dialect::tables_sql(Flavor::Rasterized)).await.expect("tables");
    let table = dialect::parse_tables(&tables).first().expect("a table").name.clone();
    let table = dialect::quote_ident(&table);

    let refused = session.query(&format!("ANALYZE SELECT * FROM {table}")).await;
    let message = refused.expect_err("ANALYZE SELECT was answered");
    assert!(message.contains("EXPLAIN ANALYZE"), "{variable}: {message}");

    session.query(&format!("ANALYZE {table}")).await.expect("ANALYZE of a table");
    let missing = session.query("ANALYZE studio_no_such_table").await;
    assert!(missing.is_err(), "{variable}: ANALYZE of a missing table answered");

    let plan = session
        .query(&format!("EXPLAIN ANALYZE SELECT * FROM {table}"))
        .await
        .expect("EXPLAIN ANALYZE");
    assert_eq!(plan.columns, vec!["QUERY PLAN".to_string()], "{variable}");
    assert!(!plan.rows.is_empty(), "{variable}: no plan");
}

#[tokio::test(flavor = "multi_thread")]
async fn analyze_of_a_statement_is_refused_and_explain_analyze_answers() {
    let _serial = serial().await;
    exercise_analyze("RDB_STUDIO_TEST_RDB").await;
}
