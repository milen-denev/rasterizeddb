//! A sequence belongs to one database.
//!
//! `SERIAL`'s counter lives in memory, and it used to be keyed by the sequence
//! name alone — so every database in the process shared it. `CREATE DATABASE
//! x` followed by `CREATE TABLE tenant (id BIGSERIAL …)` in `x` handed out 51
//! as its first key, because `tenant` in `postgres` had used 1..50: a foreign
//! key assuming 1..N broke, and one tenant's row count leaked into another's
//! keys. Nothing failed; the keys were simply the wrong numbers.
//!
//! These drive the catalogue the way the server does — one process, several
//! databases opened from one root — and check every number handed out.

use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::core::database::catalog::DatabaseCatalog;
use rasterizeddb_core::core::row::row::{Row, vec_into_rows};
use rasterizeddb_core::core::rql::executor::execute;
use rasterizeddb_core::core::rql::lexer_s1::recognize_query_purpose;
use rclite::Arc;

fn init_globals() {
    let _ = rasterizeddb_core::MAX_PERMITS_THREADS.set(4);
    let _ = rasterizeddb_core::BATCH_SIZE.set(1024);
}

async fn run(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {}", sql));
    execute(purpose, database.clone()).await
}

#[track_caller]
fn expect_ok(response: &[u8], sql: &str) {
    if response.first() == Some(&3) {
        panic!(
            "`{}` failed: {}",
            sql,
            String::from_utf8_lossy(&response[1..])
        );
    }
}

async fn ok(database: &Arc<Database>, sql: &str) {
    expect_ok(&run(database, sql).await, sql);
}

async fn rows(database: &Arc<Database>, sql: &str) -> Vec<Row> {
    let response = run(database, sql).await;
    expect_ok(&response, sql);
    vec_into_rows(&response[1..]).expect("a decodable result set")
}

/// An integer cell of any serial width, widened.
fn int(row: &Row, column: usize) -> i64 {
    let bytes = row.columns[column].data.into_slice();
    match bytes.len() {
        2 => i16::from_le_bytes(bytes.try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
        n => panic!("an integer cell of {} bytes", n),
    }
}

const CREATE_TENANT: &str =
    "CREATE TABLE tenant (id BIGSERIAL PRIMARY KEY, sid SERIAL, tid SMALLSERIAL, n INT)";

/// Inserts `count` tenants and returns the keys each serial column gave them,
/// as `(id, sid, tid)` rows in insertion order.
async fn add_tenants(database: &Arc<Database>, from: i64, count: i64) -> Vec<(i64, i64, i64)> {
    for n in from..from + count {
        ok(database, &format!("INSERT INTO tenant (n) VALUES ({})", n)).await;
    }
    let found = rows(
        database,
        &format!(
            "SELECT id, sid, tid, n FROM tenant WHERE n >= {} ORDER BY n",
            from
        ),
    )
    .await;
    assert_eq!(found.len() as i64, count, "every insert stored one row");
    found
        .iter()
        .map(|r| (int(r, 0), int(r, 1), int(r, 2)))
        .collect()
}

fn expected(first: i64, count: i64) -> Vec<(i64, i64, i64)> {
    (first..first + count).map(|k| (k, k, k)).collect()
}

/// The keys an explicit `DEFAULT nextval('shared_seq')` column handed out.
async fn add_explicit(database: &Arc<Database>, from: i64, count: i64) -> Vec<i64> {
    for n in from..from + count {
        ok(database, &format!("INSERT INTO named (n) VALUES ({})", n)).await;
    }
    rows(
        database,
        &format!("SELECT id FROM named WHERE n >= {} ORDER BY n", from),
    )
    .await
    .iter()
    .map(|r| int(r, 0))
    .collect()
}

/// The `id` each row of a table holds, in the order of its `n`.
async fn ids_by_n(database: &Arc<Database>, table: &str) -> Vec<i64> {
    rows(database, &format!("SELECT id FROM {} ORDER BY n", table))
        .await
        .iter()
        .map(|r| int(r, 0))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_database_counts_its_own_sequences() {
    init_globals();
    let root = tempfile::tempdir().unwrap();
    let root_str = root.path().to_str().unwrap().to_string();
    let old_ids;

    {
        let catalog = DatabaseCatalog::open(Some(&root_str), "postgres")
            .await
            .unwrap();
        let postgres = catalog.get("postgres").expect("the default database");
        let x = catalog.create("x").await.expect("CREATE DATABASE x");

        for database in [&postgres, &x] {
            ok(database, CREATE_TENANT).await;
            // Named by hand, so the two databases name the *same* sequence.
            ok(
                database,
                "CREATE TABLE named (id BIGINT DEFAULT nextval('shared_seq'), n INT)",
            )
            .await;
        }

        // The default database uses some keys first, then the new one starts.
        assert_eq!(add_tenants(&postgres, 1, 5).await, expected(1, 5));
        assert_eq!(
            add_tenants(&x, 1, 3).await,
            expected(1, 3),
            "a new database's first keys continued the other database's sequence"
        );
        // And neither has moved the other since.
        assert_eq!(add_tenants(&postgres, 6, 1).await, expected(6, 1));
        assert_eq!(add_tenants(&x, 4, 1).await, expected(4, 1));

        assert_eq!(add_explicit(&postgres, 1, 3).await, vec![1, 2, 3]);
        assert_eq!(
            add_explicit(&x, 1, 2).await,
            vec![1, 2],
            "a hand-named sequence was shared across databases"
        );

        // A column added with a `nextval` default to a table that already holds
        // rows backfills them from that database's sequence, starting at 1:
        // checking the default as part of the `ALTER` takes no value, and the
        // backfill draws from the database the table is in.
        for (database, stored) in [(&postgres, 3), (&x, 2)] {
            ok(
                database,
                "ALTER TABLE named ADD COLUMN k BIGINT DEFAULT nextval('k_seq')",
            )
            .await;
            let mut ks: Vec<i64> = rows(database, "SELECT k FROM named")
                .await
                .iter()
                .map(|r| int(r, 0))
                .collect();
            ks.sort_unstable();
            assert_eq!(
                ks,
                (1..=stored).collect::<Vec<i64>>(),
                "an added nextval column did not backfill from 1 in its own database"
            );
        }

        old_ids = (postgres.id, x.id);
        drop(x);
        drop(postgres);
        drop(catalog);
    }

    // A restart: each database resumes above its own stored keys, and only its
    // own.
    let catalog = DatabaseCatalog::open(Some(&root_str), "postgres")
        .await
        .unwrap();
    let postgres = catalog.get("postgres").expect("the default database");
    let x = catalog.get("x").expect("x survives a restart");
    // The counters live in one process-wide map, and the opened databases
    // above are still in it. The restart checks below prove seeding only
    // because a reopened database gets a fresh id: with a reused one, both
    // would read their old counters and pass without seeding at all.
    assert_ne!(
        old_ids.0, postgres.id,
        "postgres reopened under its old id; the restart checks prove nothing"
    );
    assert_ne!(
        old_ids.1, x.id,
        "x reopened under its old id; the restart checks prove nothing"
    );

    assert_eq!(
        add_tenants(&x, 5, 1).await,
        expected(5, 1),
        "after a restart x resumed from the wrong high-water mark"
    );
    assert_eq!(
        add_tenants(&postgres, 7, 1).await,
        expected(7, 1),
        "after a restart postgres resumed from the wrong high-water mark"
    );
    assert_eq!(add_explicit(&x, 3, 1).await, vec![3]);
    assert_eq!(add_explicit(&postgres, 4, 1).await, vec![4]);

    // A table dropped and recreated in one database starts again at 1 — not
    // from the other database's high-water mark — and the drop does not touch
    // the other database's counter.
    //
    // To see the second half, postgres's counter is first put ahead of what it
    // stores: a deleted row's key is never handed out again. A drop that
    // wrongly forgot postgres's counter too would reseed it from the rows
    // still there (max 7) and hand out 8 a second time.
    assert_eq!(add_tenants(&postgres, 8, 1).await, expected(8, 1));
    ok(&postgres, "DELETE FROM tenant WHERE n = 8").await;
    //
    // After the restart rather than before it: a table dropped, recreated and
    // then reopened comes back holding the dropped table's rows (a separate
    // fault, independent of sequences; see the ignored test below), which
    // would make the restart half of this test about that instead.
    ok(&x, "DROP TABLE tenant").await;
    ok(&x, CREATE_TENANT).await;
    assert_eq!(
        add_tenants(&x, 1, 1).await,
        expected(1, 1),
        "a recreated table inherited another database's counter"
    );
    assert_eq!(
        add_tenants(&postgres, 9, 1).await,
        expected(9, 1),
        "dropping x's tenant reset postgres's counter"
    );

    // A sequence a table names by hand outlives the table, as in PostgreSQL:
    // a table recreated on it continues where it stopped (x's `shared_seq`
    // handed out 1..3).
    ok(&x, "DROP TABLE named").await;
    ok(
        &x,
        "CREATE TABLE named (id BIGINT DEFAULT nextval('shared_seq'), n INT)",
    )
    .await;
    assert_eq!(
        add_explicit(&x, 1, 1).await,
        vec![4],
        "dropping a table forgot a sequence it only named"
    );

    // Dropping `ledger` leaves `ledger_x`'s counter alone, though `ledger_` is
    // a prefix of its name — and though `ledger`'s column `x_id` spells
    // `ledger_x_id_seq` exactly. Again the counter is ahead of the stored
    // keys, so a wrongly forgotten one shows up as a reused key.
    ok(&postgres, "CREATE TABLE ledger_x (id SERIAL, n INT)").await;
    ok(
        &postgres,
        "CREATE TABLE ledger (id SERIAL, x_id INT, n INT)",
    )
    .await;
    ok(&postgres, "INSERT INTO ledger_x (n) VALUES (1)").await;
    ok(&postgres, "INSERT INTO ledger_x (n) VALUES (2)").await;
    ok(&postgres, "DELETE FROM ledger_x WHERE n = 2").await;
    ok(&postgres, "INSERT INTO ledger (n) VALUES (1)").await;
    ok(&postgres, "DROP TABLE ledger").await;
    ok(&postgres, "INSERT INTO ledger_x (n) VALUES (3)").await;
    assert_eq!(
        ids_by_n(&postgres, "ledger_x").await,
        vec![1, 3],
        "dropping ledger took ledger_x's counter"
    );
}

/// A `SERIAL` column altered so that no live default reads its sequence any
/// more — dropped, renamed, or stripped of its default — still takes the
/// sequence with its table. Each used to leave the counter behind, so the
/// table recreated under the same name started where the old one stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_serial_column_altered_away_still_goes_with_its_table() {
    init_globals();
    let root = tempfile::tempdir().unwrap();
    let root_str = root.path().to_str().unwrap().to_string();
    let catalog = DatabaseCatalog::open(Some(&root_str), "postgres")
        .await
        .unwrap();
    let postgres = catalog.get("postgres").expect("the default database");

    for (table, alter) in [
        ("dropped_col", "ALTER TABLE dropped_col DROP COLUMN id"),
        (
            "renamed_col",
            "ALTER TABLE renamed_col RENAME COLUMN id TO ident",
        ),
        (
            "no_default",
            "ALTER TABLE no_default ALTER COLUMN id DROP DEFAULT",
        ),
    ] {
        let create = format!("CREATE TABLE {} (id SERIAL, n INT)", table);
        ok(&postgres, &create).await;
        for n in 1..=3 {
            ok(
                &postgres,
                &format!("INSERT INTO {} (n) VALUES ({})", table, n),
            )
            .await;
        }
        assert_eq!(ids_by_n(&postgres, table).await, vec![1, 2, 3]);
        ok(&postgres, alter).await;
        ok(&postgres, &format!("DROP TABLE {}", table)).await;
        ok(&postgres, &create).await;
        ok(&postgres, &format!("INSERT INTO {} (n) VALUES (1)", table)).await;
        assert_eq!(
            ids_by_n(&postgres, table).await,
            vec![1],
            "after `{}`, the recreated table continued the dropped one's counter",
            alter
        );
    }
}

/// A renamed table takes its `SERIAL` sequence with it, so a new table created
/// under the old name counts on its own.
///
/// Left reading `nextval('src_id_seq')`, the renamed table shared that counter
/// with any new `src`: the new table's first key continued the renamed one's
/// (4, not 1), and after a restart whichever table inserted first seeded the
/// shared counter from its own rows, handing the other keys from above the
/// wrong maximum — keys it could already hold.
///
/// The new `src` is created after the restart, not before it: a table created
/// under a renamed table's old name and then reopened comes back holding the
/// renamed table's rows (the fault the ignored test below records).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renamed_table_takes_its_sequence_with_it() {
    init_globals();
    let root = tempfile::tempdir().unwrap();
    let root_str = root.path().to_str().unwrap().to_string();

    {
        let catalog = DatabaseCatalog::open(Some(&root_str), "postgres")
            .await
            .unwrap();
        let postgres = catalog.get("postgres").expect("the default database");
        ok(&postgres, "CREATE TABLE src (id BIGSERIAL, n INT)").await;
        for n in 1..=3 {
            ok(&postgres, &format!("INSERT INTO src (n) VALUES ({})", n)).await;
        }
        ok(&postgres, "ALTER TABLE src RENAME TO dst").await;
        ok(&postgres, "INSERT INTO dst (n) VALUES (4)").await;
        assert_eq!(ids_by_n(&postgres, "dst").await, vec![1, 2, 3, 4]);
        drop(postgres);
        drop(catalog);
    }

    // After a restart the renamed table seeds its own sequence, under its own
    // name, from its own rows; and the old name is free.
    let catalog = DatabaseCatalog::open(Some(&root_str), "postgres")
        .await
        .unwrap();
    let postgres = catalog.get("postgres").expect("the default database");
    // `dst` first, so a shared counter would be seeded from its rows.
    ok(&postgres, "INSERT INTO dst (n) VALUES (5)").await;
    ok(&postgres, "CREATE TABLE src (id BIGSERIAL, n INT)").await;
    ok(&postgres, "INSERT INTO src (n) VALUES (1)").await;
    ok(&postgres, "INSERT INTO dst (n) VALUES (6)").await;
    ok(&postgres, "INSERT INTO src (n) VALUES (2)").await;
    assert_eq!(ids_by_n(&postgres, "dst").await, vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(
        ids_by_n(&postgres, "src").await,
        vec![1, 2],
        "a table created under a renamed table's old name took its counter"
    );

    // And the renamed table's sequence is its own to drop.
    ok(&postgres, "DROP TABLE dst").await;
    ok(&postgres, "CREATE TABLE dst (id BIGSERIAL, n INT)").await;
    ok(&postgres, "INSERT INTO dst (n) VALUES (1)").await;
    ok(&postgres, "INSERT INTO src (n) VALUES (3)").await;
    assert_eq!(ids_by_n(&postgres, "dst").await, vec![1]);
    assert_eq!(ids_by_n(&postgres, "src").await, vec![1, 2, 3]);
}

/// A table dropped, recreated and then reopened should come back as the
/// recreated table — empty of the dropped one's rows — and count from 1.
///
/// It does not: the reopen brings the dropped table's rows back, and the
/// recreated table's counter then seeds from them and skips ahead — the same
/// silent wrong key this file is about, by another route. Switch this on when
/// that is fixed. `ALTER TABLE t RENAME TO u` followed by `CREATE TABLE t` and
/// a reopen shows the same thing: `t` comes back holding `u`'s rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "drop+recreate+reopen resurrects dropped rows"]
async fn a_table_dropped_and_recreated_survives_a_restart_fresh() {
    init_globals();
    let root = tempfile::tempdir().unwrap();
    let root_str = root.path().to_str().unwrap().to_string();

    {
        let catalog = DatabaseCatalog::open(Some(&root_str), "postgres")
            .await
            .unwrap();
        let x = catalog.create("x").await.expect("CREATE DATABASE x");
        ok(&x, CREATE_TENANT).await;
        assert_eq!(add_tenants(&x, 1, 3).await, expected(1, 3));
        ok(&x, "DROP TABLE tenant").await;
        ok(&x, CREATE_TENANT).await;
        drop(x);
        drop(catalog);
    }

    let catalog = DatabaseCatalog::open(Some(&root_str), "postgres")
        .await
        .unwrap();
    let x = catalog.get("x").expect("x survives a restart");
    assert!(
        rows(&x, "SELECT id, sid, tid, n FROM tenant")
            .await
            .is_empty(),
        "the reopened table holds the dropped table's rows"
    );
    assert_eq!(
        add_tenants(&x, 1, 1).await,
        expected(1, 1),
        "the recreated table's counter seeded from the dropped table's rows"
    );
}
