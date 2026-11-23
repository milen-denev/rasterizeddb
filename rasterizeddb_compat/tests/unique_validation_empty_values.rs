//! Adding a UNIQUE constraint validates the rows already there, and an empty
//! value is a value: two `''` rows break `UNIQUE`, as do two zero-length
//! BYTEA rows. Only NULL may repeat.
//!
//! The validator used to treat empty bytes as NULL, so a constraint the data
//! already broke was accepted — on all three statements that reach it
//! (`ADD UNIQUE`, `ADD PRIMARY KEY`, `CREATE UNIQUE INDEX`) — and the table
//! then held duplicates under a constraint that claimed otherwise.

use rasterizeddb_compat::TestServer;
use sqlx::AssertSqlSafe;

async fn run(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(AssertSqlSafe(sql.to_string()))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_values_are_duplicates_and_nulls_are_not() {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");

    // Nullable TEXT holding '' twice.
    run(&pool, "CREATE TABLE ue1 (id INTEGER NOT NULL, t TEXT)").await.unwrap();
    run(&pool, "INSERT INTO ue1 (id, t) VALUES (1, ''), (2, ''), (3, 'a')").await.unwrap();
    let e = run(&pool, "ALTER TABLE ue1 ADD CONSTRAINT ue1_t UNIQUE (t)").await;
    assert!(e.is_err(), "two '' rows were accepted under ADD UNIQUE");

    // The same through CREATE UNIQUE INDEX.
    let e = run(&pool, "CREATE UNIQUE INDEX ue1_t_idx ON ue1 (t)").await;
    assert!(e.is_err(), "two '' rows were accepted under CREATE UNIQUE INDEX");

    // NOT NULL TEXT (no NULL companion) holding '' twice, under PRIMARY KEY.
    run(&pool, "CREATE TABLE ue2 (id INTEGER NOT NULL, t TEXT NOT NULL)").await.unwrap();
    run(&pool, "INSERT INTO ue2 (id, t) VALUES (1, ''), (2, '')").await.unwrap();
    let e = run(&pool, "ALTER TABLE ue2 ADD PRIMARY KEY (t)").await;
    assert!(e.is_err(), "two '' rows were accepted under ADD PRIMARY KEY");

    // Zero-length BYTEA twice.
    run(&pool, "CREATE TABLE ue3 (id INTEGER NOT NULL, b BYTEA)").await.unwrap();
    sqlx::query("INSERT INTO ue3 (id, b) VALUES ($1, $2), ($3, $4)")
        .bind(1i32)
        .bind(Vec::<u8>::new())
        .bind(2i32)
        .bind(Vec::<u8>::new())
        .execute(&pool)
        .await
        .expect("insert bytea");
    let e = run(&pool, "ALTER TABLE ue3 ADD CONSTRAINT ue3_b UNIQUE (b)").await;
    assert!(e.is_err(), "two zero-length bytea rows were accepted under ADD UNIQUE");

    // NULL may repeat, beside one '' and one 'a' — all three statements accept it.
    run(&pool, "CREATE TABLE ue4 (id INTEGER NOT NULL, t TEXT, b BYTEA)").await.unwrap();
    run(&pool, "INSERT INTO ue4 (id, t, b) VALUES (1, NULL, NULL), (2, NULL, NULL), (3, '', NULL), (4, 'a', NULL)")
        .await
        .unwrap();
    run(&pool, "ALTER TABLE ue4 ADD CONSTRAINT ue4_t UNIQUE (t)")
        .await
        .expect("repeated NULLs satisfy UNIQUE");
    run(&pool, "CREATE UNIQUE INDEX ue4_b_idx ON ue4 (b)")
        .await
        .expect("repeated NULL bytea satisfy UNIQUE");
    // And the constraint now holds at insert time: a second '' is refused, a
    // third NULL is not.
    assert!(run(&pool, "INSERT INTO ue4 (id, t) VALUES (5, '')").await.is_err());
    run(&pool, "INSERT INTO ue4 (id, t) VALUES (6, NULL)")
        .await
        .expect("another NULL");

    // A multi-column key: ('', NULL) twice is fine (a NULL), ('', 'x') twice is not.
    run(&pool, "CREATE TABLE ue5 (id INTEGER NOT NULL, a TEXT, c TEXT)").await.unwrap();
    run(&pool, "INSERT INTO ue5 (id, a, c) VALUES (1, '', NULL), (2, '', NULL)").await.unwrap();
    run(&pool, "ALTER TABLE ue5 ADD CONSTRAINT ue5_ac UNIQUE (a, c)")
        .await
        .expect("a tuple holding NULL conflicts with nothing");
    run(&pool, "CREATE TABLE ue6 (id INTEGER NOT NULL, a TEXT, c TEXT)").await.unwrap();
    run(&pool, "INSERT INTO ue6 (id, a, c) VALUES (1, '', 'x'), (2, '', 'x')").await.unwrap();
    assert!(
        run(&pool, "ALTER TABLE ue6 ADD CONSTRAINT ue6_ac UNIQUE (a, c)").await.is_err(),
        "('', 'x') twice was accepted"
    );
}
