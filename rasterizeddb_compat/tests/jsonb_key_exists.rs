//! The JSONB key-existence operators: `?`, `?|`, `?&`.
//!
//! `data ? 'k'` asks whether a document has that key at the top level. It is
//! standard PostgreSQL and an application using it for a checkpoint lookup ran
//! it on every autosave, where it failed with
//! `unexpected character '?' in WHERE clause`.

use rasterizeddb_compat::TestServer;
use sqlx::{Executor, Row};

async fn server() -> (TestServer, sqlx::PgPool) {
    let server = TestServer::start().await.expect("server should start");
    let pool = server.connect_sqlx().await.expect("should connect");
    (server, pool)
}

async fn setup(pool: &sqlx::PgPool) {
    pool.execute(sqlx::raw_sql("CREATE TABLE d (id INT PRIMARY KEY, data JSONB)"))
        .await
        .expect("create");
    for (id, doc) in [
        (1, r#"{"checkpoint_payload": {"n": 1}, "resource_id": "a"}"#),
        (2, r#"{"resource_id": "b"}"#),
        (3, r#"{"checkpoint_payload": null, "resource_id": "c"}"#),
    ] {
        pool.execute(sqlx::raw_sql(&format!(
            "INSERT INTO d (id, data) VALUES ({}, '{}')",
            id, doc
        )))
        .await
        .unwrap_or_else(|e| panic!("seed {}: {}", id, e));
    }
}

async fn ids(pool: &sqlx::PgPool, sql: &str) -> Result<String, String> {
    match sqlx::query(sql).fetch_all(pool).await {
        Ok(rows) => Ok(rows
            .iter()
            .map(|r| r.get::<i32, _>("id").to_string())
            .collect::<Vec<_>>()
            .join(",")),
        Err(e) => Err(e.to_string().lines().next().unwrap_or("").to_string()),
    }
}

/// The control: whatever `?` compiles to must agree with this.
#[tokio::test]
async fn the_path_form_finds_the_same_rows() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    for form in [
        "SELECT id FROM d WHERE data->'checkpoint_payload' IS NOT NULL ORDER BY id",
        "SELECT id FROM d WHERE data->'checkpoint_payload' <> '' ORDER BY id",
        "SELECT id FROM d WHERE data->>'checkpoint_payload' IS NOT NULL ORDER BY id",
        "SELECT id FROM d WHERE data->'checkpoint_payload' != '' ORDER BY id",
    ] {
        println!("{:<70} -> {:?}", form.split("WHERE ").nth(1).unwrap_or(form), ids(&pool, form).await);
    }
}

/// A JSON `null` value still means the key exists — the distinction that makes
/// `?` different from `->> IS NOT NULL`.
#[tokio::test]
async fn key_exists_includes_a_json_null_value() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let got = ids(&pool, "SELECT id FROM d WHERE data ? 'checkpoint_payload' ORDER BY id").await;
    assert_eq!(
        got,
        Ok("1,3".to_string()),
        "rows 1 and 3 have the key; row 3's value is JSON null, which still exists"
    );
}

#[tokio::test]
async fn key_exists_excludes_a_missing_key() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    assert_eq!(
        ids(&pool, "SELECT id FROM d WHERE data ? 'resource_id' ORDER BY id").await,
        Ok("1,2,3".to_string())
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM d WHERE data ? 'nope' ORDER BY id").await,
        Ok(String::new())
    );
}

/// The production statement's shape: `?` alongside other predicates and bound
/// parameters.
#[tokio::test]
async fn key_exists_combines_with_parameters() {
    let (_s, pool) = server().await;
    setup(&pool).await;
    let rows = sqlx::query(
        "SELECT id FROM d WHERE data ? 'checkpoint_payload' AND data->>'resource_id' = $1 ORDER BY id",
    )
    .bind("a")
    .fetch_all(&pool)
    .await
    .expect("? with a bound parameter");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>("id"), 1);
}

/// The production statement, in shape: `?` alongside a cast, a text path
/// comparison, several bound parameters, an ORDER BY and a LIMIT.
///
/// The cast is the part that took work: `(data->>'resource_type_code')::bigint`
/// extracts text and asks for it to be compared as a number, so the target type
/// travels with the path operand and the extract is re-read in it. Dropping the
/// cast instead compares the JSON string `"3"` against the number `3` and
/// matches nothing — which, for this statement, would have numbered every
/// version 1 forever without an error anywhere.
#[tokio::test]
async fn the_audit_checkpoint_lookup_runs() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE user_table_row (\
           id INT PRIMARY KEY, domain_id INT NOT NULL, user_table_id INT NOT NULL, \
           entry_order INT NOT NULL, data JSONB)",
    ))
    .await
    .expect("create");

    for (id, order, doc) in [
        (1, 1, r#"{"checkpoint_payload": {"n": 1}, "checkpoint_number": "7", "resource_type_code": "3", "resource_id": "r1"}"#),
        (2, 2, r#"{"checkpoint_payload": {"n": 2}, "checkpoint_number": "8", "resource_type_code": "3", "resource_id": "r1"}"#),
        (3, 3, r#"{"checkpoint_number": "9", "resource_type_code": "3", "resource_id": "r1"}"#),
    ] {
        pool.execute(sqlx::raw_sql(&format!(
            "INSERT INTO user_table_row (id, domain_id, user_table_id, entry_order, data) \
             VALUES ({}, 1, 1, {}, '{}')",
            id, order, doc
        )))
        .await
        .unwrap_or_else(|e| panic!("seed {}: {}", id, e));
    }

    let row = sqlx::query(
        "SELECT data->>'checkpoint_number' AS checkpoint_number \
         FROM user_table_row \
         WHERE domain_id = $1 \
           AND user_table_id = $2 \
           AND data ? 'checkpoint_payload' \
           AND (data->>'resource_type_code')::bigint = $3 \
           AND data->>'resource_id' = $4 \
         ORDER BY entry_order DESC, id DESC \
         LIMIT 1",
    )
    .bind(1i32)
    .bind(1i32)
    .bind(3i64)
    .bind("r1")
    .fetch_one(&pool)
    .await
    .expect("the audit checkpoint lookup must run");

    // Row 3 has no checkpoint_payload, so the newest *with* one is row 2.
    assert_eq!(
        row.get::<Option<String>, _>("checkpoint_number"),
        Some("8".to_string()),
        "the newest row carrying a checkpoint_payload"
    );
}

/// The cast has to discriminate, not just parse.
///
/// A cast that silently matched everything, or nothing, would pass a test that
/// only checks the statement runs. These pin both directions, and the
/// text-versus-number distinction the cast exists for.
///
/// Every document here *has* the key and its value parses. The case where it
/// does not is `a_cast_over_an_absent_key_is_a_known_gap` below.
#[tokio::test]
async fn a_cast_over_a_path_compares_as_the_cast_type() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql("CREATE TABLE c (id INT PRIMARY KEY, data JSONB)"))
        .await
        .expect("create");
    for (id, doc) in [
        (1, r#"{"code": "3", "n": "10"}"#),
        (2, r#"{"code": "4", "n": "9"}"#),
        (3, r#"{"code": "5", "n": "1"}"#),
    ] {
        pool.execute(sqlx::raw_sql(&format!(
            "INSERT INTO c (id, data) VALUES ({}, '{}')",
            id, doc
        )))
        .await
        .unwrap_or_else(|e| panic!("seed {}: {}", id, e));
    }

    // Matches the row it should, and only that row.
    assert_eq!(
        ids(&pool, "SELECT id FROM c WHERE (data->>'code')::bigint = 3 ORDER BY id").await,
        Ok("1".to_string())
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM c WHERE (data->>'code')::bigint = 4 ORDER BY id").await,
        Ok("2".to_string())
    );
    assert_eq!(
        ids(&pool, "SELECT id FROM c WHERE (data->>'code')::bigint = 99 ORDER BY id").await,
        Ok(String::new())
    );

    // Numeric, not lexicographic: as text, "10" sorts before "9".
    assert_eq!(
        ids(&pool, "SELECT id FROM c WHERE (data->>'n')::bigint > 5 ORDER BY id").await,
        Ok("1,2".to_string()),
        "10 and 9 are both > 5; a text comparison would drop 9"
    );

    // Without the cast the same comparison is a text one.
    assert_eq!(
        ids(&pool, "SELECT id FROM c WHERE data->>'code' = '3' ORDER BY id").await,
        Ok("1".to_string()),
        "the uncast path still compares as text"
    );
}

/// A cast over a value that cannot be read: absent, JSON `null`, or not a
/// number.
///
/// All three make the extract produce nothing. A fixed-width comparison cannot
/// take a zero-length operand, so this used to fail the entire statement —
/// one unreadable row killing a query every other row answered.
///
/// The middle case is the one that matters in practice: an optional field
/// serialised from `None` is present with a JSON `null` value, not absent, and
/// it can reach the same table as the rows the predicate is really about.
#[tokio::test]
async fn a_cast_over_an_unreadable_value_is_false_not_an_error() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql("CREATE TABLE g (id INT PRIMARY KEY, data JSONB)"))
        .await
        .expect("create");
    for (id, doc) in [
        (1, r#"{"code": "3"}"#),
        (2, r#"{"other": "1"}"#),
        (3, r#"{"code": null}"#),
        (4, r#"{"code": "not a number"}"#),
    ] {
        pool.execute(sqlx::raw_sql(&format!(
            "INSERT INTO g (id, data) VALUES ({}, '{}')",
            id, doc
        )))
        .await
        .unwrap_or_else(|e| panic!("seed {}: {}", id, e));
    }

    // The readable row matches; the other three are simply not matches.
    assert_eq!(
        ids(&pool, "SELECT id FROM g WHERE (data->>'code')::bigint = 3 ORDER BY id").await,
        Ok("1".to_string()),
        "an unreadable value must not fail the statement"
    );

    // And it must not match zero either — the trap in filling in the type's
    // zeroes, which would sweep unrelated rows into the result.
    assert_eq!(
        ids(&pool, "SELECT id FROM g WHERE (data->>'code')::bigint = 0 ORDER BY id").await,
        Ok(String::new()),
        "absent, null and non-numeric must not match 0"
    );

    // A range comparison, which is where a substituted zero would also show.
    assert_eq!(
        ids(&pool, "SELECT id FROM g WHERE (data->>'code')::bigint < 100 ORDER BY id").await,
        Ok("1".to_string())
    );

    // Each shape on its own, so a regression names which one broke.
    for (label, sql, expected) in [
        ("absent key", "SELECT id FROM g WHERE (data->>'nope')::bigint = 3 ORDER BY id", ""),
        ("json null", "SELECT id FROM g WHERE (data->>'code')::bigint = 3 AND id = 3 ORDER BY id", ""),
        ("non-numeric", "SELECT id FROM g WHERE (data->>'code')::bigint = 3 AND id = 4 ORDER BY id", ""),
    ] {
        assert_eq!(ids(&pool, sql).await, Ok(expected.to_string()), "{}", label);
    }
}

/// The events table as it actually is: one table per domain, holding rows
/// whose `resource_type_code` is JSON `null` because the field is optional and
/// many event kinds do not set it.
///
/// The checkpoint query scans that table and filters. It must find the right
/// row and be unbothered by the null-coded ones sharing it — and it must not
/// depend on which `AND` arm the planner tests first, because SQL guarantees
/// no order between them.
#[tokio::test]
async fn null_coded_rows_do_not_disturb_the_checkpoint_lookup() {
    let (_s, pool) = server().await;
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE user_table_row (           id INT PRIMARY KEY, domain_id INT NOT NULL, user_table_id INT NOT NULL,            entry_order INT NOT NULL, data JSONB)",
    ))
    .await
    .expect("create");

    let rows = [
        // Real checkpoint rows for the resource being asked about.
        (1, 1, r#"{"checkpoint_payload": {"n": 1}, "checkpoint_number": "7", "resource_type_code": "3", "resource_id": "r1"}"#),
        (2, 2, r#"{"checkpoint_payload": {"n": 2}, "checkpoint_number": "8", "resource_type_code": "3", "resource_id": "r1"}"#),
        // Event kinds that pass no code at all — present, JSON null.
        (3, 3, r#"{"checkpoint_payload": {"n": 9}, "checkpoint_number": "99", "resource_type_code": null, "resource_id": "r1"}"#),
        (4, 4, r#"{"resource_type_code": null, "resource_id": "r1"}"#),
        // A newer row for the same resource with no payload at all.
        (5, 5, r#"{"checkpoint_number": "10", "resource_type_code": "3", "resource_id": "r1"}"#),
    ];
    for (id, order, doc) in rows {
        pool.execute(sqlx::raw_sql(&format!(
            "INSERT INTO user_table_row (id, domain_id, user_table_id, entry_order, data)              VALUES ({}, 1, 1, {}, '{}')",
            id, order, doc
        )))
        .await
        .unwrap_or_else(|e| panic!("seed {}: {}", id, e));
    }

    let row = sqlx::query(
        "SELECT data->>'checkpoint_number' AS checkpoint_number          FROM user_table_row          WHERE domain_id = $1            AND user_table_id = $2            AND data ? 'checkpoint_payload'            AND (data->>'resource_type_code')::bigint = $3            AND data->>'resource_id' = $4          ORDER BY entry_order DESC, id DESC          LIMIT 1",
    )
    .bind(1i32)
    .bind(1i32)
    .bind(3i64)
    .bind("r1")
    .fetch_one(&pool)
    .await
    .expect("the lookup must survive null-coded rows in the same table");

    // Row 3 has a payload but a null code, so it must not win despite being
    // newer than row 2. Row 5 has the right code but no payload.
    assert_eq!(
        row.get::<Option<String>, _>("checkpoint_number"),
        Some("8".to_string()),
        "the newest row with both a payload and a matching code"
    );

    // The null-coded rows must not be reachable through the code predicate at
    // all, under any value — including zero.
    for probe in [0i64, 3, 99] {
        let found = sqlx::query(
            "SELECT id FROM user_table_row WHERE (data->>'resource_type_code')::bigint = $1              AND id IN (3, 4) ORDER BY id",
        )
        .bind(probe)
        .fetch_all(&pool)
        .await
        .unwrap_or_else(|e| panic!("probe {}: {}", probe, e));
        assert!(
            found.is_empty(),
            "a null resource_type_code must not match {}",
            probe
        );
    }
}
