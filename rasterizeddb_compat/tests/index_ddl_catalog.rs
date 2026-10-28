//! Index DDL as a client sees it: what `pg_indexes` lists, the tag each
//! statement answers with, and the forms refused rather than ignored.
//!
//! Found by probing a server the way a migration tool does (2026-10-28):
//!
//! - One index is built per columns and method, and `pg_indexes` listed only
//!   what was built, so a second `CREATE INDEX` on the same columns, a UNIQUE
//!   over indexed columns and a primary key under an earlier `CREATE INDEX
//!   (id)` all went missing from it while being enforced. A tool that cannot
//!   find an index creates it again; one that cannot find the pkey decides the
//!   key is gone.
//! - The dedup compared the method declared with the one built, so a primary
//!   key recorded both in the header and as a key was built and listed twice.
//! - `UNIQUE NULLS NOT DISTINCT` was read as a plain UNIQUE, so the rows it
//!   exists to refuse went in.
//! - `CREATE INDEX` answered `OK` (or `ALTER TABLE` for a unique one) and ran
//!   inside a READ ONLY transaction; `USING gin` on a text column was XX000.

#[path = "support/wire_frames.rs"]
mod wire;

use rasterizeddb_compat::TestServer;
use wire::Wire;

async fn indexes(c: &mut Wire, table: &str) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = wire::text_rows(
        &c.simple_ok(&format!("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '{table}'"))
            .await,
    )
    .into_iter()
    .map(|row| (row[0].clone().unwrap_or_default(), row[1].clone().unwrap_or_default()))
    .collect();
    v.sort();
    v
}

async fn names(c: &mut Wire, table: &str) -> Vec<String> {
    indexes(c, table).await.into_iter().map(|(n, _)| n).collect()
}

/// The `CommandComplete` tag of a statement that must succeed.
async fn tag(c: &mut Wire, sql: &str) -> String {
    let replies = c.simple_ok(sql).await;
    replies
        .iter()
        .find_map(|r| match r {
            wire::Reply::Complete(tag) => Some(tag.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("{sql}: no CommandComplete in {}", wire::render(&replies)))
}

#[tokio::test(flavor = "multi_thread")]
async fn every_declared_index_is_listed_once_by_its_own_name() {
    let server = TestServer::start().await.expect("server");
    let addr = format!("127.0.0.1:{}", server.port());
    let mut c = Wire::connect(&addr, &[("user", "postgres"), ("database", "postgres")]).await;

    // An inline PRIMARY KEY is listed once, as the pkey: the column's own
    // unique flag is that index, not a `w_id_key` beside it.
    c.simple_ok("CREATE TABLE w (id INT PRIMARY KEY, b TEXT)").await;
    assert_eq!(names(&mut c, "w").await, ["w_pkey"]);
    c.simple_ok("CREATE INDEX w_b ON w (b)").await;
    c.simple_ok("CREATE UNIQUE INDEX w_u ON w (b)").await;
    c.simple_ok("CREATE INDEX w_b2 ON w (b)").await;
    c.simple_ok("CREATE INDEX w_id ON w (id)").await;
    assert_eq!(
        indexes(&mut c, "w").await,
        [
            ("w_b", "CREATE INDEX w_b ON public.w USING btree (b)"),
            ("w_b2", "CREATE INDEX w_b2 ON public.w USING btree (b)"),
            ("w_id", "CREATE INDEX w_id ON public.w USING btree (id)"),
            ("w_pkey", "CREATE UNIQUE INDEX w_pkey ON public.w USING btree (id)"),
            ("w_u", "CREATE UNIQUE INDEX w_u ON public.w USING btree (b)"),
        ]
        .map(|(n, d)| (n.to_string(), d.to_string()))
    );
    // Listed, and still enforced through the index they share.
    c.simple_ok("INSERT INTO w VALUES (1, 'a')").await;
    assert_eq!(wire::error_code(&c.simple("INSERT INTO w VALUES (2, 'a')").await).as_deref(), Some("23505"));
    assert_eq!(wire::error_code(&c.simple("INSERT INTO w VALUES (1, 'b')").await).as_deref(), Some("23505"));
    // Dropping the one that was built leaves the other listed.
    c.simple_ok("DROP INDEX w_b").await;
    assert_eq!(names(&mut c, "w").await, ["w_b2", "w_id", "w_pkey", "w_u"]);

    // A primary key moved to a uuid column: built and listed once.
    c.simple_ok("CREATE TABLE k (id INT NOT NULL, uid UUID NOT NULL, CONSTRAINT k_pkey PRIMARY KEY (id))").await;
    c.simple_ok("ALTER TABLE k DROP CONSTRAINT k_pkey").await;
    assert_eq!(names(&mut c, "k").await, Vec::<String>::new());
    c.simple_ok("ALTER TABLE k ADD CONSTRAINT k_pkey PRIMARY KEY (uid)").await;
    assert_eq!(names(&mut c, "k").await, ["k_pkey"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn nulls_not_distinct_is_refused_not_ignored() {
    let server = TestServer::start().await.expect("server");
    let addr = format!("127.0.0.1:{}", server.port());
    let mut c = Wire::connect(&addr, &[("user", "postgres"), ("database", "postgres")]).await;

    for sql in [
        "CREATE TABLE n1 (id INT PRIMARY KEY, a INT, b INT, UNIQUE NULLS NOT DISTINCT (a, b))",
        "CREATE TABLE n1 (id INT PRIMARY KEY, b INT UNIQUE NULLS NOT DISTINCT)",
    ] {
        let r = c.simple(sql).await;
        assert_eq!(wire::error_code(&r).as_deref(), Some("0A000"), "{sql}: {}", wire::render(&r));
    }

    c.simple_ok("CREATE TABLE n2 (id INT PRIMARY KEY, a INT, b INT)").await;
    for sql in [
        "CREATE UNIQUE INDEX n2_ab ON n2 (a, b) NULLS NOT DISTINCT",
        "CREATE UNIQUE INDEX n2_ab ON n2 (a, b) NULLS NOT DISTINCT WHERE a > 0",
        "ALTER TABLE n2 ADD CONSTRAINT n2_ab UNIQUE NULLS NOT DISTINCT (a, b)",
    ] {
        let r = c.simple(sql).await;
        assert_eq!(wire::error_code(&r).as_deref(), Some("0A000"), "{sql}: {}", wire::render(&r));
    }
    assert_eq!(names(&mut c, "n2").await, ["n2_pkey"]);

    // The default, spelled out, is what is enforced, so it is accepted.
    c.simple_ok("CREATE UNIQUE INDEX n2_d ON n2 (a, b) NULLS DISTINCT").await;
    c.simple_ok("INSERT INTO n2 VALUES (1, 1, NULL)").await;
    c.simple_ok("INSERT INTO n2 VALUES (2, 1, NULL)").await;
    assert_eq!(wire::error_code(&c.simple("INSERT INTO n2 VALUES (3, 1, 1), (4, 1, 1)").await).as_deref(), Some("23505"));
}

#[tokio::test(flavor = "multi_thread")]
async fn index_statements_answer_with_their_own_tags_and_codes() {
    let server = TestServer::start().await.expect("server");
    let addr = format!("127.0.0.1:{}", server.port());
    let mut c = Wire::connect(&addr, &[("user", "postgres"), ("database", "postgres")]).await;

    c.simple_ok("CREATE TABLE g (id INT PRIMARY KEY, b TEXT, doc JSONB)").await;
    assert_eq!(tag(&mut c, "CREATE INDEX g_b ON g (b)").await, "CREATE INDEX");
    assert_eq!(tag(&mut c, "CREATE UNIQUE INDEX g_u ON g (b)").await, "CREATE INDEX");
    assert_eq!(tag(&mut c, "CREATE UNIQUE INDEX IF NOT EXISTS g_u ON g (b)").await, "CREATE INDEX");
    assert_eq!(tag(&mut c, "CREATE INDEX g_doc ON g USING gin (doc)").await, "CREATE INDEX");
    assert_eq!(tag(&mut c, "DROP INDEX g_b").await, "DROP INDEX");
    assert_eq!(tag(&mut c, "DROP INDEX g_doc").await, "DROP INDEX");

    // A gin over a column that is not jsonb asks for a missing feature.
    let r = c.simple("CREATE INDEX g_gin ON g USING gin (b)").await;
    assert_eq!(wire::error_code(&r).as_deref(), Some("0A000"), "{}", wire::render(&r));

    // An insert's duplicate names no row being written.
    c.simple_ok("INSERT INTO g (id, b) VALUES (1, 'x')").await;
    let r = c.simple("INSERT INTO g (id, b) VALUES (2, 'x')").await;
    let message = r
        .iter()
        .find_map(|reply| match reply {
            wire::Reply::Error { code, message, .. } if code == "23505" => Some(message.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no 23505: {}", wire::render(&r)));
    assert!(message.starts_with("duplicate value for UNIQUE constraint") && !message.contains("None"), "{message}");

    // Index DDL is a write: refused in a read-only transaction.
    for sql in [
        "CREATE INDEX g_ro ON g (b)",
        "CREATE UNIQUE INDEX g_ro ON g (id, b)",
        "DROP INDEX g_u",
        "ALTER TABLE g ADD CONSTRAINT g_ro UNIQUE (id, b)",
    ] {
        c.simple_ok("BEGIN READ ONLY").await;
        let r = c.simple(sql).await;
        assert_eq!(wire::error_code(&r).as_deref(), Some("25006"), "{sql}: {}", wire::render(&r));
        c.simple_ok("ROLLBACK").await;
    }
    assert_eq!(names(&mut c, "g").await, ["g_pkey", "g_u"]);
}

/// `ALTER TABLE … ADD COLUMN c T UNIQUE` dropped the UNIQUE: only nullability
/// was read from the words after the type, so duplicates went in.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_declared_on_an_added_column_is_enforced() {
    let server = TestServer::start().await.expect("server");
    let addr = format!("127.0.0.1:{}", server.port());
    let mut c = Wire::connect(&addr, &[("user", "postgres"), ("database", "postgres")]).await;

    c.simple_ok("CREATE TABLE ac (id INT PRIMARY KEY)").await;
    c.simple_ok("INSERT INTO ac VALUES (1), (2)").await;
    c.simple_ok("ALTER TABLE ac ADD COLUMN e INT UNIQUE").await;
    assert_eq!(names(&mut c, "ac").await, ["ac_e_key", "ac_pkey"]);
    c.simple_ok("UPDATE ac SET e = 5 WHERE id = 1").await;
    let r = c.simple("UPDATE ac SET e = 5 WHERE id = 2").await;
    assert_eq!(wire::error_code(&r).as_deref(), Some("23505"), "{}", wire::render(&r));
    // NULLs stay distinct under it.
    c.simple_ok("INSERT INTO ac (id) VALUES (3), (4)").await;

    // Re-run under IF NOT EXISTS: the column and its key are both there.
    c.simple_ok("ALTER TABLE ac ADD COLUMN IF NOT EXISTS e INT UNIQUE").await;
    assert_eq!(names(&mut c, "ac").await, ["ac_e_key", "ac_pkey"]);

    // A constraint the existing rows break is refused, as PostgreSQL refuses
    // it, and the column goes with it: the statement changes nothing.
    for (sql, code, column) in [
        ("ALTER TABLE ac ADD COLUMN d INT DEFAULT 7 UNIQUE", "23505", "d"),
        ("ALTER TABLE ac ADD COLUMN k INT DEFAULT 0 CHECK (k > 0)", "23514", "k"),
    ] {
        let r = c.simple(sql).await;
        assert_eq!(wire::error_code(&r).as_deref(), Some(code), "{sql}: {}", wire::render(&r));
        let r = c.simple(&format!("SELECT {column} FROM ac")).await;
        assert!(wire::error_code(&r).is_some(), "{sql} left column {column} behind: {}", wire::render(&r));
    }

    // A foreign key and a check written on the column are enforced too.
    c.simple_ok("CREATE TABLE par (id INT PRIMARY KEY)").await;
    c.simple_ok("INSERT INTO par VALUES (1)").await;
    c.simple_ok("ALTER TABLE ac ADD COLUMN p INT REFERENCES par (id) CHECK (p > 0)").await;
    c.simple_ok("UPDATE ac SET p = 1 WHERE id = 1").await;
    let r = c.simple("UPDATE ac SET p = 9 WHERE id = 2").await;
    assert_eq!(wire::error_code(&r).as_deref(), Some("23503"), "{}", wire::render(&r));
    c.simple_ok("INSERT INTO par VALUES (-1)").await;
    let r = c.simple("UPDATE ac SET p = -1 WHERE id = 2").await;
    assert_eq!(wire::error_code(&r).as_deref(), Some("23514"), "{}", wire::render(&r));
    // A re-run finds the column, and adds none of its constraints again.
    c.simple_ok("ALTER TABLE ac ADD COLUMN IF NOT EXISTS p INT REFERENCES par (id) CHECK (p > 0)").await;

    // A whole statement goes when one column's constraint fails, not only
    // that column.
    let r = c.simple("ALTER TABLE ac ADD COLUMN a1 INT UNIQUE, ADD COLUMN a2 INT DEFAULT 0 UNIQUE").await;
    assert_eq!(wire::error_code(&r).as_deref(), Some("23505"), "{}", wire::render(&r));
    for column in ["a1", "a2"] {
        let r = c.simple(&format!("SELECT {column} FROM ac")).await;
        assert!(wire::error_code(&r).is_some(), "column {column} was left behind: {}", wire::render(&r));
    }

    // A name before an inline CHECK is the check's: it drops by that name.
    c.simple_ok("ALTER TABLE ac ADD COLUMN pos INT CONSTRAINT pos_positive CHECK (pos > 0)").await;
    c.simple_ok("ALTER TABLE ac DROP CONSTRAINT pos_positive").await;
    c.simple_ok("UPDATE ac SET pos = -1 WHERE id = 1").await;

    // A table whose name holds the word "references" is no foreign key: its
    // keys were read as one and failed to parse.
    c.simple_ok("CREATE TABLE user_preferences (k INT)").await;
    c.simple_ok("ALTER TABLE user_preferences ADD COLUMN x INT UNIQUE").await;
    c.simple_ok("ALTER TABLE user_preferences ADD COLUMN id INT PRIMARY KEY").await;
    assert_eq!(names(&mut c, "user_preferences").await, ["user_preferences_pkey", "user_preferences_x_key"]);

    // Refused before anything runs: no column is left behind.
    let r = c.simple("ALTER TABLE ac ADD COLUMN n INT UNIQUE NULLS NOT DISTINCT").await;
    assert_eq!(wire::error_code(&r).as_deref(), Some("0A000"), "{}", wire::render(&r));
    let r = c.simple("SELECT n FROM ac").await;
    assert!(wire::error_code(&r).is_some(), "column n was added: {}", wire::render(&r));
}
