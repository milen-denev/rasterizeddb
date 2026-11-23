//! What the last of the pms migrations' `DO` blocks needed, each a general
//! gap rather than that block's own (m20260706_000001_drop_redundant_indexes,
//! which walks `pg_index` for indexes that are a prefix of another):
//!
//! - arrays compared in a `WHERE` (`a = b`, `a[1:n] = b`, `a <> ARRAY[…]`):
//!   "operation is not supported for this column type", and "malformed
//!   array literal" for a literal that was well formed;
//! - `generate_subscripts(array, dim [, reverse])`;
//! - `*` or `t.*` beside other select items ("Column 't.*' not found in
//!   schema");
//! - a correlated `EXISTS` that compares an outer column by other than `=`;
//! - `pg_index.indclass`, `indoption`, a NULL `indpred`, and `indkey::text`
//!   spaced as PostgreSQL's int2vector prints;
//! - `DROP INDEX public.idx`, which answered DROP INDEX and dropped nothing.
//!
//! Every expectation is PostgreSQL 17's answer to the same statement, in
//! the same order.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Executor, Row};

async fn rows(pool: &sqlx::PgPool, sql: &str) -> Vec<String> {
    let rows = sqlx::raw_sql(AssertSqlSafe(sql.to_string()))
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` should succeed: {e}"));
    rows.iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.try_get_unchecked::<Option<String>, _>(i).expect("text").unwrap_or_else(|| "NULL".into()))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

async fn code(pool: &sqlx::PgPool, sql: &str) -> String {
    let e = sqlx::raw_sql(AssertSqlSafe(sql.to_string())).fetch_all(pool).await.expect_err(sql);
    e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string()).unwrap_or_else(|| format!("no code: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn arrays_subscripts_stars_and_exists_answer_as_postgres() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (r#"CREATE TABLE ar (id int PRIMARY KEY, a text[], b text[], n int, i int[])"#, vec![]),
        (r#"INSERT INTO ar VALUES (1, '{x,y}', '{x}', 1, '{1,2}'), (2, '{x,y}', '{x,y}', 2, '{3}'), (3, NULL, '{x}', 1, NULL), (4, '{p,q,r}', '{p,q}', 2, '{1,2}')"#, vec![]),
        (r#"SELECT id FROM ar WHERE a = b ORDER BY id"#, vec![r#"2"#]),
        (r#"SELECT id FROM ar WHERE a[1:n] = b ORDER BY id"#, vec![r#"1"#, r#"2"#, r#"4"#]),
        (r#"SELECT id FROM ar WHERE a <> b ORDER BY id"#, vec![r#"1"#, r#"4"#]),
        (r#"SELECT id FROM ar WHERE a = ARRAY['x','y'] ORDER BY id"#, vec![r#"1"#, r#"2"#]),
        (r#"SELECT id FROM ar WHERE a = '{x,y}' ORDER BY id"#, vec![r#"1"#, r#"2"#]),
        (r#"SELECT id FROM ar WHERE b = a[1:n] AND id > 0 ORDER BY id"#, vec![r#"1"#, r#"2"#, r#"4"#]),
        (r#"SELECT id FROM ar WHERE a > b ORDER BY id"#, vec![r#"1"#, r#"4"#]),
        (r#"SELECT id FROM ar WHERE NOT (a = b) ORDER BY id"#, vec![r#"1"#, r#"4"#]),
        (r#"SELECT id FROM ar WHERE NOT (a = b AND id > 0) ORDER BY id"#, vec![r#"1"#, r#"4"#]),
        (r#"SELECT id FROM ar WHERE (a = b OR a <> b) AND id < 3 ORDER BY id"#, vec![r#"1"#, r#"2"#]),
        (r#"SELECT id FROM ar WHERE a = b OR id = 3 ORDER BY id"#, vec![r#"2"#, r#"3"#]),
        (r#"SELECT id FROM ar WHERE i = '{1,2}' ORDER BY id"#, vec![r#"1"#, r#"4"#]),
        (r#"SELECT id FROM ar WHERE i = ARRAY[1,2] AND a <> b ORDER BY id"#, vec![r#"1"#, r#"4"#]),
        (r#"SELECT count(*) FROM ar WHERE a = b AND a IS NOT NULL"#, vec![r#"1"#]),
        (r#"SELECT id FROM ar WHERE a[1] = 'x' AND 'y' = ANY(a) ORDER BY id"#, vec![r#"1"#, r#"2"#]),
        (r#"SELECT i FROM generate_subscripts(ARRAY['a','b'], 1) AS g(i)"#, vec![r#"1"#, r#"2"#]),
        (r#"SELECT i FROM generate_subscripts(ARRAY['a','b','c'], 1, true) AS g(i)"#, vec![r#"3"#, r#"2"#, r#"1"#]),
        (r#"SELECT count(*) FROM generate_subscripts(ARRAY[]::text[], 1) AS g(i)"#, vec![r#"0"#]),
        (r#"SELECT id, (SELECT array_agg(ar.a[s] || '!' ORDER BY s) FROM generate_subscripts(ar.a, 1) AS g(s)) FROM ar ORDER BY id"#, vec![r#"1|{x!,y!}"#, r#"2|{x!,y!}"#, r#"3|NULL"#, r#"4|{p!,q!,r!}"#]),
        (r#"SELECT *, n + 1 AS m FROM ar WHERE id = 1"#, vec![r#"1|{x,y}|{x}|1|{1,2}|2"#]),
        (r#"SELECT x.id, x.*, upper(x.a[1]) FROM ar x WHERE x.id = 2"#, vec![r#"2|2|{x,y}|{x,y}|2|{3}|X"#]),
        (r#"SELECT 1 AS one, ar.* FROM ar WHERE id = 3"#, vec![r#"1|3|NULL|{x}|1|NULL"#]),
        (r#"WITH raw AS (SELECT id, a FROM ar WHERE id < 3) SELECT raw.*, ('x' = ANY(raw.a)) AS h FROM raw ORDER BY id"#, vec![r#"1|{x,y}|t"#, r#"2|{x,y}|t"#]),
        (r#"SELECT id FROM ar WHERE EXISTS (SELECT 1 FROM ar o WHERE o.n > ar.n) ORDER BY id"#, vec![r#"1"#, r#"3"#]),
        (r#"SELECT id FROM ar WHERE NOT EXISTS (SELECT 1 FROM ar o WHERE o.n > ar.n) ORDER BY id"#, vec![r#"2"#, r#"4"#]),
        (r#"SELECT id FROM ar WHERE EXISTS (SELECT 1 FROM ar o WHERE o.n = ar.n AND o.id <> ar.id) ORDER BY id"#, vec![r#"1"#, r#"2"#, r#"3"#, r#"4"#]),
        (r#"SELECT id FROM ar WHERE EXISTS (SELECT 1 FROM ar o WHERE o.n = ar.n AND o.id >= ar.id AND (o.id > ar.id OR o.a = ar.a)) ORDER BY id"#, vec![r#"1"#, r#"2"#, r#"4"#]),
        (r#"SELECT count(*) FROM ar WHERE EXISTS (SELECT 1 FROM ar o WHERE o.id < ar.id) AND EXISTS (SELECT 1 FROM ar p WHERE p.id > ar.id)"#, vec![r#"2"#]),
        (r#"SELECT id FROM ar WHERE EXISTS (SELECT 1 FROM ar o WHERE o.n = ar.n AND o.id = 1) ORDER BY id"#, vec![r#"1"#, r#"3"#]),
        (r#"CREATE INDEX ar_n ON ar (n)"#, vec![]),
        (r#"CREATE INDEX ar_n_id ON ar (n, id)"#, vec![]),
        (r#"SELECT c.relname, ix.indnkeyatts, ix.indkey::text, ix.indoption::text, ix.indpred IS NULL, ix.indexprs IS NULL, (string_to_array(ix.indkey::text, ' '))[1:ix.indnkeyatts] FROM pg_index ix JOIN pg_class c ON c.oid = ix.indexrelid JOIN pg_class t ON t.oid = ix.indrelid WHERE t.relname = 'ar' ORDER BY 1"#, vec![r#"ar_n|1|4|0|t|t|{4}"#, r#"ar_n_id|2|4 1|0 0|t|t|{4,1}"#, r#"ar_pkey|1|1|0|t|t|{1}"#]),
        (r#"SELECT count(*) FROM pg_index ix JOIN pg_class t ON t.oid = ix.indrelid WHERE t.relname = 'ar' AND ix.indpred IS NOT NULL"#, vec![r#"0"#]),
        (r#"DROP INDEX public.ar_n"#, vec![]),
        (r#"DROP INDEX IF EXISTS "public"."ar_n_id""#, vec![]),
        (r#"SELECT indexname FROM pg_indexes WHERE tablename = 'ar' ORDER BY 1"#, vec![r#"ar_pkey"#]),
    ];
    for (sql, expected) in cases {
        let first = sql.split_whitespace().next().unwrap_or("").to_ascii_uppercase();
        if first == "SELECT" || first == "WITH" {
            assert_eq!(rows(&pool, sql).await, expected, "{sql}");
        } else {
            pool.execute(sqlx::raw_sql(AssertSqlSafe(sql))).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
    }
    // A literal that is no array, compared with one: 22P02, not no rows.
    pool.execute(sqlx::raw_sql("CREATE TABLE lit (id int PRIMARY KEY, a text[], i int[]); INSERT INTO lit VALUES (1, '{x}', '{1}')"))
        .await
        .expect("table");
    for sql in ["SELECT id FROM lit WHERE i = '{1,2'", "SELECT id FROM lit WHERE a = 'x'"] {
        assert_eq!(code(&pool, sql).await, "22P02", "{sql}");
    }
}

/// The block itself, over a table with an index that is a prefix of another,
/// an exact duplicate, and a unique index it must leave.
#[tokio::test(flavor = "multi_thread")]
async fn the_redundant_index_cleanup_drops_what_postgres_drops() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    pool.execute(sqlx::raw_sql(
        "CREATE TABLE dlm (id bigint PRIMARY KEY, object_key text, n int);
         CREATE INDEX idx_a ON dlm (object_key);
         CREATE INDEX idx_ab ON dlm (object_key, id);
         CREATE INDEX idx_n ON dlm (n);
         CREATE INDEX idx_n2 ON dlm (n);
         CREATE UNIQUE INDEX uq_n ON dlm (n, id);",
    ))
    .await
    .expect("setup");
    pool.execute(sqlx::raw_sql(CLEANUP)).await.expect("the block");
    assert_eq!(
        rows(&pool, "SELECT indexname FROM pg_indexes WHERE tablename = 'dlm' ORDER BY 1").await,
        ["dlm_pkey", "idx_ab", "uq_n"]
    );
    // Again: nothing left to drop, and nothing dropped.
    pool.execute(sqlx::raw_sql(CLEANUP)).await.expect("the block, again");
    assert_eq!(
        rows(&pool, "SELECT indexname FROM pg_indexes WHERE tablename = 'dlm' ORDER BY 1").await,
        ["dlm_pkey", "idx_ab", "uq_n"]
    );
}

/// The second loop of the migration's block, as it is written there.
const CLEANUP: &str = r#"DO $$
DECLARE
    r       record;
    dropped int := 0;
BEGIN
    -- OVERLAPPING indexes: a btree index whose ordered key signature is a
    -- strict left-prefix of another valid btree index on the same table.
    FOR r IN
        WITH raw AS (
            SELECT
                ix.indexrelid            AS index_oid,
                ix.indrelid              AS table_oid,
                n.nspname                AS schema_name,
                c.relname                AS index_name,
                ix.indisunique           AS is_unique,
                ix.indisprimary          AS is_primary,
                ix.indisvalid            AS is_valid,
                (ix.indpred IS NOT NULL) AS is_partial,
                ix.indnkeyatts           AS nkey,
                (string_to_array(ix.indkey::text,    ' '))[1:ix.indnkeyatts] AS katt,
                (string_to_array(ix.indclass::text,  ' '))[1:ix.indnkeyatts] AS kcls,
                (string_to_array(ix.indoption::text, ' '))[1:ix.indnkeyatts] AS kopt
            FROM pg_index ix
            JOIN pg_class c    ON c.oid = ix.indexrelid
            JOIN pg_class t    ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am      ON am.oid = c.relam
            WHERE am.amname = 'btree'
              AND n.nspname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')
              AND ix.indexrelid NOT IN (SELECT inhrelid FROM pg_inherits)
        ),
        sig AS (
            SELECT
                raw.*,
                ('0' = ANY(raw.katt)) AS has_expr,
                (SELECT array_agg(raw.katt[i] || ':' || raw.kcls[i] || ':' || raw.kopt[i]
                                  ORDER BY i)
                   FROM generate_subscripts(raw.katt, 1) AS g(i)) AS keysig
            FROM raw
        )
        SELECT cand.schema_name, cand.index_name
        FROM sig cand
        WHERE NOT cand.is_primary
          AND NOT cand.is_unique
          AND NOT cand.is_partial
          AND NOT cand.has_expr
          AND cand.is_valid
          AND EXISTS (
              SELECT 1
              FROM sig cov
              WHERE cov.table_oid = cand.table_oid
                AND cov.index_oid <> cand.index_oid
                AND cov.is_valid
                AND NOT cov.is_partial
                AND NOT cov.has_expr
                AND cov.nkey >= cand.nkey
                AND cov.keysig[1:cand.nkey] = cand.keysig
                -- longer index wins; for exact duplicates keep the lower oid
                AND (cov.nkey > cand.nkey OR cov.index_oid < cand.index_oid)
          )
    LOOP
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', r.schema_name, r.index_name);
        RAISE NOTICE 'dropped redundant index %.%', r.schema_name, r.index_name;
        dropped := dropped + 1;
    END LOOP;
    RAISE NOTICE 'redundant-index cleanup complete: % index(es) dropped', dropped;
END $$"#;
