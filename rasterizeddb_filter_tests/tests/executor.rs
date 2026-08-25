//! The full executor path: SQL text in, result set out.
//!
//! Aggregates and joins both scan through `Table::query_row`, which is where
//! the candidate mask is attached, so they inherit filtering. That makes them
//! worth testing here rather than only at the filter's own API: a mask that is
//! wrong in a way the row-level tests miss would surface as a wrong `COUNT` or
//! a missing join partner.

use rasterizeddb_filter_tests::*;

async fn seeded(dir: &std::path::Path) -> rclite::Arc<rasterizeddb_core::core::database::Database> {
    let db = open_db(dir).await;
    exec(
        &db,
        "CREATE TABLE items (
            id UBIGINT,
            cat INTEGER,
            name VARCHAR,
            score REAL
        );",
    )
    .await
    .expect_ok("create items");

    for i in 0..300u64 {
        let sql = format!(
            "INSERT INTO items (id, cat, name, score) VALUES ({}, {}, 'n{}', {});",
            i,
            i % 10,
            i,
            (i % 7) as f64
        );
        exec(&db, &sql).await.expect_ok(&sql);
    }
    db
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filtered_select_returns_exactly_the_matches() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(dir.path()).await;

    let sql = "SELECT id FROM items WHERE id = 42;";
    let rows = exec(&db, sql).await.rows(sql);
    assert_eq!(first_col_i64(&rows), vec![42]);

    let sql = "SELECT id FROM items WHERE cat = 3;";
    let rows = exec(&db, sql).await.rows(sql);
    let expected: Vec<i64> = (0..300i64).filter(|i| i % 10 == 3).collect();
    assert_eq!(first_col_i64(&rows), expected);

    let sql = "SELECT id FROM items WHERE id = 9999;";
    assert!(exec(&db, sql).await.rows(sql).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn count_matches_the_rows_a_select_returns() {
    // An aggregate scans through the same masked path, so a mask that pruned a
    // matching row would show up here as an undercount.
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(dir.path()).await;

    for (predicate, expected) in [
        ("", 300i64),
        ("WHERE cat = 3", 30),
        ("WHERE id = 42", 1),
        ("WHERE id = 9999", 0),
        ("WHERE id > 250", 49),
        ("WHERE cat = 3 AND id > 100", 20),
        ("WHERE id = 5 OR id = 9", 2),
    ] {
        let count_sql = format!("SELECT COUNT(*) FROM items {};", predicate);
        let count = exec(&db, &count_sql).await.rows(&count_sql);
        assert_eq!(
            first_col_i64(&count),
            vec![expected],
            "`{}` counted wrong",
            count_sql
        );

        // And it must agree with actually fetching them.
        let select_sql = format!("SELECT id FROM items {};", predicate);
        let rows = exec(&db, &select_sql).await.rows(&select_sql);
        assert_eq!(
            rows.len() as i64,
            expected,
            "`{}` returned a different number of rows than COUNT",
            select_sql
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregates_over_a_filtered_scan_are_exact() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(dir.path()).await;

    // cat = 3 selects ids 3, 13, 23, ... 293.
    let ids: Vec<i64> = (0..300i64).filter(|i| i % 10 == 3).collect();
    let want_sum: i64 = ids.iter().sum();
    let want_min = *ids.iter().min().unwrap();
    let want_max = *ids.iter().max().unwrap();

    for (f, want) in [
        ("SUM(id)", want_sum),
        ("MIN(id)", want_min),
        ("MAX(id)", want_max),
        ("COUNT(id)", ids.len() as i64),
    ] {
        let sql = format!("SELECT {} FROM items WHERE cat = 3;", f);
        let rows = exec(&db, &sql).await.rows(&sql);
        assert_eq!(first_col_i64(&rows), vec![want], "`{}`", sql);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_covers_every_group() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(dir.path()).await;

    let sql = "SELECT cat, COUNT(*) FROM items GROUP BY cat;";
    let rows = exec(&db, sql).await.rows(sql);
    assert_eq!(rows.len(), 10, "expected one row per category");

    // Every group has 30 members, so the counts column must be all 30s.
    let counts: Vec<i64> = rows
        .iter()
        .map(|r| {
            let c = r.columns.get(1).expect("count column");
            let b = c.data.into_slice();
            match b.len() {
                4 => i32::from_le_bytes(b.try_into().unwrap()) as i64,
                8 => i64::from_le_bytes(b.try_into().unwrap()),
                16 => i128::from_le_bytes(b.try_into().unwrap()) as i64,
                n => panic!("unexpected count width {}", n),
            }
        })
        .collect();
    assert!(counts.iter().all(|c| *c == 30), "group counts: {:?}", counts);
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_over_a_filtered_scan_is_exact() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(dir.path()).await;

    let sql = "SELECT cat, COUNT(*) FROM items WHERE id > 150 GROUP BY cat;";
    let rows = exec(&db, sql).await.rows(sql);
    let total: i64 = rows
        .iter()
        .map(|r| {
            let b = r.columns.get(1).unwrap().data.into_slice();
            match b.len() {
                4 => i32::from_le_bytes(b.try_into().unwrap()) as i64,
                8 => i64::from_le_bytes(b.try_into().unwrap()),
                16 => i128::from_le_bytes(b.try_into().unwrap()) as i64,
                n => panic!("width {}", n),
            }
        })
        .sum();
    assert_eq!(total, 149, "grouped counts must sum to the filtered row count");
}

#[tokio::test(flavor = "multi_thread")]
async fn distinct_does_not_lose_values() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(dir.path()).await;

    let sql = "SELECT DISTINCT cat FROM items;";
    let rows = exec(&db, sql).await.rows(sql);
    let mut cats = first_col_i64(&rows);
    cats.dedup();
    assert_eq!(cats, (0..10i64).collect::<Vec<_>>());

    let sql = "SELECT DISTINCT cat FROM items WHERE id > 290;";
    let rows = exec(&db, sql).await.rows(sql);
    // ids 291..299 -> cats 1..9
    assert_eq!(first_col_i64(&rows), (1..10i64).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

async fn joined(dir: &std::path::Path) -> rclite::Arc<rasterizeddb_core::core::database::Database> {
    let db = open_db(dir).await;
    exec(
        &db,
        "CREATE TABLE owners (oid UBIGINT, oname VARCHAR, region INTEGER);",
    )
    .await
    .expect_ok("create owners");
    exec(
        &db,
        "CREATE TABLE pets (pid UBIGINT, owner_id UBIGINT, pname VARCHAR);",
    )
    .await
    .expect_ok("create pets");

    for o in 0..20u64 {
        let sql = format!(
            "INSERT INTO owners (oid, oname, region) VALUES ({}, 'o{}', {});",
            o,
            o,
            o % 4
        );
        exec(&db, &sql).await.expect_ok(&sql);
    }
    // Two pets per owner, plus five orphans with no matching owner.
    for p in 0..40u64 {
        let sql = format!(
            "INSERT INTO pets (pid, owner_id, pname) VALUES ({}, {}, 'p{}');",
            p,
            p / 2,
            p
        );
        exec(&db, &sql).await.expect_ok(&sql);
    }
    for p in 40..45u64 {
        let sql = format!(
            "INSERT INTO pets (pid, owner_id, pname) VALUES ({}, 999, 'orphan{}');",
            p, p
        );
        exec(&db, &sql).await.expect_ok(&sql);
    }
    db
}

#[tokio::test(flavor = "multi_thread")]
async fn an_inner_join_pairs_every_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = joined(dir.path()).await;

    let sql = "SELECT pets.pid FROM pets INNER JOIN owners ON pets.owner_id = owners.oid;";
    let rows = exec(&db, sql).await.rows(sql);
    assert_eq!(rows.len(), 40, "every non-orphan pet should pair exactly once");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filtered_join_prunes_without_losing_partners() {
    // The per-relation predicate goes through the masked scan, so a mask that
    // pruned a matching owner would silently drop that owner's pets.
    let dir = tempfile::tempdir().unwrap();
    let db = joined(dir.path()).await;

    let sql = "SELECT pets.pid FROM pets INNER JOIN owners ON pets.owner_id = owners.oid \
               WHERE owners.oid = 7;";
    let rows = exec(&db, sql).await.rows(sql);
    assert_eq!(rows.len(), 2, "owner 7 has exactly two pets");

    let sql = "SELECT pets.pid FROM pets INNER JOIN owners ON pets.owner_id = owners.oid \
               WHERE owners.region = 1;";
    // Owners 1, 5, 9, 13, 17 are region 1; two pets each.
    assert_eq!(exec(&db, sql).await.rows(sql).len(), 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_left_join_keeps_unmatched_rows() {
    let dir = tempfile::tempdir().unwrap();
    let db = joined(dir.path()).await;

    let sql = "SELECT pets.pid FROM pets LEFT JOIN owners ON pets.owner_id = owners.oid;";
    let rows = exec(&db, sql).await.rows(sql);
    assert_eq!(
        rows.len(),
        45,
        "a LEFT JOIN must keep the five orphans as well as the 40 matches"
    );
}

// ---------------------------------------------------------------------------
// Writes through the executor
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn deletes_and_updates_through_sql_keep_the_index_current() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(dir.path()).await;

    exec(&db, "DELETE FROM items WHERE id = 42;").await.expect_ok("delete");
    let sql = "SELECT id FROM items WHERE id = 42;";
    assert!(exec(&db, sql).await.rows(sql).is_empty(), "deleted row still returned");

    let sql = "SELECT COUNT(*) FROM items;";
    let rows = exec(&db, sql).await.rows(sql);
    assert_eq!(first_col_i64(&rows), vec![299]);

    exec(&db, "UPDATE items SET id = 777777 WHERE id = 43;")
        .await
        .expect_ok("update");
    let sql = "SELECT id FROM items WHERE id = 43;";
    assert!(exec(&db, sql).await.rows(sql).is_empty(), "old value still findable");
    let sql = "SELECT id FROM items WHERE id = 777777;";
    assert_eq!(first_col_i64(&exec(&db, sql).await.rows(sql)), vec![777777]);
}

#[tokio::test(flavor = "multi_thread")]
async fn random_predicates_through_sql_match_brute_force() {
    let dir = tempfile::tempdir().unwrap();
    let db = seeded(dir.path()).await;

    for _ in 0..40 {
        let (clause, want): (String, Vec<i64>) = match fastrand::usize(0..5) {
            0 => {
                let v = fastrand::i64(0..320);
                (format!("WHERE id = {}", v), (0..300i64).filter(|i| *i == v).collect())
            }
            1 => {
                let v = fastrand::i64(0..12);
                (format!("WHERE cat = {}", v), (0..300i64).filter(|i| i % 10 == v).collect())
            }
            2 => {
                let v = fastrand::i64(0..300);
                (format!("WHERE id > {}", v), (0..300i64).filter(|i| *i > v).collect())
            }
            3 => {
                let (a, b) = (fastrand::i64(0..300), fastrand::i64(0..300));
                (
                    format!("WHERE id = {} OR id = {}", a, b),
                    (0..300i64).filter(|i| *i == a || *i == b).collect(),
                )
            }
            _ => {
                let v = fastrand::i64(0..300);
                let c = v % 10;
                (
                    format!("WHERE cat = {} AND id > {}", c, v),
                    (0..300i64).filter(|i| i % 10 == c && *i > v).collect(),
                )
            }
        };
        let sql = format!("SELECT id FROM items {};", clause);
        let rows = exec(&db, &sql).await.rows(&sql);
        assert_eq!(first_col_i64(&rows), want, "`{}`", sql);

        // COUNT must agree with the rows fetched.
        let csql = format!("SELECT COUNT(*) FROM items {};", clause);
        let crows = exec(&db, &csql).await.rows(&csql);
        assert_eq!(
            first_col_i64(&crows),
            vec![want.len() as i64],
            "`{}` disagreed with the row count",
            csql
        );
    }
}
