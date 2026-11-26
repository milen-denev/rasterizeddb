//! A derived table joined to a few rows is built for those rows' keys
//! (`rql::derived_pushdown`), not whole: EF Core's `Take(10)` joined to a
//! derived table of every variant, or of every image numbered by
//! `ROW_NUMBER()`, built the whole table. The answers are the ones the
//! data gives, where the keys are pushed and where they must not be. Over
//! pgwire: the native protocol takes no derived table.

use super::correlated_subquery_tests::pg_rows;
use super::owner_tests::{Session, ok, session};
use super::*;
use crate::core::rql::derived_pushdown::test_hooks::take;

const PRODUCTS: i64 = 300;
const VARIANTS: i64 = 3000;

fn product_of(v: i64) -> i64 {
    v * 7 % 290 + 1
}
fn ord(v: i64) -> i64 {
    v % 13
}
fn color(v: i64) -> String {
    format!("c{}", v % 5)
}
fn hidden(v: i64) -> bool {
    v % 4 == 0
}

async fn fill(db: &Arc<Database>, s: &mut Session) {
    ok(db, s, "CREATE TABLE dp_products (id INT NOT NULL PRIMARY KEY, grp INT NOT NULL, ref INT)").await;
    ok(db, s, "CREATE TABLE dp_variants (id INT NOT NULL PRIMARY KEY, product_id INT NOT NULL, color TEXT, ord INT NOT NULL, hidden BOOLEAN NOT NULL)")
        .await;
    ok(db, s, "CREATE INDEX dp_variants_product ON dp_variants (product_id)").await;
    // `ref` is NULL for every third product: a NULL key joins nothing.
    let values: Vec<String> = (1..=PRODUCTS)
        .map(|p| format!("({p}, {}, {})", p % 10, if p % 3 == 0 { "NULL".to_string() } else { (p % 50 + 1).to_string() }))
        .collect();
    ok(db, s, &format!("INSERT INTO dp_products (id, grp, ref) VALUES {}", values.join(", "))).await;
    let values: Vec<String> =
        (1..=VARIANTS).map(|v| format!("({v}, {}, '{}', {}, {})", product_of(v), color(v), ord(v), hidden(v))).collect();
    ok(db, s, &format!("INSERT INTO dp_variants (id, product_id, color, ord, hidden) VALUES {}", values.join(", "))).await;
}

type Rows = Vec<Vec<Option<String>>>;

/// The statement's rows.
async fn both(db: &Arc<Database>, s: &mut Session, sql: &str) -> Rows {
    pg_rows(db, s, sql).await
}

fn cell(v: impl ToString) -> Option<String> {
    Some(v.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_derived_table_is_built_for_the_keys_it_joins() {
    let db = crate::replication::test_support::in_memory_database().await;
    let mut s = session(&db);
    fill(&db, &mut s).await;
    // The ten products of group 7 first in id order: 7, 17, … 97.
    let top: Vec<i64> = (1..=PRODUCTS).filter(|p| p % 10 == 7).take(10).collect();
    let picked = "(SELECT d.id FROM dp_products AS d WHERE d.grp = 7 ORDER BY d.id LIMIT 10)";

    // INNER JOIN to a filtered derived table (the product page's colours).
    let sql = format!(
        "SELECT dpv.color, p.id, dpv.id FROM {picked} AS p INNER JOIN (SELECT d10.color, d10.id, d10.ord, d10.product_id FROM dp_variants \
         AS d10 WHERE d10.hidden = false) AS dpv ON p.id = dpv.product_id ORDER BY p.id, dpv.ord"
    );
    take("dpv");
    let got = both(&db, &mut s, &sql).await;
    let mut want: Vec<(i64, i64, i64)> =
        (1..=VARIANTS).filter(|v| !hidden(*v) && top.contains(&product_of(*v))).map(|v| (product_of(v), ord(v), v)).collect();
    want.sort();
    let want: Rows = want.iter().map(|(p, _, v)| vec![cell(color(*v)), cell(p), cell(v)]).collect();
    assert_eq!(got, want, "{sql}");
    // Built for the ten keys.
    assert_eq!(take("dpv"), Some(10));

    // LEFT JOIN to each product's first variant by `ord`, numbered by a
    // window inside a nested derived table: the keys reach the window's
    // own table through the statement's WHERE on the level between.
    let edge = "(SELECT d.id FROM dp_products AS d WHERE d.id >= 288 ORDER BY d.id LIMIT 10)";
    let sql = format!(
        "SELECT p.id, dpw.color FROM {edge} AS p LEFT JOIN (SELECT dpn.color, dpn.product_id FROM (SELECT d2.color, d2.product_id, \
         ROW_NUMBER() OVER (PARTITION BY d2.product_id ORDER BY d2.ord) AS rn FROM dp_variants AS d2) AS dpn WHERE dpn.rn <= 1) AS dpw \
         ON p.id = dpw.product_id ORDER BY p.id"
    );
    take("dpw");
    take("dpn:where");
    let got = both(&db, &mut s, &sql).await;
    let want: Rows = (288..=297)
        .map(|p| {
            // Products past 290 have no variant: kept, with a NULL.
            let first = (1..=VARIANTS).filter(|v| product_of(*v) == p).min_by_key(|v| ord(*v));
            vec![cell(p), first.map(color)]
        })
        .collect();
    assert_eq!(got, want, "{sql}");
    assert_eq!(take("dpw"), Some(10));
    assert!(take("dpn:where").is_some(), "the nested window's table was built whole");

    // No row on the other side: nothing is built, nothing answered.
    let none = "(SELECT d.id FROM dp_products AS d WHERE d.grp = 99 ORDER BY d.id LIMIT 10)";
    let sql = format!(
        "SELECT p.id, dpe.id FROM {none} AS p INNER JOIN (SELECT d10.id, d10.product_id FROM dp_variants AS d10) AS dpe \
         ON p.id = dpe.product_id ORDER BY p.id"
    );
    take("dpe");
    assert!(both(&db, &mut s, &sql).await.is_empty());
    assert_eq!(take("dpe"), Some(0));

    // NULL keys join nothing, and are not pushed as values.
    let sql = "SELECT l.id, dpr.grp FROM (SELECT d.id, d.ref FROM dp_products AS d WHERE d.id <= 6) AS l INNER JOIN \
               (SELECT e.id, e.grp FROM dp_products AS e) AS dpr ON l.ref = dpr.id ORDER BY l.id";
    take("dpr");
    let got = both(&db, &mut s, sql).await;
    let want: Rows = (1..=6i64).filter(|p| p % 3 != 0).map(|p| vec![cell(p), cell((p % 50 + 1) % 10)]).collect();
    assert_eq!(got, want);
    assert_eq!(take("dpr"), Some(4));

    // The statement's own WHERE over literals reaches the body too.
    let sql = "SELECT dpx.id FROM (SELECT d.id, d.grp FROM dp_products AS d) AS dpx WHERE dpx.grp IN (7, 8) AND dpx.id > 280 ORDER BY dpx.id";
    take("dpx:where");
    let got = both(&db, &mut s, sql).await;
    assert_eq!(got, vec![vec![cell(287)], vec![cell(288)], vec![cell(297)], vec![cell(298)]]);
    assert!(take("dpx:where").is_some());
}

/// What a predicate inside the body would change is built whole, and
/// answered the same: a body with a LIMIT, one that folds every row, a
/// window over another key, and a join on something other than a column.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_the_keys_would_change_is_built_whole() {
    let db = crate::replication::test_support::in_memory_database().await;
    let mut s = session(&db);
    fill(&db, &mut s).await;
    let picked = "(SELECT d.id FROM dp_products AS d WHERE d.grp = 7 ORDER BY d.id LIMIT 3)";
    let top = [7i64, 17, 27];

    // The body's own LIMIT decides its rows first: variants 1 to 40 only.
    let sql = format!(
        "SELECT p.id, dql.id FROM {picked} AS p INNER JOIN (SELECT d10.id, d10.product_id FROM dp_variants AS d10 ORDER BY d10.id LIMIT 40) \
         AS dql ON p.id = dql.product_id ORDER BY p.id, dql.id"
    );
    take("dql");
    let got = both(&db, &mut s, &sql).await;
    let mut want: Vec<(i64, i64)> = (1..=40).filter(|v| top.contains(&product_of(*v))).map(|v| (product_of(v), v)).collect();
    want.sort();
    assert_eq!(got, want.iter().map(|(p, v)| vec![cell(p), cell(v)]).collect::<Rows>(), "{sql}");
    assert_eq!(take("dql"), None);

    // Numbered over the whole table, not per product: row numbers would
    // change with the rows left out.
    let sql = format!(
        "SELECT p.id, dqw.rn FROM {picked} AS p INNER JOIN (SELECT d2.product_id, d2.id, ROW_NUMBER() OVER (ORDER BY d2.id) AS rn \
         FROM dp_variants AS d2) AS dqw ON p.id = dqw.product_id WHERE dqw.id <= 60 ORDER BY p.id, dqw.rn"
    );
    take("dqw");
    let got = both(&db, &mut s, &sql).await;
    let mut want: Vec<(i64, i64)> = (1..=60).filter(|v| top.contains(&product_of(*v))).map(|v| (product_of(v), v)).collect();
    want.sort();
    assert_eq!(got, want.iter().map(|(p, v)| vec![cell(p), cell(v)]).collect::<Rows>(), "{sql}");
    assert_eq!(take("dqw"), None);

    // Grouped by the key: whole groups are left out, the counts stand.
    let sql = format!(
        "SELECT p.id, dqg.n FROM {picked} AS p INNER JOIN (SELECT d10.product_id, COUNT(*) AS n FROM dp_variants AS d10 \
         GROUP BY d10.product_id) AS dqg ON p.id = dqg.product_id ORDER BY p.id"
    );
    take("dqg");
    let got = both(&db, &mut s, &sql).await;
    let want: Rows = top.iter().map(|p| vec![cell(p), cell((1..=VARIANTS).filter(|v| product_of(*v) == *p).count())]).collect();
    assert_eq!(got, want, "{sql}");
    assert_eq!(take("dqg"), Some(3));
}

/// A pushed list compares as the join does, or is not pushed: each
/// statement here answers as it does with the pushdown off (the body given
/// a LIMIT, which leaves it whole). Case- and accent-insensitive keys match
/// their other spellings inside the body as the join matches them;
/// `character(n)`, a key of another kind than the column, and text under
/// two collations are built whole.
#[tokio::test(flavor = "multi_thread")]
async fn a_pushed_list_compares_as_the_join_does() {
    let db = crate::replication::test_support::in_memory_database().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE dk_keys (id INT NOT NULL PRIMARY KEY, ci TEXT COLLATE rdb_ci, ai TEXT COLLATE rdb_ci_ai, bin TEXT, \
                     ch CHARACTER(4), n INT, u TEXT, g UUID)")
        .await;
    ok(&db, &mut s, "CREATE TABLE dk_rows (id INT NOT NULL PRIMARY KEY, ci TEXT COLLATE rdb_ci, ai TEXT COLLATE rdb_ci_ai, bin TEXT, \
                     ch CHARACTER(6), big BIGINT, g UUID, v TEXT)")
        .await;
    ok(&db, &mut s, "INSERT INTO dk_keys VALUES (1, 'ABC', 'é', 'Abc', 'ab', 7, '0A000000-0000-0000-0000-00000000000B', '0a000000-0000-0000-0000-00000000000b'), \
                     (2, 'zzz', 'zzz', 'zzz', 'zz', 8, 'ffffffff-0000-0000-0000-000000000000', NULL)")
        .await;
    ok(&db, &mut s, "INSERT INTO dk_rows VALUES (10, 'abc', 'e', 'abc', 'ab', 7, '0a000000-0000-0000-0000-00000000000b', 'hit'), \
                     (11, 'other', 'x', 'Abc', 'cd', 9, '11111111-0000-0000-0000-000000000000', 'bin'), \
                     (12, NULL, NULL, NULL, NULL, NULL, NULL, 'nulls')")
        .await;
    // (the key column on the built side, the body's column, pushed?, the ids joined to key row 1)
    let cases: [(&str, &str, bool, &[i64]); 7] = [
        ("ci", "ci", true, &[10]),
        ("ai", "ai", true, &[10]),
        ("bin", "bin", true, &[11]),
        ("g", "g", true, &[10]),
        ("n", "big", true, &[10]),
        // character(n) compares padded: left to the join.
        ("ch", "ch", false, &[10]),
        // A binary key against a case-insensitive column (and the reverse)
        // is the join's to decide.
        ("bin", "ci", false, &[]),
    ];
    for (key, column, pushed, want) in cases {
        let statement = |limit: &str| {
            format!(
                "SELECT k.id, dkj.id FROM (SELECT d.id, d.{key} FROM dk_keys AS d WHERE d.id = 1 ORDER BY d.id LIMIT 5) AS k \
                 INNER JOIN (SELECT r.id, r.{column} FROM dk_rows AS r{limit}) AS dkj ON k.{key} = dkj.{column} ORDER BY dkj.id"
            )
        };
        take("dkj");
        let on = super::owner_tests::send(&db, &mut s, &statement("")).await.map(|_| ());
        let was_pushed = take("dkj").is_some();
        assert_eq!(was_pushed, pushed, "{key} = {column}");
        // The same statement, its body left whole.
        let off = super::owner_tests::send(&db, &mut s, &statement(" ORDER BY r.id LIMIT 1000")).await.map(|_| ());
        assert_eq!(take("dkj"), None);
        assert_eq!(on.is_ok(), off.is_ok(), "{key} = {column}: {on:?} / {off:?}");
        if on.is_ok() {
            let rows_on = both(&db, &mut s, &statement("")).await;
            let rows_off = both(&db, &mut s, &statement(" ORDER BY r.id LIMIT 1000")).await;
            assert_eq!(rows_on, rows_off, "{key} = {column}");
            if !want.is_empty() || pushed {
                let ids: Vec<i64> = rows_on.iter().map(|r| r[1].as_deref().unwrap().parse().unwrap()).collect();
                assert_eq!(ids, want, "{key} = {column}");
            }
        }
    }
    // A uuid written in another case, held as text, against a uuid column:
    // two kinds, built whole, answered as the join answers.
    let statement = |limit: &str| {
        format!(
            "SELECT k.id, dku.id FROM (SELECT d.id, d.u FROM dk_keys AS d WHERE d.id = 1 ORDER BY d.id LIMIT 5) AS k \
             INNER JOIN (SELECT r.id, r.g FROM dk_rows AS r{limit}) AS dku ON k.u = dku.g ORDER BY dku.id"
        )
    };
    take("dku");
    let on = super::owner_tests::send(&db, &mut s, &statement("")).await.map(|_| ());
    assert_eq!(take("dku"), None);
    let off = super::owner_tests::send(&db, &mut s, &statement(" ORDER BY r.id LIMIT 1000")).await.map(|_| ());
    assert_eq!(on.is_ok(), off.is_ok(), "{on:?} / {off:?}");
    if on.is_ok() {
        assert_eq!(both(&db, &mut s, &statement("")).await, both(&db, &mut s, &statement(" ORDER BY r.id LIMIT 1000")).await);
    }
}

/// A derived table whose body reads more than one relation (a join, or a
/// select-list subquery, which is joined as a LATERAL item) keeps each
/// column's collation in its copy. Copied as a plain `uuid`, EF Core's
/// parent query (`FROM (SELECT TOP … two subqueries …) AS d6 LEFT JOIN … ORDER
/// BY d6.Id`) came back in byte order while its split child query, whose
/// body is one table, came in SQL Server's: EF Core pairs the two by
/// position, and the children went to the wrong parents. A text column
/// lost its case-insensitive comparison the same way.
#[tokio::test(flavor = "multi_thread")]
async fn a_body_of_several_relations_keeps_its_collations() {
    let db = crate::replication::test_support::in_memory_database().await;
    let mut s = session(&db);
    ok(&db, &mut s, "CREATE TABLE dc_products (id uuid COLLATE rdb_guid_order NOT NULL PRIMARY KEY, name text COLLATE rdb_ci NOT NULL, n INT NOT NULL)")
        .await;
    ok(&db, &mut s, "CREATE TABLE dc_prices (id INT NOT NULL PRIMARY KEY, product_id uuid COLLATE rdb_guid_order NOT NULL, price INT NOT NULL)").await;
    ok(&db, &mut s, "CREATE TABLE dc_images (id INT NOT NULL PRIMARY KEY, product_id uuid COLLATE rdb_guid_order NOT NULL, ord INT NOT NULL, url TEXT)")
        .await;
    // SQL Server orders a uniqueidentifier by its last six bytes first:
    // ascending `k` here, which is descending by the bytes.
    const N: i64 = 12;
    let id = |k: i64| format!("{:08x}-0000-0000-0000-{k:012}", 0xff00 - k);
    let rows: Vec<String> = (1..=N).map(|k| format!("('{}', 'Name{k}', {k})", id(k))).collect();
    ok(&db, &mut s, &format!("INSERT INTO dc_products (id, name, n) VALUES {}", rows.join(", "))).await;
    let rows: Vec<String> = (1..=N).map(|k| format!("({k}, '{}', {})", id(k), k * 10)).collect();
    ok(&db, &mut s, &format!("INSERT INTO dc_prices (id, product_id, price) VALUES {}", rows.join(", "))).await;
    let rows: Vec<String> = (1..=N * 2).map(|i| format!("({i}, '{}', {}, 'u{i}')", id((i + 1) / 2), i % 2)).collect();
    ok(&db, &mut s, &format!("INSERT INTO dc_images (id, product_id, ord, url) VALUES {}", rows.join(", "))).await;
    let ns = |rows: &Rows, at: usize| -> Vec<i64> { rows.iter().map(|r| r[at].as_deref().unwrap().parse().unwrap()).collect() };
    let ascending: Vec<i64> = (2..=N).filter(|k| k % 2 == 0).collect();

    // The parent: select-list subqueries inside the derived table.
    let parent = "SELECT d6.id, d6.n, d6.c, d8.url FROM (SELECT d.id, d.n, (SELECT d3.price FROM dc_prices AS d3 WHERE d.id = d3.product_id \
                  ORDER BY d3.id LIMIT 1) AS c, (SELECT COUNT(*) FROM dc_images AS d4 WHERE d.id = d4.product_id) AS c0 FROM dc_products AS d \
                  WHERE d.n % 2 = 0 LIMIT 10) AS d6 LEFT JOIN (SELECT d7.url, d7.product_id FROM (SELECT d2.url, d2.product_id, \
                  ROW_NUMBER() OVER (PARTITION BY d2.product_id ORDER BY d2.ord) AS rn FROM dc_images AS d2) AS d7 WHERE d7.rn <= 1) AS d8 \
                  ON d6.id = d8.product_id ORDER BY d6.id";
    let got = pg_rows(&db, &mut s, parent).await;
    assert_eq!(ns(&got, 1), ascending, "{got:?}");
    for row in &got {
        let k: i64 = row[1].as_deref().unwrap().parse().unwrap();
        assert_eq!(row[2], cell(k * 10), "{row:?}");
        assert_eq!(row[3], cell(format!("u{}", k * 2)), "{row:?}");
    }
    // Its split child, a body of one table: the same products, in the same
    // order.
    let child = "SELECT d11.id, d11.n, d14.id FROM (SELECT d.id, d.n FROM dc_products AS d WHERE d.n % 2 = 0 LIMIT 10) AS d11 INNER JOIN \
                 (SELECT d10.id, d10.ord, d10.product_id FROM dc_images AS d10) AS d14 ON d11.id = d14.product_id ORDER BY d11.id, d14.ord";
    let children = pg_rows(&db, &mut s, child).await;
    let mut parents_of_children = ns(&children, 1);
    parents_of_children.dedup();
    assert_eq!(parents_of_children, ascending, "{children:?}");
    assert_eq!(got.iter().map(|r| r[0].clone()).collect::<Vec<_>>(), {
        let mut ids: Vec<Option<String>> = children.iter().map(|r| r[0].clone()).collect();
        ids.dedup();
        ids
    });

    // A body that is a join itself, ordered by either side's key, with the
    // key renamed, and descending.
    let joined = "(SELECT a.id, b.product_id AS pid, a.n, a.name FROM dc_products AS a INNER JOIN dc_prices AS b ON a.id = b.product_id) AS dj";
    let all: Vec<i64> = (1..=N).collect();
    for (order, want) in [
        ("dj.id", all.clone()),
        ("dj.pid", all.clone()),
        ("dj.id DESC", all.iter().rev().copied().collect::<Vec<_>>()),
    ] {
        let sql = format!("SELECT dj.n FROM {joined} ORDER BY {order}");
        assert_eq!(ns(&pg_rows(&db, &mut s, &sql).await, 0), want, "{sql}");
    }
    // Its text column compares without case, as the table's does.
    let sql = format!("SELECT dj.n FROM {joined} WHERE dj.name = 'NAME7'");
    assert_eq!(ns(&pg_rows(&db, &mut s, &sql).await, 0), [7], "{sql}");
    // A star's columns keep theirs; an expression named as a column is not
    // taken for it.
    let starred = "(SELECT a.*, b.price FROM dc_products AS a INNER JOIN dc_prices AS b ON a.id = b.product_id) AS dj";
    let sql = format!("SELECT dj.n FROM {starred} WHERE dj.name = 'name3' OR dj.price >= 110 ORDER BY dj.id DESC");
    assert_eq!(ns(&pg_rows(&db, &mut s, &sql).await, 0), [12, 11, 3], "{sql}");
    let sql = "SELECT dj.n FROM (SELECT a.n, 'Name' || a.n::text AS name, b.price FROM dc_products AS a INNER JOIN dc_prices AS b \
               ON a.id = b.product_id) AS dj WHERE dj.name = 'name3' OR dj.name = 'Name4'";
    assert_eq!(ns(&pg_rows(&db, &mut s, sql).await, 0), [4], "{sql}");
    // A CTE of a join is copied the same way.
    let sql = "WITH dw AS (SELECT a.id, a.n, b.price FROM dc_products AS a INNER JOIN dc_prices AS b ON a.id = b.product_id WHERE b.price > 90) \
               SELECT dw.n FROM dw ORDER BY dw.id";
    assert_eq!(ns(&pg_rows(&db, &mut s, sql).await, 0), [10, 11, 12], "{sql}");
    // MIN / MAX over the copy are the order's.
    let sql = format!("SELECT MIN(dj.id), MAX(dj.id) FROM {joined}");
    assert_eq!(pg_rows(&db, &mut s, &sql).await, vec![vec![cell(id(1)), cell(id(N))]], "{sql}");
}
