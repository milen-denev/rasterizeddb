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
