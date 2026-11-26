//! A correlated scalar subquery in a select list is run as a `LATERAL` item
//! (`rql::correlated`), once per distinct outer value (`lateral::materialize`).
//! It ran for every row of the outer relation, whatever the statement's
//! `WHERE` kept: one customer of a thousand cost a thousand runs, each an
//! index probe. It now runs for the rows the statement's own conjuncts over
//! the outer item keep (`outer_only_filter`), and only where leaving the rest
//! to the statement cannot change the answer. Every query here runs over
//! both protocols, pgwire and native.

use std::collections::HashMap;

use tokio::io::AsyncReadExt;

use super::owner_tests::{Session, ok, session};
use super::*;
use crate::core::rql::lateral::test_hooks::take_runs;

const CUSTOMERS: i64 = 300;
/// The outer item's alias in each test, distinct from every other test's,
/// so the runs each counts are its own (`take_runs`): the tests run at once.
const ALIAS: &str = "ocq";
const ALIAS_LEFT: &str = "ocl";
const ORDERS: i64 = 3000;

async fn db() -> Arc<Database> {
    crate::replication::test_support::in_memory_database().await
}

/// A database on disk, under `dir`: an in-memory one has no partitioned
/// tables.
async fn disk_db(dir: &std::path::Path, name: &str) -> Arc<Database> {
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, name).await)
}

/// A statement's rows over pgwire, each cell as text.
pub(super) async fn pg_rows(db: &Arc<Database>, s: &mut Session, sql: &str) -> Vec<Vec<Option<String>>> {
    Box::pin(execute_recorded(db, &mut s.state, &mut s.server, sql, &[], false, None))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'D' => {
                let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                let mut at = 2;
                let mut row = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if l < 0 {
                        row.push(None);
                    } else {
                        row.push(Some(String::from_utf8_lossy(&body[at..at + l as usize]).into_owned()));
                        at += l as usize;
                    }
                }
                out.push(row);
            }
            b'C' => return out,
            b'E' => panic!("{sql}: {}", String::from_utf8_lossy(&body)),
            _ => {}
        }
    }
}

/// A statement's rows over native, each cell as text.
async fn native_rows(db: &Arc<Database>, sql: &str) -> Vec<Vec<Option<String>>> {
    let out = crate::core::database::run_native_query_for_tests(sql, db.clone()).await;
    assert_eq!(out.first(), Some(&2u8), "{sql}: {}", String::from_utf8_lossy(&out));
    crate::core::row::row::vec_into_rows(&out[1..])
        .unwrap()
        .iter()
        .map(|r| {
            r.columns
                .iter()
                .map(|c| {
                    (c.column_type != crate::core::db_type::DbType::NULL)
                        .then(|| crate::core::rql::subquery::value_to_literal(&c.column_type, c.data.into_slice()))
                        .flatten()
                        .map(|t| t.trim_matches('\'').to_string())
                })
                .collect()
        })
        .collect()
}

/// The customer an order belongs to: never one of the last ten, which have
/// no orders (a count of 0, a sum of NULL).
fn owner(order: i64) -> i64 {
    order * 7 % (CUSTOMERS - 10) + 1
}

fn amount(order: i64) -> i64 {
    order % 50
}

async fn fill(db: &Arc<Database>, s: &mut Session, customers: &str, orders: &str, partitioned: bool) {
    let (c_tail, o_tail) = if partitioned {
        (" PARTITION BY HASH (id)", ", PRIMARY KEY (id, customer_id)) PARTITION BY HASH (customer_id)")
    } else {
        ("", ", PRIMARY KEY (id))")
    };
    ok(db, s, &format!("CREATE TABLE {customers} (id INT NOT NULL PRIMARY KEY, name TEXT){c_tail}")).await;
    ok(db, s, &format!("CREATE TABLE {orders} (id INT NOT NULL, customer_id INT NOT NULL, amount INT{o_tail}")).await;
    ok(db, s, &format!("CREATE INDEX {orders}_customer ON {orders} (customer_id)")).await;
    let values: Vec<String> = (1..=CUSTOMERS).map(|c| format!("({c}, 'c{c}')")).collect();
    ok(db, s, &format!("INSERT INTO {customers} (id, name) VALUES {}", values.join(", "))).await;
    let values: Vec<String> = (1..=ORDERS).map(|o| format!("({o}, {}, {})", owner(o), amount(o))).collect();
    ok(db, s, &format!("INSERT INTO {orders} (id, customer_id, amount) VALUES {}", values.join(", "))).await;
}

type Answer = (i64, i64, Option<i64>);

/// Each customer's order count and amount sum, as the data was written.
fn expected() -> HashMap<i64, (i64, Option<i64>)> {
    let mut out: HashMap<i64, (i64, Option<i64>)> = (1..=CUSTOMERS).map(|c| (c, (0, None))).collect();
    for o in 1..=ORDERS {
        let entry = out.get_mut(&owner(o)).unwrap();
        entry.0 += 1;
        entry.1 = Some(entry.1.unwrap_or(0) + amount(o));
    }
    out
}

fn want(ids: impl Iterator<Item = i64>) -> Vec<Answer> {
    let all = expected();
    let mut out: Vec<Answer> = ids.map(|c| (c, all[&c].0, all[&c].1)).collect();
    out.sort();
    out
}

fn parsed(rows: Vec<Vec<Option<String>>>) -> Vec<Answer> {
    let int = |c: &Option<String>| c.as_deref().map(|t| t.parse::<i64>().unwrap_or_else(|_| panic!("not an integer: {t}")));
    let mut out: Vec<Answer> = rows.iter().map(|r| (int(&r[0]).unwrap(), int(&r[1]).unwrap_or(0), int(&r[2]))).collect();
    out.sort();
    out
}

/// The statement's answers over both protocols, which must agree, and the
/// runs its `LATERAL` items took over the two.
async fn answers(db: &Arc<Database>, s: &mut Session, sql: &str, outer: &str, alias: &str) -> (Vec<Answer>, usize) {
    take_runs(outer, alias);
    let pg = parsed(pg_rows(db, s, sql).await);
    let pg_runs = take_runs(outer, alias);
    let native = parsed(native_rows(db, sql).await);
    let native_runs = take_runs(outer, alias);
    assert_eq!(pg, native, "pgwire and native disagree: {sql}");
    assert_eq!(pg_runs, native_runs, "pgwire ran its items {pg_runs} times, native {native_runs}: {sql}");
    (pg, pg_runs)
}

fn query(customers: &str, orders: &str, rest: &str) -> String {
    format!(
        "SELECT ocq.id, (SELECT COUNT(*) FROM {orders} o WHERE o.customer_id = ocq.id), \
         (SELECT SUM(amount) FROM {orders} o WHERE o.customer_id = ocq.id) FROM {customers} ocq {rest}"
    )
}

/// The answers, and the runs the two items took: one per kept customer each,
/// per protocol, on plain and partitioned tables alike; and, where the
/// statement's filter is not one that may be applied first, every customer,
/// still answered right.
#[tokio::test(flavor = "multi_thread")]
async fn a_select_list_subquery_runs_for_the_rows_kept() {
    for partitioned in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = if partitioned { disk_db(dir.path(), "correlated_select_list").await } else { db().await };
        let mut s = session(&db);
        let (customers, orders) =
            if partitioned { ("csq_customers_p", "csq_orders_p") } else { ("csq_customers", "csq_orders") };
        fill(&db, &mut s, customers, orders, partitioned).await;

        let kept: [(&str, Vec<i64>); 6] = [
            ("WHERE ocq.id BETWEEN 20 AND 21 OR ocq.id = 299", vec![20, 21, 299]),
            ("WHERE ocq.id < 6", (1..6).collect()),
            ("WHERE ocq.id < 3 OR ocq.id > 297", vec![1, 2, 298, 299, 300]),
            ("WHERE ocq.id IN (7, 295, 9) AND ocq.name LIKE 'c%'", vec![7, 9, 295]),
            ("WHERE ocq.id BETWEEN 10 AND 12 ORDER BY ocq.id LIMIT 5", vec![10, 11, 12]),
            ("WHERE ocq.id = 290 AND ocq.name IS NOT NULL", vec![290]),
        ];
        for (rest, ids) in kept {
            let sql = query(customers, orders, rest);
            let (got, runs) = answers(&db, &mut s, &sql, customers, ALIAS).await;
            assert_eq!(got, want(ids.iter().copied()), "partitioned {partitioned}: {sql}");
            // Two items, each run once per kept customer.
            assert_eq!(runs, 2 * ids.len(), "partitioned {partitioned}: {sql}");
        }

        // Not applied first: a call, which could be volatile. Still right,
        // every customer run.
        let sql = query(customers, orders, "WHERE abs(ocq.id) < 4");
        let (got, runs) = answers(&db, &mut s, &sql, customers, ALIAS).await;
        assert_eq!(got, want(1..4), "partitioned {partitioned}");
        assert_eq!(runs, 2 * CUSTOMERS as usize, "partitioned {partitioned}: {sql}");
        // No WHERE: every customer.
        let sql = query(customers, orders, "");
        let (got, runs) = answers(&db, &mut s, &sql, customers, ALIAS).await;
        assert_eq!(got, want(1..=CUSTOMERS), "partitioned {partitioned}");
        assert_eq!(runs, 2 * CUSTOMERS as usize);
        drop(s);
        drop(db);
        crate::core::wal::remove_for(&dir.path().to_string_lossy());
    }
}

/// A conjunct over another item is left to the statement, and so is every
/// conjunct when the outer item is not the first one: the answers are the
/// ones written.
#[tokio::test(flavor = "multi_thread")]
async fn a_filter_that_is_not_the_outer_item_s_own_is_left_to_the_statement() {
    let db = db().await;
    let mut s = session(&db);
    fill(&db, &mut s, "csl_customers", "csl_orders", false).await;
    ok(&db, &mut s, "CREATE TABLE csl_pick (cid INT PRIMARY KEY)").await;
    ok(&db, &mut s, "INSERT INTO csl_pick (cid) VALUES (3), (295), (400)").await;

    // Joined to another item, filtered by it and by the outer item: only the
    // outer item's own conjunct is applied first.
    let sql = "SELECT ocl.id, (SELECT COUNT(*) FROM csl_orders o WHERE o.customer_id = ocl.id), \
               (SELECT SUM(amount) FROM csl_orders o WHERE o.customer_id = ocl.id) \
               FROM csl_customers ocl JOIN csl_pick p ON p.cid = ocl.id WHERE p.cid > 1 AND ocl.id < 299";
    let (got, runs) = answers(&db, &mut s, sql, "csl_customers", ALIAS_LEFT).await;
    assert_eq!(got, want([3, 295].into_iter()));
    assert_eq!(runs, 2 * 298);

    // The outer item second in the FROM list: nothing applied first.
    let sql = "SELECT ocl.id, (SELECT COUNT(*) FROM csl_orders o WHERE o.customer_id = ocl.id), \
               (SELECT SUM(amount) FROM csl_orders o WHERE o.customer_id = ocl.id) \
               FROM csl_pick p JOIN csl_customers ocl ON p.cid = ocl.id WHERE ocl.id < 10";
    let (got, runs) = answers(&db, &mut s, sql, "csl_customers", ALIAS_LEFT).await;
    assert_eq!(got, want([3].into_iter()));
    assert_eq!(runs, 2 * CUSTOMERS as usize);
}

#[test]
fn only_the_outer_item_s_own_conjuncts_are_applied_first() {
    use crate::core::rql::lateral::reads_only_for_tests as reads_only;
    for yes in [
        "c.id < 6",
        "c.id IN (1, 2)",
        "c.name LIKE 'c%'",
        "c.id BETWEEN 1 AND 2",
        "c.n IS NOT NULL",
        "c.a = 1 OR c.b = 'x'",
        "c.d::text = '1'",
        "NOT c.flag",
        "c.a IS DISTINCT FROM 3",
    ] {
        assert!(reads_only(yes, "c"), "{yes}");
    }
    for no in [
        "abs(c.id) < 6",
        "c.id < p.id",
        "id < 6",
        "c.id = (SELECT 1)",
        "c.id = $1",
        "\"c\".id = 1",
        "c.id < random()",
        "1 = 1",
        "c.v::double precision > 1",
        "__cs0.__v = 0",
    ] {
        assert!(!reads_only(no, "c"), "{no}");
    }
}

/// A tag-like EXISTS over the outer item narrows its runs, as EF Core's
/// related-items query filters products by a tag: two select-list scalars
/// ran for every product (all of one company), 14.5 s, where the EXISTS kept
/// a handful. Also through a join inside the EXISTS, with an IN list, and
/// however an earlier pass rewrites the EXISTS (`exists_to_in`).
#[tokio::test(flavor = "multi_thread")]
async fn an_exists_over_the_outer_item_narrows_its_runs() {
    const ALIAS_EXISTS: &str = "oex";
    let db = db().await;
    let mut s = session(&db);
    let (customers, orders) = ("cex_customers", "cex_orders");
    fill(&db, &mut s, customers, orders, false).await;
    ok(&db, &mut s, "CREATE TABLE cex_tags (id INT PRIMARY KEY, content TEXT)").await;
    ok(&db, &mut s, "INSERT INTO cex_tags VALUES (1, 'x'), (2, 'y')").await;
    // The customers with an order of amount 7 (and, joined, tag 'x').
    let tagged: Vec<i64> = {
        let mut ids: Vec<i64> = (1..=ORDERS).filter(|o| amount(*o) == 7).map(owner).collect();
        ids.sort();
        ids.dedup();
        ids
    };
    let q = |filter: &str| {
        format!(
            "SELECT {ALIAS_EXISTS}.id, (SELECT COUNT(*) FROM {orders} o WHERE o.customer_id = {ALIAS_EXISTS}.id), \
             (SELECT SUM(amount) FROM {orders} o WHERE o.customer_id = {ALIAS_EXISTS}.id) FROM {customers} {ALIAS_EXISTS} \
             WHERE {ALIAS_EXISTS}.name LIKE 'c%' AND {filter}"
        )
    };
    for filter in [
        format!("EXISTS (SELECT 1 FROM {orders} t WHERE {ALIAS_EXISTS}.id = t.customer_id AND t.amount = 7)"),
        format!(
            "EXISTS (SELECT 1 FROM {orders} t INNER JOIN cex_tags g ON g.id = 1 WHERE {ALIAS_EXISTS}.id = t.customer_id \
             AND g.content IN ('x', 'z') AND t.amount = 7)"
        ),
        // As the SQL Server front end writes EF Core's `SELECT 1`.
        format!("EXISTS (SELECT CAST(1 AS integer) FROM {orders} AS t WHERE {ALIAS_EXISTS}.id = t.customer_id AND t.amount = 7)"),
        // An ORM's quoted names and aliases.
        format!(
            "EXISTS (SELECT 1 FROM \"{orders}\" AS \"t\" INNER JOIN \"cex_tags\" AS \"g\" ON \"g\".\"id\" = 1 \
             WHERE \"{ALIAS_EXISTS}\".\"id\" = \"t\".\"customer_id\" AND \"t\".\"amount\" = 7)"
        ),
    ] {
        let sql = q(&filter);
        let (got, runs) = answers(&db, &mut s, &sql, customers, ALIAS_EXISTS).await;
        assert_eq!(got, want(tagged.iter().copied()), "{sql}");
        assert_eq!(runs, 2 * tagged.len(), "{sql}");
    }
    // The outer item's alias quoted too (`FROM t AS "oex"`).
    let sql = format!(
        "SELECT \"{ALIAS_EXISTS}\".\"id\", (SELECT COUNT(*) FROM {orders} o WHERE o.customer_id = \"{ALIAS_EXISTS}\".\"id\"), \
         (SELECT SUM(amount) FROM {orders} o WHERE o.customer_id = \"{ALIAS_EXISTS}\".\"id\") FROM \"{customers}\" AS \"{ALIAS_EXISTS}\" \
         WHERE EXISTS (SELECT 1 FROM \"{orders}\" AS \"t\" WHERE \"{ALIAS_EXISTS}\".\"id\" = \"t\".\"customer_id\" AND \"t\".\"amount\" = 7)"
    );
    let (got, runs) = answers(&db, &mut s, &sql, customers, ALIAS_EXISTS).await;
    assert_eq!(got, want(tagged.iter().copied()), "{sql}");
    assert_eq!(runs, 2 * tagged.len(), "{sql}");
}


/// EF Core's related-items statement as the SQL Server front end sends it
/// (the pilot's ERP #13): uniqueidentifier keys in SQL Server's order, a
/// case-insensitive tag, dotted quoted names, two select-list subqueries
/// (one over a join), a tag EXISTS, ORDER BY and a limit. Its subqueries
/// run for the products the EXISTS keeps, not for every product.
#[tokio::test(flavor = "multi_thread")]
async fn the_related_items_statement_runs_its_subqueries_for_the_tagged() {
    const ALIAS_RI: &str = "dri";
    let db = db().await;
    let mut s = session(&db);
    let (p, pr, pt, t) = ("\"ri\u{2024}erp\u{2024}products\"", "\"ri\u{2024}erp\u{2024}prices\"", "\"ri\u{2024}erp\u{2024}producttags\"", "\"ri\u{2024}erp\u{2024}tags\"");
    ok(&db, &mut s, &format!("CREATE TABLE {p} (id uuid COLLATE rdb_guid_order NOT NULL PRIMARY KEY, companyid uuid COLLATE rdb_guid_order NOT NULL, state integer NOT NULL, status integer NOT NULL, processed boolean NULL, name text COLLATE rdb_ci NULL, n integer NOT NULL)")).await;
    ok(&db, &mut s, &format!("CREATE TABLE {pr} (id integer NOT NULL PRIMARY KEY, productid uuid COLLATE rdb_guid_order NOT NULL, pricetype integer NOT NULL, countryid text COLLATE rdb_ci NULL, price numeric(19,6) NOT NULL)")).await;
    ok(&db, &mut s, &format!("CREATE TABLE {pt} (id integer NOT NULL PRIMARY KEY, productsid uuid COLLATE rdb_guid_order NOT NULL, tagsid uuid COLLATE rdb_guid_order NOT NULL)")).await;
    ok(&db, &mut s, &format!("CREATE TABLE {t} (id uuid COLLATE rdb_guid_order NOT NULL PRIMARY KEY, content text COLLATE rdb_ci NOT NULL)")).await;
    let company = "4f914341-68b3-40a3-3ad8-08da6c2740bf";
    let pid = |k: i64| format!("00000000-0000-0000-0000-{k:012}");
    const PRODUCTS: i64 = 200;
    let rows: Vec<String> = (1..=PRODUCTS).map(|k| format!("('{}', '{company}', 100, 1, true, 'P{k}', {k})", pid(k))).collect();
    ok(&db, &mut s, &format!("INSERT INTO {p} (id, companyid, state, status, processed, name, n) VALUES {}", rows.join(", "))).await;
    let rows: Vec<String> = (1..=PRODUCTS).map(|k| format!("({k}, '{}', 100, NULL, {k}.5)", pid(k))).collect();
    ok(&db, &mut s, &format!("INSERT INTO {pr} (id, productid, pricetype, countryid, price) VALUES {}", rows.join(", "))).await;
    ok(&db, &mut s, &format!("INSERT INTO {t} (id, content) VALUES ('11111111-0000-0000-0000-000000000001', 'Realme C53'), ('11111111-0000-0000-0000-000000000002', 'Other')")).await;
    // Products 3, 77 and 150 carry the tag; every product carries 'Other'.
    let mut rows: Vec<String> = (1..=PRODUCTS).map(|k| format!("({k}, '{}', '11111111-0000-0000-0000-000000000002')", pid(k))).collect();
    for (n, k) in [3i64, 77, 150].iter().enumerate() {
        rows.push(format!("({}, '{}', '11111111-0000-0000-0000-000000000001')", 1000 + n, pid(*k)));
    }
    ok(&db, &mut s, &format!("INSERT INTO {pt} (id, productsid, tagsid) VALUES {}", rows.join(", "))).await;
    let sql = format!(
        "SELECT {ALIAS_RI}.n AS \"N\", (SELECT d2.price FROM {pr} AS d2 WHERE {ALIAS_RI}.id = d2.productid AND d2.pricetype = 100 \
         ORDER BY CASE WHEN d2.countryid IS NULL THEN CAST(1 AS boolean) ELSE CAST(0 AS boolean) END DESC NULLS LAST LIMIT 1) AS \"Price\", \
         (SELECT d4.content FROM {pt} AS d3 INNER JOIN {t} AS d4 ON d3.tagsid = d4.id WHERE {ALIAS_RI}.id = d3.productsid \
         ORDER BY d4.id LIMIT 1) AS \"Tag\" \
         FROM {p} AS {ALIAS_RI} WHERE {ALIAS_RI}.companyid = '{company}' AND {ALIAS_RI}.state NOT IN (140, 160) AND {ALIAS_RI}.status = 1 \
         AND COALESCE({ALIAS_RI}.processed, CAST(0 AS boolean)) = CAST(1 AS boolean) AND {ALIAS_RI}.id <> '{}' \
         AND EXISTS (SELECT CAST(1 AS integer) FROM {pt} AS d0 INNER JOIN {t} AS d1 ON d0.tagsid = d1.id \
         WHERE {ALIAS_RI}.id = d0.productsid AND d1.content IN ('realme c53', 'nothing')) ORDER BY {ALIAS_RI}.id LIMIT 10",
        pid(77)
    );
    use crate::core::rql::lateral::test_hooks::{take_limited, take_narrowed_copies};
    take_runs("ri\u{2024}erp\u{2024}products", ALIAS_RI);
    take_narrowed_copies(ALIAS_RI);
    take_limited(ALIAS_RI);
    let rows = pg_rows(&db, &mut s, &sql).await;
    let runs = take_runs("ri\u{2024}erp\u{2024}products", ALIAS_RI);
    let ns: Vec<String> = rows.iter().map(|r| r[0].clone().unwrap_or_default()).collect();
    assert_eq!(ns, ["3", "150"], "{rows:?}");
    assert_eq!(rows[0][1].as_deref(), Some("3.500000"));
    assert!(rows.iter().all(|r| r[2].is_some()), "{rows:?}");
    // Two items, each for the two products kept (not for all 200).
    assert_eq!(runs, 2 * 2, "the subqueries ran {runs} times");
    // The filter holds a subquery (the EXISTS): the outer rows it keeps are
    // copied once, before anything is probed, and nothing after runs it
    // again: no copy needs narrowing.
    assert_eq!(take_limited(ALIAS_RI), 1);
    assert_eq!(take_narrowed_copies(ALIAS_RI), 0);
    // Native answers the same, the same way.
    let native = native_rows(&db, &sql).await;
    assert_eq!(native.iter().map(|r| r[0].clone().unwrap_or_default()).collect::<Vec<_>>(), ["3", "150"]);
    assert_eq!(take_limited(ALIAS_RI), 1);
    assert_eq!(take_narrowed_copies(ALIAS_RI), 0);
    assert_eq!(take_runs("ri\u{2024}erp\u{2024}products", ALIAS_RI), 2 * 2);
    // Without the LIMIT the same cut is made: the EXISTS is still run once.
    let unlimited = sql.strip_suffix(" LIMIT 10").expect("the limit").to_string();
    let rows = pg_rows(&db, &mut s, &unlimited).await;
    assert_eq!(rows.iter().map(|r| r[0].clone().unwrap_or_default()).collect::<Vec<_>>(), ["3", "150"]);
    assert_eq!(take_limited(ALIAS_RI), 1);
    assert_eq!(take_runs("ri\u{2024}erp\u{2024}products", ALIAS_RI), 2 * 2);

    // A filter of the outer item's own columns, no subquery and no LIMIT:
    // probed, and each item's copy of the outer rows (made to carry its
    // join key) holds the kept products only: both copies took the filter.
    let plain = format!(
        "SELECT {ALIAS_RI}.n, (SELECT d2.price FROM {pr} AS d2 WHERE {ALIAS_RI}.id = d2.productid LIMIT 1), \
         (SELECT COUNT(*) FROM {pt} AS d3 WHERE {ALIAS_RI}.id = d3.productsid) FROM {p} AS {ALIAS_RI} \
         WHERE {ALIAS_RI}.companyid = '{company}' AND {ALIAS_RI}.n IN (3, 150) ORDER BY {ALIAS_RI}.id"
    );
    let rows = pg_rows(&db, &mut s, &plain).await;
    assert_eq!(
        rows,
        vec![
            vec![Some("3".to_string()), Some("3.500000".to_string()), Some("2".to_string())],
            vec![Some("150".to_string()), Some("150.500000".to_string()), Some("2".to_string())]
        ]
    );
    assert_eq!(take_limited(ALIAS_RI), 0);
    assert_eq!(take_narrowed_copies(ALIAS_RI), 2);
    assert_eq!(take_runs("ri\u{2024}erp\u{2024}products", ALIAS_RI), 2 * 2);

    // A statement whose WHERE keeps an outer row the filter alone would
    // not decide (an OR with another item is left to the statement): the
    // copy is whole, the answer the same as without the subqueries.
    let all = format!(
        "SELECT {ALIAS_RI}.n, (SELECT d2.price FROM {pr} AS d2 WHERE {ALIAS_RI}.id = d2.productid LIMIT 1) FROM {p} AS {ALIAS_RI} \
         WHERE abs({ALIAS_RI}.n) < 4 ORDER BY {ALIAS_RI}.n"
    );
    let rows = pg_rows(&db, &mut s, &all).await;
    assert_eq!(rows.iter().map(|r| r[0].clone().unwrap_or_default()).collect::<Vec<_>>(), ["1", "2", "3"]);
    assert_eq!(rows[2][1].as_deref(), Some("3.500000"));
    assert_eq!(take_narrowed_copies(ALIAS_RI), 0);
}

/// One distinct tuple of outer values (a point lookup, or several outer
/// rows sharing the value the item reads): the item is a constant, built
/// once and joined as an item that reads nothing outside is, with no keyed
/// relation and no copy of the outer item. A product page's base query paid
/// 4 ms an item for those. The answers are the general path's.
#[tokio::test(flavor = "multi_thread")]
async fn one_outer_tuple_is_a_constant_item() {
    use crate::core::rql::lateral::test_hooks::take_constants;
    const ALIAS_ONE: &str = "oone";
    let db = db().await;
    let mut s = session(&db);
    let (customers, orders) = ("con_customers", "con_orders");
    fill(&db, &mut s, customers, orders, false).await;
    let q = |rest: &str| {
        format!(
            "SELECT {ALIAS_ONE}.id, (SELECT COUNT(*) FROM {orders} o WHERE o.customer_id = {ALIAS_ONE}.id), \
             (SELECT SUM(amount) FROM {orders} o WHERE o.customer_id = {ALIAS_ONE}.id) FROM {customers} {ALIAS_ONE} {rest}"
        )
    };
    // A customer with orders, and one with none (a count of 0, a NULL sum:
    // the row is kept).
    for id in [5i64, 299] {
        let sql = q(&format!("WHERE {ALIAS_ONE}.id = {id}"));
        take_constants(ALIAS_ONE);
        let pg = parsed(pg_rows(&db, &mut s, &sql).await);
        assert_eq!(take_constants(ALIAS_ONE), 2, "{sql}");
        let native = parsed(native_rows(&db, &sql).await);
        assert_eq!(take_constants(ALIAS_ONE), 2, "{sql}");
        assert_eq!(pg, want(std::iter::once(id)), "{sql}");
        assert_eq!(native, pg, "{sql}");
    }
    // Two tuples: the general path, as before.
    let sql = q(&format!("WHERE {ALIAS_ONE}.id IN (5, 6)"));
    take_constants(ALIAS_ONE);
    assert_eq!(parsed(pg_rows(&db, &mut s, &sql).await), want([5i64, 6].into_iter()));
    assert_eq!(take_constants(ALIAS_ONE), 0);

    // Several outer rows, one value read: each row gets the item's row; a
    // LIMIT 1 item without one (NULL), and with a select list of its own.
    let orders_of_5: Vec<i64> = (1..=ORDERS).filter(|o| owner(*o) == 5).collect();
    let sql = format!(
        "SELECT {ALIAS_ONE}.id, (SELECT c.name FROM {customers} c WHERE c.id = {ALIAS_ONE}.customer_id LIMIT 1), \
         (SELECT c.name FROM {customers} c WHERE c.id = {ALIAS_ONE}.customer_id AND c.id > 1000 LIMIT 1) \
         FROM {orders} {ALIAS_ONE} WHERE {ALIAS_ONE}.customer_id = 5 ORDER BY {ALIAS_ONE}.id"
    );
    take_constants(ALIAS_ONE);
    let rows = pg_rows(&db, &mut s, &sql).await;
    assert_eq!(take_constants(ALIAS_ONE), 2);
    assert_eq!(rows.len(), orders_of_5.len());
    for (row, id) in rows.iter().zip(&orders_of_5) {
        assert_eq!(row, &vec![Some(id.to_string()), Some("c5".to_string()), None], "{sql}");
    }

    // A scalar subquery of more than one row is still PostgreSQL's 21000.
    let sql = format!(
        "SELECT {ALIAS_ONE}.id, (SELECT o.amount FROM {orders} o WHERE o.customer_id = {ALIAS_ONE}.id) FROM {customers} {ALIAS_ONE} \
         WHERE {ALIAS_ONE}.id = 5"
    );
    let e = super::owner_tests::send(&db, &mut s, &sql).await.expect_err("more than one row");
    assert!(e.contains("more than one row returned by a subquery used as an expression"), "{e}");
}

/// A LIMIT is taken before the select list's subqueries when they would run
/// for more outer values than it keeps rows: they are a value per row, so
/// the rows the limit keeps are the relation's, its WHERE's and its ORDER
/// BY's to decide (`lateral::limited_outer`). EF Core's related-items query
/// with a tag most products carry ran its two subqueries for every product
/// the tag kept, then kept ten. The answers are the ones written, where the
/// limit goes first, where it must not, and where there is nothing to cut.
#[tokio::test(flavor = "multi_thread")]
async fn a_limit_is_taken_before_the_select_list_subqueries() {
    use crate::core::rql::lateral::test_hooks::take_limited;
    const ALIAS_LIM: &str = "olim";
    let db = db().await;
    let mut s = session(&db);
    let (customers, orders) = ("lim_customers", "lim_orders");
    fill(&db, &mut s, customers, orders, false).await;
    let q = |list_tail: &str, rest: &str| {
        format!(
            "SELECT {ALIAS_LIM}.id, (SELECT COUNT(*) FROM {orders} o WHERE o.customer_id = {ALIAS_LIM}.id) AS n, \
             (SELECT SUM(amount) FROM {orders} o WHERE o.customer_id = {ALIAS_LIM}.id) AS total{list_tail} FROM {customers} {ALIAS_LIM} {rest}"
        )
    };
    let all = expected();
    let answer = |id: i64| (id, all[&id].0, all[&id].1);
    let in_order = |rows: Vec<Vec<Option<String>>>| -> Vec<Answer> {
        let int = |c: &Option<String>| c.as_deref().map(|t| t.parse::<i64>().unwrap());
        rows.iter().map(|r| (int(&r[0]).unwrap(), int(&r[1]).unwrap_or(0), int(&r[2]))).collect()
    };
    // Each: the rows wanted, in order; every customer past 20 passes the
    // WHERE (280 of them), ten are kept.
    let cases: Vec<(String, Vec<i64>)> = vec![
        (format!("WHERE {ALIAS_LIM}.id > 20 ORDER BY {ALIAS_LIM}.id LIMIT 10"), (21..=30).collect()),
        (format!("WHERE {ALIAS_LIM}.id > 20 ORDER BY {ALIAS_LIM}.id DESC LIMIT 10"), (291..=300).rev().collect()),
        (format!("WHERE {ALIAS_LIM}.id > 20 ORDER BY {ALIAS_LIM}.id LIMIT 10 OFFSET 5"), (26..=35).collect()),
        (format!("WHERE {ALIAS_LIM}.name LIKE 'c29%' ORDER BY {ALIAS_LIM}.name DESC NULLS LAST, {ALIAS_LIM}.id LIMIT 3"), vec![299, 298, 297]),
        (format!("ORDER BY {ALIAS_LIM}.id LIMIT 4"), vec![1, 2, 3, 4]),
    ];
    for (rest, ids) in &cases {
        let sql = q("", rest);
        take_runs(customers, ALIAS_LIM);
        take_limited(ALIAS_LIM);
        let got = in_order(pg_rows(&db, &mut s, &sql).await);
        assert_eq!(take_limited(ALIAS_LIM), 1, "{sql}");
        assert_eq!(got, ids.iter().map(|id| answer(*id)).collect::<Vec<_>>(), "{sql}");
        // Two items, each run for the rows kept.
        assert_eq!(take_runs(customers, ALIAS_LIM), 2 * ids.len(), "{sql}");
        // Native answers the same, the same way.
        assert_eq!(in_order(native_rows(&db, &sql).await), got, "{sql}");
        assert_eq!(take_limited(ALIAS_LIM), 1, "{sql}");
        assert_eq!(take_runs(customers, ALIAS_LIM), 2 * ids.len(), "{sql}");
    }
    // No ORDER BY: any ten of the rows the WHERE keeps, each with its own
    // values.
    let sql = q("", &format!("WHERE {ALIAS_LIM}.id > 20 LIMIT 10"));
    take_runs(customers, ALIAS_LIM);
    let got = in_order(pg_rows(&db, &mut s, &sql).await);
    assert_eq!(got.len(), 10);
    assert!(got.iter().all(|row| row.0 > 20 && *row == answer(row.0)), "{got:?}");
    assert_eq!(take_runs(customers, ALIAS_LIM), 2 * 10, "{sql}");
    assert_eq!(take_limited(ALIAS_LIM), 1, "{sql}");

    // Nothing to cut: no more outer values than the limit keeps (a lookup
    // of one row, which is a constant item; three rows of ten). No copy.
    for (rest, ids) in [
        (format!("WHERE {ALIAS_LIM}.id = 5 ORDER BY {ALIAS_LIM}.id LIMIT 1"), vec![5i64]),
        (format!("WHERE {ALIAS_LIM}.id BETWEEN 7 AND 9 ORDER BY {ALIAS_LIM}.id LIMIT 10"), vec![7, 8, 9]),
    ] {
        let sql = q("", &rest);
        take_runs(customers, ALIAS_LIM);
        let got = in_order(pg_rows(&db, &mut s, &sql).await);
        assert_eq!(got, ids.iter().map(|id| answer(*id)).collect::<Vec<_>>(), "{sql}");
        assert_eq!(take_limited(ALIAS_LIM), 0, "{sql}");
        assert_eq!(take_runs(customers, ALIAS_LIM), 2 * ids.len(), "{sql}");
    }

    // Where the limit does not go first: the rows are ordered by a
    // subquery's value (an output name, an ordinal), or are not the
    // relation's (DISTINCT). Every row the WHERE keeps is run, as before.
    let by_count: Vec<i64> = {
        let mut ids: Vec<i64> = (281..=300).collect();
        ids.sort_by_key(|id| (std::cmp::Reverse(all[id].0), *id));
        ids.truncate(3);
        ids
    };
    for order in ["n DESC", "2 DESC"] {
        let sql = q("", &format!("WHERE {ALIAS_LIM}.id > 280 ORDER BY {order}, {ALIAS_LIM}.id LIMIT 3"));
        take_runs(customers, ALIAS_LIM);
        let got = in_order(pg_rows(&db, &mut s, &sql).await);
        assert_eq!(got, by_count.iter().map(|id| answer(*id)).collect::<Vec<_>>(), "{sql}");
        assert_eq!(take_runs(customers, ALIAS_LIM), 2 * 20, "{sql}");
        assert_eq!(take_limited(ALIAS_LIM), 0, "{sql}");
    }
    // The last ten customers have no orders: one distinct (count, sum).
    let sql = format!(
        "SELECT DISTINCT (SELECT COUNT(*) FROM {orders} o WHERE o.customer_id = {ALIAS_LIM}.id) AS n FROM {customers} {ALIAS_LIM} \
         WHERE {ALIAS_LIM}.id > 290 LIMIT 5"
    );
    assert_eq!(pg_rows(&db, &mut s, &sql).await, vec![vec![Some("0".to_string())]], "{sql}");
    assert_eq!(take_limited(ALIAS_LIM), 0, "{sql}");
}

/// [`a_limit_is_taken_before_the_select_list_subqueries`], the statement
/// and its cut as text: what moves into the copy, what stays, and what is
/// left as written.
#[test]
fn the_rows_a_limit_keeps_are_the_outer_item_s_to_decide() {
    use crate::core::rql::lateral::limited_outer_for_tests as cut;
    let item = "LEFT JOIN LATERAL (SELECT count(*) AS __v FROM u WHERE u.k = a.id) __cs0 ON true";
    let two = format!("{item} LEFT JOIN LATERAL (SELECT max(v) AS __v FROM u WHERE u.k = a.id) __cs1 ON true");
    let sql = format!("SELECT a.x, __cs0.__v AS \"n\" FROM t a {two} WHERE a.g = 1 AND a.h IN (2, 3) ORDER BY a.x DESC NULLS LAST, a.id LIMIT 10 OFFSET 5");
    assert_eq!(
        cut(&sql, "a", "t", 16),
        Some((
            "SELECT a.* FROM t a WHERE a.g = 1 AND a.h IN (2, 3) ORDER BY a.x DESC NULLS LAST, a.id LIMIT 10 OFFSET 5".to_string(),
            format!("SELECT a.x, __cs0.__v AS \"n\" FROM <copy> a {two} ORDER BY a.x DESC NULLS LAST, a.id"),
        ))
    );
    // No more outer values than rows kept: as written.
    assert_eq!(cut(&sql, "a", "t", 15), None);
    // Unless the WHERE holds a subquery: cut whatever the count, with a
    // LIMIT or without.
    let exists = "WHERE a.g = 1 AND EXISTS (SELECT 1 FROM w WHERE w.k = a.id)";
    assert_eq!(
        cut(&format!("SELECT a.x, __cs0.__v FROM t a {item} {exists} ORDER BY a.x LIMIT 10"), "a", "t", 1),
        Some((format!("SELECT a.* FROM t a {exists} ORDER BY a.x LIMIT 10"), format!("SELECT a.x, __cs0.__v FROM <copy> a {item} ORDER BY a.x")))
    );
    assert_eq!(
        cut(&format!("SELECT a.x, __cs0.__v FROM t a {item} {exists}"), "a", "t", 1),
        Some((format!("SELECT a.* FROM t a {exists}"), format!("SELECT a.x, __cs0.__v FROM <copy> a {item}")))
    );
    // No WHERE, no ORDER BY, `AS`.
    assert_eq!(
        cut(&format!("SELECT a.x, __cs0.__v FROM t AS a {item} LIMIT 3"), "a", "t", 4),
        Some(("SELECT a.* FROM t AS a LIMIT 3".to_string(), format!("SELECT a.x, __cs0.__v FROM <copy> a {item}")))
    );
    for left in [
        // Ordered by an item's value: an output name, an ordinal, the item.
        format!("SELECT a.x, __cs0.__v AS n FROM t a {item} ORDER BY n LIMIT 3"),
        format!("SELECT a.x, __cs0.__v AS n FROM t a {item} ORDER BY 2 LIMIT 3"),
        format!("SELECT a.x, __cs0.__v AS n FROM t a {item} ORDER BY __cs0.__v LIMIT 3"),
        format!("SELECT a.x, __cs0.__v AS n FROM t a {item} ORDER BY a.x + 1 LIMIT 3"),
        // Rows that are not the outer item's.
        format!("SELECT DISTINCT a.x, __cs0.__v FROM t a {item} LIMIT 3"),
        format!("SELECT count(*), max(__cs0.__v) FROM t a {item} LIMIT 3"),
        format!("SELECT a.x, row_number() OVER (ORDER BY a.x), __cs0.__v FROM t a {item} LIMIT 3"),
        format!("SELECT a.x, __cs0.__v FROM t a {item} GROUP BY a.x, __cs0.__v LIMIT 3"),
        format!("SELECT a.x, __cs0.__v FROM t a LEFT JOIN LATERAL (SELECT v FROM u WHERE u.k = a.id) r ON true {item} LIMIT 3"),
        // A WHERE that reads an item, a parameter, a limit that is not a number.
        format!("SELECT a.x, __cs0.__v FROM t a {two} WHERE __cs1.__v > 0 LIMIT 3"),
        format!("SELECT a.x, __cs0.__v FROM t a {item} WHERE a.g = $1 LIMIT 3"),
        format!("SELECT a.x, __cs0.__v FROM t a {item} LIMIT ALL"),
        format!("SELECT a.x, __cs0.__v FROM t a {item} WHERE a.g = 1"),
        format!("SELECT a.x, __cs0.__v FROM t a {item} ORDER BY a.x LIMIT 3 FOR UPDATE"),
    ] {
        assert_eq!(cut(&left, "a", "t", 1000), None, "{left}");
    }
}

/// [`pg_rows`], with the names of the columns.
async fn pg_named(db: &Arc<Database>, s: &mut Session, sql: &str) -> (Vec<String>, Vec<Vec<Option<String>>>) {
    Box::pin(execute_recorded(db, &mut s.state, &mut s.server, sql, &[], false, None))
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let (mut names, mut out) = (Vec::new(), Vec::new());
    loop {
        let kind = s.client.read_u8().await.unwrap();
        let len = s.client.read_i32().await.unwrap() as usize;
        let mut body = vec![0u8; len - 4];
        s.client.read_exact(&mut body).await.unwrap();
        match kind {
            b'T' => {
                let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                let mut at = 2;
                for _ in 0..n {
                    let end = at + body[at..].iter().position(|b| *b == 0).unwrap();
                    names.push(String::from_utf8_lossy(&body[at..end]).into_owned());
                    at = end + 1 + 18;
                }
            }
            b'D' => {
                let n = i16::from_be_bytes([body[0], body[1]]) as usize;
                let mut at = 2;
                let mut row = Vec::with_capacity(n);
                for _ in 0..n {
                    let l = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                    at += 4;
                    if l < 0 {
                        row.push(None);
                    } else {
                        row.push(Some(String::from_utf8_lossy(&body[at..at + l as usize]).into_owned()));
                        at += l as usize;
                    }
                }
                out.push(row);
            }
            b'C' => return (names, out),
            b'E' => panic!("{sql}: {}", String::from_utf8_lossy(&body)),
            _ => {}
        }
    }
}

/// A LIMIT is taken before the select list's subqueries through a join to
/// other tables too (`lateral::joined_cut`): the join and the WHERE decide
/// the rows, the copy is of the rows the limit keeps with the columns read
/// of each table, and the subqueries run for those. EF Core's category
/// page, products joined to their category with a first-image subquery in
/// the list and `TOP 9`, ran the subquery for every product of the
/// category. The answers and the column names are the ones written:
/// inner and left joins, a filter and an order on the joined table (its
/// collation with it), a row with no partner, two joins, a filter holding
/// a subquery; and the statements this leaves as they were.
#[tokio::test(flavor = "multi_thread")]
async fn a_limit_is_taken_before_the_subqueries_through_a_join() {
    use crate::core::rql::lateral::test_hooks::take_limited;
    const A: &str = "ojl";
    let db = db().await;
    let mut s = session(&db);
    let (products, categories, groups, images) = ("jl_products", "jl_categories", "jl_groups", "jl_images");
    ok(&db, &mut s, &format!("CREATE TABLE {products} (id INT NOT NULL PRIMARY KEY, name TEXT, category_id INT NOT NULL, rank INT NOT NULL, note TEXT)")).await;
    ok(&db, &mut s, &format!("CREATE TABLE {categories} (id INT NOT NULL PRIMARY KEY, slug TEXT COLLATE rdb_ci, pos INT NOT NULL, grp INT NOT NULL)")).await;
    ok(&db, &mut s, &format!("CREATE TABLE {groups} (id INT NOT NULL PRIMARY KEY, title TEXT)")).await;
    ok(&db, &mut s, &format!("CREATE TABLE {images} (id INT NOT NULL PRIMARY KEY, product_id INT NOT NULL, url TEXT, listing INT NOT NULL)")).await;
    ok(&db, &mut s, &format!("CREATE INDEX {images}_product ON {images} (product_id)")).await;
    // A product's category is `id % 6`: none for 0. Its rank repeats.
    let category = |p: i64| p % 6;
    let rank = |p: i64| (p * 7) % 50;
    const SLUGS: [&str; 5] = ["b", "A", "c", "D", "e"];
    let slug = |c: i64| SLUGS[(c - 1) as usize];
    let pos = |c: i64| 10 - c;
    let group = |c: i64| c % 2 + 1;
    // Three images for three products of four; the first by listing, then id.
    let first_url = |p: i64| -> Option<String> {
        (p % 4 != 0).then(|| {
            let k = (0..3i64).min_by_key(|k| ((p + k * 2) % 3, p * 3 + k)).unwrap();
            format!("u{p}_{k}")
        })
    };
    let values: Vec<String> = (1..=300i64).map(|p| format!("({p}, 'p{p}', {}, {}, 'n{p}')", category(p), rank(p))).collect();
    ok(&db, &mut s, &format!("INSERT INTO {products} (id, name, category_id, rank, note) VALUES {}", values.join(", "))).await;
    let values: Vec<String> = (1..=5i64).map(|c| format!("({c}, '{}', {}, {})", slug(c), pos(c), group(c))).collect();
    ok(&db, &mut s, &format!("INSERT INTO {categories} (id, slug, pos, grp) VALUES {}", values.join(", "))).await;
    ok(&db, &mut s, &format!("INSERT INTO {groups} (id, title) VALUES (1, 'g1'), (2, 'g2')")).await;
    let values: Vec<String> = (1..=300i64)
        .filter(|p| p % 4 != 0)
        .flat_map(|p| (0..3i64).map(move |k| format!("({}, {p}, 'u{p}_{k}', {})", p * 3 + k, (p + k * 2) % 3)))
        .collect();
    ok(&db, &mut s, &format!("INSERT INTO {images} (id, product_id, url, listing) VALUES {}", values.join(", "))).await;

    let q = |list: &str, join: &str, rest: &str| {
        format!(
            "SELECT {A}.name, (SELECT i.url FROM {images} i WHERE i.product_id = {A}.id ORDER BY i.listing, i.id LIMIT 1) AS url, {A}.id{list} \
             FROM {products} {A} {join} {rest}"
        )
    };
    let cell = |v: i64| Some(v.to_string());
    let text = |v: &str| Some(v.to_string());
    let head = |p: i64| vec![text(&format!("p{p}")), first_url(p), cell(p)];
    let left = format!("LEFT JOIN {categories} c ON {A}.category_id = c.id");
    let inner = format!("JOIN {categories} c ON c.id = {A}.category_id");
    struct Case {
        sql: String,
        names: Vec<&'static str>,
        want: Vec<Vec<Option<String>>>,
    }
    let mut cases: Vec<Case> = Vec::new();
    // The reported shape: a left join, the filter on the joined table's
    // case-insensitive column, the order over both, the joined table's
    // columns as they are.
    {
        let mut ids: Vec<i64> = (1..=300).filter(|p| category(*p) == 4 && rank(*p) < 40).collect();
        ids.sort_by_key(|p| (std::cmp::Reverse(rank(*p)), *p));
        ids.truncate(9);
        cases.push(Case {
            sql: q(", c.id, c.slug", &left, &format!("WHERE {A}.rank < 40 AND c.slug = 'd' ORDER BY {A}.rank DESC, {A}.id, c.id LIMIT 9")),
            names: vec!["name", "url", "id", "id", "slug"],
            want: ids.iter().map(|p| [head(*p), vec![cell(4), text("D")]].concat()).collect(),
        });
    }
    // An inner join ordered by the joined table's column, in its collation
    // ('A' < 'b' < 'c' < 'D'; by bytes 'D' follows 'A'), with an offset.
    {
        let by_slug = [2i64, 1, 3, 4, 5];
        let mut ids: Vec<i64> = Vec::new();
        for c in by_slug {
            let mut of: Vec<i64> = (1..=120).filter(|p| category(*p) == c).collect();
            of.reverse();
            ids.extend(of);
        }
        let ids: Vec<i64> = ids.into_iter().skip(17).take(7).collect();
        assert_eq!(ids.iter().map(|p| category(*p)).collect::<Vec<_>>(), [2, 2, 2, 1, 1, 1, 1]);
        cases.push(Case {
            sql: q(", c.slug AS s", &inner, &format!("WHERE {A}.id <= 120 ORDER BY c.slug, {A}.id DESC LIMIT 7 OFFSET 17")),
            names: vec!["name", "url", "id", "s"],
            want: ids.iter().map(|p| [head(*p), vec![text(slug(category(*p)))]].concat()).collect(),
        });
    }
    // A left join's row with no partner: NULLs of the joined table.
    cases.push(Case {
        sql: q(", c.slug AS s, c.pos", &left, &format!("WHERE {A}.id > 100 ORDER BY {A}.id LIMIT 12")),
        names: vec!["name", "url", "id", "s", "pos"],
        want: (101..=112i64)
            .map(|p| match category(p) {
                0 => [head(p), vec![None, None]].concat(),
                c => [head(p), vec![text(slug(c)), cell(pos(c))]].concat(),
            })
            .collect(),
    });
    // Two joins, a value computed of both under a name.
    {
        let mut ids: Vec<i64> = (1..=300).filter(|p| category(*p) != 0 && group(category(*p)) == 2 && rank(*p) > 10).collect();
        ids.sort_by_key(|p| (pos(category(*p)), std::cmp::Reverse(*p)));
        ids.truncate(5);
        cases.push(Case {
            sql: q(
                ", g.title AS t, c.pos + g.id AS x",
                &format!("{inner} LEFT JOIN {groups} g ON g.id = c.grp"),
                &format!("WHERE g.title = 'g2' AND {A}.rank > 10 ORDER BY g.id, c.pos, {A}.id DESC LIMIT 5"),
            ),
            names: vec!["name", "url", "id", "t", "x"],
            want: ids.iter().map(|p| [head(*p), vec![text("g2"), cell(pos(category(*p)) + 2)]].concat()).collect(),
        });
    }
    for case in &cases {
        let Case { sql, names, want } = case;
        take_runs(products, A);
        take_limited(A);
        let (got_names, got) = pg_named(&db, &mut s, sql).await;
        assert_eq!(take_limited(A), 1, "{sql}");
        assert_eq!(&got, want, "{sql}");
        assert_eq!(&got_names, names, "{sql}");
        // One item, run for the rows kept.
        assert_eq!(take_runs(products, A), want.len(), "{sql}");
        assert_eq!(&native_rows(&db, sql).await, want, "{sql}");
        assert_eq!(take_limited(A), 1, "{sql}");
        assert_eq!(take_runs(products, A), want.len(), "{sql}");
    }
    // A filter holding a subquery, no LIMIT: the rows it keeps are copied
    // once, before anything is probed.
    {
        let ids: Vec<i64> = (1..30).filter(|p| category(*p) > 2 && p % 4 != 0).collect();
        let sql = q(
            ", c.pos",
            &inner,
            &format!("WHERE c.pos < 8 AND EXISTS (SELECT 1 FROM {images} x WHERE x.product_id = {A}.id) AND {A}.id < 30 ORDER BY {A}.id"),
        );
        take_runs(products, A);
        let got = pg_rows(&db, &mut s, &sql).await;
        let want: Vec<Vec<Option<String>>> = ids.iter().map(|p| [head(*p), vec![cell(pos(category(*p)))]].concat()).collect();
        assert_eq!(got, want, "{sql}");
        assert_eq!(take_limited(A), 1, "{sql}");
        assert_eq!(take_runs(products, A), ids.len(), "{sql}");
    }
    // Left as written, and answered the same: a value computed of the
    // joined table with no name of its own, and a column of it named bare.
    let ids: Vec<i64> = (101..=112).collect();
    for (list, value) in [(", c.pos + 1", 1i64), (", pos", 0)] {
        let sql = q(list, &inner, &format!("WHERE {A}.id > 100 ORDER BY {A}.id LIMIT 10"));
        take_runs(products, A);
        let got = pg_rows(&db, &mut s, &sql).await;
        let want: Vec<Vec<Option<String>>> =
            ids.iter().filter(|p| category(**p) != 0).take(10).map(|p| [head(*p), vec![cell(pos(category(*p)) + value)]].concat()).collect();
        assert_eq!(got, want, "{sql}");
        assert_eq!(take_limited(A), 0, "{sql}");
    }
}

/// [`a_limit_is_taken_before_the_subqueries_through_a_join`], the statement
/// and its cut as text: the copy's columns and the names they have in it,
/// the statement reading them of the copy, and what is left as written.
#[test]
fn the_rows_a_limit_keeps_are_the_join_s_to_decide() {
    use crate::core::rql::lateral::joined_cut_for_tests as cut;
    let tables: &[(&str, &[&str])] = &[("t", &["id", "x", "r", "wide"]), ("w", &["id", "y", "g", "z", "at", "__t7_at"])];
    let item = |g: &str| format!("LEFT JOIN LATERAL (SELECT max(u.v) AS __v FROM u WHERE u.k = a.id AND u.g = {g}) __cs0 ON true");
    let sql = format!(
        "SELECT a.x, b.y, b.z AS \"Z\", __rdb_t7_text(b.at, b.__t7_at) AS at, __cs0.__v AS n FROM t a JOIN w b ON b.id = a.r {} \
         WHERE b.z = 1 ORDER BY a.x DESC, b.id LIMIT 10",
        item("b.g")
    );
    assert_eq!(
        cut(&sql, "a", "t", 11, tables),
        Ok(Some((
            "SELECT \"a\".\"id\", \"a\".\"x\", \"b\".\"y\" AS \"rdbj0_y\", \"b\".\"z\" AS \"rdbj0_z\", \"b\".\"at\" AS \"rdbj0_at\", \
             CAST(\"b\".\"__t7_at\" AS smallint) AS \"rdbj0___t7_at\", \"b\".\"g\" AS \"rdbj0_g\", \"b\".\"id\" AS \"rdbj0_id\" \
             FROM t a JOIN w b ON b.id = a.r WHERE b.z = 1 ORDER BY a.x DESC, b.id LIMIT 10"
                .to_string(),
            format!(
                "SELECT a.x, a.rdbj0_y AS \"y\", a.rdbj0_z AS \"Z\", __rdb_t7_text(a.rdbj0_at, a.rdbj0___t7_at) AS at, __cs0.__v AS n FROM <copy> a {} \
                 ORDER BY a.x DESC, a.rdbj0_id",
                item("a.rdbj0_g")
            ),
        )))
    );
    // No more outer values than rows kept: as written.
    assert_eq!(cut(&sql, "a", "t", 10, tables), Ok(None));
    // Two joins, a left one; the second table unaliased.
    let sql = format!("SELECT a.x, b.y AS y, w2.id AS k, __cs0.__v FROM t AS a LEFT JOIN w AS b ON b.id = a.r INNER JOIN w2 ON w2.id = b.g {} LIMIT 3", item("1"));
    assert_eq!(
        cut(&sql, "a", "t", 4, &[tables[0], tables[1], ("w2", &["id"])]),
        Ok(Some((
            "SELECT \"a\".\"id\", \"a\".\"x\", \"b\".\"y\" AS \"rdbj0_y\", \"w2\".\"id\" AS \"rdbj1_id\" FROM t AS a LEFT JOIN w AS b ON b.id = a.r INNER JOIN w2 ON w2.id = b.g LIMIT 3"
                .to_string(),
            format!("SELECT a.x, a.rdbj0_y AS y, a.rdbj1_id AS k, __cs0.__v FROM <copy> a {}", item("1")),
        )))
    );
    // A read of the joined table that cannot move: the statement is left.
    for list in [
        // A value with no name of its own (its name may be its text).
        "a.x, b.y + 1",
        // A column of it named bare, a whole row, a star.
        "a.x, y",
        "a.x, to_json(b) AS j",
        "a.x, b.*",
        // A column that needs its quotes.
        "a.x, b.\"Mixed\" AS m",
        // A column it does not have.
        "a.x, b.missing AS m",
    ] {
        let sql = format!("SELECT {list}, __cs0.__v AS n FROM t a JOIN w b ON b.id = a.r {} ORDER BY a.x LIMIT 3", item("1"));
        assert_eq!(cut(&sql, "a", "t", 1000, tables), Err(()), "{sql}");
    }
    // Not cut at all: a join that is not an inner or left join to a table
    // on a plain condition, and an order that is not by qualified columns.
    for (join, order) in [
        ("RIGHT JOIN w b ON b.id = a.r", "a.x"),
        ("FULL JOIN w b ON b.id = a.r", "a.x"),
        ("CROSS JOIN w b", "a.x"),
        ("NATURAL JOIN w b", "a.x"),
        ("JOIN w b USING (id)", "a.x"),
        (", w b", "a.x"),
        ("JOIN (SELECT id, y FROM w) b ON b.id = a.r", "a.x"),
        ("JOIN w b ON b.id = (SELECT max(id) FROM w)", "a.x"),
        ("JOIN t a2 ON a2.id = a.r JOIN w a2 ON a2.id = a.r", "a.x"),
        ("JOIN w b ON b.id = a.r", "y"),
        ("JOIN w b ON b.id = a.r", "c.y"),
        ("JOIN w b ON b.id = a.r", "b.y + 1"),
    ] {
        let sql = format!("SELECT a.x, __cs0.__v AS n FROM t a {join} {} ORDER BY {order} LIMIT 3", item("1"));
        assert_eq!(cut(&sql, "a", "t", 1000, tables), Ok(None), "{sql}");
    }
}

/// The copy of the outer item made to carry an item's join key holds the
/// columns the statement names, not every column: EF Core's related-items
/// query read four columns of a table of 59, and typing and filling a copy
/// of all of them, once an item, was a third of its time. The answers are
/// the same, pruned or not.
#[tokio::test(flavor = "multi_thread")]
async fn the_outer_copy_holds_the_columns_the_statement_names() {
    use crate::core::rql::lateral::test_hooks::take_pruned_copies;
    const ALIAS_PRN: &str = "oprn";
    let db = db().await;
    let mut s = session(&db);
    let (wide, orders) = ("prn_wide", "prn_orders");
    ok(&db, &mut s, &format!("CREATE TABLE {wide} (id INT NOT NULL PRIMARY KEY, name TEXT COLLATE rdb_ci, qty INT, note TEXT, price NUMERIC(10,2), seen TIMESTAMP, flag BOOLEAN)"))
        .await;
    ok(&db, &mut s, &format!("CREATE TABLE {orders} (id INT NOT NULL PRIMARY KEY, customer_id INT NOT NULL, amount INT)")).await;
    let values: Vec<String> = (1..=40).map(|c| format!("({c}, 'Name{c}', {}, 'b{c}', {c}.25, '2026-01-01 00:00:00', {})", c * 2, c % 2 == 0)).collect();
    ok(&db, &mut s, &format!("INSERT INTO {wide} (id, name, qty, note, price, seen, flag) VALUES {}", values.join(", "))).await;
    let values: Vec<String> = (1..=120).map(|o| format!("({o}, {}, {})", o % 30 + 1, o % 7)).collect();
    ok(&db, &mut s, &format!("INSERT INTO {orders} (id, customer_id, amount) VALUES {}", values.join(", "))).await;
    let count = |c: i64| (1..=120).filter(|o| o % 30 + 1 == c).count() as i64;
    let sum = |c: i64| -> Option<i64> {
        let amounts: Vec<i64> = (1..=120).filter(|o| o % 30 + 1 == c).map(|o| o % 7).collect();
        (!amounts.is_empty()).then(|| amounts.iter().sum())
    };
    let items = format!(
        "(SELECT COUNT(*) FROM {orders} o WHERE o.customer_id = {ALIAS_PRN}.id) AS n, \
         (SELECT SUM(o.amount) FROM {orders} o WHERE o.customer_id = {ALIAS_PRN}.id) AS total"
    );
    let cell = |v: i64| Some(v.to_string());
    // Two columns named of seven: the first item's copy is made of those
    // (and its key), and the second item's is a copy of that one, with
    // nothing more to leave out.
    let sql = format!("SELECT {ALIAS_PRN}.id, {ALIAS_PRN}.price, {items} FROM {wide} {ALIAS_PRN} WHERE {ALIAS_PRN}.id IN (5, 29, 35) ORDER BY {ALIAS_PRN}.id");
    take_pruned_copies(ALIAS_PRN);
    let got = pg_rows(&db, &mut s, &sql).await;
    assert_eq!(take_pruned_copies(ALIAS_PRN), 1, "{sql}");
    let want: Vec<Vec<Option<String>>> =
        [5i64, 29, 35].iter().map(|c| vec![cell(*c), Some(format!("{c}.25")), cell(count(*c)), sum(*c).map(|v| v.to_string())]).collect();
    assert_eq!(got, want, "{sql}");
    assert_eq!(native_rows(&db, &sql).await, want, "{sql}");
    take_pruned_copies(ALIAS_PRN);
    // A column read only by the WHERE and the ORDER BY is kept for them,
    // with its collation: 'NAME5' finds 'Name5', and the names sort.
    let sql = format!(
        "SELECT {ALIAS_PRN}.id, {items} FROM {wide} {ALIAS_PRN} WHERE {ALIAS_PRN}.name IN ('NAME5', 'name29', 'Name36') ORDER BY {ALIAS_PRN}.name DESC"
    );
    let got = pg_rows(&db, &mut s, &sql).await;
    assert_eq!(take_pruned_copies(ALIAS_PRN), 1, "{sql}");
    let want: Vec<Vec<Option<String>>> = [5i64, 36, 29].iter().map(|c| vec![cell(*c), cell(count(*c)), sum(*c).map(|v| v.to_string())]).collect();
    assert_eq!(got, want, "{sql}");
    // Every column named: nothing to leave out.
    let sql = format!(
        "SELECT {ALIAS_PRN}.id, {ALIAS_PRN}.name, {ALIAS_PRN}.qty, {ALIAS_PRN}.note, {ALIAS_PRN}.price, {ALIAS_PRN}.seen, {ALIAS_PRN}.flag, {items} FROM {wide} {ALIAS_PRN} \
         WHERE {ALIAS_PRN}.id IN (5, 29) ORDER BY {ALIAS_PRN}.id"
    );
    let got = pg_rows(&db, &mut s, &sql).await;
    assert_eq!(take_pruned_copies(ALIAS_PRN), 0, "{sql}");
    assert_eq!(got.len(), 2);
    assert_eq!(got[1][..5], [cell(29), Some("Name29".to_string()), cell(58), Some("b29".to_string()), Some("29.25".to_string())], "{sql}");
    assert_eq!(got[1][7..], [cell(count(29)), sum(29).map(|v| v.to_string())], "{sql}");
}

/// [`the_outer_copy_holds_the_columns_the_statement_names`], as text: what
/// a copy keeps, and the statements whose copy is left whole because they
/// could read a column without naming it.
#[test]
fn a_copy_is_pruned_to_the_columns_named() {
    use crate::core::rql::lateral::pruned_copy_for_tests as pruned;
    let copy = "SELECT \"id\", \"name\", \"note\", \"at\", CAST(d.\"__t7_at\" AS smallint) AS \"__t7_at\", d.id::text AS __j0 FROM t d";
    let after = " LEFT JOIN k __cs0 ON __cs0.__lat_k1 = d.__j0 LEFT JOIN LATERAL (SELECT count(*) AS __v FROM u WHERE u.k = d.id) __cs1 ON true WHERE d.id > 3";
    // The key stays whatever names it; `note` and the time column go.
    assert_eq!(
        pruned(copy, "d", "SELECT d.id, d.Name * 2, __cs0.__v FROM ", after).as_deref(),
        Some("SELECT \"id\", \"name\", d.id::text AS __j0 FROM t d")
    );
    // A time column named keeps its hidden digit only when that is named.
    assert_eq!(
        pruned(copy, "d", "SELECT \"d\".\"at\", __rdb_t7_text(d.at, d.__t7_at) FROM ", after).as_deref(),
        Some("SELECT \"id\", \"at\", CAST(d.\"__t7_at\" AS smallint) AS \"__t7_at\", d.id::text AS __j0 FROM t d")
    );
    // Every column named: as it is.
    assert_eq!(pruned(copy, "d", "SELECT d.id, d.name, d.note, d.at, d.__t7_at FROM ", after), None);
    for before in [
        // A star that could be the item's, the item whole, a NATURAL join.
        "SELECT d.*, __cs0.__v FROM ",
        "SELECT *, __cs0.__v FROM ",
        "SELECT DISTINCT * FROM ",
        "SELECT d.id, to_json(d) FROM ",
        "SELECT d.id, (SELECT x.* FROM x NATURAL JOIN y) FROM ",
        "SELECT d.id, (SELECT 1 FROM x d WHERE true) FROM ",
    ] {
        assert_eq!(pruned(copy, "d", before, after), None, "{before}");
    }
    // Another item's star, a string's and a multiplication are not the
    // item's.
    assert!(pruned(copy, "d", "SELECT d.id, x.*, 'a * b', d.id * 2 FROM ", after).is_some());
}

