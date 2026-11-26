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
    take_runs("ri\u{2024}erp\u{2024}products", ALIAS_RI);
    crate::core::rql::lateral::test_hooks::take_narrowed_copies(ALIAS_RI);
    let rows = pg_rows(&db, &mut s, &sql).await;
    let runs = take_runs("ri\u{2024}erp\u{2024}products", ALIAS_RI);
    let narrowed_copies = crate::core::rql::lateral::test_hooks::take_narrowed_copies(ALIAS_RI);
    let ns: Vec<String> = rows.iter().map(|r| r[0].clone().unwrap_or_default()).collect();
    assert_eq!(ns, ["3", "150"], "{rows:?}");
    assert_eq!(rows[0][1].as_deref(), Some("3.500000"));
    assert!(rows.iter().all(|r| r[2].is_some()), "{rows:?}");
    // Two items, each for the two products kept (not for all 200).
    assert!(runs <= 2 * 3, "the subqueries ran {runs} times");
    // And the outer item's copy (made to carry each item's join key) holds
    // the kept products only: both copies took the filter.
    assert_eq!(narrowed_copies, 2);
    // Native answers the same.
    let native = native_rows(&db, &sql).await;
    assert_eq!(native.iter().map(|r| r[0].clone().unwrap_or_default()).collect::<Vec<_>>(), ["3", "150"]);
    // (Native narrowed its two copies as well.)
    assert_eq!(crate::core::rql::lateral::test_hooks::take_narrowed_copies(ALIAS_RI), 2);

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
    assert_eq!(crate::core::rql::lateral::test_hooks::take_narrowed_copies(ALIAS_RI), 0);
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
