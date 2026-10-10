//! TEMPORARY audit probe matrix. Delete before finishing.
//!
//! Every probe prints one line: `OK`, `MISMATCH` (got vs PostgreSQL's answer)
//! or `ERR`. Nothing panics until the end, so the whole surface is visible in
//! one run. Run with `-- --nocapture`.

use rasterizeddb_compat::TestServer;
use sqlx::{AssertSqlSafe, Column, Executor, Row, ValueRef};

type Rows = Result<Vec<Vec<String>>, String>;

fn err_line(e: sqlx::Error) -> String {
    let s = e.to_string();
    s.lines().next().unwrap_or("").to_string()
}

/// Simple-protocol fetch (text format), every value rendered as text.
async fn q(pool: &sqlx::PgPool, sql: &str) -> Rows {
    let rows = pool
        .fetch_all(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map_err(err_line)?;
    let mut out = Vec::new();
    for row in rows {
        let mut vals = Vec::new();
        for i in 0..row.len() {
            let v = row.try_get_raw(i).map_err(err_line)?;
            if v.is_null() {
                vals.push("NULL".to_string());
            } else {
                match v.as_str() {
                    Ok(s) => vals.push(s.to_string()),
                    Err(_) => vals.push(format!("<bin:{}>", v.as_bytes().map(|b| b.len()).unwrap_or(0))),
                }
            }
        }
        out.push(vals);
    }
    Ok(out)
}

/// Simple-protocol execute, rows affected.
async fn exec(pool: &sqlx::PgPool, sql: &str) -> Result<u64, String> {
    pool.execute(sqlx::raw_sql(AssertSqlSafe(sql.to_string())))
        .await
        .map(|r| r.rows_affected())
        .map_err(err_line)
}

fn render(r: &Rows) -> String {
    match r {
        Err(e) => format!("ERR: {}", e),
        Ok(rows) => rows
            .iter()
            .map(|r| r.join("|"))
            .collect::<Vec<_>>()
            .join(";"),
    }
}

struct Report {
    lines: Vec<String>,
    mismatches: usize,
}

impl Report {
    fn new() -> Self {
        Self { lines: Vec::new(), mismatches: 0 }
    }
    fn check(&mut self, label: &str, sql: &str, got: &Rows, want: &str) {
        let g = render(got);
        if g == want {
            self.lines.push(format!("OK        {} :: {}", label, sql));
        } else if g.starts_with("ERR") {
            self.lines.push(format!("ERR       {} :: {}\n            got  {}\n            want {}", label, sql, g, want));
        } else {
            self.mismatches += 1;
            self.lines.push(format!("MISMATCH  {} :: {}\n            got  {}\n            want {}", label, sql, g, want));
        }
    }
    fn check_exec(&mut self, label: &str, sql: &str, got: &Result<u64, String>, want: Option<u64>) {
        match (got, want) {
            (Ok(n), Some(w)) if *n == w => self.lines.push(format!("OK        {} :: {}", label, sql)),
            (Ok(_), None) => self.lines.push(format!("OK        {} :: {}", label, sql)),
            (Ok(n), Some(w)) => {
                self.mismatches += 1;
                self.lines.push(format!("MISMATCH  {} :: {}\n            got  {} rows\n            want {} rows", label, sql, n, w));
            }
            (Err(e), _) => self.lines.push(format!("ERR       {} :: {}\n            got  {}", label, sql, e)),
        }
    }
    fn dump(&self, title: &str) {
        println!("\n===== {} =====", title);
        for l in &self.lines {
            println!("{}", l);
        }
        println!("===== {} : {} mismatches, {} probes =====\n", title, self.mismatches, self.lines.len());
    }
}

macro_rules! probe {
    ($rep:expr, $pool:expr, $label:expr, $sql:expr, $want:expr) => {{
        let r = q($pool, $sql).await;
        $rep.check($label, $sql, &r, $want);
    }};
}

macro_rules! run {
    ($rep:expr, $pool:expr, $label:expr, $sql:expr, $want:expr) => {{
        let r = exec($pool, $sql).await;
        $rep.check_exec($label, $sql, &r, $want);
    }};
}

async fn seed_p(pool: &sqlx::PgPool) {
    exec(pool, "CREATE TABLE p (id INT NOT NULL, a INT, b INT, s TEXT, f DOUBLE PRECISION, flag BOOLEAN, big BIGINT, sm SMALLINT, n NUMERIC(10,2), ts TIMESTAMP, d DATE)").await.expect("create p");
    let rows = [
        "(1, 1, 1, 'a', 1.5, true, 1, 1, 10.50, '2024-01-01 10:00:00', '2024-01-01')",
        "(2, 2, NULL, NULL, NULL, false, 4294967297, 2, 20.00, '2024-01-02 10:00:00', NULL)",
        "(3, NULL, 3, '', 3.5, NULL, 3, 3, NULL, NULL, '2024-01-03')",
        "(4, 0, 0, 'b', 0, true, 0, 0, 0.00, '2024-01-04 00:00:00', '2024-01-04')",
        "(5, NULL, 0, 'c', NULL, NULL, 5, 5, 5.00, NULL, NULL)",
        "(6, 0, NULL, 'd', NULL, NULL, 6, 6, 6.00, NULL, NULL)",
        "(7, -1, NULL, 'e', NULL, NULL, 7, 7, 7.00, NULL, NULL)",
        "(8, 1, NULL, 'f', NULL, NULL, 4294967297, 1, 8.00, NULL, NULL)",
    ];
    for r in rows {
        exec(pool, &format!("INSERT INTO p (id, a, b, s, f, flag, big, sm, n, ts, d) VALUES {}", r)).await.expect("insert p");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_where_and_null_logic() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    seed_p(&pool).await;
    let mut rep = Report::new();
    let p = &pool;

    // Sanity
    probe!(rep, p, "sanity", "SELECT id FROM p ORDER BY id", "1;2;3;4;5;6;7;8");
    probe!(rep, p, "sanity nulls", "SELECT id, a, b, s FROM p ORDER BY id", "1|1|1|a;2|2|NULL|NULL;3|NULL|3|;4|0|0|b;5|NULL|0|c;6|0|NULL|d;7|-1|NULL|e;8|1|NULL|f");

    // --- two nullable columns in one comparison ---
    probe!(rep, p, "null a=b", "SELECT id FROM p WHERE a = b ORDER BY id", "1;4");
    probe!(rep, p, "null b=a", "SELECT id FROM p WHERE b = a ORDER BY id", "1;4");
    probe!(rep, p, "null a+b>0", "SELECT id FROM p WHERE a + b > 0 ORDER BY id", "1");
    probe!(rep, p, "null a+b>=0", "SELECT id FROM p WHERE a + b >= 0 ORDER BY id", "1;4");
    probe!(rep, p, "null a+1=b", "SELECT id FROM p WHERE a + 1 = b ORDER BY id", "");
    probe!(rep, p, "null b=a+1", "SELECT id FROM p WHERE b = a + 1 ORDER BY id", "");
    probe!(rep, p, "null 0=b", "SELECT id FROM p WHERE 0 = b ORDER BY id", "4;5");
    probe!(rep, p, "null b=0", "SELECT id FROM p WHERE b = 0 ORDER BY id", "4;5");
    probe!(rep, p, "null a<>b", "SELECT id FROM p WHERE a <> b ORDER BY id", "");
    probe!(rep, p, "null a<b", "SELECT id FROM p WHERE a < b ORDER BY id", "");
    probe!(rep, p, "null a>=b", "SELECT id FROM p WHERE a >= b ORDER BY id", "1;4");
    probe!(rep, p, "null (a)=(b)", "SELECT id FROM p WHERE (a) = (b) ORDER BY id", "1;4");
    probe!(rep, p, "null a*2=b*2", "SELECT id FROM p WHERE a * 2 = b * 2 ORDER BY id", "1;4");
    probe!(rep, p, "null s=s'' ", "SELECT id FROM p WHERE s = '' ORDER BY id", "3");
    probe!(rep, p, "null s<>''", "SELECT id FROM p WHERE s <> '' ORDER BY id", "1;4;5;6;7;8");
    probe!(rep, p, "null a IS DISTINCT FROM b", "SELECT id FROM p WHERE a IS DISTINCT FROM b ORDER BY id", "2;3;5;6;7;8");
    probe!(rep, p, "null a IS NOT DISTINCT FROM b", "SELECT id FROM p WHERE a IS NOT DISTINCT FROM b ORDER BY id", "1;4");

    // --- = NULL folding ---
    probe!(rep, p, "a = NULL", "SELECT id FROM p WHERE a = NULL", "");
    probe!(rep, p, "a <> NULL", "SELECT id FROM p WHERE a <> NULL", "");
    probe!(rep, p, "a=1 OR b=NULL", "SELECT id FROM p WHERE a = 1 OR b = NULL ORDER BY id", "1;8");
    probe!(rep, p, "a=1 OR\\nb=NULL", "SELECT id FROM p WHERE a = 1 OR\nb = NULL ORDER BY id", "1;8");
    probe!(rep, p, "a=1 OR\\tb=NULL", "SELECT id FROM p WHERE a = 1 OR\tb = NULL ORDER BY id", "1;8");
    probe!(rep, p, "a=1 or b=NULL lower", "SELECT id FROM p WHERE a = 1 or b = NULL ORDER BY id", "1;8");
    probe!(rep, p, "b=NULL OR a=1", "SELECT id FROM p WHERE b = NULL OR a = 1 ORDER BY id", "1;8");
    probe!(rep, p, "a=1 AND b<>NULL", "SELECT id FROM p WHERE a = 1 AND b <> NULL ORDER BY id", "");
    probe!(rep, p, "NOT (a = NULL)", "SELECT id FROM p WHERE NOT (a = NULL) ORDER BY id", "");
    probe!(rep, p, "NULL = a", "SELECT id FROM p WHERE NULL = a ORDER BY id", "");
    probe!(rep, p, "a IN (1, NULL)", "SELECT id FROM p WHERE a IN (1, NULL) ORDER BY id", "1;8");
    probe!(rep, p, "a NOT IN (1, NULL)", "SELECT id FROM p WHERE a NOT IN (1, NULL) ORDER BY id", "");
    probe!(rep, p, "a NOT IN (1, 2)", "SELECT id FROM p WHERE a NOT IN (1, 2) ORDER BY id", "4;6;7");
    probe!(rep, p, "a IN (1, 2)", "SELECT id FROM p WHERE a IN (1, 2) ORDER BY id", "1;2;8");
    probe!(rep, p, "NULL IN (1)", "SELECT id FROM p WHERE NULL IN (1) ORDER BY id", "");
    probe!(rep, p, "NULL IS NULL", "SELECT id FROM p WHERE NULL IS NULL ORDER BY id", "1;2;3;4;5;6;7;8");
    probe!(rep, p, "a IS NULL", "SELECT id FROM p WHERE a IS NULL ORDER BY id", "3;5");
    probe!(rep, p, "a IS NOT NULL AND b IS NULL", "SELECT id FROM p WHERE a IS NOT NULL AND b IS NULL ORDER BY id", "2;6;7;8");
    probe!(rep, p, "a BETWEEN 0 AND 1", "SELECT id FROM p WHERE a BETWEEN 0 AND 1 ORDER BY id", "1;4;6;8");
    probe!(rep, p, "a NOT BETWEEN 0 AND 1", "SELECT id FROM p WHERE a NOT BETWEEN 0 AND 1 ORDER BY id", "2;7");
    probe!(rep, p, "a BETWEEN -1 AND 0", "SELECT id FROM p WHERE a BETWEEN -1 AND 0 ORDER BY id", "4;6;7");
    probe!(rep, p, "a BETWEEN b AND 5", "SELECT id FROM p WHERE a BETWEEN b AND 5 ORDER BY id", "1;4");
    probe!(rep, p, "b BETWEEN a AND 5", "SELECT id FROM p WHERE b BETWEEN a AND 5 ORDER BY id", "1;4");

    // --- precedence ---
    probe!(rep, p, "OR then AND", "SELECT id FROM p WHERE a = 1 OR a = 2 AND b = 3 ORDER BY id", "1;8");
    probe!(rep, p, "AND then OR", "SELECT id FROM p WHERE a = 2 AND b = 3 OR a = 1 ORDER BY id", "1;8");
    probe!(rep, p, "paren OR AND", "SELECT id FROM p WHERE (a = 1 OR a = 2) AND b = 3 ORDER BY id", "");
    probe!(rep, p, "paren OR AND 2", "SELECT id FROM p WHERE (a = 1 OR a = 2) AND b = 1 ORDER BY id", "1");
    probe!(rep, p, "A AND (B OR C)", "SELECT id FROM p WHERE b IS NULL AND (a = 0 OR a = 2) ORDER BY id", "2;6");
    probe!(rep, p, "A OR B AND C OR D", "SELECT id FROM p WHERE id = 3 OR a = 1 AND b = 1 OR id = 5 ORDER BY id", "1;3;5");
    probe!(rep, p, "nested", "SELECT id FROM p WHERE ((a = 1 AND b = 1) OR (a = 0 AND b = 0)) AND id < 4 ORDER BY id", "1");
    probe!(rep, p, "NOT (a = 1)", "SELECT id FROM p WHERE NOT (a = 1) ORDER BY id", "2;4;6;7");
    probe!(rep, p, "NOT a = 1", "SELECT id FROM p WHERE NOT a = 1 ORDER BY id", "2;4;6;7");
    probe!(rep, p, "NOT (a = 1 OR a = 2)", "SELECT id FROM p WHERE NOT (a = 1 OR a = 2) ORDER BY id", "4;6;7");
    probe!(rep, p, "NOT a IN", "SELECT id FROM p WHERE NOT a IN (1, 2) ORDER BY id", "4;6;7");
    probe!(rep, p, "NOT a IS NULL", "SELECT id FROM p WHERE NOT a IS NULL ORDER BY id", "1;2;4;6;7;8");
    probe!(rep, p, "NOT flag", "SELECT id FROM p WHERE NOT flag ORDER BY id", "2");
    probe!(rep, p, "flag", "SELECT id FROM p WHERE flag ORDER BY id", "1;4");
    probe!(rep, p, "flag = 't'", "SELECT id FROM p WHERE flag = 't' ORDER BY id", "1;4");
    probe!(rep, p, "flag IS NOT TRUE", "SELECT id FROM p WHERE flag IS NOT TRUE ORDER BY id", "2;3;5;6;7;8");
    probe!(rep, p, "flag IS FALSE", "SELECT id FROM p WHERE flag IS FALSE ORDER BY id", "2");
    probe!(rep, p, "flag = false", "SELECT id FROM p WHERE flag = false ORDER BY id", "2");
    probe!(rep, p, "NOT flag OR a = 1", "SELECT id FROM p WHERE NOT flag OR a = 1 ORDER BY id", "1;2;8");
    probe!(rep, p, "a=1 AND NOT flag", "SELECT id FROM p WHERE a = 2 AND NOT flag ORDER BY id", "2");
    probe!(rep, p, "BETWEEN OR", "SELECT id FROM p WHERE a BETWEEN 1 AND 2 OR id = 4 ORDER BY id", "1;2;4;8");
    probe!(rep, p, "-a = 1", "SELECT id FROM p WHERE -a = 1 ORDER BY id", "7");
    probe!(rep, p, "a * -1 = 1", "SELECT id FROM p WHERE a * -1 = 1 ORDER BY id", "7");
    probe!(rep, p, "a = -1", "SELECT id FROM p WHERE a = -1 ORDER BY id", "7");
    probe!(rep, p, "a=-1 nospace", "SELECT id FROM p WHERE a=-1 ORDER BY id", "7");
    probe!(rep, p, "a - 1 = -2", "SELECT id FROM p WHERE a - 1 = -2 ORDER BY id", "7");
    probe!(rep, p, "a -1 = -2", "SELECT id FROM p WHERE a -1 = -2 ORDER BY id", "7");
    probe!(rep, p, "1 - a = 2", "SELECT id FROM p WHERE 1 - a = 2 ORDER BY id", "7");
    probe!(rep, p, "a % 2 = 0", "SELECT id FROM p WHERE a % 2 = 0 ORDER BY id", "2;4;6");
    probe!(rep, p, "a + 1 * 2 = 3", "SELECT id FROM p WHERE a + 1 * 2 = 3 ORDER BY id", "1;8");
    probe!(rep, p, "(a + 1) * 2 = 4", "SELECT id FROM p WHERE (a + 1) * 2 = 4 ORDER BY id", "1;8");
    probe!(rep, p, "a / 2 = 1", "SELECT id FROM p WHERE a / 2 = 1 ORDER BY id", "2");
    probe!(rep, p, "s || 'x' = 'ax'", "SELECT id FROM p WHERE s || 'x' = 'ax' ORDER BY id", "1");
    probe!(rep, p, "chain a = b = true", "SELECT id FROM p WHERE (a = b) = true ORDER BY id", "1;4");
    probe!(rep, p, "1 = 1", "SELECT id FROM p WHERE 1 = 1 ORDER BY id", "1;2;3;4;5;6;7;8");
    probe!(rep, p, "true", "SELECT id FROM p WHERE true ORDER BY id", "1;2;3;4;5;6;7;8");
    probe!(rep, p, "false", "SELECT id FROM p WHERE false ORDER BY id", "");
    probe!(rep, p, "a = 1 trailing junk", "SELECT id FROM p WHERE a = 1 junk", "ERR");
    probe!(rep, p, "a = 1 1", "SELECT id FROM p WHERE a = 1 1", "ERR");
    probe!(rep, p, "a = 1 b = 1", "SELECT id FROM p WHERE a = 1 b = 1", "ERR");

    // --- type coercion in comparisons ---
    probe!(rep, p, "big = a (i64 vs i32)", "SELECT id FROM p WHERE big = a ORDER BY id", "1;4");
    probe!(rep, p, "a = big", "SELECT id FROM p WHERE a = big ORDER BY id", "1;4");
    probe!(rep, p, "big > a", "SELECT id FROM p WHERE big > a ORDER BY id", "2;6;7;8");
    probe!(rep, p, "sm = a (i16 vs i32)", "SELECT id FROM p WHERE sm = a ORDER BY id", "1;4;8");
    probe!(rep, p, "a = sm", "SELECT id FROM p WHERE a = sm ORDER BY id", "1;4;8");
    probe!(rep, p, "a = f (int vs float)", "SELECT id FROM p WHERE a = f ORDER BY id", "4");
    probe!(rep, p, "f = a", "SELECT id FROM p WHERE f = a ORDER BY id", "4");
    probe!(rep, p, "a < f", "SELECT id FROM p WHERE a < f ORDER BY id", "1");
    probe!(rep, p, "a > 0.5", "SELECT id FROM p WHERE a > 0.5 ORDER BY id", "1;2;8");
    probe!(rep, p, "a >= 0.5", "SELECT id FROM p WHERE a >= 0.5 ORDER BY id", "1;2;8");
    probe!(rep, p, "a < 0.5", "SELECT id FROM p WHERE a < 0.5 ORDER BY id", "4;6;7");
    probe!(rep, p, "a = 1.0", "SELECT id FROM p WHERE a = 1.0 ORDER BY id", "1;8");
    probe!(rep, p, "a = 1.5", "SELECT id FROM p WHERE a = 1.5 ORDER BY id", "");
    probe!(rep, p, "0.5 < a", "SELECT id FROM p WHERE 0.5 < a ORDER BY id", "1;2;8");
    probe!(rep, p, "a = '1'", "SELECT id FROM p WHERE a = '1' ORDER BY id", "1;8");
    probe!(rep, p, "a > '0'", "SELECT id FROM p WHERE a > '0' ORDER BY id", "1;2;8");
    probe!(rep, p, "s = 1 (text vs int -> error)", "SELECT id FROM p WHERE s = 1 ORDER BY id", "ERR");
    probe!(rep, p, "big = 4294967297", "SELECT id FROM p WHERE big = 4294967297 ORDER BY id", "2;8");
    probe!(rep, p, "big > 2147483647", "SELECT id FROM p WHERE big > 2147483647 ORDER BY id", "2;8");
    probe!(rep, p, "sm = 1", "SELECT id FROM p WHERE sm = 1 ORDER BY id", "1;8");
    probe!(rep, p, "sm > 40000 (out of range)", "SELECT id FROM p WHERE sm > 40000 ORDER BY id", "");
    probe!(rep, p, "a > 3000000000 (out of range)", "SELECT id FROM p WHERE a > 3000000000 ORDER BY id", "");
    probe!(rep, p, "a < 3000000000", "SELECT id FROM p WHERE a < 3000000000 ORDER BY id", "1;2;4;6;7;8");
    probe!(rep, p, "f > 1", "SELECT id FROM p WHERE f > 1 ORDER BY id", "1;3");
    probe!(rep, p, "f = 0", "SELECT id FROM p WHERE f = 0 ORDER BY id", "4");
    probe!(rep, p, "n > 10.5", "SELECT id FROM p WHERE n > 10.5 ORDER BY id", "2");
    probe!(rep, p, "n = 10.5", "SELECT id FROM p WHERE n = 10.5 ORDER BY id", "1");
    probe!(rep, p, "n >= 10.50", "SELECT id FROM p WHERE n >= 10.50 ORDER BY id", "1;2");
    probe!(rep, p, "n < 1", "SELECT id FROM p WHERE n < 1 ORDER BY id", "4");
    probe!(rep, p, "n = 0", "SELECT id FROM p WHERE n = 0 ORDER BY id", "4");
    probe!(rep, p, "n = 20", "SELECT id FROM p WHERE n = 20 ORDER BY id", "2");
    probe!(rep, p, "n = '20.00'", "SELECT id FROM p WHERE n = '20.00' ORDER BY id", "2");
    probe!(rep, p, "n > 5.005", "SELECT id FROM p WHERE n > 5.005 AND n < 6.5 ORDER BY id", "6");
    probe!(rep, p, "n BETWEEN 5 AND 7", "SELECT id FROM p WHERE n BETWEEN 5 AND 7 ORDER BY id", "5;6;7");
    probe!(rep, p, "n * 2 > 12", "SELECT id FROM p WHERE n * 2 > 12 ORDER BY id", "1;2;7;8");
    probe!(rep, p, "n = a (numeric vs int)", "SELECT id FROM p WHERE n = a ORDER BY id", "4");
    probe!(rep, p, "ts > date literal", "SELECT id FROM p WHERE ts > '2024-01-01' ORDER BY id", "1;2;4");
    probe!(rep, p, "ts >= '2024-01-02'", "SELECT id FROM p WHERE ts >= '2024-01-02' ORDER BY id", "2;4");
    probe!(rep, p, "ts = '2024-01-04'", "SELECT id FROM p WHERE ts = '2024-01-04' ORDER BY id", "4");
    probe!(rep, p, "ts::date = '2024-01-01'", "SELECT id FROM p WHERE ts::date = '2024-01-01' ORDER BY id", "1");
    probe!(rep, p, "d = '2024-01-01'", "SELECT id FROM p WHERE d = '2024-01-01' ORDER BY id", "1");
    probe!(rep, p, "d > '2024-01-01'", "SELECT id FROM p WHERE d > '2024-01-01' ORDER BY id", "3;4");
    probe!(rep, p, "d = ts (date vs ts)", "SELECT id FROM p WHERE d = ts ORDER BY id", "4");
    probe!(rep, p, "ts < NOW()", "SELECT id FROM p WHERE ts < NOW() ORDER BY id", "1;2;4");
    probe!(rep, p, "d < CURRENT_DATE", "SELECT id FROM p WHERE d < CURRENT_DATE ORDER BY id", "1;3;4");
    probe!(rep, p, "ts + interval", "SELECT id FROM p WHERE ts + INTERVAL '1 day' > '2024-01-04 12:00:00' ORDER BY id", "4");
    probe!(rep, p, "ts > NOW() - INTERVAL '1 day'", "SELECT id FROM p WHERE ts > NOW() - INTERVAL '1 day' ORDER BY id", "");
    probe!(rep, p, "s > 'a'", "SELECT id FROM p WHERE s > 'a' ORDER BY id", "4;5;6;7;8");
    probe!(rep, p, "s LIKE 'a%'", "SELECT id FROM p WHERE s LIKE 'a%' ORDER BY id", "1");
    probe!(rep, p, "s LIKE '%'", "SELECT id FROM p WHERE s LIKE '%' ORDER BY id", "1;3;4;5;6;7;8");
    probe!(rep, p, "s NOT LIKE 'a%'", "SELECT id FROM p WHERE s NOT LIKE 'a%' ORDER BY id", "3;4;5;6;7;8");
    probe!(rep, p, "s LIKE s2 (column pattern)", "SELECT id FROM p WHERE s LIKE s ORDER BY id", "1;3;4;5;6;7;8");
    probe!(rep, p, "s ILIKE 'A'", "SELECT id FROM p WHERE s ILIKE 'A' ORDER BY id", "1");
    probe!(rep, p, "s LIKE 'a_'", "SELECT id FROM p WHERE s LIKE '_' ORDER BY id", "1;4;5;6;7;8");
    probe!(rep, p, "s IN ('a','b')", "SELECT id FROM p WHERE s IN ('a', 'b') ORDER BY id", "1;4");
    probe!(rep, p, "s = ANY('{a,b}')", "SELECT id FROM p WHERE s = ANY('{a,b}') ORDER BY id", "1;4");
    probe!(rep, p, "s <> ALL('{a,b}')", "SELECT id FROM p WHERE s <> ALL('{a,b}') ORDER BY id", "3;5;6;7;8");
    probe!(rep, p, "lower(s) = 'a'", "SELECT id FROM p WHERE lower(s) = 'a' ORDER BY id", "1");
    probe!(rep, p, "length(s) = 1", "SELECT id FROM p WHERE length(s) = 1 ORDER BY id", "1;4;5;6;7;8");
    probe!(rep, p, "COALESCE(b, 0) = 0", "SELECT id FROM p WHERE COALESCE(b, 0) = 0 ORDER BY id", "2;4;5;6;7;8");
    probe!(rep, p, "CASE in WHERE", "SELECT id FROM p WHERE CASE WHEN a IS NULL THEN 1 ELSE 0 END = 1 ORDER BY id", "3;5");
    probe!(rep, p, "(a, b) = (1, 1)", "SELECT id FROM p WHERE (a, b) = (1, 1) ORDER BY id", "1");
    probe!(rep, p, "(a, id) > (0, 4)", "SELECT id FROM p WHERE (a, id) > (0, 4) ORDER BY id", "1;2;6;8");
    probe!(rep, p, "(a, b) IN ((1,1),(0,0))", "SELECT id FROM p WHERE (a, b) IN ((1, 1), (0, 0)) ORDER BY id", "1;4");
    probe!(rep, p, "p.a = 1", "SELECT p.id FROM p WHERE p.a = 1 ORDER BY p.id", "1;8");
    probe!(rep, p, "alias x.a", "SELECT x.id FROM p x WHERE x.a = 1 ORDER BY x.id", "1;8");
    probe!(rep, p, "alias AS x", "SELECT x.id FROM p AS x WHERE x.a = 1 ORDER BY x.id", "1;8");
    probe!(rep, p, "\"p\".\"a\"", "SELECT \"p\".\"id\" FROM \"p\" WHERE \"p\".\"a\" = 1 ORDER BY \"p\".\"id\"", "1;8");
    probe!(rep, p, "A = 1 (case fold)", "SELECT ID FROM P WHERE A = 1 ORDER BY ID", "1;8");
    probe!(rep, p, "IN empty subquery", "SELECT id FROM p WHERE a IN (SELECT a FROM p WHERE id = 99) ORDER BY id", "");
    probe!(rep, p, "NOT IN empty subquery", "SELECT id FROM p WHERE a NOT IN (SELECT a FROM p WHERE id = 99) ORDER BY id", "1;2;4;6;7;8");
    probe!(rep, p, "IN subquery with NULL", "SELECT id FROM p WHERE a IN (SELECT b FROM p) ORDER BY id", "1;4;6;8");
    probe!(rep, p, "NOT IN subquery with NULL", "SELECT id FROM p WHERE a NOT IN (SELECT b FROM p) ORDER BY id", "");
    probe!(rep, p, "NOT IN subquery no NULL", "SELECT id FROM p WHERE a NOT IN (SELECT b FROM p WHERE b IS NOT NULL) ORDER BY id", "2;7");
    probe!(rep, p, "= scalar subquery", "SELECT id FROM p WHERE a = (SELECT MIN(b) FROM p) ORDER BY id", "4;6");
    probe!(rep, p, "= scalar subquery NULL", "SELECT id FROM p WHERE a = (SELECT b FROM p WHERE id = 2) ORDER BY id", "");
    probe!(rep, p, "IN subquery ORDER LIMIT", "SELECT id FROM p WHERE id IN (SELECT id FROM p ORDER BY id DESC LIMIT 1) ORDER BY id", "8");
    probe!(rep, p, "EXISTS correlated", "SELECT id FROM p WHERE EXISTS (SELECT 1 FROM p x WHERE x.b = p.a) ORDER BY id", "1;4;6;8");
    probe!(rep, p, "NOT EXISTS correlated", "SELECT id FROM p WHERE NOT EXISTS (SELECT 1 FROM p x WHERE x.b = p.a) ORDER BY id", "2;3;5;7");
    probe!(rep, p, "EXISTS uncorrelated", "SELECT id FROM p WHERE EXISTS (SELECT 1 FROM p WHERE id = 99) ORDER BY id", "");
    probe!(rep, p, "literal on left", "SELECT id FROM p WHERE 1 = a ORDER BY id", "1;8");
    probe!(rep, p, "string 'a' containing WHERE", "SELECT id FROM p WHERE s = ' WHERE ' ORDER BY id", "");
    probe!(rep, p, "string containing LIMIT", "SELECT id FROM p WHERE s = 'x LIMIT 1' ORDER BY id", "");
    probe!(rep, p, "string containing ORDER BY", "SELECT id FROM p WHERE s = 'x ORDER BY y' ORDER BY id", "");
    probe!(rep, p, "string containing AND", "SELECT id FROM p WHERE s = ' AND ' OR a = 1 ORDER BY id", "1;8");
    probe!(rep, p, "string with quote", "SELECT id FROM p WHERE s = 'it''s' OR a = 1 ORDER BY id", "1;8");
    probe!(rep, p, "string with paren", "SELECT id FROM p WHERE (s = ')' OR a = 1) AND b = 1 ORDER BY id", "1");
    probe!(rep, p, "comment in where", "SELECT id FROM p WHERE a = 1 -- AND b = 99\n ORDER BY id", "1;8");
    probe!(rep, p, "block comment in where", "SELECT id FROM p WHERE a = 1 /* AND b = 99 */ ORDER BY id", "1;8");
    probe!(rep, p, "comment-looking string", "SELECT id FROM p WHERE s = '--' OR a = 1 ORDER BY id", "1;8");
    probe!(rep, p, "semicolon in string", "SELECT id FROM p WHERE s = ';' OR a = 1 ORDER BY id", "1;8");

    rep.dump("WHERE / NULL / precedence / coercion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_select_list_and_clauses() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    seed_p(&pool).await;
    let mut rep = Report::new();
    let p = &pool;

    // --- NULL in expressions ---
    probe!(rep, p, "a + b NULL", "SELECT a + b FROM p WHERE id = 2", "NULL");
    probe!(rep, p, "a + b", "SELECT a + b FROM p WHERE id = 1", "2");
    probe!(rep, p, "COALESCE", "SELECT COALESCE(b, -1) FROM p WHERE id = 2", "-1");
    probe!(rep, p, "COALESCE 3", "SELECT COALESCE(b, a, -1) FROM p WHERE id = 2", "2");
    probe!(rep, p, "NULLIF eq", "SELECT NULLIF(a, 1) FROM p WHERE id = 1", "NULL");
    probe!(rep, p, "NULLIF ne", "SELECT NULLIF(a, 2) FROM p WHERE id = 1", "1");
    probe!(rep, p, "CASE IS NULL", "SELECT CASE WHEN b IS NULL THEN 'null' ELSE 'val' END FROM p WHERE id = 2", "null");
    probe!(rep, p, "CASE no ELSE NULL", "SELECT CASE WHEN b > 0 THEN 'pos' END FROM p WHERE id = 2", "NULL");
    probe!(rep, p, "CASE no ELSE hit", "SELECT CASE WHEN b > 0 THEN 'pos' END FROM p WHERE id = 1", "pos");
    probe!(rep, p, "CASE b > 0 false", "SELECT CASE WHEN b > 0 THEN 'pos' ELSE 'no' END FROM p WHERE id = 4", "no");
    probe!(rep, p, "CASE simple", "SELECT CASE a WHEN 1 THEN 'one' ELSE 'other' END FROM p WHERE id = 1", "one");
    probe!(rep, p, "CASE simple NULL", "SELECT CASE a WHEN 1 THEN 'one' ELSE 'other' END FROM p WHERE id = 3", "other");
    probe!(rep, p, "CASE nested", "SELECT CASE WHEN a = 1 THEN CASE WHEN b = 1 THEN 'x' ELSE 'y' END ELSE 'z' END FROM p WHERE id = 1", "x");
    probe!(rep, p, "CASE AND arms", "SELECT CASE WHEN a = 0 AND b = 0 THEN 'zz' WHEN a = 0 THEN 'z' ELSE 'n' END FROM p WHERE id = 6", "z");
    probe!(rep, p, "concat NULL", "SELECT s || 'x' FROM p WHERE id = 2", "NULL");
    probe!(rep, p, "concat empty", "SELECT s || 'x' FROM p WHERE id = 3", "x");
    probe!(rep, p, "concat num", "SELECT a || '' FROM p WHERE id = 1", "1");
    probe!(rep, p, "int div", "SELECT 7 / 2 FROM p WHERE id = 1", "3");
    probe!(rep, p, "neg int div", "SELECT -7 / 2 FROM p WHERE id = 1", "-3");
    probe!(rep, p, "mod", "SELECT 7 % 3 FROM p WHERE id = 1", "1");
    probe!(rep, p, "float div", "SELECT 7 / 2.0 FROM p WHERE id = 1", "3.5");
    probe!(rep, p, "unary minus col", "SELECT -a FROM p WHERE id = 1", "-1");
    probe!(rep, p, "precedence * over +", "SELECT 2 + 3 * 4 FROM p WHERE id = 1", "14");
    probe!(rep, p, "paren", "SELECT (2 + 3) * 4 FROM p WHERE id = 1", "20");
    probe!(rep, p, "left assoc -", "SELECT 10 - 2 - 3 FROM p WHERE id = 1", "5");
    probe!(rep, p, "left assoc /", "SELECT 8 / 2 / 2 FROM p WHERE id = 1", "2");
    probe!(rep, p, "-2 + 3", "SELECT -2 + 3 FROM p WHERE id = 1", "1");
    probe!(rep, p, "2 - -3", "SELECT 2 - -3 FROM p WHERE id = 1", "5");
    probe!(rep, p, "a - -1", "SELECT a - -1 FROM p WHERE id = 1", "2");
    probe!(rep, p, "a - 1 - 1", "SELECT a - 1 - 1 FROM p WHERE id = 1", "-1");
    probe!(rep, p, "a * 2 + b", "SELECT a * 2 + b FROM p WHERE id = 1", "3");
    probe!(rep, p, "concat vs +", "SELECT 'x' || 1 + 1 FROM p WHERE id = 1", "x2");
    probe!(rep, p, "1e3", "SELECT 1e3 FROM p WHERE id = 1", "1000");
    probe!(rep, p, ".5", "SELECT .5 FROM p WHERE id = 1", "0.5");
    probe!(rep, p, "1.", "SELECT 1. FROM p WHERE id = 1", "1");
    probe!(rep, p, "1e-2", "SELECT 1e-2 FROM p WHERE id = 1", "0.01");
    probe!(rep, p, "int overflow", "SELECT 2147483647 + 1 FROM p WHERE id = 1", "ERR");
    probe!(rep, p, "bigint literal", "SELECT 9223372036854775807 FROM p WHERE id = 1", "9223372036854775807");
    probe!(rep, p, "it''s", "SELECT 'it''s' FROM p WHERE id = 1", "it's");
    probe!(rep, p, "E'a\\nb'", "SELECT E'a\\nb' FROM p WHERE id = 1", "a\nb");
    probe!(rep, p, "E'it\\'s'", "SELECT E'it\\'s' FROM p WHERE id = 1", "it's");
    probe!(rep, p, "$$it's$$", "SELECT $$it's$$ FROM p WHERE id = 1", "it's");
    probe!(rep, p, "$q$x$q$", "SELECT $q$x$q$ FROM p WHERE id = 1", "x");
    probe!(rep, p, "'x -- y'", "SELECT 'x -- y' FROM p WHERE id = 1", "x -- y");
    probe!(rep, p, "'/* z */'", "SELECT '/* z */' FROM p WHERE id = 1", "/* z */");
    probe!(rep, p, "1 /* c */ + 1", "SELECT 1 /* c */ + 1 FROM p WHERE id = 1", "2");
    probe!(rep, p, "1 -- c \\n + 1", "SELECT 1 -- c\n+ 1 FROM p WHERE id = 1", "2");
    probe!(rep, p, "'a;b'", "SELECT 'a;b' FROM p WHERE id = 1", "a;b");
    probe!(rep, p, "unicode", "SELECT 'ünï 😀' FROM p WHERE id = 1", "ünï 😀");
    probe!(rep, p, "'a\\b' std strings", "SELECT 'a\\b' FROM p WHERE id = 1", "a\\b");
    probe!(rep, p, "x'ff'", "SELECT x'ff' FROM p WHERE id = 1", "\\xff");
    probe!(rep, p, "B'101'", "SELECT B'101' FROM p WHERE id = 1", "101");
    probe!(rep, p, "TRUE literal", "SELECT TRUE FROM p WHERE id = 1", "t");
    probe!(rep, p, "'yes'::bool", "SELECT 'yes'::bool FROM p WHERE id = 1", "t");
    probe!(rep, p, "NULL literal", "SELECT NULL FROM p WHERE id = 1", "NULL");
    probe!(rep, p, "'infinity'::float", "SELECT 'infinity'::float8 FROM p WHERE id = 1", "Infinity");

    // no FROM
    probe!(rep, p, "SELECT 1", "SELECT 1", "1");
    probe!(rep, p, "SELECT 1 + 1", "SELECT 1 + 1", "2");
    probe!(rep, p, "SELECT 'x' AS v", "SELECT 'x' AS v", "x");
    probe!(rep, p, "SELECT 7 / 2", "SELECT 7 / 2", "3");
    probe!(rep, p, "SELECT 1 WHERE false", "SELECT 1 WHERE false", "");
    probe!(rep, p, "SELECT 1 WHERE 1 = 0", "SELECT 1 WHERE 1 = 0", "");
    probe!(rep, p, "SELECT 1 WHERE 1 = 1", "SELECT 1 WHERE 1 = 1", "1");
    probe!(rep, p, "SELECT 'a' || 'b'", "SELECT 'a' || 'b'", "ab");
    probe!(rep, p, "SELECT '5'::int + 1", "SELECT '5'::int + 1", "6");
    probe!(rep, p, "SELECT CAST", "SELECT CAST('5' AS int) + 1", "6");
    probe!(rep, p, "SELECT 1::bigint::text", "SELECT 1::bigint::text", "1");
    probe!(rep, p, "SELECT NOW() IS NOT NULL", "SELECT NOW() IS NOT NULL", "t");
    probe!(rep, p, "SELECT COALESCE(NULL, 1)", "SELECT COALESCE(NULL, 1)", "1");
    probe!(rep, p, "SELECT 1, 2", "SELECT 1, 2", "1|2");
    probe!(rep, p, "SELECT 1 UNION SELECT 2", "SELECT 1 UNION SELECT 2 ORDER BY 1", "1;2");

    // --- functions ---
    probe!(rep, p, "length", "SELECT length(s) FROM p WHERE id = 1", "1");
    probe!(rep, p, "upper", "SELECT upper(s) FROM p WHERE id = 1", "A");
    probe!(rep, p, "lower lit", "SELECT lower('AB') FROM p WHERE id = 1", "ab");
    probe!(rep, p, "substring from for", "SELECT substring('abcdef' from 2 for 3) FROM p WHERE id = 1", "bcd");
    probe!(rep, p, "substr", "SELECT substr('abcdef', 2, 3) FROM p WHERE id = 1", "bcd");
    probe!(rep, p, "trim", "SELECT trim('  x  ') FROM p WHERE id = 1", "x");
    probe!(rep, p, "concat()", "SELECT concat(s, 'y') FROM p WHERE id = 1", "ay");
    probe!(rep, p, "concat() NULL", "SELECT concat(s, 'y') FROM p WHERE id = 2", "y");
    probe!(rep, p, "replace", "SELECT replace('aXb', 'X', '-') FROM p WHERE id = 1", "a-b");
    probe!(rep, p, "position", "SELECT position('b' in 'abc') FROM p WHERE id = 1", "2");
    probe!(rep, p, "left", "SELECT left('abc', 2) FROM p WHERE id = 1", "ab");
    probe!(rep, p, "abs", "SELECT abs(-5) FROM p WHERE id = 1", "5");
    probe!(rep, p, "round", "SELECT round(2.567, 2) FROM p WHERE id = 1", "2.57");
    probe!(rep, p, "floor", "SELECT floor(2.7) FROM p WHERE id = 1", "2");
    probe!(rep, p, "greatest", "SELECT greatest(1, 5, 3) FROM p WHERE id = 1", "5");
    probe!(rep, p, "least", "SELECT least(1, 5, 3) FROM p WHERE id = 1", "1");
    probe!(rep, p, "greatest NULL", "SELECT greatest(b, 5) FROM p WHERE id = 2", "5");
    probe!(rep, p, "EXTRACT YEAR", "SELECT EXTRACT(YEAR FROM ts) FROM p WHERE id = 1", "2024");
    probe!(rep, p, "EXTRACT EPOCH", "SELECT EXTRACT(EPOCH FROM ts) FROM p WHERE id = 1", "1704103200");
    probe!(rep, p, "ts + interval", "SELECT ts + INTERVAL '1 day' FROM p WHERE id = 1", "2024-01-02 10:00:00");
    probe!(rep, p, "ts - ts", "SELECT ts - '2024-01-01 00:00:00' FROM p WHERE id = 1", "10:00:00");
    probe!(rep, p, "date_trunc", "SELECT date_trunc('day', ts) FROM p WHERE id = 1", "2024-01-01 00:00:00");
    probe!(rep, p, "ts::date", "SELECT ts::date FROM p WHERE id = 1", "2024-01-01");
    probe!(rep, p, "a::text || 'x'", "SELECT a::text || 'x' FROM p WHERE id = 1", "1x");
    probe!(rep, p, "'1.9'::numeric::int", "SELECT '1.9'::numeric::int FROM p WHERE id = 1", "2");
    probe!(rep, p, "f::int rounds", "SELECT f::int FROM p WHERE id = 1", "2");
    probe!(rep, p, "n::text", "SELECT n::text FROM p WHERE id = 1", "10.50");
    probe!(rep, p, "n + 1", "SELECT n + 1 FROM p WHERE id = 1", "11.50");
    probe!(rep, p, "n * 2", "SELECT n * 2 FROM p WHERE id = 1", "21.00");
    probe!(rep, p, "n / 3", "SELECT n / 3 FROM p WHERE id = 1", "3.5000000000000000");
    probe!(rep, p, "COUNT(*) FILTER", "SELECT COUNT(*) FILTER (WHERE b IS NULL) FROM p", "4");
    probe!(rep, p, "COUNT(DISTINCT b)", "SELECT COUNT(DISTINCT b) FROM p", "3");
    probe!(rep, p, "COUNT(DISTINCT s)", "SELECT COUNT(DISTINCT s) FROM p", "7");
    probe!(rep, p, "aggs ignore NULL", "SELECT COUNT(*), COUNT(b), SUM(b), AVG(b), MIN(b), MAX(b) FROM p", "8|4|4|1.0000000000000000|0|3");
    probe!(rep, p, "SUM empty NULL", "SELECT SUM(a), COUNT(a), MAX(s) FROM p WHERE id > 100", "NULL|0|NULL");
    probe!(rep, p, "SUM(CASE)", "SELECT SUM(CASE WHEN b IS NULL THEN 1 ELSE 0 END) FROM p", "4");
    probe!(rep, p, "MAX - MIN", "SELECT MAX(a) - MIN(a) FROM p", "3");
    probe!(rep, p, "AVG(f)", "SELECT AVG(f) FROM p", "1.6666666666666667");
    probe!(rep, p, "SUM(n)", "SELECT SUM(n) FROM p", "56.50");
    probe!(rep, p, "MIN(s) MAX(s)", "SELECT MIN(s), MAX(s) FROM p", "|f");
    probe!(rep, p, "COUNT(*) FILTER + COUNT", "SELECT COUNT(*) FILTER (WHERE a = 1), COUNT(*) FROM p", "2|8");
    probe!(rep, p, "STRING_AGG", "SELECT STRING_AGG(s, ',' ORDER BY s) FROM p WHERE id IN (1, 4)", "a,b");
    probe!(rep, p, "BOOL_OR", "SELECT BOOL_OR(flag), BOOL_AND(flag) FROM p", "t|f");

    // --- GROUP BY / DISTINCT / ORDER BY with NULLs ---
    probe!(rep, p, "GROUP BY NULL key", "SELECT b, COUNT(*) FROM p GROUP BY b ORDER BY b", "0|2;1|1;3|1;NULL|4");
    probe!(rep, p, "GROUP BY 1 ordinal", "SELECT b, COUNT(*) FROM p GROUP BY 1 ORDER BY 1", "0|2;1|1;3|1;NULL|4");
    probe!(rep, p, "GROUP BY alias", "SELECT b AS k, COUNT(*) AS c FROM p GROUP BY k ORDER BY k", "0|2;1|1;3|1;NULL|4");
    probe!(rep, p, "GROUP BY NULL text key", "SELECT s IS NULL AS sn, COUNT(*) FROM p GROUP BY sn ORDER BY sn", "f|7;t|1");
    probe!(rep, p, "GROUP BY text with '' and NULL", "SELECT s, COUNT(*) FROM p WHERE id IN (2, 3) GROUP BY s ORDER BY s", "|1;NULL|1");
    probe!(rep, p, "DISTINCT NULL", "SELECT DISTINCT b FROM p ORDER BY b", "0;1;3;NULL");
    probe!(rep, p, "DISTINCT '' vs NULL", "SELECT DISTINCT s FROM p WHERE id IN (2, 3) ORDER BY s", ";NULL");
    probe!(rep, p, "DISTINCT two cols", "SELECT DISTINCT a, b FROM p WHERE id IN (1, 8, 2) ORDER BY a, b", "1|1;1|NULL;2|NULL");
    probe!(rep, p, "ORDER BY b, id", "SELECT id FROM p ORDER BY b, id", "4;5;1;3;2;6;7;8");
    probe!(rep, p, "ORDER BY b DESC, id", "SELECT id FROM p ORDER BY b DESC, id", "2;6;7;8;3;1;4;5");
    probe!(rep, p, "ORDER BY b NULLS FIRST, id", "SELECT id FROM p ORDER BY b NULLS FIRST, id", "2;6;7;8;4;5;1;3");
    probe!(rep, p, "ORDER BY b DESC NULLS LAST, id", "SELECT id FROM p ORDER BY b DESC NULLS LAST, id", "3;1;4;5;2;6;7;8");
    probe!(rep, p, "ORDER BY s, id", "SELECT id FROM p ORDER BY s, id", "3;1;4;5;6;7;8;2");
    probe!(rep, p, "ORDER BY s DESC", "SELECT id FROM p ORDER BY s DESC, id", "2;8;7;6;5;4;1;3");
    probe!(rep, p, "ORDER BY f (float, nulls)", "SELECT id FROM p ORDER BY f, id", "4;1;3;2;5;6;7;8");
    probe!(rep, p, "ORDER BY alias k", "SELECT id AS k FROM p ORDER BY k DESC LIMIT 1", "8");
    probe!(rep, p, "ORDER BY alias shadows column", "SELECT a AS b, b AS a FROM p WHERE id IN (2, 3) ORDER BY a", "NULL|3;2|NULL");
    probe!(rep, p, "ORDER BY 1 DESC", "SELECT id FROM p ORDER BY 1 DESC LIMIT 2", "8;7");
    probe!(rep, p, "ORDER BY 2", "SELECT id, b FROM p WHERE b IS NOT NULL ORDER BY 2, 1", "4|0;5|0;1|1;3|3");
    probe!(rep, p, "ORDER BY a + b", "SELECT id FROM p WHERE a IS NOT NULL AND b IS NOT NULL ORDER BY a + b DESC, id", "1;4");
    probe!(rep, p, "ORDER BY lower(s)", "SELECT id FROM p WHERE id IN (1, 4) ORDER BY lower(s) DESC", "4;1");
    probe!(rep, p, "ORDER BY CASE", "SELECT id FROM p WHERE id IN (1, 2, 3) ORDER BY CASE WHEN a IS NULL THEN 0 ELSE 1 END, id", "3;1;2");
    probe!(rep, p, "ORDER BY unselected", "SELECT s FROM p WHERE id IN (1, 4) ORDER BY id DESC", "b;a");
    probe!(rep, p, "DISTINCT ORDER BY unselected (PG error)", "SELECT DISTINCT a FROM p ORDER BY s", "ERR");
    probe!(rep, p, "ORDER BY p.id", "SELECT id FROM p ORDER BY p.id DESC LIMIT 1", "8");
    probe!(rep, p, "ORDER BY x.id alias", "SELECT x.id FROM p x ORDER BY x.id DESC LIMIT 1", "8");
    probe!(rep, p, "HAVING no GROUP BY false", "SELECT COUNT(*) FROM p HAVING COUNT(*) > 100", "");
    probe!(rep, p, "HAVING no GROUP BY true", "SELECT COUNT(*) FROM p HAVING COUNT(*) > 1", "8");
    probe!(rep, p, "HAVING with GROUP BY", "SELECT b, COUNT(*) FROM p GROUP BY b HAVING COUNT(*) > 1 ORDER BY b", "0|2;NULL|4");
    probe!(rep, p, "GROUP BY expr", "SELECT a % 2 AS m, COUNT(*) FROM p WHERE a IS NOT NULL GROUP BY a % 2 ORDER BY m", "-1|1;0|3;1|2");
    probe!(rep, p, "GROUP BY only", "SELECT b FROM p GROUP BY b ORDER BY b", "0;1;3;NULL");
    probe!(rep, p, "COUNT(*) GROUP BY ORDER BY COUNT", "SELECT b, COUNT(*) AS c FROM p GROUP BY b ORDER BY c DESC, b", "NULL|4;0|2;1|1;3|1");

    // --- LIMIT / OFFSET forms ---
    probe!(rep, p, "LIMIT 2", "SELECT id FROM p ORDER BY id LIMIT 2", "1;2");
    probe!(rep, p, "LIMIT 2 OFFSET 1", "SELECT id FROM p ORDER BY id LIMIT 2 OFFSET 1", "2;3");
    probe!(rep, p, "OFFSET 6", "SELECT id FROM p ORDER BY id OFFSET 6", "7;8");
    probe!(rep, p, "LIMIT ALL", "SELECT id FROM p ORDER BY id LIMIT ALL", "1;2;3;4;5;6;7;8");
    probe!(rep, p, "OFFSET 6 ROWS", "SELECT id FROM p ORDER BY id OFFSET 6 ROWS", "7;8");
    probe!(rep, p, "FETCH FIRST 2", "SELECT id FROM p ORDER BY id FETCH FIRST 2 ROWS ONLY", "1;2");
    probe!(rep, p, "OFFSET FETCH NEXT", "SELECT id FROM p ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY", "2;3");
    probe!(rep, p, "LIMIT 0", "SELECT id FROM p ORDER BY id LIMIT 0", "");
    probe!(rep, p, "LIMIT NULL", "SELECT id FROM p ORDER BY id LIMIT NULL", "1;2;3;4;5;6;7;8");
    probe!(rep, p, "OFFSET then LIMIT", "SELECT id FROM p ORDER BY id OFFSET 1 LIMIT 2", "2;3");
    probe!(rep, p, "LIMIT expr", "SELECT id FROM p ORDER BY id LIMIT 1 + 1", "1;2");
    probe!(rep, p, "LIMIT no ORDER count", "SELECT COUNT(*) FROM (SELECT id FROM p LIMIT 3) x", "3");
    probe!(rep, p, "LIMIT with WHERE", "SELECT id FROM p WHERE a IS NOT NULL ORDER BY id DESC LIMIT 2", "8;7");
    probe!(rep, p, "LIMIT with GROUP BY", "SELECT b, COUNT(*) FROM p GROUP BY b ORDER BY b LIMIT 2", "0|2;1|1");
    probe!(rep, p, "LIMIT OFFSET GROUP BY", "SELECT b, COUNT(*) FROM p GROUP BY b ORDER BY b LIMIT 2 OFFSET 1", "1|1;3|1");
    probe!(rep, p, "DISTINCT LIMIT", "SELECT DISTINCT b FROM p ORDER BY b LIMIT 2", "0;1");

    // --- SELECT * and t.* ---
    probe!(rep, p, "SELECT *", "SELECT * FROM p WHERE id = 2", "2|2|NULL|NULL|NULL|f|4294967297|2|20.00|2024-01-02 10:00:00|NULL");
    probe!(rep, p, "SELECT p.*", "SELECT p.* FROM p WHERE id = 1", "1|1|1|a|1.5|t|1|1|10.50|2024-01-01 10:00:00|2024-01-01");
    probe!(rep, p, "SELECT *, extra", "SELECT *, id + 1 FROM p WHERE id = 1", "1|1|1|a|1.5|t|1|1|10.50|2024-01-01 10:00:00|2024-01-01|2");
    probe!(rep, p, "SELECT id, * ", "SELECT id, * FROM p WHERE id = 4", "4|4|0|0|b|0|t|0|0|0.00|2024-01-04 00:00:00|2024-01-04");
    probe!(rep, p, "SELECT id id (alias no AS)", "SELECT id k FROM p WHERE id = 1", "1");
    probe!(rep, p, "SELECT a, a", "SELECT a, a FROM p WHERE id = 1", "1|1");
    probe!(rep, p, "SELECT id AS \"Mixed Case\"", "SELECT id AS \"Mixed Case\" FROM p WHERE id = 1", "1");
    probe!(rep, p, "SELECT id AS from", "SELECT id AS \"from\" FROM p WHERE id = 1", "1");
    probe!(rep, p, "SELECT CASE alias no AS", "SELECT CASE WHEN a = 1 THEN 'y' ELSE 'n' END lbl FROM p WHERE id = 1", "y");
    probe!(rep, p, "SELECT (a) FROM", "SELECT (a) FROM p WHERE id = 1", "1");
    probe!(rep, p, "SELECT id FROM p WHERE id = 1 -- trailing", "SELECT id FROM p WHERE id = 1 -- trailing", "1");
    probe!(rep, p, "SELECT id FROM p WHERE id = 1;", "SELECT id FROM p WHERE id = 1;", "1");
    probe!(rep, p, "FROM p AS p", "SELECT p.id FROM p AS p WHERE id = 1", "1");
    probe!(rep, p, "alias same as col", "SELECT a.id FROM p AS a WHERE a.a = 1 ORDER BY a.id", "1;8");
    probe!(rep, p, "s = 'a' AS lit", "SELECT id FROM p WHERE s = 'AS' OR id = 1", "1");
    probe!(rep, p, "select 'from' literal", "SELECT 'from' FROM p WHERE id = 1", "from");
    probe!(rep, p, "select 'select' literal", "SELECT 'select', id FROM p WHERE id = 1", "select|1");
    probe!(rep, p, "select 'x from y' literal", "SELECT 'x from y' FROM p WHERE id = 1", "x from y");
    probe!(rep, p, "select 'x from y' 2", "SELECT id, 'x from y' FROM p WHERE id = 1", "1|x from y");
    probe!(rep, p, "select ')' literal", "SELECT ')' FROM p WHERE id = 1", ")");
    probe!(rep, p, "select ',' literal", "SELECT ',', id FROM p WHERE id = 1", ",|1");
    probe!(rep, p, "select 'a, b'", "SELECT 'a, b' AS v, id FROM p WHERE id = 1", "a, b|1");
    probe!(rep, p, "select 'AS'", "SELECT 'x AS y' FROM p WHERE id = 1", "x AS y");

    rep.dump("SELECT list / clauses");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_identifiers_and_dml() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let mut rep = Report::new();
    let p = &pool;

    // --- keyword column names ---
    run!(rep, p, "create kw", "CREATE TABLE kw (\"order\" INT, \"user\" TEXT, value TEXT, key TEXT, type TEXT, name TEXT, \"select\" INT, \"from\" INT, id INT, id_user INT, \"limit\" INT, \"desc\" TEXT, status TEXT, count INT, date TEXT, year INT, \"group\" TEXT, \"where\" TEXT, \"table\" TEXT, \"end\" TEXT, \"case\" TEXT, \"in\" TEXT, \"is\" TEXT, \"not\" TEXT, \"and\" TEXT, \"or\" TEXT, \"like\" TEXT, \"between\" TEXT, \"null\" TEXT, \"asc\" TEXT, \"by\" TEXT, \"on\" TEXT, \"as\" TEXT, \"join\" TEXT, \"left\" TEXT, \"set\" TEXT, \"values\" TEXT, \"returning\" TEXT, \"default\" TEXT)", None);
    run!(rep, p, "insert kw", "INSERT INTO kw (\"order\", \"user\", value, key, type, name, \"select\", \"from\", id, id_user, \"limit\", \"desc\", status, count, date, year, \"group\", \"where\", \"table\", \"end\", \"case\", \"in\", \"is\", \"not\", \"and\", \"or\", \"like\", \"between\", \"null\", \"asc\", \"by\", \"on\", \"as\", \"join\", \"left\", \"set\", \"values\", \"returning\", \"default\") VALUES (1, 'u', 'v', 'k', 't', 'n', 5, 6, 7, 8, 9, 'd', 's', 10, 'dt', 2024, 'g', 'w', 'tb', 'e', 'c', 'i', 'is', 'no', 'a', 'o', 'l', 'b', 'nl', 'as', 'by', 'on', 'az', 'j', 'lf', 'st', 'vl', 'r', 'df')", Some(1));
    probe!(rep, p, "kw select all named", "SELECT \"order\", \"user\", value, key, type, name, \"select\", \"from\", id, id_user, \"limit\", \"desc\" FROM kw", "1|u|v|k|t|n|5|6|7|8|9|d");
    probe!(rep, p, "kw select 2", "SELECT status, count, date, year, \"group\", \"where\", \"table\", \"end\", \"case\", \"in\", \"is\", \"not\" FROM kw", "s|10|dt|2024|g|w|tb|e|c|i|is|no");
    probe!(rep, p, "kw select 3", "SELECT \"and\", \"or\", \"like\", \"between\", \"null\", \"asc\", \"by\", \"on\", \"as\", \"join\", \"left\", \"set\", \"values\", \"returning\", \"default\" FROM kw", "a|o|l|b|nl|as|by|on|az|j|lf|st|vl|r|df");
    probe!(rep, p, "kw where id_user", "SELECT id FROM kw WHERE id_user = 8", "7");
    probe!(rep, p, "kw where id", "SELECT id_user FROM kw WHERE id = 7", "8");
    probe!(rep, p, "kw where id = 8 (no)", "SELECT id_user FROM kw WHERE id = 8", "");
    probe!(rep, p, "kw where \"order\"", "SELECT id FROM kw WHERE \"order\" = 1", "7");
    probe!(rep, p, "kw where value/key/type/name", "SELECT id FROM kw WHERE value = 'v' AND key = 'k' AND type = 't' AND name = 'n'", "7");
    probe!(rep, p, "kw where \"user\"", "SELECT id FROM kw WHERE \"user\" = 'u'", "7");
    probe!(rep, p, "kw where \"from\"/\"select\"", "SELECT id FROM kw WHERE \"from\" = 6 AND \"select\" = 5", "7");
    probe!(rep, p, "kw where \"limit\"", "SELECT id FROM kw WHERE \"limit\" = 9", "7");
    probe!(rep, p, "kw where \"desc\"", "SELECT id FROM kw WHERE \"desc\" = 'd'", "7");
    probe!(rep, p, "kw where \"in\"/\"is\"/\"not\"", "SELECT id FROM kw WHERE \"in\" = 'i' AND \"is\" = 'is' AND \"not\" = 'no'", "7");
    probe!(rep, p, "kw where \"and\"/\"or\"", "SELECT id FROM kw WHERE \"and\" = 'a' AND \"or\" = 'o'", "7");
    probe!(rep, p, "kw where \"like\"/\"between\"/\"null\"", "SELECT id FROM kw WHERE \"like\" = 'l' AND \"between\" = 'b' AND \"null\" = 'nl'", "7");
    probe!(rep, p, "kw where \"where\"", "SELECT id FROM kw WHERE \"where\" = 'w'", "7");
    probe!(rep, p, "kw where \"end\"/\"case\"", "SELECT id FROM kw WHERE \"end\" = 'e' AND \"case\" = 'c'", "7");
    probe!(rep, p, "kw where \"on\"/\"as\"/\"join\"", "SELECT id FROM kw WHERE \"on\" = 'on' AND \"as\" = 'az' AND \"join\" = 'j'", "7");
    probe!(rep, p, "kw where \"set\"/\"values\"", "SELECT id FROM kw WHERE \"set\" = 'st' AND \"values\" = 'vl'", "7");
    probe!(rep, p, "kw where \"returning\"/\"default\"", "SELECT id FROM kw WHERE \"returning\" = 'r' AND \"default\" = 'df'", "7");
    probe!(rep, p, "kw where count/date/year", "SELECT id FROM kw WHERE count = 10 AND date = 'dt' AND year = 2024", "7");
    probe!(rep, p, "kw order by \"desc\" DESC", "SELECT \"desc\" FROM kw ORDER BY \"desc\" DESC", "d");
    probe!(rep, p, "kw order by \"order\"", "SELECT id FROM kw ORDER BY \"order\"", "7");
    probe!(rep, p, "kw alias k.id", "SELECT k.id FROM kw AS k WHERE k.id_user = 8 ORDER BY k.id", "7");
    probe!(rep, p, "kw.\"order\"", "SELECT kw.\"order\" FROM kw WHERE kw.\"user\" = 'u'", "1");
    probe!(rep, p, "kw where status/key/type", "SELECT id FROM kw WHERE status = 's'", "7");
    run!(rep, p, "kw update value/key", "UPDATE kw SET value = 'v2', key = 'k2', \"order\" = 2, \"user\" = 'u2', \"limit\" = 10, \"desc\" = 'd2', \"where\" = 'w2', \"in\" = 'i2' WHERE type = 't'", Some(1));
    probe!(rep, p, "kw after update", "SELECT value, key, \"order\", \"user\", \"limit\", \"desc\", \"where\", \"in\" FROM kw", "v2|k2|2|u2|10|d2|w2|i2");
    probe!(rep, p, "kw update returning", "UPDATE kw SET count = count + 1 WHERE id = 7 RETURNING count, \"order\"", "11|2");
    probe!(rep, p, "kw group by \"order\"", "SELECT \"order\", COUNT(*) FROM kw GROUP BY \"order\"", "2|1");
    probe!(rep, p, "kw delete returning", "DELETE FROM kw WHERE \"order\" = 2 RETURNING id, \"user\"", "7|u2");

    // --- mixed case identifiers ---
    run!(rep, p, "create mc", "CREATE TABLE mc (\"MixedCase\" INT, lower_col INT, \"UPPER\" INT)", None);
    run!(rep, p, "insert mc", "INSERT INTO mc (\"MixedCase\", lower_col, \"UPPER\") VALUES (1, 2, 3)", Some(1));
    probe!(rep, p, "mc quoted", "SELECT \"MixedCase\" FROM mc", "1");
    probe!(rep, p, "mc LOWER_COL folds", "SELECT LOWER_COL FROM mc", "2");
    probe!(rep, p, "mc Lower_Col folds", "SELECT Lower_Col FROM mc WHERE LOWER_COL = 2", "2");
    probe!(rep, p, "mc mixedcase unquoted (PG error)", "SELECT mixedcase FROM mc", "ERR");
    probe!(rep, p, "mc \"UPPER\"", "SELECT \"UPPER\" FROM mc WHERE \"UPPER\" = 3", "3");
    probe!(rep, p, "mc upper unquoted (PG error)", "SELECT upper FROM mc", "ERR");
    probe!(rep, p, "mc SELECT *", "SELECT * FROM mc", "1|2|3");

    // --- prefix collisions ---
    run!(rep, p, "create pc", "CREATE TABLE pc (id INT, id_user INT, ident INT, name TEXT, name2 TEXT, named TEXT, a INT, ab INT, abc INT, order_id INT, limit_n INT, group_n INT, from_x INT, where_x INT, and_x INT, or_x INT, in_x INT, is_x INT, not_x INT, null_x INT, like_x TEXT, between_x INT, desc_x INT, asc_x INT, select_x INT, case_x INT, end_x INT, when_x INT, then_x INT, else_x INT, on_x INT, as_x INT, by_x INT, having_x INT, offset_x INT, distinct_x INT, union_x INT, join_x INT, left_x INT, using_x INT, set_x INT, values_x INT, returning_x INT, default_x INT, exists_x INT, any_x INT, all_x INT, some_x INT, true_x INT, false_x INT, interval_x INT, cast_x INT, date_x INT, time_x INT, timestamp_x INT, text_x INT, int_x INT)", None);
    run!(rep, p, "insert pc", "INSERT INTO pc (id, id_user, ident, name, name2, named, a, ab, abc, order_id, limit_n, group_n, from_x, where_x, and_x, or_x, in_x, is_x, not_x, null_x, like_x, between_x, desc_x, asc_x, select_x, case_x, end_x, when_x, then_x, else_x, on_x, as_x, by_x, having_x, offset_x, distinct_x, union_x, join_x, left_x, using_x, set_x, values_x, returning_x, default_x, exists_x, any_x, all_x, some_x, true_x, false_x, interval_x, cast_x, date_x, time_x, timestamp_x, text_x, int_x) VALUES (1, 2, 3, 'n', 'n2', 'nd', 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 'lk', 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53)", Some(1));
    probe!(rep, p, "pc id vs id_user vs ident", "SELECT id, id_user, ident FROM pc WHERE id = 1 AND id_user = 2 AND ident = 3", "1|2|3");
    probe!(rep, p, "pc id = 2 no", "SELECT id FROM pc WHERE id = 2", "");
    probe!(rep, p, "pc name/name2/named", "SELECT name, name2, named FROM pc WHERE name = 'n' AND name2 = 'n2' AND named = 'nd'", "n|n2|nd");
    probe!(rep, p, "pc a/ab/abc", "SELECT a, ab, abc FROM pc WHERE a = 4 AND ab = 5 AND abc = 6", "4|5|6");
    probe!(rep, p, "pc a = 5 no", "SELECT id FROM pc WHERE a = 5", "");
    probe!(rep, p, "pc abc/ab/a order", "SELECT abc, ab, a FROM pc", "6|5|4");
    probe!(rep, p, "pc keywordish 1", "SELECT order_id, limit_n, group_n, from_x, where_x, and_x, or_x, in_x, is_x, not_x, null_x, like_x, between_x FROM pc", "7|8|9|10|11|12|13|14|15|16|17|lk|18");
    probe!(rep, p, "pc keywordish 2", "SELECT desc_x, asc_x, select_x, case_x, end_x, when_x, then_x, else_x, on_x, as_x, by_x, having_x, offset_x FROM pc", "19|20|21|22|23|24|25|26|27|28|29|30|31");
    probe!(rep, p, "pc keywordish 3", "SELECT distinct_x, union_x, join_x, left_x, using_x, set_x, values_x, returning_x, default_x, exists_x, any_x, all_x, some_x FROM pc", "32|33|34|35|36|37|38|39|40|41|42|43|44");
    probe!(rep, p, "pc keywordish 4", "SELECT true_x, false_x, interval_x, cast_x, date_x, time_x, timestamp_x, text_x, int_x FROM pc", "45|46|47|48|49|50|51|52|53");
    probe!(rep, p, "pc where keywordish 1", "SELECT id FROM pc WHERE order_id = 7 AND limit_n = 8 AND group_n = 9 AND from_x = 10 AND where_x = 11 AND and_x = 12 AND or_x = 13 AND in_x = 14 AND is_x = 15 AND not_x = 16 AND null_x = 17 AND like_x = 'lk' AND between_x = 18", "1");
    probe!(rep, p, "pc where keywordish 2", "SELECT id FROM pc WHERE desc_x = 19 AND asc_x = 20 AND select_x = 21 AND case_x = 22 AND end_x = 23 AND when_x = 24 AND then_x = 25 AND else_x = 26 AND on_x = 27 AND as_x = 28 AND by_x = 29 AND having_x = 30 AND offset_x = 31", "1");
    probe!(rep, p, "pc where keywordish 3", "SELECT id FROM pc WHERE distinct_x = 32 AND union_x = 33 AND join_x = 34 AND left_x = 35 AND using_x = 36 AND set_x = 37 AND values_x = 38 AND returning_x = 39 AND default_x = 40 AND exists_x = 41 AND any_x = 42 AND all_x = 43 AND some_x = 44", "1");
    probe!(rep, p, "pc where keywordish 4", "SELECT id FROM pc WHERE true_x = 45 AND false_x = 46 AND interval_x = 47 AND cast_x = 48 AND date_x = 49 AND time_x = 50 AND timestamp_x = 51 AND text_x = 52 AND int_x = 53", "1");
    probe!(rep, p, "pc order by order_id", "SELECT id FROM pc ORDER BY order_id DESC", "1");
    probe!(rep, p, "pc order by desc_x", "SELECT id FROM pc ORDER BY desc_x", "1");
    probe!(rep, p, "pc where or_x=13 OR and_x=0", "SELECT id FROM pc WHERE or_x = 13 OR and_x = 0", "1");
    probe!(rep, p, "pc where not_x", "SELECT id FROM pc WHERE not_x = 16", "1");
    probe!(rep, p, "pc where in_x IN", "SELECT id FROM pc WHERE in_x IN (14)", "1");
    probe!(rep, p, "pc where is_x IS NOT NULL", "SELECT id FROM pc WHERE is_x IS NOT NULL", "1");
    probe!(rep, p, "pc where between_x BETWEEN", "SELECT id FROM pc WHERE between_x BETWEEN 18 AND 18", "1");
    probe!(rep, p, "pc where like_x LIKE", "SELECT id FROM pc WHERE like_x LIKE 'l%'", "1");
    probe!(rep, p, "pc where null_x IS NULL", "SELECT id FROM pc WHERE null_x IS NULL", "");
    probe!(rep, p, "pc where end_x = 23 in CASE", "SELECT CASE WHEN end_x = 23 THEN case_x ELSE when_x END FROM pc", "22");
    probe!(rep, p, "pc select from_x in expr", "SELECT from_x + where_x FROM pc", "21");
    probe!(rep, p, "pc select as_x AS on_x", "SELECT as_x AS on_x FROM pc", "28");
    probe!(rep, p, "pc group by group_n", "SELECT group_n, COUNT(*) FROM pc GROUP BY group_n", "9|1");
    probe!(rep, p, "pc update set_x", "UPDATE pc SET set_x = 99, values_x = 98 WHERE set_x = 37 RETURNING set_x, values_x", "99|98");
    probe!(rep, p, "pc limit_n LIMIT", "SELECT limit_n FROM pc ORDER BY limit_n LIMIT 1", "8");
    probe!(rep, p, "pc offset_x OFFSET", "SELECT offset_x FROM pc ORDER BY offset_x OFFSET 0", "31");
    probe!(rep, p, "pc select from_x last", "SELECT id, from_x FROM pc", "1|10");
    probe!(rep, p, "pc select union_x", "SELECT union_x FROM pc WHERE union_x = 33", "33");
    probe!(rep, p, "pc select distinct_x", "SELECT distinct_x FROM pc", "32");
    probe!(rep, p, "pc select DISTINCT distinct_x", "SELECT DISTINCT distinct_x FROM pc", "32");
    probe!(rep, p, "pc select exists_x", "SELECT exists_x FROM pc WHERE exists_x = 41", "41");
    probe!(rep, p, "pc having_x", "SELECT having_x FROM pc GROUP BY having_x HAVING having_x > 1", "30");

    // --- DML ---
    run!(rep, p, "create u", "CREATE TABLE u (id INT NOT NULL UNIQUE, v INT, note TEXT DEFAULT 'dflt', cnt INT DEFAULT 0 NOT NULL)", None);
    probe!(rep, p, "multi insert conflict nothing returning", "INSERT INTO u (id, v) VALUES (1, 10), (2, 20) ON CONFLICT DO NOTHING RETURNING id", "1;2");
    probe!(rep, p, "multi insert conflict nothing again", "INSERT INTO u (id, v) VALUES (1, 10), (2, 20) ON CONFLICT DO NOTHING RETURNING id", "");
    run!(rep, p, "multi insert conflict nothing again (count)", "INSERT INTO u (id, v) VALUES (1, 10), (2, 20) ON CONFLICT DO NOTHING", Some(0));
    probe!(rep, p, "multi insert conflict partial", "INSERT INTO u (id, v) VALUES (2, 21), (3, 30) ON CONFLICT DO NOTHING RETURNING id, v", "3|30");
    probe!(rep, p, "multi insert do update returning", "INSERT INTO u (id, v) VALUES (4, 40), (1, 99) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v RETURNING id, v", "4|40;1|99");
    probe!(rep, p, "after upsert", "SELECT id, v FROM u ORDER BY id", "1|99;2|20;3|30;4|40");
    probe!(rep, p, "upsert do update with where", "INSERT INTO u (id, v) VALUES (2, 5) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE u.v > 100 RETURNING id", "");
    probe!(rep, p, "upsert do update expr", "INSERT INTO u (id, v) VALUES (2, 5) ON CONFLICT (id) DO UPDATE SET v = u.v + EXCLUDED.v RETURNING v", "25");
    run!(rep, p, "insert DEFAULT", "INSERT INTO u (id, v, note) VALUES (5, DEFAULT, DEFAULT)", Some(1));
    probe!(rep, p, "after DEFAULT", "SELECT v, note, cnt FROM u WHERE id = 5", "NULL|dflt|0");
    run!(rep, p, "multi insert DEFAULT", "INSERT INTO u (id, v, note) VALUES (6, 60, DEFAULT), (7, DEFAULT, 'n7')", Some(2));
    probe!(rep, p, "after multi DEFAULT", "SELECT id, v, note FROM u WHERE id IN (6, 7) ORDER BY id", "6|60|dflt;7|NULL|n7");
    run!(rep, p, "insert no column list", "INSERT INTO u VALUES (8, 80, 'n8', 1)", Some(1));
    probe!(rep, p, "after no column list", "SELECT id, v, note, cnt FROM u WHERE id = 8", "8|80|n8|1");
    run!(rep, p, "insert select", "INSERT INTO u (id, v) SELECT id + 100, v FROM u WHERE id <= 2", Some(2));
    probe!(rep, p, "after insert select", "SELECT id, v, note FROM u WHERE id > 100 ORDER BY id", "101|99|dflt;102|25|dflt");
    run!(rep, p, "insert DEFAULT VALUES", "INSERT INTO u (id) VALUES (9)", Some(1));
    probe!(rep, p, "multi insert returning expr", "INSERT INTO u (id, v) VALUES (10, 1), (11, 2) RETURNING id, v * 10 AS t", "10|10;11|20");
    probe!(rep, p, "multi insert returning *", "INSERT INTO u (id, v) VALUES (12, 1), (13, 2) RETURNING *", "12|1|dflt|0;13|2|dflt|0");
    run!(rep, p, "update cnt = cnt + 1 where cnt = 0", "UPDATE u SET cnt = cnt + 1 WHERE cnt = 0", Some(14));
    probe!(rep, p, "after self-ref update", "SELECT SUM(cnt), MAX(cnt), COUNT(*) FROM u", "16|2|15");
    run!(rep, p, "update v = v + 1 where v = 20", "UPDATE u SET v = v + 1 WHERE v = 20", Some(1));
    probe!(rep, p, "after v+1", "SELECT id FROM u WHERE v = 21 ORDER BY id", "2");
    run!(rep, p, "update comma in string", "UPDATE u SET v = v * 2, note = 'x, y' WHERE id = 1", Some(1));
    probe!(rep, p, "after comma string", "SELECT v, note FROM u WHERE id = 1", "198|x, y");
    run!(rep, p, "update quote", "UPDATE u SET note = 'it''s' WHERE id = 2", Some(1));
    probe!(rep, p, "after quote", "SELECT note FROM u WHERE id = 2", "it's");
    run!(rep, p, "update paren string", "UPDATE u SET note = 'a) b (c' WHERE id = 3", Some(1));
    probe!(rep, p, "after paren", "SELECT note FROM u WHERE id = 3", "a) b (c");
    run!(rep, p, "update eq in string", "UPDATE u SET note = 'a = b, c = d' WHERE id = 4", Some(1));
    probe!(rep, p, "after eq string", "SELECT note FROM u WHERE id = 4", "a = b, c = d");
    run!(rep, p, "update to NULL", "UPDATE u SET v = NULL WHERE id = 3", Some(1));
    probe!(rep, p, "after NULL", "SELECT v FROM u WHERE id = 3", "NULL");
    run!(rep, p, "update DEFAULT", "UPDATE u SET note = DEFAULT, v = DEFAULT WHERE id = 8", Some(1));
    probe!(rep, p, "after DEFAULT update", "SELECT v, note FROM u WHERE id = 8", "NULL|dflt");
    run!(rep, p, "update swap", "UPDATE u SET v = cnt, cnt = v WHERE id = 6", Some(1));
    probe!(rep, p, "after swap", "SELECT v, cnt FROM u WHERE id = 6", "1|60");
    run!(rep, p, "update CASE", "UPDATE u SET note = CASE WHEN v IS NULL THEN 'none' ELSE 'some' END WHERE id IN (5, 7, 9)", Some(3));
    probe!(rep, p, "after update CASE", "SELECT id, note FROM u WHERE id IN (5, 7, 9) ORDER BY id", "5|none;7|none;9|none");
    run!(rep, p, "update where col in set", "UPDATE u SET v = v + 100 WHERE v > 50 AND v < 100", Some(1));
    probe!(rep, p, "after where col in set", "SELECT id, v FROM u WHERE v > 150 ORDER BY id", "1|198");
    run!(rep, p, "update row ctor", "UPDATE u SET (v, note) = (77, 'row') WHERE id = 7", Some(1));
    probe!(rep, p, "after row ctor", "SELECT v, note FROM u WHERE id = 7", "77|row");
    probe!(rep, p, "update returning expr", "UPDATE u SET v = 7 WHERE id = 9 RETURNING id, v * 2 AS dbl, note || '!'", "9|14|none!");
    probe!(rep, p, "update returning old-style", "UPDATE u SET v = v + 1 WHERE id = 9 RETURNING v", "8");
    run!(rep, p, "update no match", "UPDATE u SET v = 1 WHERE id = 9999", Some(0));
    run!(rep, p, "update all", "UPDATE u SET cnt = cnt", Some(15));

    run!(rep, p, "create q", "CREATE TABLE q (id INT, pid INT)", None);
    run!(rep, p, "insert q", "INSERT INTO q (id, pid) VALUES (1, 1), (2, NULL), (3, 9)", Some(3));
    run!(rep, p, "update from", "UPDATE u SET v = q.pid FROM q WHERE u.id = q.id", Some(3));
    probe!(rep, p, "after update from", "SELECT id, v FROM u WHERE id <= 3 ORDER BY id", "1|1;2|NULL;3|9");
    run!(rep, p, "update from with alias", "UPDATE u AS x SET note = 'fromq' FROM q WHERE x.id = q.id AND q.pid IS NOT NULL", Some(2));
    probe!(rep, p, "after update from alias", "SELECT id, note FROM u WHERE note = 'fromq' ORDER BY id", "1|fromq;3|fromq");
    probe!(rep, p, "delete using returning", "DELETE FROM u USING q WHERE u.id = q.id AND q.pid IS NULL RETURNING u.id", "2");
    probe!(rep, p, "delete in subquery returning", "DELETE FROM u WHERE id IN (SELECT id FROM q) RETURNING id", "1;3");
    run!(rep, p, "delete > 100", "DELETE FROM u WHERE id > 100", Some(2));
    probe!(rep, p, "delete returning expr", "DELETE FROM u WHERE id = 4 RETURNING id * 2, note", "8|a = b, c = d");
    run!(rep, p, "delete no match", "DELETE FROM u WHERE id = 9999", Some(0));
    probe!(rep, p, "remaining", "SELECT id FROM u ORDER BY id", "5;6;7;8;9;10;11;12;13");
    run!(rep, p, "delete all", "DELETE FROM u", Some(9));
    probe!(rep, p, "after delete all", "SELECT COUNT(*) FROM u", "0");

    // --- literal fidelity on INSERT ---
    run!(rep, p, "create lit", "CREATE TABLE lit (id INT, t TEXT, b BOOLEAN, i INT, f DOUBLE PRECISION, n NUMERIC(10,3))", None);
    run!(rep, p, "lit semicolon", "INSERT INTO lit (id, t) VALUES (1, 'semi;colon')", Some(1));
    run!(rep, p, "lit dash", "INSERT INTO lit (id, t) VALUES (2, 'has -- dash')", Some(1));
    run!(rep, p, "lit slash", "INSERT INTO lit (id, t) VALUES (3, 'slash /* c */')", Some(1));
    run!(rep, p, "lit E escaped quote", "INSERT INTO lit (id, t) VALUES (4, E'esc\\'aped')", Some(1));
    run!(rep, p, "lit E tab", "INSERT INTO lit (id, t) VALUES (5, E'a\\tb')", Some(1));
    run!(rep, p, "lit dollar", "INSERT INTO lit (id, t) VALUES (6, $$dollar 'q'$$)", Some(1));
    run!(rep, p, "lit unicode", "INSERT INTO lit (id, t) VALUES (7, 'ünï 😀')", Some(1));
    run!(rep, p, "lit backslash", "INSERT INTO lit (id, t) VALUES (8, 'a\\b')", Some(1));
    run!(rep, p, "lit doubled quote", "INSERT INTO lit (id, t) VALUES (9, 'it''s')", Some(1));
    run!(rep, p, "lit comma paren", "INSERT INTO lit (id, t) VALUES (10, 'a, b) (c')", Some(1));
    run!(rep, p, "lit cast", "INSERT INTO lit (id, t) VALUES (11, 'cast'::text)", Some(1));
    run!(rep, p, "lit empty", "INSERT INTO lit (id, t) VALUES (12, '')", Some(1));
    run!(rep, p, "lit NULL text", "INSERT INTO lit (id, t) VALUES (13, 'NULL')", Some(1));
    run!(rep, p, "lit tagged dollar", "INSERT INTO lit (id, t) VALUES (14, $tag$x$y$tag$)", Some(1));
    run!(rep, p, "lit concat expr", "INSERT INTO lit (id, t) VALUES (15, 'a' || 'b')", Some(1));
    run!(rep, p, "lit function", "INSERT INTO lit (id, t) VALUES (16, upper('ab'))", Some(1));
    probe!(rep, p, "lit readback", "SELECT id, t FROM lit ORDER BY id", "1|semi;colon;2|has -- dash;3|slash /* c */;4|esc'aped;5|a\tb;6|dollar 'q';7|ünï 😀;8|a\\b;9|it's;10|a, b) (c;11|cast;12|;13|NULL;14|x$y;15|ab;16|AB");
    probe!(rep, p, "lit NULL text is not NULL", "SELECT id FROM lit WHERE t IS NULL", "");
    probe!(rep, p, "lit where t = 'NULL'", "SELECT id FROM lit WHERE t = 'NULL'", "13");
    run!(rep, p, "lit bools", "INSERT INTO lit (id, b) VALUES (20, 't'), (21, 'yes'), (22, '1'), (23, TRUE), (24, 'off'), (25, 'f'), (26, 'true'), (27, false)", Some(8));
    probe!(rep, p, "lit bool readback", "SELECT id, b FROM lit WHERE id >= 20 ORDER BY id", "20|t;21|t;22|t;23|t;24|f;25|f;26|t;27|f");
    probe!(rep, p, "lit bool where b", "SELECT id FROM lit WHERE b ORDER BY id", "20;21;22;23;26");
    probe!(rep, p, "lit bool where b = false", "SELECT id FROM lit WHERE b = false ORDER BY id", "24;25;27");
    run!(rep, p, "lit nums", "INSERT INTO lit (id, i, f, n) VALUES (30, 1e2, 1e-2, 1.5e1), (31, -0, .5, 1.), (32, +5, -.5, -1.5), (33, '7', '2.5', '3.25'), (34, 2147483647, 1e308, 9999999.999)", Some(5));
    probe!(rep, p, "lit num readback", "SELECT id, i, f, n FROM lit WHERE id >= 30 ORDER BY id", "30|100|0.01|15.000;31|0|0.5|1.000;32|5|-0.5|-1.500;33|7|2.5|3.250;34|2147483647|1e+308|9999999.999");
    run!(rep, p, "lit int overflow refused", "INSERT INTO lit (id, i) VALUES (40, 3000000000)", Some(0));
    probe!(rep, p, "lit int overflow not stored", "SELECT i FROM lit WHERE id = 40", "");
    run!(rep, p, "lit numeric overflow refused", "INSERT INTO lit (id, n) VALUES (41, 12345678.5)", Some(0));
    probe!(rep, p, "lit numeric overflow not stored", "SELECT n FROM lit WHERE id = 41", "");
    run!(rep, p, "lit numeric rounds", "INSERT INTO lit (id, n) VALUES (42, 1.23456)", Some(1));
    probe!(rep, p, "lit numeric rounded", "SELECT n FROM lit WHERE id = 42", "1.235");
    run!(rep, p, "lit int from float refused", "INSERT INTO lit (id, i) VALUES (43, 1.5)", Some(0));
    probe!(rep, p, "lit int from float", "SELECT i FROM lit WHERE id = 43", "");
    run!(rep, p, "lit text into int refused", "INSERT INTO lit (id, i) VALUES (44, 'abc')", Some(0));
    probe!(rep, p, "lit text into int", "SELECT i FROM lit WHERE id = 44", "");
    run!(rep, p, "lit bool junk refused", "INSERT INTO lit (id, b) VALUES (45, 'maybe')", Some(0));
    probe!(rep, p, "lit bool junk", "SELECT b FROM lit WHERE id = 45", "");
    run!(rep, p, "lit too many values", "INSERT INTO lit (id, t) VALUES (46, 'a', 'b')", Some(0));
    run!(rep, p, "lit too few values", "INSERT INTO lit (id, t) VALUES (47)", Some(0));
    probe!(rep, p, "lit bad rows not stored", "SELECT id FROM lit WHERE id IN (46, 47)", "");

    // --- statement splitting (simple protocol, multiple statements) ---
    probe!(rep, p, "multi stmt semicolon in string", "INSERT INTO lit (id, t) VALUES (50, 'x;y'); SELECT t FROM lit WHERE id = 50", "x;y");
    probe!(rep, p, "multi stmt comment with ;", "SELECT id FROM lit WHERE id = 1 -- ; nope\n", "1");
    probe!(rep, p, "multi stmt block comment ;", "SELECT id FROM lit /* ; */ WHERE id = 1", "1");
    probe!(rep, p, "multi stmt dollar ;", "INSERT INTO lit (id, t) VALUES (51, $$a;b$$); SELECT t FROM lit WHERE id = 51", "a;b");
    probe!(rep, p, "multi stmt two selects", "SELECT id FROM lit WHERE id = 1; SELECT id FROM lit WHERE id = 2", "1;2");
    probe!(rep, p, "trailing ;;", "SELECT id FROM lit WHERE id = 1;;", "1");

    rep.dump("identifiers / DML / literals");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_joins_ctes_params() {
    let server = TestServer::start().await.expect("server");
    let pool = server.connect_sqlx().await.expect("connect");
    let mut rep = Report::new();
    let p = &pool;

    run!(rep, p, "create j1", "CREATE TABLE j1 (id INT NOT NULL, name TEXT)", None);
    run!(rep, p, "create j2", "CREATE TABLE j2 (id INT NOT NULL, j1_id INT, val TEXT)", None);
    run!(rep, p, "insert j1", "INSERT INTO j1 (id, name) VALUES (1, 'x'), (2, 'y'), (3, NULL)", Some(3));
    run!(rep, p, "insert j2", "INSERT INTO j2 (id, j1_id, val) VALUES (10, 1, 'a'), (11, 1, 'b'), (12, 2, NULL), (13, NULL, 'z')", Some(4));

    probe!(rep, p, "join *", "SELECT * FROM j1 JOIN j2 ON j1.id = j2.j1_id ORDER BY j2.id", "1|x|10|1|a;1|x|11|1|b;2|y|12|2|NULL");
    probe!(rep, p, "join j1.*", "SELECT j1.*, j2.val FROM j1 JOIN j2 ON j1.id = j2.j1_id ORDER BY j2.id", "1|x|a;1|x|b;2|y|NULL");
    probe!(rep, p, "join j2.*", "SELECT j2.*, j1.name FROM j1 JOIN j2 ON j1.id = j2.j1_id ORDER BY j2.id", "10|1|a|x;11|1|b|x;12|2|NULL|y");
    probe!(rep, p, "join same-named id", "SELECT j1.id, j2.id FROM j1 JOIN j2 ON j1.id = j2.j1_id WHERE j2.id > 10 ORDER BY j2.id", "1|11;2|12");
    probe!(rep, p, "join where j1.id", "SELECT j2.id FROM j1 JOIN j2 ON j1.id = j2.j1_id WHERE j1.id = 1 ORDER BY j2.id", "10;11");
    probe!(rep, p, "join alias", "SELECT a.name, b.val FROM j1 a JOIN j2 b ON a.id = b.j1_id WHERE b.val = 'a'", "x|a");
    probe!(rep, p, "left join nulls", "SELECT j1.id, j2.val FROM j1 LEFT JOIN j2 ON j1.id = j2.j1_id ORDER BY j1.id, j2.id", "1|a;1|b;2|NULL;3|NULL");
    probe!(rep, p, "left join IS NULL anti", "SELECT j1.id FROM j1 LEFT JOIN j2 ON j1.id = j2.j1_id WHERE j2.id IS NULL ORDER BY j1.id", "3");
    probe!(rep, p, "left join where right col", "SELECT j1.id FROM j1 LEFT JOIN j2 ON j1.id = j2.j1_id WHERE j2.val = 'a'", "1");
    probe!(rep, p, "left join count", "SELECT j1.id, COUNT(j2.id) FROM j1 LEFT JOIN j2 ON j1.id = j2.j1_id GROUP BY j1.id ORDER BY j1.id", "1|2;2|1;3|0");
    probe!(rep, p, "join OR across", "SELECT j2.id FROM j1 JOIN j2 ON j1.id = j2.j1_id WHERE j1.name = 'y' OR j2.val = 'a' ORDER BY j2.id", "10;12");
    probe!(rep, p, "join ON extra cond", "SELECT j2.id FROM j1 JOIN j2 ON j1.id = j2.j1_id AND j2.val = 'b'", "11");
    probe!(rep, p, "join NULL key no match", "SELECT COUNT(*) FROM j1 JOIN j2 ON j1.id = j2.j1_id", "3");
    probe!(rep, p, "join expr items", "SELECT j1.id + j2.id, j1.name || j2.val FROM j1 JOIN j2 ON j1.id = j2.j1_id ORDER BY j2.id", "11|xa;12|xb;14|NULL");
    probe!(rep, p, "join count star", "SELECT COUNT(*), COUNT(j2.val) FROM j1 JOIN j2 ON j1.id = j2.j1_id", "3|2");
    probe!(rep, p, "join order by right", "SELECT j2.val FROM j1 JOIN j2 ON j1.id = j2.j1_id ORDER BY j2.val DESC NULLS LAST", "b;a;NULL");
    probe!(rep, p, "join limit", "SELECT j2.id FROM j1 JOIN j2 ON j1.id = j2.j1_id ORDER BY j2.id DESC LIMIT 1", "12");
    probe!(rep, p, "self join", "SELECT a.id, b.id FROM j2 a JOIN j2 b ON a.j1_id = b.j1_id AND a.id < b.id", "10|11");
    probe!(rep, p, "inner join unqualified", "SELECT name, val FROM j1 JOIN j2 ON j1.id = j2.j1_id WHERE val = 'a'", "x|a");
    probe!(rep, p, "cross join", "SELECT COUNT(*) FROM j1, j2", "12");
    probe!(rep, p, "implicit join", "SELECT j1.name FROM j1, j2 WHERE j1.id = j2.j1_id AND j2.val = 'b'", "x");
    probe!(rep, p, "right join", "SELECT j2.id, j1.name FROM j1 RIGHT JOIN j2 ON j1.id = j2.j1_id ORDER BY j2.id", "10|x;11|x;12|y;13|NULL");
    probe!(rep, p, "join using", "SELECT j2.val FROM j2 JOIN j1 ON j1.id = j2.j1_id WHERE j1.id = 1 ORDER BY j2.val", "a;b");
    probe!(rep, p, "join where name IS NULL", "SELECT j1.id FROM j1 LEFT JOIN j2 ON j1.id = j2.j1_id WHERE j1.name IS NULL", "3");
    probe!(rep, p, "join where j2.val IS NULL", "SELECT j1.id, j2.id FROM j1 JOIN j2 ON j1.id = j2.j1_id WHERE j2.val IS NULL", "2|12");
    probe!(rep, p, "left join where j2.val IS NULL", "SELECT j1.id, j2.id FROM j1 LEFT JOIN j2 ON j1.id = j2.j1_id WHERE j2.val IS NULL ORDER BY j1.id", "2|12;3|NULL");

    // --- CTEs / subqueries ---
    probe!(rep, p, "cte", "WITH c AS (SELECT id FROM j2 WHERE j1_id IS NULL) SELECT id FROM c", "13");
    probe!(rep, p, "cte twice", "WITH c AS (SELECT id, j1_id FROM j2) SELECT c1.id FROM c c1 JOIN c c2 ON c1.id = c2.id ORDER BY c1.id", "10;11;12;13");
    probe!(rep, p, "cte twice scalar", "WITH c AS (SELECT id FROM j2) SELECT (SELECT COUNT(*) FROM c) + (SELECT COUNT(*) FROM c)", "8");
    probe!(rep, p, "cte shadows table", "WITH j1 AS (SELECT 42 AS id) SELECT id FROM j1", "42");
    probe!(rep, p, "cte chain", "WITH a AS (SELECT id FROM j2 WHERE id > 10), b AS (SELECT id FROM a WHERE id < 13) SELECT id FROM b ORDER BY id", "11;12");
    probe!(rep, p, "WITH RECURSIVE refused", "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r", "1;2;3");
    probe!(rep, p, "cte with where outside", "WITH c AS (SELECT id, val FROM j2) SELECT id FROM c WHERE val = 'a'", "10");
    probe!(rep, p, "cte column list", "WITH c(k) AS (SELECT id FROM j2 WHERE id = 10) SELECT k FROM c", "10");
    probe!(rep, p, "cte agg", "WITH c AS (SELECT j1_id, COUNT(*) AS n FROM j2 GROUP BY j1_id) SELECT j1_id, n FROM c WHERE n > 1", "1|2");
    probe!(rep, p, "derived table", "SELECT x.id FROM (SELECT id FROM j2 WHERE j1_id IS NOT NULL) x ORDER BY x.id", "10;11;12");
    probe!(rep, p, "derived agg", "SELECT MAX(n) FROM (SELECT j1_id, COUNT(*) AS n FROM j2 GROUP BY j1_id) x", "2");
    probe!(rep, p, "scalar subquery select list", "SELECT id, (SELECT COUNT(*) FROM j2) AS c FROM j1 WHERE id = 1", "1|4");
    probe!(rep, p, "correlated scalar select list", "SELECT j1.id, (SELECT COUNT(*) FROM j2 WHERE j2.j1_id = j1.id) FROM j1 ORDER BY j1.id", "1|2;2|1;3|0");
    probe!(rep, p, "correlated scalar NULL", "SELECT j1.id, (SELECT val FROM j2 WHERE j2.j1_id = j1.id AND j2.id = 12) FROM j1 WHERE j1.id IN (2, 3) ORDER BY j1.id", "2|NULL;3|NULL");
    probe!(rep, p, "union", "SELECT id FROM j1 UNION SELECT j1_id FROM j2 ORDER BY id", "1;2;3;NULL");
    probe!(rep, p, "union all", "SELECT COUNT(*) FROM (SELECT id FROM j1 UNION ALL SELECT id FROM j2) x", "7");
    probe!(rep, p, "union dedup", "SELECT COUNT(*) FROM (SELECT j1_id FROM j2 UNION SELECT j1_id FROM j2) x", "3");
    probe!(rep, p, "IN subquery", "SELECT id FROM j1 WHERE id IN (SELECT j1_id FROM j2) ORDER BY id", "1;2");
    probe!(rep, p, "NOT IN subquery NULL", "SELECT id FROM j1 WHERE id NOT IN (SELECT j1_id FROM j2) ORDER BY id", "");
    probe!(rep, p, "NOT IN subquery filtered", "SELECT id FROM j1 WHERE id NOT IN (SELECT j1_id FROM j2 WHERE j1_id IS NOT NULL) ORDER BY id", "3");
    probe!(rep, p, "EXISTS", "SELECT id FROM j1 WHERE EXISTS (SELECT 1 FROM j2 WHERE j2.j1_id = j1.id) ORDER BY id", "1;2");
    probe!(rep, p, "NOT EXISTS", "SELECT id FROM j1 WHERE NOT EXISTS (SELECT 1 FROM j2 WHERE j2.j1_id = j1.id) ORDER BY id", "3");
    probe!(rep, p, "EXISTS with extra", "SELECT id FROM j1 WHERE EXISTS (SELECT 1 FROM j2 WHERE j2.j1_id = j1.id AND j2.val = 'b') ORDER BY id", "1");
    probe!(rep, p, "ANY subquery", "SELECT id FROM j1 WHERE id = ANY (SELECT j1_id FROM j2) ORDER BY id", "1;2");
    probe!(rep, p, "> ALL subquery", "SELECT id FROM j2 WHERE id > ALL (SELECT id FROM j1) ORDER BY id", "10;11;12;13");
    probe!(rep, p, "window row_number", "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM j2 ORDER BY id", "10|1;11|2;12|3;13|4");
    probe!(rep, p, "window partition", "SELECT id, COUNT(*) OVER (PARTITION BY j1_id) FROM j2 ORDER BY id", "10|2;11|2;12|1;13|1");
    probe!(rep, p, "DISTINCT ON", "SELECT DISTINCT ON (j1_id) j1_id, id FROM j2 ORDER BY j1_id, id", "1|10;2|12;NULL|13");

    // --- parameters (extended protocol) ---
    run!(rep, p, "create pp", "CREATE TABLE pp (id INT, s TEXT, n INT, f DOUBLE PRECISION, flag BOOLEAN, ts TIMESTAMP)", None);
    run!(rep, p, "insert pp", "INSERT INTO pp (id, s, n, f, flag, ts) VALUES (1, 'it''s', 5, 1.5, true, '2024-01-01 10:00:00'), (2, 'a;b -- c', NULL, NULL, false, NULL), (3, '$1', 7, 2.5, NULL, '2024-06-01 00:00:00')", Some(3));

    async fn ids(pool: &sqlx::PgPool, sql: &str, binder: impl FnOnce(sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>) -> sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>) -> Rows {
        let qq = sqlx::query(AssertSqlSafe(sql.to_string()));
        let qq = binder(qq);
        let rows = qq.fetch_all(pool).await.map_err(err_line)?;
        let mut out = Vec::new();
        for row in rows {
            let mut vals = Vec::new();
            for i in 0..row.len() {
                let v: String = if let Ok(x) = row.try_get::<Option<i32>, _>(i) {
                    x.map(|x| x.to_string()).unwrap_or("NULL".into())
                } else if let Ok(x) = row.try_get::<Option<i64>, _>(i) {
                    x.map(|x| x.to_string()).unwrap_or("NULL".into())
                } else if let Ok(x) = row.try_get::<Option<i16>, _>(i) {
                    x.map(|x| x.to_string()).unwrap_or("NULL".into())
                } else if let Ok(x) = row.try_get::<Option<String>, _>(i) {
                    x.unwrap_or("NULL".into())
                } else if let Ok(x) = row.try_get::<Option<f64>, _>(i) {
                    x.map(|x| x.to_string()).unwrap_or("NULL".into())
                } else if let Ok(x) = row.try_get::<Option<bool>, _>(i) {
                    x.map(|x| x.to_string()).unwrap_or("NULL".into())
                } else {
                    format!("<undecodable {}>", row.column(i).name())
                };
                vals.push(v);
            }
            out.push(vals);
        }
        Ok(out)
    }

    let r = ids(p, "SELECT id FROM pp WHERE s = $1", |qq| qq.bind("it's")).await;
    rep.check("param quote", "s = $1 [it's]", &r, "1");
    let r = ids(p, "SELECT id FROM pp WHERE s = $1", |qq| qq.bind("a;b -- c")).await;
    rep.check("param semicolon/comment", "s = $1 [a;b -- c]", &r, "2");
    let r = ids(p, "SELECT id FROM pp WHERE s = $1", |qq| qq.bind("$1")).await;
    rep.check("param literal $1", "s = $1 [$1]", &r, "3");
    let r = ids(p, "SELECT id FROM pp WHERE s = $1", |qq| qq.bind("x' OR 1=1 --")).await;
    rep.check("param injection", "s = $1 [x' OR 1=1 --]", &r, "");
    let r = ids(p, "SELECT id FROM pp WHERE n = $1", |qq| qq.bind(5i32)).await;
    rep.check("param int", "n = $1 [5]", &r, "1");
    let r = ids(p, "SELECT id FROM pp WHERE n > $1", |qq| qq.bind(5i64)).await;
    rep.check("param int8 vs int4 col", "n > $1 [5i64]", &r, "3");
    let r = ids(p, "SELECT id FROM pp WHERE f > $1", |qq| qq.bind(2.0f64)).await;
    rep.check("param float", "f > $1 [2.0]", &r, "3");
    let r = ids(p, "SELECT id FROM pp WHERE flag = $1", |qq| qq.bind(true)).await;
    rep.check("param bool", "flag = $1 [true]", &r, "1");
    let r = ids(p, "SELECT id FROM pp WHERE n = $1", |qq| qq.bind(None::<i32>)).await;
    rep.check("param NULL", "n = $1 [NULL]", &r, "");
    let r = ids(p, "SELECT id FROM pp WHERE n IS NOT DISTINCT FROM $1", |qq| qq.bind(None::<i32>)).await;
    rep.check("param NULL not distinct", "n IS NOT DISTINCT FROM $1 [NULL]", &r, "2");
    let r = ids(p, "SELECT id FROM pp WHERE n = ANY($1) ORDER BY id", |qq| qq.bind(vec![5i32, 7])).await;
    rep.check("param = ANY(array)", "n = ANY($1) [5,7]", &r, "1;3");
    let r = ids(p, "SELECT id FROM pp WHERE n IN ($1, $2) ORDER BY id", |qq| qq.bind(5i32).bind(7i32)).await;
    rep.check("param IN", "n IN ($1,$2)", &r, "1;3");
    let r = ids(p, "SELECT id FROM pp WHERE s LIKE $1", |qq| qq.bind("it%")).await;
    rep.check("param LIKE", "s LIKE $1 [it%]", &r, "1");
    let r = ids(p, "SELECT id FROM pp WHERE s LIKE '%' || $1 || '%'", |qq| qq.bind("b --")).await;
    rep.check("param LIKE concat", "s LIKE '%' || $1 || '%'", &r, "2");
    let r = ids(p, "SELECT $1::int + $2::int", |qq| qq.bind(2i32).bind(3i32)).await;
    rep.check("param cast add", "SELECT $1::int + $2::int", &r, "5");
    let r = ids(p, "SELECT $1", |qq| qq.bind("x")).await;
    rep.check("param select", "SELECT $1 [x]", &r, "x");
    let r = ids(p, "SELECT id FROM pp ORDER BY id LIMIT $1", |qq| qq.bind(2i64)).await;
    rep.check("param LIMIT", "LIMIT $1 [2]", &r, "1;2");
    let r = ids(p, "SELECT id FROM pp ORDER BY id LIMIT $1 OFFSET $2", |qq| qq.bind(1i64).bind(1i64)).await;
    rep.check("param LIMIT OFFSET", "LIMIT $1 OFFSET $2", &r, "2");
    let r = ids(p, "SELECT id FROM pp WHERE ts > $1::timestamp ORDER BY id", |qq| qq.bind("2024-03-01 00:00:00")).await;
    rep.check("param ts cast", "ts > $1::timestamp", &r, "3");
    let r = ids(p, "SELECT id FROM pp WHERE ts > $1 ORDER BY id", |qq| qq.bind(chrono::NaiveDate::from_ymd_opt(2024, 3, 1).unwrap().and_hms_opt(0, 0, 0).unwrap())).await;
    rep.check("param ts chrono", "ts > $1 [chrono]", &r, "3");
    let r = ids(p, "INSERT INTO pp (id, s, n) VALUES ($1, $2, $3) RETURNING id, s", |qq| qq.bind(4i32).bind("O'Brien; -- x").bind(None::<i32>)).await;
    rep.check("param insert returning", "INSERT ... VALUES ($1,$2,$3) RETURNING", &r, "4|O'Brien; -- x");
    let r = ids(p, "SELECT s, n FROM pp WHERE id = 4", |qq| qq).await;
    rep.check("param insert stored", "SELECT s, n WHERE id = 4", &r, "O'Brien; -- x|NULL");
    let r = ids(p, "UPDATE pp SET n = $1 WHERE id = $2 RETURNING n", |qq| qq.bind(None::<i32>).bind(1i32)).await;
    rep.check("param update NULL", "UPDATE SET n = $1 [NULL]", &r, "NULL");
    let r = ids(p, "SELECT id FROM pp WHERE n = $1 OR s = $2 ORDER BY id", |qq| qq.bind(7i32).bind("it's")).await;
    rep.check("param two", "n = $1 OR s = $2", &r, "1;3");
    let r = ids(p, "SELECT id FROM pp WHERE $1 = $1 AND id = 1", |qq| qq.bind(1i32)).await;
    rep.check("param reused", "$1 = $1", &r, "1");
    let r = ids(p, "SELECT id FROM pp WHERE COALESCE(n, 0) = $1 ORDER BY id", |qq| qq.bind(0i32)).await;
    rep.check("param in COALESCE", "COALESCE(n, 0) = $1", &r, "1;2;4");
    let r = ids(p, "SELECT id FROM pp WHERE id = $1 AND (n IS NULL OR n > $2)", |qq| qq.bind(3i32).bind(1i32)).await;
    rep.check("param paren OR", "id = $1 AND (n IS NULL OR n > $2)", &r, "3");
    let r = ids(p, "SELECT CASE WHEN n IS NULL THEN $1 ELSE $2 END FROM pp WHERE id = 2", |qq| qq.bind("none").bind("some")).await;
    rep.check("param in CASE", "CASE ... $1 ... $2", &r, "none");
    let r = ids(p, "SELECT id FROM pp WHERE s = $1", |qq| qq.bind("")).await;
    rep.check("param empty string", "s = $1 ['']", &r, "");
    let r = ids(p, "SELECT id FROM pp WHERE n = $1", |qq| qq.bind(5i16)).await;
    rep.check("param int2", "n = $1 [5i16]", &r, "");
    let r = ids(p, "SELECT id FROM pp WHERE id = $1 AND s <> $2", |qq| qq.bind(2i32).bind("zzz")).await;
    rep.check("param <>", "id = $1 AND s <> $2", &r, "2");
    let r = ids(p, "SELECT id FROM pp WHERE id BETWEEN $1 AND $2 ORDER BY id", |qq| qq.bind(2i32).bind(3i32)).await;
    rep.check("param BETWEEN", "id BETWEEN $1 AND $2", &r, "2;3");

    rep.dump("joins / CTEs / params");
}
