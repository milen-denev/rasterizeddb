//! One raw-SQL probe per language feature.
//!
//! Probes are deliberately small: when one fails, the name says which feature
//! is missing rather than which ORM call broke. [`crate::probes::run_all`]
//! executes the list against a live server and reports a pass/fail matrix.

use sqlx::AssertSqlSafe;
use sqlx::{Row, ValueRef};

use crate::schema;

/// What a probe expects back.
#[derive(Debug, Clone)]
pub enum Expect {
    /// The statement runs without error; the result is not inspected.
    Succeeds,
    /// The statement is *refused*.
    ///
    /// A feature can be missing in two ways — unimplemented, or implemented and
    /// wrong — and only the second is dangerous. A probe that asserts a refusal
    /// pins the cases where accepting would be worse than declining.
    Fails,
    /// Exactly this many rows come back.
    Rows(usize),
    /// The first column, rendered as text, equals this sequence in order.
    FirstColumn(&'static [&'static str]),
    /// Every returned row rendered as `col|col|col`, in order.
    Grid(&'static [&'static str]),
}

#[derive(Debug, Clone)]
pub struct Probe {
    /// Feature family, used to group the report.
    pub area: &'static str,
    pub name: &'static str,
    pub sql: &'static str,
    pub expect: Expect,
}

#[derive(Debug, Clone)]
pub struct ProbeResult {
    pub probe: Probe,
    /// `None` when the probe passed, otherwise why it did not.
    pub failure: Option<String>,
}

impl ProbeResult {
    pub fn passed(&self) -> bool {
        self.failure.is_none()
    }
}

const fn p(area: &'static str, name: &'static str, sql: &'static str, expect: Expect) -> Probe {
    Probe { area, name, sql, expect }
}

/// The full compatibility surface being chased, whether or not it works yet.
pub fn probes() -> Vec<Probe> {
    use Expect::*;
    vec![
        // ---- projection ----------------------------------------------------
        p("projection", "select_star", "SELECT * FROM domain", Rows(2)),
        p("projection", "select_columns", "SELECT name FROM domain", Rows(2)),
        p("projection", "column_alias", "SELECT name AS domain_name FROM domain", Rows(2)),
        p("projection", "qualified_column", "SELECT domain.name FROM domain", Rows(2)),
        p("projection", "table_alias", "SELECT d.name FROM domain AS d", Rows(2)),
        p("projection", "quoted_identifier", r#"SELECT "name" FROM domain"#, Rows(2)),
        p("projection", "literal_expression", "SELECT 1 FROM domain", Rows(2)),
        p("projection", "distinct", "SELECT DISTINCT country FROM users", Rows(3)),
        p("projection", "arithmetic_expression", "SELECT net_amount + vat_amount FROM invoice", Rows(5)),

        // ---- filtering -----------------------------------------------------
        p("filter", "equals_text", "SELECT name FROM domain WHERE name = 'Acme'", FirstColumn(&["Acme"])),
        p("filter", "equals_uuid", "SELECT name FROM users WHERE id = 'aaaaaaaa-0000-4000-8000-000000000001'", FirstColumn(&["Alice"])),
        p("filter", "not_equals", "SELECT name FROM domain WHERE name != 'Acme'", FirstColumn(&["Globex"])),
        p("filter", "and_or", "SELECT name FROM users WHERE country = 'BG' AND is_legal_entity = true", FirstColumn(&["Alice"])),
        // `A AND (B OR C)` must not flatten to `(A AND B) OR C`. Both operand
        // orders are checked because the flattened form is right by luck in one
        // of them, which is how the bug survived.
        p("filter", "and_over_or_group", "SELECT name FROM users WHERE country = 'BG' AND (name = 'Alice' OR name = 'Carol' OR name = 'Dave') ORDER BY name ASC", FirstColumn(&["Alice"])),
        p("filter", "or_group_before_and", "SELECT name FROM users WHERE (name = 'Alice' OR name = 'Carol' OR name = 'Dave') AND country = 'BG' ORDER BY name ASC", FirstColumn(&["Alice"])),
        p("filter", "and_over_in_list", "SELECT name FROM users WHERE country = 'BG' AND name IN ('Alice','Carol','Dave') ORDER BY name ASC", FirstColumn(&["Alice"])),
        p("filter", "in_list_before_and", "SELECT name FROM users WHERE name IN ('Alice','Carol','Dave') AND country = 'BG' ORDER BY name ASC", FirstColumn(&["Alice"])),
        p("filter", "nested_groups", "SELECT name FROM users WHERE (country = 'BG' AND (name = 'Alice' OR name = 'Bob')) OR name = 'Carol' ORDER BY name ASC", FirstColumn(&["Alice", "Bob", "Carol"])),
        p("filter", "parenthesised_groups", "SELECT name FROM users WHERE (country = 'DE' OR country = 'BG') AND billing_setup = true", Rows(3)),
        p("filter", "numeric_comparison", "SELECT invoice_number FROM invoice WHERE net_amount > 200", Rows(3)),
        p("filter", "boolean_column", "SELECT name FROM users WHERE is_service_account = false", Rows(4)),
        p("filter", "is_null", "SELECT name FROM users WHERE country IS NULL", FirstColumn(&["Dave"])),
        p("filter", "is_not_null", "SELECT name FROM users WHERE username IS NOT NULL", FirstColumn(&["Bob"])),
        p("filter", "in_list", "SELECT name FROM users WHERE country IN ('BG', 'DE')", Rows(3)),
        p("filter", "not_in_list", "SELECT name FROM users WHERE country NOT IN ('DE')", Rows(2)),
        p("filter", "between", "SELECT invoice_number FROM invoice WHERE net_amount BETWEEN 80 AND 300", Rows(3)),
        p("filter", "like_prefix", "SELECT name FROM users WHERE email LIKE 'alice%'", FirstColumn(&["Alice"])),
        p("filter", "like_contains", "SELECT name FROM users WHERE email LIKE '%acme%'", Rows(2)),
        p("filter", "ilike", "SELECT name FROM users WHERE email ILIKE '%ACME%'", Rows(2)),
        p("filter", "ilike_prefix", "SELECT name FROM users WHERE email ILIKE 'ALICE%'", FirstColumn(&["Alice"])),
        p("filter", "ilike_exact", "SELECT name FROM users WHERE name ILIKE 'aLiCe'", FirstColumn(&["Alice"])),
        p("filter", "like_stays_case_sensitive", "SELECT name FROM users WHERE email LIKE '%ACME%'", Rows(0)),
        p("filter", "not_operator", "SELECT name FROM users WHERE NOT is_legal_entity", Rows(2)),
        p("filter", "string_range", "SELECT name FROM users WHERE name < 'C' ORDER BY name ASC", FirstColumn(&["Alice", "Bob"])),
        p("filter", "string_range_between", "SELECT name FROM users WHERE name BETWEEN 'B' AND 'D' ORDER BY name ASC", FirstColumn(&["Bob", "Carol"])),
        p("filter", "timestamp_comparison", "SELECT id FROM user_session WHERE expires_at > '2026-06-01 00:00:00'", Rows(2)),
        p("filter", "null_safe_combination", "SELECT id FROM user_session WHERE expires_at > '2026-06-01 00:00:00' AND revoked_at IS NULL", Rows(1)),

        // ---- ordering and paging ------------------------------------------
        p("order", "order_by_asc", "SELECT name FROM users ORDER BY name ASC", FirstColumn(&["Alice", "Bob", "Carol", "Dave"])),
        p("order", "order_by_desc", "SELECT name FROM users ORDER BY name DESC", FirstColumn(&["Dave", "Carol", "Bob", "Alice"])),
        p("order", "order_by_multiple", "SELECT name FROM users ORDER BY country ASC, name DESC", Rows(4)),
        p("order", "order_by_numeric", "SELECT invoice_number FROM invoice ORDER BY net_amount DESC", FirstColumn(&["1005", "1004", "1002", "1001", "1003"])),
        p("order", "limit", "SELECT name FROM users ORDER BY name ASC LIMIT 2", FirstColumn(&["Alice", "Bob"])),
        p("order", "offset", "SELECT name FROM users ORDER BY name ASC LIMIT 2 OFFSET 2", FirstColumn(&["Carol", "Dave"])),

        // ---- aggregates ----------------------------------------------------
        p("aggregate", "count_star", "SELECT COUNT(*) FROM users", FirstColumn(&["4"])),
        p("aggregate", "count_column", "SELECT COUNT(country) FROM users", FirstColumn(&["3"])),
        p("aggregate", "sum", "SELECT SUM(net_amount) FROM invoice", FirstColumn(&["1930.5"])),
        p("aggregate", "min_max", "SELECT MIN(invoice_number), MAX(invoice_number) FROM invoice", Grid(&["1001|1005"])),
        p("aggregate", "avg", "SELECT AVG(invoice_number) FROM invoice", FirstColumn(&["1003"])),
        p("aggregate", "group_by", "SELECT status, COUNT(*) FROM invoice GROUP BY status ORDER BY status ASC", Grid(&["open|1", "paid|3", "void|1"])),
        p("aggregate", "group_by_having", "SELECT status, COUNT(*) FROM invoice GROUP BY status HAVING COUNT(*) > 1", Grid(&["paid|3"])),
        // A LIMIT on an aggregate applies to the *output*. If it reached the
        // scan it would also switch on early termination and stop after N
        // input rows, returning a plausible wrong count with no error.
        p("aggregate", "count_ignores_limit", "SELECT COUNT(*) FROM invoice LIMIT 1", FirstColumn(&["5"])),
        p("aggregate", "group_by_limits_groups_not_rows", "SELECT status, COUNT(*) FROM invoice GROUP BY status ORDER BY status ASC LIMIT 2", Grid(&["open|1", "paid|3"])),
        p("aggregate", "count_with_where", "SELECT COUNT(*) FROM invoice WHERE status = 'paid'", FirstColumn(&["3"])),

        // ---- joins ---------------------------------------------------------
        p("join", "inner_join", "SELECT users.name FROM users INNER JOIN domain ON users.domain_id = domain.id WHERE domain.name = 'Acme' ORDER BY users.name ASC", FirstColumn(&["Alice", "Bob"])),
        p("join", "left_join", "SELECT roles.name FROM roles LEFT JOIN domain ON roles.domain_id = domain.id ORDER BY roles.name ASC", Rows(3)),
        p("join", "join_with_alias", "SELECT u.name FROM users AS u INNER JOIN domain AS d ON u.domain_id = d.id WHERE d.name = 'Globex' ORDER BY u.name ASC", FirstColumn(&["Carol", "Dave"])),
        p("join", "three_table_join", "SELECT u.name FROM users AS u INNER JOIN user_roles AS ur ON u.id = ur.user_id INNER JOIN roles AS r ON ur.role_id = r.id WHERE r.name = 'member' ORDER BY u.name ASC", FirstColumn(&["Alice", "Bob"])),
        p("join", "join_with_aggregate", "SELECT d.name, COUNT(*) FROM users AS u INNER JOIN domain AS d ON u.domain_id = d.id GROUP BY d.name ORDER BY d.name ASC", Grid(&["Acme|2", "Globex|2"])),

        // ---- subqueries ----------------------------------------------------
        p("join", "join_with_aggregate_and_having", "SELECT d.name, COUNT(*) FROM users AS u INNER JOIN domain AS d ON u.domain_id = d.id GROUP BY d.name HAVING COUNT(*) > 1 ORDER BY d.name ASC", Grid(&["Acme|2", "Globex|2"])),
        p("subquery", "in_subquery", "SELECT name FROM users WHERE domain_id IN (SELECT id FROM domain WHERE name = 'Acme') ORDER BY name ASC", FirstColumn(&["Alice", "Bob"])),
        p("subquery", "not_in_subquery", "SELECT name FROM users WHERE domain_id NOT IN (SELECT id FROM domain WHERE name = 'Acme') ORDER BY name ASC", FirstColumn(&["Carol", "Dave"])),
        // A subquery matching nothing makes the predicate unsatisfiable, not a
        // syntax error and not a predicate that matches everything.
        p("subquery", "empty_subquery_matches_nothing", "SELECT name FROM users WHERE domain_id IN (SELECT id FROM domain WHERE name = 'Nope')", Rows(0)),
        p("subquery", "exists", "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM user_roles WHERE user_roles.user_id = users.id) ORDER BY name ASC", Rows(3)),
        p("subquery", "scalar_subquery", "SELECT (SELECT COUNT(*) FROM users) FROM domain", Rows(2)),

        // ---- writes --------------------------------------------------------
        p("write", "insert_returning", "INSERT INTO domain (id, name, default_subscription) VALUES ('33333333-3333-4333-8333-333333333333', 'Initech', NULL) RETURNING id", Rows(1)),
        p("write", "update_where", "UPDATE users SET country = 'FR' WHERE name = 'Dave'", Succeeds),
        p("write", "update_multiple_columns", "UPDATE users SET country = 'FR', legal_entity_name = 'Dave SARL' WHERE name = 'Dave'", Succeeds),
        p("write", "delete_where", "DELETE FROM domain WHERE name = 'Initech'", Succeeds),
        p("write", "insert_multiple_rows", "INSERT INTO domain (id, name, default_subscription) VALUES ('44444444-4444-4444-8444-444444444444', 'Umbrella', NULL), ('55555555-5555-4555-8555-555555555555', 'Soylent', NULL)", Succeeds),
        p("write", "update_returning", "UPDATE users SET country = 'FR' WHERE name = 'Dave' RETURNING name", FirstColumn(&["Dave"])),
        p("write", "delete_returning", "DELETE FROM domain WHERE name = 'Nothing' RETURNING id", Rows(0)),
        p("write", "update_null", "UPDATE users SET country = NULL WHERE name = 'Dave'", Succeeds),

        // ---- types ---------------------------------------------------------
        p("type", "bytea_roundtrip", "SELECT content FROM attachment WHERE name = 'logo'", FirstColumn(&["deadbeef"])),
        p("type", "bytea_null", "SELECT content FROM attachment WHERE name = 'empty'", FirstColumn(&[""])),
        p("type", "array_roundtrip", "SELECT tags FROM attachment WHERE name = 'logo'", FirstColumn(&["alpha|beta"])),
        p("type", "array_empty", "SELECT tags FROM attachment WHERE name = 'empty'", FirstColumn(&[""])),
        p("type", "array_with_comma_and_null", "SELECT tags FROM attachment WHERE name = 'odd'", FirstColumn(&["has,comma|<null>"])),
        p("type", "inet_roundtrip", "SELECT ip_inet FROM user_session WHERE ip_inet = '10.0.0.1'", FirstColumn(&["10.0.0.1"])),
        p("type", "inet_ipv6_roundtrip", "SELECT ip_inet FROM user_session WHERE id = 'dddddddd-0000-4000-8000-000000000003'", FirstColumn(&["2001:db8::1"])),
        p("type", "uuid_roundtrip", "SELECT id FROM domain WHERE name = 'Acme'", FirstColumn(&[schema::ids::DOMAIN_ACME])),
        p("type", "numeric_roundtrip", "SELECT vat_rate FROM users WHERE name = 'Carol'", FirstColumn(&["19"])),
        p("type", "timestamp_roundtrip", "SELECT created_at FROM users WHERE name = 'Alice'", FirstColumn(&["2026-01-01 08:00:00+00:00"])),
        p("type", "boolean_roundtrip", "SELECT billing_setup FROM users WHERE name = 'Alice'", FirstColumn(&["t"])),
        p("type", "null_roundtrip", "SELECT default_subscription FROM domain WHERE name = 'Globex'", FirstColumn(&[""])),
        p("type", "bigint_roundtrip", "SELECT invoice_number FROM invoice WHERE invoice_number = 1001", FirstColumn(&["1001"])),
        p("type", "smallint_roundtrip", "SELECT kind FROM subscriptions WHERE group_name = 'org'", FirstColumn(&["1"])),

        // ---- json ----------------------------------------------------------
        // The document is reassembled from the table it was mapped onto, so a
        // faithful round trip is evidence that every key kept its type: a
        // number that had become text would come back quoted.
        // `seen` comes back as the string that was written — `2026-08-20T14:30:00Z`,
        // not `2026-08-20 14:30:00`. The change is deliberate and it is a fix.
        // Shredding inferred a `TIMESTAMPTZ` column from the *shape* of that
        // string, stored an instant, and re-rendered it on the way out, so a
        // client that wrote an ISO-8601 instant read back a different string
        // and nothing reported the substitution. JSON has no timestamp type: a
        // string is a string, and a document has to survive the round trip
        // unchanged. Keys are sorted because the stored form is canonical,
        // which is also PostgreSQL's `jsonb` behaviour.
        p("json", "jsonb_roundtrip", "SELECT payload FROM document WHERE name = 'full'",
          FirstColumn(&[r#"{"active":true,"hits":42,"ip":"10.0.0.1","name":"acme","ratio":1.5,"seen":"2026-08-20T14:30:00Z","tags":["a","b"]}"#])),
        p("json", "json_nested_object", "SELECT payload FROM document WHERE name = 'nested'",
          FirstColumn(&[r#"{"inner":{"a":1,"b":"deep"},"outer":"x"}"#])),
        p("json", "json_null", "SELECT payload FROM document WHERE name = 'none'", FirstColumn(&[""])),
        p("json", "json_column_is_a_column", "SELECT name FROM document WHERE payload IS NOT NULL", Rows(2)),

        // ---- transactions --------------------------------------------------
        // Each probe runs on its own connection from the pool, so these check
        // that the statements are accepted and reported; whether they actually
        // isolate is `tests/transactions.rs`, which holds one session open.
        p("transaction", "begin_commit", "BEGIN", Succeeds),
        p("transaction", "commit", "COMMIT", Succeeds),
        p("transaction", "rollback", "ROLLBACK", Succeeds),
        p("transaction", "begin_isolation_level", "BEGIN ISOLATION LEVEL REPEATABLE READ", Succeeds),
        p("transaction", "set_transaction_isolation", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", Succeeds),
        p("transaction", "start_transaction", "START TRANSACTION", Succeeds),
        // Savepoints need a block open, so these run as one statement each on a
        // pooled connection and only check the statement is accepted. What they
        // actually do is `tests/transactions.rs`, which holds one session.
        p("transaction", "savepoint_outside_block_refused", "SAVEPOINT probe_sp", Fails),
        p("transaction", "release_unknown_refused", "RELEASE SAVEPOINT probe_sp", Fails),

        // ---- aggregate + LIMIT ---------------------------------------------
        // A LIMIT applies to the aggregated *output*, not to the input. Getting
        // that wrong makes the scan stop early and return a smaller number,
        // which is the worst kind of wrong answer: plausible, and silent.
        p("aggregate", "count_ignores_limit_on_input", "SELECT COUNT(*) FROM invoice LIMIT 2", FirstColumn(&["5"])),
        p("aggregate", "sum_ignores_limit_on_input", "SELECT SUM(net_amount) FROM invoice LIMIT 2", FirstColumn(&["1930.5"])),
    ]
}

/// Applies the schema and seed data, then runs every probe.
pub async fn run_all(pool: &sqlx::PgPool) -> anyhow::Result<Vec<ProbeResult>> {
    let mut results = Vec::new();
    for probe in probes() {
        let failure = match run_one(pool, &probe).await {
            Ok(()) => None,
            Err(e) => Some(first_line(&e.to_string())),
        };
        results.push(ProbeResult { probe, failure });
    }
    Ok(results)
}

async fn run_one(pool: &sqlx::PgPool, probe: &Probe) -> anyhow::Result<()> {
    // One connection, held for this probe and cleaned before it goes back.
    //
    // Running straight against the pool made the probes share state: `BEGIN`,
    // `START TRANSACTION` and `BEGIN ISOLATION LEVEL …` each leave their
    // connection inside an open block, sqlx returns it to the pool as it is,
    // and a later probe handed that connection inherits the block. So
    // `SAVEPOINT probe_sp`, which must be refused *outside* a transaction, was
    // sometimes correctly accepted — the engine right both times and the probe
    // red on a coin toss. With one connection it reproduces every run.
    //
    // The `ROLLBACK` is what makes the claim above this list — that each probe
    // runs on its own connection — actually true. Outside a block it is a
    // no-op, and its own failure is not this probe's result.
    let mut connection = pool.acquire().await?;

    let outcome = if matches!(probe.expect, Expect::Fails) {
        match sqlx::query(probe.sql).fetch_all(&mut *connection).await {
            Err(_) => Ok(Vec::new()),
            Ok(_) => Err(anyhow::anyhow!(
                "the statement was accepted, but it must be refused"
            )),
        }
    } else {
        sqlx::query(probe.sql)
            .fetch_all(&mut *connection)
            .await
            .map_err(anyhow::Error::from)
    };
    let _ = sqlx::query("ROLLBACK").execute(&mut *connection).await;

    let rows = outcome?;
    if matches!(probe.expect, Expect::Fails) {
        return Ok(());
    }

    match &probe.expect {
        Expect::Succeeds => Ok(()),
        Expect::Fails => unreachable!("handled above"),
        Expect::Rows(n) => {
            anyhow::ensure!(rows.len() == *n, "expected {} rows, got {}", n, rows.len());
            Ok(())
        }
        Expect::FirstColumn(expected) => {
            let got: Vec<String> = rows.iter().map(|r| render_cell(r, 0)).collect();
            anyhow::ensure!(
                got.iter().map(String::as_str).eq(expected.iter().copied()),
                "expected {:?}, got {:?}",
                expected,
                got
            );
            Ok(())
        }
        Expect::Grid(expected) => {
            let got: Vec<String> = rows
                .iter()
                .map(|r| {
                    (0..r.columns().len())
                        .map(|i| render_cell(r, i))
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect();
            anyhow::ensure!(
                got.iter().map(String::as_str).eq(expected.iter().copied()),
                "expected {:?}, got {:?}",
                expected,
                got
            );
            Ok(())
        }
    }
}

/// Renders a cell by decoding it as the type the server declared.
///
/// Decoding rather than dumping the raw bytes is the point: drivers negotiate
/// the binary format for prepared statements, so a probe that compared raw
/// bytes would pass on a wrong encoding and fail on a right one. Going through
/// sqlx's decoders means the probe exercises the same path sea-orm does.
fn render_cell(row: &sqlx::postgres::PgRow, idx: usize) -> String {
    use sqlx::TypeInfo;

    let raw = match row.try_get_raw(idx) {
        Ok(v) => v,
        Err(e) => return format!("<error: {}>", e),
    };
    if raw.is_null() {
        return String::new();
    }

    let type_name = raw.type_info().name().to_ascii_uppercase();

    macro_rules! decode {
        ($ty:ty) => {
            match row.try_get::<$ty, _>(idx) {
                Ok(v) => return v.to_string(),
                Err(e) => return format!("<decode {}: {}>", type_name, first_line(&e.to_string())),
            }
        };
    }

    match type_name.as_str() {
        "UUID" => decode!(uuid::Uuid),
        "BOOL" => {
            return match row.try_get::<bool, _>(idx) {
                // Postgres' own text rendering of a boolean, so expectations
                // read the way they would in psql.
                Ok(v) => if v { "t" } else { "f" }.to_string(),
                Err(e) => format!("<decode BOOL: {}>", first_line(&e.to_string())),
            };
        }
        "BYTEA" => {
            return match row.try_get::<Vec<u8>, _>(idx) {
                // Rendered as hex so an expectation reads as bytes rather than
                // as whatever those bytes happen to be in UTF-8.
                Ok(v) => v.iter().map(|b| format!("{:02x}", b)).collect(),
                Err(e) => format!("<decode BYTEA: {}>", first_line(&e.to_string())),
            };
        }
        "TEXT[]" | "_TEXT" | "VARCHAR[]" => {
            return match row.try_get::<Vec<Option<String>>, _>(idx) {
                Ok(v) => v
                    .into_iter()
                    .map(|e| e.unwrap_or_else(|| "<null>".to_string()))
                    .collect::<Vec<_>>()
                    .join("|"),
                Err(e) => format!("<decode TEXT[]: {}>", first_line(&e.to_string())),
            };
        }
        // Decoded as JSON rather than as text, which is what an application
        // does with it — and what the probe is for. `jsonb`'s *binary* wire
        // form is a one-byte format version followed by the document, so
        // reading it as a string yields the document with a stray `\u{1}` on
        // the front. sqlx's `serde_json::Value` decoder handles the version
        // byte, exactly as sea-orm does.
        "JSONB" | "JSON" => {
            return match row.try_get::<serde_json::Value, _>(idx) {
                Ok(v) => v.to_string(),
                Err(e) => format!("<decode {}: {}>", type_name, first_line(&e.to_string())),
            };
        }
        "INT2" => decode!(i16),
        "INT4" => decode!(i32),
        "INT8" => decode!(i64),
        "FLOAT4" => decode!(f32),
        "FLOAT8" => decode!(f64),
        "NUMERIC" => decode!(rust_decimal::Decimal),
        "TIMESTAMPTZ" => {
            return match row.try_get::<chrono::DateTime<chrono::Utc>, _>(idx) {
                Ok(v) => v.format("%Y-%m-%d %H:%M:%S%:z").to_string(),
                Err(e) => format!("<decode TIMESTAMPTZ: {}>", first_line(&e.to_string())),
            };
        }
        "TIMESTAMP" => {
            return match row.try_get::<chrono::NaiveDateTime, _>(idx) {
                Ok(v) => v.format("%Y-%m-%d %H:%M:%S").to_string(),
                Err(e) => format!("<decode TIMESTAMP: {}>", first_line(&e.to_string())),
            };
        }
        // TEXT, INET and anything else: the wire bytes are already text.
        _ => match raw.as_bytes() {
            Ok(b) => String::from_utf8_lossy(b).into_owned(),
            Err(_) => "<unreadable>".to_string(),
        },
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").trim().to_string()
}

/// Creates every table and inserts the seed rows.
pub async fn apply_schema(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    // sqlx 0.9 accepts only `&'static str` in `query()`, so that dynamic SQL has
    // to be audited rather than merely compiled. Both statements below are built
    // by this crate — a fixed DDL array and the seed statements it generates —
    // and neither interpolates anything that came from outside it.
    for ddl in schema::CREATE_TABLES {
        sqlx::query(AssertSqlSafe(*ddl))
            .execute(pool)
            .await
            .map_err(|e| anyhow::anyhow!("DDL failed: {}\n{}", first_line(&e.to_string()), ddl))?;
    }
    for stmt in schema::seed_statements() {
        sqlx::query(AssertSqlSafe(stmt.as_str()))
            .execute(pool)
            .await
            .map_err(|e| anyhow::anyhow!("seed failed: {}\n{}", first_line(&e.to_string()), stmt))?;
    }
    Ok(())
}
