//! Routed INSERT (`docs/partitioning.md` §9, phase 2a slice S9), through the
//! wire front door: every row lands in the partition owning its point (I4),
//! insert and query agree on that point for every spelling of a value (I2),
//! uniqueness holds per partition (I10, partial), a `SERIAL` keeps counting
//! across restarts (§10), a transaction is whole across partitions, and what
//! is still refused writes nothing and materializes nothing.
//!
//! What the partitioned table stores is read through the engine's API
//! (`stored_rows`), below SQL, so a read path cannot hide a wrong write.

mod partition_wire;

use std::collections::{BTreeMap, BTreeSet};

use partition_wire::*;
use rasterizeddb_core::core::database::Database;
use rasterizeddb_core::core::partition::Route;
use rclite::Arc;

fn column<'a>(row: &'a StoredRow, name: &str) -> &'a [u8] {
    row.iter()
        .find(|(n, _)| n == name)
        .map(|(_, b)| b.as_slice())
        .unwrap_or_else(|| panic!("no column {name} in {row:?}"))
}

fn i64_of(row: &StoredRow, name: &str) -> i64 {
    i64::from_le_bytes(column(row, name).try_into().expect("8 bytes"))
}

fn i32_of(row: &StoredRow, name: &str) -> i32 {
    i32::from_le_bytes(column(row, name).try_into().expect("4 bytes"))
}

fn text_of(row: &StoredRow, name: &str) -> String {
    String::from_utf8(column(row, name).to_vec()).expect("UTF-8")
}

/// Partitions `SHOW PARTITIONS` reports materialized.
fn materialized(c: &mut Client, table: &str) -> usize {
    let r = c.ok(&format!("SHOW PARTITIONS {table}"));
    let state = r.columns.iter().position(|c| c == "state").expect("a state column");
    r.rows.iter().filter(|row| row[state].as_deref() == Some("materialized")).count()
}

/// The pid owning the point of a key whose stored bytes are `key`.
fn pid_of_key(db: &Arc<Database>, table: &str, key: &[&[u8]]) -> u32 {
    let pt = db.partitioned.get(table).expect("partitioned").value().clone();
    let point = pt.spec().unwrap().point_of(key).unwrap();
    pt.epoch().unwrap().map.pid_of(point)
}

#[track_caller]
fn expect_code(out: Outcome, code: &str) -> WireError {
    match out {
        Err(e) if e.code == code => e,
        other => panic!("expected {code}, got {other:?}"),
    }
}

const ACCT: &str = "(tenant_id BIGINT NOT NULL, id INT NOT NULL, label TEXT, PRIMARY KEY (tenant_id, id))";

/// I4 globally: one statement writing many tenants puts each row in the
/// partition whose `[lo, hi)` holds its point, materializes exactly the
/// partitions those points reach, and keeps them there across a restart.
#[test]
fn a_multi_row_multi_tenant_insert_puts_every_row_in_its_partition() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok(&format!("CREATE TABLE acct {ACCT} PARTITION BY HASH (tenant_id) PARTITIONS 16"));
    let rows: Vec<String> =
        (1..=30i64).flat_map(|t| (1..=2).map(move |i| format!("({t}, {i}, 'r{t}_{i}')"))).collect();
    let r = c.ok(&format!("INSERT INTO acct (tenant_id, id, label) VALUES {}", rows.join(", ")));
    assert_eq!(r.tags, vec!["INSERT 0 60".to_string()]);

    let db = server.db.clone();
    let stored = stored_rows(&db, "acct");
    assert_eq!(stored.len(), 60);
    assert_eq!(misplaced_rows(&db, "acct"), Vec::<String>::new());
    let want: BTreeSet<u32> = (1..=30i64).map(|t| pid_of_key(&db, "acct", &[&t.to_le_bytes()[..]])).collect();
    let have: BTreeSet<u32> = stored_rows_by_partition(&db, "acct")
        .into_iter()
        .filter(|(_, rows)| !rows.is_empty())
        .map(|(pid, _)| pid)
        .collect();
    assert_eq!(have, want, "the partitions holding rows are not the ones their points reach");
    assert_eq!(materialized(&mut c, "acct"), want.len(), "a partition no row reached was materialized");
    for (pid, rows) in stored_rows_by_partition(&db, "acct") {
        for row in rows {
            let t = i64_of(&row, "tenant_id");
            assert_eq!(pid_of_key(&db, "acct", &[&t.to_le_bytes()[..]]), pid, "tenant {t}");
            assert_eq!(text_of(&row, "label"), format!("r{t}_{}", i32_of(&row, "id")));
        }
    }

    drop(c);
    drop(db);
    server = server.restart();
    let db = server.db.clone();
    assert_eq!(stored_rows(&db, "acct"), stored, "a restart changed what is stored");
    assert_eq!(misplaced_rows(&db, "acct"), Vec::<String>::new());
    drop(db);
    drop(server);
}

/// §10 and plan fact 5: one sequence per logical table, seeded at the first
/// insert after a restart from the maximum over every partition. Seeded from
/// the shell, which holds no rows, it restarted at 1 after every restart: on
/// columns with no unique key, as here, a silent duplicate.
#[test]
fn serial_and_bigserial_keep_counting_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    {
        let mut c = server.connect();
        c.ok(
            "CREATE TABLE ser (tenant_id BIGINT NOT NULL, a SERIAL, b BIGSERIAL, body TEXT) \
             PARTITION BY HASH (tenant_id) PARTITIONS 8",
        );
        c.ok("INSERT INTO ser (tenant_id, body) VALUES (1, 'x'), (2, 'x'), (3, 'x')");
        c.ok("INSERT INTO ser (tenant_id, body) VALUES (4, 'x')");
    }
    for round in 0..2i64 {
        server = server.restart();
        let mut c = server.connect();
        let base = 5 + round * 3;
        c.ok(&format!("INSERT INTO ser (tenant_id, body) VALUES ({base}, 'y'), ({}, 'y')", base + 1));
        c.ok(&format!("INSERT INTO ser (tenant_id, body) VALUES ({}, 'y')", base + 2));
    }
    let stored = stored_rows(&server.db, "ser");
    assert_eq!(stored.len(), 10);
    let a: Vec<i32> = {
        let mut v: Vec<i32> = stored.iter().map(|r| i32_of(r, "a")).collect();
        v.sort();
        v
    };
    let b: Vec<i64> = {
        let mut v: Vec<i64> = stored.iter().map(|r| i64_of(r, "b")).collect();
        v.sort();
        v
    };
    assert_eq!(a, (1..=10).collect::<Vec<i32>>(), "SERIAL repeated or skipped values across restarts");
    assert_eq!(b, (1..=10).collect::<Vec<i64>>(), "BIGSERIAL repeated or skipped values across restarts");
    drop(server);
}

/// A duplicate key is refused within the partition, and the same id under
/// another tenant is not a duplicate. Eight sessions inserting one key at
/// once, into a partition none has written yet, store it once: the
/// materialization race and the unique check are both exact (I10, partial).
#[test]
fn duplicates_are_refused_and_racing_duplicates_store_one_row() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok(&format!("CREATE TABLE dup {ACCT} PARTITION BY HASH (tenant_id) PARTITIONS 8"));
    c.ok("INSERT INTO dup (tenant_id, id, label) VALUES (1, 1, 'a')");
    expect_code(c.simple("INSERT INTO dup (tenant_id, id, label) VALUES (1, 1, 'again')"), "23505");
    c.ok("INSERT INTO dup (tenant_id, id, label) VALUES (2, 1, 'b')");
    // One statement duplicating itself stores neither row.
    expect_code(c.simple("INSERT INTO dup (tenant_id, id, label) VALUES (3, 1, 'x'), (3, 1, 'y')"), "23505");
    let stored = stored_rows(&server.db, "dup");
    assert_eq!(stored.len(), 2, "{stored:?}");

    for round in 0..3i64 {
        let tenant = 100 + round;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|i| {
                let mut client = server.connect();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    let out = client.simple(&format!(
                        "INSERT INTO dup (tenant_id, id, label) VALUES ({tenant}, 7, 'racer {i}')"
                    ));
                    drop(client);
                    out
                })
            })
            .collect();
        let outcomes: Vec<Outcome> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let won = outcomes.iter().filter(|o| o.is_ok()).count();
        assert_eq!(won, 1, "round {round}: {outcomes:?}");
        for o in &outcomes {
            if let Err(e) = o {
                assert!(e.code == "23505" || e.code == "40001", "round {round}: {e}");
            }
        }
        let mine = stored_rows(&server.db, "dup").into_iter().filter(|r| i64_of(r, "tenant_id") == tenant).count();
        assert_eq!(mine, 1, "round {round}: the key is stored {mine} times");
    }
    assert_eq!(misplaced_rows(&server.db, "dup"), Vec::<String>::new());
    drop(c);
    drop(server);
}

/// `ON CONFLICT` stays refused (phase 2b, S18) with 0A000, with or without
/// `RETURNING`, in every statement shape, before anything is written or
/// materialized and before an `INSERT … SELECT` runs its source. (`RETURNING`
/// alone is built since S17: `partition_returning.rs`.)
#[test]
fn on_conflict_is_refused_before_writing() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok(&format!("CREATE TABLE refu {ACCT} PARTITION BY HASH (tenant_id) PARTITIONS 8"));
    c.ok("CREATE TABLE src (tenant_id BIGINT, id INT, label TEXT)");
    c.ok("INSERT INTO src (tenant_id, id, label) VALUES (1, 1, 's'), (2, 2, 's')");
    let cols = "(tenant_id, id, label)";
    for sql in [
        format!("INSERT INTO refu {cols} VALUES (1, 1, 'a') ON CONFLICT DO NOTHING"),
        format!("INSERT INTO refu {cols} VALUES (1, 1, 'a') ON CONFLICT (tenant_id, id) DO NOTHING"),
        format!("INSERT INTO refu {cols} VALUES (1, 1, 'a') ON CONFLICT (tenant_id, id) DO UPDATE SET label = 'u'"),
        format!("INSERT INTO refu {cols} VALUES (1, 1, 'a') ON CONFLICT DO NOTHING RETURNING id"),
        format!("INSERT INTO refu {cols} VALUES (1, 1, 'a') ON CONFLICT (tenant_id, id) DO UPDATE SET label = 'u' RETURNING *"),
        format!("INSERT INTO refu {cols} VALUES (1, 1, 'a'), (2, 2, 'b') ON CONFLICT DO NOTHING"),
        format!("INSERT INTO refu {cols} VALUES (1, 1, 'a'), (2, 2, 'b') ON CONFLICT DO NOTHING RETURNING tenant_id, id"),
        format!("INSERT INTO refu {cols} SELECT tenant_id, id, label FROM src ON CONFLICT DO NOTHING"),
        format!("INSERT INTO refu {cols} SELECT tenant_id, id, label FROM src ON CONFLICT DO NOTHING RETURNING id"),
    ] {
        let e = expect_code(c.simple(&sql), "0A000");
        assert!(e.is_unsupported(), "{sql}: {e}");
        assert!(stored_rows(&server.db, "refu").is_empty(), "{sql} wrote a row");
        assert_eq!(materialized(&mut c, "refu"), 0, "{sql} materialized a partition");
    }
    // Prepared, and inside a block: the block then fails like any statement.
    let out = match c.prepare(
        "p1",
        "INSERT INTO refu (tenant_id, id, label) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING id",
    ) {
        Ok(_) => c.execute_prepared("p1", &[Some("5".into()), Some("5".into()), Some("p".into())]),
        Err(e) => Err(e),
    };
    let e = expect_code(out, "0A000");
    assert!(e.is_unsupported(), "{e}");
    assert!(stored_rows(&server.db, "refu").is_empty());
    c.ok("BEGIN");
    c.ok("INSERT INTO refu (tenant_id, id, label) VALUES (9, 9, 'in block')");
    expect_code(c.simple("INSERT INTO refu (tenant_id, id, label) VALUES (9, 10, 'b') ON CONFLICT DO NOTHING"), "0A000");
    c.ok("ROLLBACK");
    assert!(stored_rows(&server.db, "refu").is_empty());
    drop(c);
    drop(server);
}

/// A row refused by a CHECK, a NULL or missing key, or a bad key value is
/// refused before routing: it materializes nothing, so `SHOW PARTITIONS` is
/// unchanged. A NULL key is refused (§1.5): it would hash like `0`.
#[test]
fn refused_rows_materialize_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok(
        "CREATE TABLE chk (tenant_id BIGINT NOT NULL, n INT CHECK (n > 0), label TEXT) \
         PARTITION BY HASH (tenant_id) PARTITIONS 8",
    );
    expect_code(c.simple("INSERT INTO chk (tenant_id, n) VALUES (5, -1)"), "23514");
    expect_code(c.simple("INSERT INTO chk (tenant_id, n) VALUES (NULL, 1)"), "23502");
    expect_code(c.simple("INSERT INTO chk (n, label) VALUES (1, 'no key')"), "23502");
    assert!(c.simple("INSERT INTO chk (tenant_id, n) VALUES ('abc', 1)").is_err());
    assert_eq!(materialized(&mut c, "chk"), 0, "a refused row materialized a partition");
    assert!(stored_rows(&server.db, "chk").is_empty());
    // A statement whose later row is refused stores none of its rows; the
    // earlier row's partition may stay materialized, empty (materializing
    // is not transactional, and an empty partition holds no row to find).
    expect_code(c.simple("INSERT INTO chk (tenant_id, n) VALUES (6, 1), (7, -2)"), "23514");
    assert!(stored_rows(&server.db, "chk").is_empty(), "a failed statement kept its first row");

    let before = materialized(&mut c, "chk");
    assert!(before <= 1, "{before}");
    c.ok("INSERT INTO chk (tenant_id, n) VALUES (5, 1)");
    let after = materialized(&mut c, "chk");
    assert!(after >= 1 && after <= before + 1, "{before} then {after}");
    assert_eq!(stored_rows(&server.db, "chk").len(), 1);
    drop(c);
    drop(server);
}

/// One transaction id across partitions: a block writing several partitions
/// is invisible until COMMIT, gone whole on ROLLBACK, and gone whole when a
/// later statement in it fails, then durable across a restart.
#[test]
fn a_transaction_across_partitions_commits_or_rolls_back_whole() {
    let dir = tempfile::tempdir().unwrap();
    let mut server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok(&format!("CREATE TABLE tx {ACCT} PARTITION BY HASH (tenant_id) PARTITIONS 16"));
    let insert = |c: &mut Client, t: i64| {
        c.ok(&format!("INSERT INTO tx (tenant_id, id, label) VALUES ({t}, 1, 't{t}')"));
    };

    c.ok("BEGIN");
    for t in 1..=8 {
        insert(&mut c, t);
    }
    assert!(stored_rows(&server.db, "tx").is_empty(), "uncommitted rows are visible");
    c.ok("ROLLBACK");
    assert!(stored_rows(&server.db, "tx").is_empty(), "a rolled-back block left rows");

    c.ok("BEGIN");
    for t in 1..=8 {
        insert(&mut c, t);
    }
    assert!(stored_rows(&server.db, "tx").is_empty(), "uncommitted rows are visible");
    c.ok("COMMIT");
    let stored = stored_rows(&server.db, "tx");
    assert_eq!(stored.len(), 8);
    let pids: BTreeSet<u32> = stored_rows_by_partition(&server.db, "tx")
        .into_iter()
        .filter(|(_, rows)| !rows.is_empty())
        .map(|(pid, _)| pid)
        .collect();
    assert!(pids.len() > 1, "the block reached one partition only: {pids:?}");

    c.ok("BEGIN");
    insert(&mut c, 20);
    insert(&mut c, 21);
    expect_code(c.simple("INSERT INTO tx (tenant_id, id, label) VALUES (1, 1, 'dup')"), "23505");
    // An aborted block ends as a rollback whatever COMMIT answers.
    let _ = c.simple("COMMIT");
    assert_eq!(stored_rows(&server.db, "tx"), stored, "a failed block committed part of itself");

    drop(c);
    server = server.restart();
    assert_eq!(stored_rows(&server.db, "tx"), stored, "a restart changed the committed rows");
    drop(server);
}

/// I2: insert and query agree on a value's point for every spelling the
/// evaluator calls equal. Each value is inserted under every spelling (a
/// bare literal, a quoted one, two casts, a `$1` in text and in binary), into
/// an INT-keyed and a BIGINT-keyed table: every copy must sit in the one
/// partition the key's point names, the same in both tables (INT = BIGINT),
/// and every query spelling must route there and nowhere else.
#[test]
fn insert_and_query_agree_on_every_spelling() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path().to_str().unwrap());
    let mut c = server.connect();
    c.ok("CREATE TABLE i2b (k BIGINT NOT NULL, label TEXT) PARTITION BY HASH (k) PARTITIONS 64");
    c.ok("CREATE TABLE i2i (k INT NOT NULL, label TEXT) PARTITION BY HASH (k) PARTITIONS 64");
    let values: [i64; 8] = [0, 1, -1, 7, 42, 1000, i32::MAX as i64, i32::MIN as i64 + 1];
    // (table, OID of `$1` in binary, its bytes)
    let tables: [(&str, u32, fn(i64) -> Vec<u8>); 2] =
        [("i2b", 20, |v| v.to_be_bytes().to_vec()), ("i2i", 23, |v| (v as i32).to_be_bytes().to_vec())];
    let literal_spellings: [fn(i64) -> String; 4] =
        [|v| format!("{v}"), |v| format!("'{v}'"), |v| format!("{v}::bigint"), |v| format!("CAST({v} AS BIGINT)")];
    for (table, oid, binary) in tables {
        let text_name = format!("{table}_text");
        let bin_name = format!("{table}_bin");
        c.prepare(&text_name, &format!("INSERT INTO {table} (k, label) VALUES ($1, $2)")).unwrap();
        c.prepare_typed(&bin_name, &format!("INSERT INTO {table} (k, label) VALUES ($1, $2)"), &[oid, 25]).unwrap();
        for v in values {
            for (s, spell) in literal_spellings.iter().enumerate() {
                c.ok(&format!("INSERT INTO {table} (k, label) VALUES ({}, '{v}/{s}')", spell(v)));
            }
            c.execute_prepared(&text_name, &[Some(v.to_string()), Some(format!("{v}/text"))])
                .unwrap_or_else(|e| panic!("{table} $1 text {v}: {e}"));
            c.execute_binary(&bin_name, &[Some(binary(v)), Some(format!("{v}/binary").into_bytes())])
                .unwrap_or_else(|e| panic!("{table} $1 binary {v}: {e}"));
        }
    }
    let db = server.db.clone();
    let mut home: BTreeMap<(&str, i64), u32> = BTreeMap::new();
    for (table, _, _) in tables {
        let pt = db.partitioned.get(table).unwrap().value().clone();
        let epoch = pt.epoch().unwrap();
        let mut seen: BTreeMap<i64, BTreeSet<u32>> = BTreeMap::new();
        let mut copies: BTreeMap<i64, usize> = BTreeMap::new();
        for (pid, rows) in stored_rows_by_partition(&db, table) {
            for row in rows {
                let label = text_of(&row, "label");
                let v: i64 = label.split('/').next().unwrap().parse().unwrap();
                seen.entry(v).or_default().insert(pid);
                *copies.entry(v).or_default() += 1;
            }
        }
        for v in values {
            assert_eq!(copies.get(&v), Some(&6), "{table}: copies of {v}");
            let key = if table == "i2b" { v.to_le_bytes().to_vec() } else { (v as i32).to_le_bytes().to_vec() };
            let want = pid_of_key(&db, table, &[key.as_slice()]);
            assert_eq!(seen[&v], BTreeSet::from([want]), "{table}: the spellings of {v} landed apart");
            home.insert((table, v), want);
            for q in [
                format!("k = {v}"),
                format!("k = '{v}'"),
                format!("k = {v}::bigint"),
                format!("k = CAST({v} AS BIGINT)"),
                format!("{v} = k"),
                format!("k IN ({v})"),
                format!("k IN ('{v}')"),
            ] {
                // Agreement: the route reaches the rows. A narrowed route is
                // exactly their point's partition; only the plainest spellings
                // must narrow, the others may route everywhere (a superset).
                let route = pt.route_where(&q).unwrap();
                let pids = route.pids(&epoch.map);
                assert!(pids.contains(&want), "{table}: `{q}` routes to {pids:?}, away from its rows in {want}");
                let must_narrow = q == format!("k = {v}") || q == format!("k = '{v}'");
                if must_narrow || !matches!(route, Route::All) {
                    assert_eq!(pids, vec![want], "{table}: `{q}` is not narrowed to its point's partition");
                }
            }
        }
    }
    for v in values {
        assert_eq!(home[&("i2b", v)], home[&("i2i", v)], "INT {v} and BIGINT {v} have different points");
    }
    drop(c);
    drop(db);
    drop(server);
}
