//! A point key answers as the text it stands for (`point_key`): over a
//! unique index, the candidates `Table::candidates_for_key` gives for a
//! full-key equality are exactly the ones `build_candidates` gives for the
//! clause `col = literal AND …`, for keys present and missing, after rows
//! move, and across an index dropped and made again; and every key it cannot
//! answer that way is refused rather than answered differently.

use std::path::Path;
use std::sync::Arc as StdArc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rclite::Arc;

use crate::core::database::Database;
use crate::core::db_type::DbType;
use crate::core::filter::CandidateBitmap;
use crate::core::row::point_key::{self, PointKey};
use crate::core::row::table::Table;
use crate::core::storage_providers::backend::DbStorageProvider;

type DbTable = Table<DbStorageProvider>;

async fn open(dir: &Path, name: &str) -> Arc<Database> {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    Arc::new(Database::try_new_named(&dir.to_string_lossy(), false, name).await.expect("open the database"))
}

fn close(dir: &Path, db: Arc<Database>) {
    drop(db);
    crate::core::wal::remove_for(&dir.to_string_lossy());
}

async fn exec(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql).unwrap_or_else(|| panic!("parse {sql}"));
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    assert!(!out.is_empty() && out[0] != 3, "{sql}: {}", String::from_utf8_lossy(&out));
    out
}

fn table(db: &Database, name: &str) -> Arc<DbTable> {
    db.table_for_tests(name).expect("the table")
}

/// The point indexes once the build that follows a change has finished.
async fn point_indexes_when_ready(t: &DbTable, want: usize) -> Vec<Vec<(u32, DbType, StdArc<str>)>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let found = t.point_indexes().unwrap();
        if found.len() >= want {
            return found;
        }
        assert!(std::time::Instant::now() < deadline, "the indexes were never built: {found:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A slot set and the length it was sampled at, comparable whichever form
/// the bitmap took.
fn slots(b: &CandidateBitmap) -> (Vec<u64>, u64) {
    (b.slots(), b.n_slots())
}

/// A key column's stored bytes for an integer value, at its type.
fn int_bytes(t: &DbType, v: i64) -> Vec<u8> {
    match t {
        DbType::I8 => (v as i8).to_le_bytes().to_vec(),
        DbType::I16 => (v as i16).to_le_bytes().to_vec(),
        DbType::I32 => (v as i32).to_le_bytes().to_vec(),
        DbType::I64 => v.to_le_bytes().to_vec(),
        DbType::U8 => (v as u8).to_le_bytes().to_vec(),
        DbType::U16 => (v as u16).to_le_bytes().to_vec(),
        DbType::U32 => (v as u32).to_le_bytes().to_vec(),
        DbType::U64 => (v as u64).to_le_bytes().to_vec(),
        other => panic!("not an integer key column: {other:?}"),
    }
}

/// The key's candidates and the text's, which must be the same set.
async fn both(t: &DbTable, index: &[(u32, DbType, StdArc<str>)], values: &[Vec<u8>], clause: &str) -> (Vec<u64>, u64) {
    let levels: Vec<(u32, DbType, &[u8])> =
        index.iter().zip(values).map(|((wo, ty, _), v)| (*wo, ty.clone(), v.as_slice())).collect();
    let names: Vec<&str> = index.iter().map(|(_, _, n)| &**n).collect();
    let key = t.candidates_for_key(&levels, &names).await.unwrap().unwrap_or_else(|| panic!("{clause}: the key was refused"));
    let text = t.build_candidates(clause, None).await.unwrap().unwrap_or_else(|| panic!("{clause}: the text found no index"));
    assert_eq!(slots(&key), slots(&text), "{clause}: the key and the text disagree");
    slots(&key)
}

const COMPOSITE: &str = "CREATE TABLE pk4 (tenant_id BIGINT, year INT, month INT, event_id BIGINT, v TEXT, \
                         PRIMARY KEY (tenant_id, year, month, event_id))";

fn composite_key(i: i64) -> [i64; 4] {
    [100_000 + i % 37, 2024 + i % 3, 1 + i % 12, i]
}

/// The user's shape: a four-column primary key. Every present key, a sample
/// of missing ones, then again after `UPDATE`s that write new versions of
/// some rows (their old versions still indexed until vacuumed).
///
/// Mutation: build the predicate at the engine's default width rather than
/// the column's (`int_bytes` as `I64` for an `INT`): the key finds nothing
/// where the text finds the row.
#[tokio::test(flavor = "multi_thread")]
async fn a_composite_key_answers_as_its_clause() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "pk_comp").await;
    exec(&db, COMPOSITE).await;
    let n = 400i64;
    let values: Vec<String> = (0..n)
        .map(|i| {
            let [t, y, m, e] = composite_key(i);
            format!("({t}, {y}, {m}, {e}, 'v{i}')")
        })
        .collect();
    exec(&db, &format!("INSERT INTO pk4 (tenant_id, year, month, event_id, v) VALUES {}", values.join(", "))).await;
    let t = table(&db, "pk4");
    let indexes = point_indexes_when_ready(&t, 1).await;
    let index = indexes.iter().find(|i| i.len() == 4).expect("the primary key is a point index");
    let names: Vec<&str> = index.iter().map(|(_, _, n)| &**n).collect();
    assert_eq!(names, ["tenant_id", "year", "month", "event_id"], "key order");

    let check = |i: i64, present: bool| {
        let t = &t;
        async move {
            let [tn, y, m, e] = composite_key(i);
            let vals: Vec<Vec<u8>> = [tn, y, m, e].iter().zip(index).map(|(v, (_, ty, _))| int_bytes(ty, *v)).collect();
            let clause = format!("tenant_id = {tn} AND year = {y} AND month = {m} AND event_id = {e}");
            let (found, _) = both(t, index, &vals, &clause).await;
            if present {
                assert!(!found.is_empty(), "{clause}: a present key found no slot");
            }
        }
    };
    for i in 0..n {
        check(i, true).await;
    }
    for i in [n, n + 1, 10_000, 1 << 40] {
        check(i, false).await;
    }
    // A key whose columns each exist in other rows, but not together.
    let [tn, _, m, e] = composite_key(5);
    let [_, y, _, _] = composite_key(6);
    let vals = vec![int_bytes(&index[0].1, tn), int_bytes(&index[1].1, y), int_bytes(&index[2].1, m), int_bytes(&index[3].1, e)];
    both(&t, index, &vals, &format!("tenant_id = {tn} AND year = {y} AND month = {m} AND event_id = {e}")).await;

    exec(&db, "UPDATE pk4 SET v = 'moved' WHERE event_id < 50").await;
    exec(&db, "DELETE FROM pk4 WHERE event_id >= 380").await;
    for i in (0..n).step_by(7) {
        check(i, i < 380).await;
    }
    drop(t);
    close(dir.path(), db);
}

/// A unique text key, and the same across an index dropped and made again:
/// while it is gone the key is refused (no index keys the column), and once
/// rebuilt it answers as the text again.
#[tokio::test(flavor = "multi_thread")]
async fn a_text_key_answers_as_its_clause_across_a_rebuilt_index() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "pk_text").await;
    exec(&db, "CREATE TABLE codes (id BIGINT PRIMARY KEY, code TEXT, note TEXT)").await;
    exec(&db, "ALTER TABLE codes ADD CONSTRAINT codes_code UNIQUE (code)").await;
    let values: Vec<String> = (0..300).map(|i| format!("({i}, 'c-{i:04}', 'n{i}')")).collect();
    exec(&db, &format!("INSERT INTO codes (id, code, note) VALUES {}", values.join(", "))).await;
    let t = table(&db, "codes");
    let indexes = point_indexes_when_ready(&t, 2).await;
    let by_code = indexes.iter().find(|i| i.len() == 1 && &*i[0].2 == "code").expect("the code index").clone();
    assert_eq!(by_code[0].1, DbType::STRING);
    let check = |code: String| {
        let (t, by_code) = (&t, &by_code);
        async move { both(t, by_code, &[code.clone().into_bytes()], &format!("code = '{code}'")).await }
    };
    for i in (0..300).step_by(11) {
        assert_eq!(check(format!("c-{i:04}")).await.0.len(), 1);
    }
    for missing in ["c-9999", "", "C-0001", "c-0001 "] {
        assert!(check(missing.to_string()).await.0.is_empty(), "{missing:?}");
    }

    exec(&db, "ALTER TABLE codes DROP CONSTRAINT codes_code").await;
    // The constraint is gone; its index may stay until it is rebuilt. While
    // it does, the key answers as the text does (both ask it); once it is
    // gone the key is refused. Never anything else, and a duplicate the
    // constraint no longer stops is found by both.
    let wo = by_code[0].0;
    exec(&db, "INSERT INTO codes (id, code, note) VALUES (1000, 'c-0001', 'twin')").await;
    let agree = |code: &'static str| {
        let t = &t;
        async move {
            let key = t.candidates_for_key(&[(wo, DbType::STRING, code.as_bytes())], &["code"]).await.unwrap();
            if let Some(key) = key {
                let text = t.build_candidates(&format!("code = '{code}'"), None).await.unwrap();
                let text = text.unwrap_or_else(|| panic!("{code}: the key answered where the text read no index"));
                assert_eq!(slots(&key), slots(&text), "{code}: after the drop, the key and the text disagree");
            }
        }
    };
    agree("c-0001").await;
    agree("c-0002").await;
    // The slot list does not hold rows appended since its length was
    // sampled (they are candidates past it), so whether the twin is found is
    // the statement's answer, not a count: in the key's scope as without it.
    let sql = "SELECT id FROM codes WHERE code = 'c-0001' ORDER BY id";
    let without = exec(&db, sql).await;
    let key = PointKey {
        table: "codes".into(),
        where_clause: "code = 'c-0001'".into(),
        levels: smallvec::smallvec![(wo, DbType::STRING, smallvec::SmallVec::from_slice(b"c-0001"))],
        names: smallvec::smallvec!["code".into()],
    };
    let with = point_key::scope(key, exec(&db, sql)).await;
    assert_eq!(with, without, "in scope the twin is lost");
    let rows = crate::core::row::row::serialized_rows(&without[1..]).count();
    assert_eq!(rows, 2, "the twin is not in the answer");
    exec(&db, "DELETE FROM codes WHERE id = 1000").await;

    exec(&db, "ALTER TABLE codes ADD CONSTRAINT codes_code UNIQUE (code)").await;
    let indexes = point_indexes_when_ready(&t, 2).await;
    let rebuilt = indexes.iter().find(|i| i.len() == 1 && &*i[0].2 == "code").expect("the rebuilt index").clone();
    assert_eq!(both(&t, &rebuilt, &[b"c-0042".to_vec()], "code = 'c-0042'").await.0.len(), 1);
    drop(t);
    close(dir.path(), db);
}

/// What a key cannot stand for is refused, never answered: a name, a type or
/// a write order that is not the live column's, a type off the allowlist,
/// and a non-unique index, which is never a point index at all.
#[tokio::test(flavor = "multi_thread")]
async fn a_key_it_cannot_stand_for_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "pk_refuse").await;
    exec(&db, "CREATE TABLE r (id BIGINT PRIMARY KEY, price NUMERIC(10, 2), tag TEXT, f DOUBLE PRECISION)").await;
    exec(&db, "ALTER TABLE r ADD CONSTRAINT r_price UNIQUE (price)").await;
    exec(&db, "ALTER TABLE r ADD CONSTRAINT r_f UNIQUE (f)").await;
    exec(&db, "CREATE INDEX r_tag ON r (tag)").await;
    exec(&db, "INSERT INTO r (id, price, tag, f) VALUES (1, 1.50, 'a', 1.5), (2, 2.50, 'b', 2.5)").await;
    let t = table(&db, "r");
    let indexes = point_indexes_when_ready(&t, 1).await;
    let listed: Vec<Vec<&str>> = indexes.iter().map(|i| i.iter().map(|(_, _, n)| &**n).collect()).collect();
    assert_eq!(listed, vec![vec!["id"]], "only the integer primary key is a point index");
    let id = &indexes[0][0];
    let one = 1i64.to_le_bytes();
    // The live column, answered.
    assert!(t.candidates_for_key(&[(id.0, id.1.clone(), &one)], &["id"]).await.unwrap().is_some());
    // Another name, another type, a write order no column has.
    assert!(t.candidates_for_key(&[(id.0, id.1.clone(), &one)], &["ID"]).await.unwrap().is_none());
    assert!(t.candidates_for_key(&[(id.0, DbType::I32, &one[..4])], &["id"]).await.unwrap().is_none());
    // The same width under another type: the bytes would be read as the
    // column's own, so only the type check stands between the two.
    assert!(t.candidates_for_key(&[(id.0, DbType::U64, &one)], &["id"]).await.unwrap().is_none());
    assert!(t.candidates_for_key(&[(999, id.1.clone(), &one)], &["id"]).await.unwrap().is_none());
    // Types off the allowlist, though a unique index keys them.
    let field = |name: &str| t.schema.fields.iter().find(|f| f.name == name).unwrap().clone();
    let (price, f, tag) = (field("price"), field("f"), field("tag"));
    let p = 150i128.to_le_bytes();
    assert!(t.candidates_for_key(&[(price.write_order as u32, price.db_type.clone(), &p)], &["price"]).await.unwrap().is_none());
    let fb = 1.5f64.to_le_bytes();
    assert!(t.candidates_for_key(&[(f.write_order as u32, f.db_type.clone(), &fb)], &["f"]).await.unwrap().is_none());
    // A column only a non-unique index keys: allowed as a type, and answered
    // as its text would be (a superset), but never listed as a point.
    assert!(indexes.iter().all(|i| i.iter().all(|(_, _, n)| &**n != "tag")));
    // Mismatched lengths and an empty key.
    assert!(t.candidates_for_key(&[(id.0, id.1.clone(), &one)], &[]).await.unwrap().is_none());
    assert!(t.candidates_for_key(&[], &[]).await.unwrap().is_none());
    let _ = tag;
    drop(t);
    close(dir.path(), db);
}

/// In scope, the scan of the key's own table under the key's own clause
/// takes its candidates from the key, and the statement answers as without
/// it; any other clause or table in scope is read from its text.
///
/// Mutation: take the key whatever the clause (drop the `where_clause`
/// comparison): the second statement, another clause, is answered from the
/// first one's key and loses its row.
#[tokio::test(flavor = "multi_thread")]
async fn in_scope_only_its_own_scan_takes_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "pk_scope").await;
    exec(&db, "CREATE TABLE s (id BIGINT PRIMARY KEY, v TEXT)").await;
    let values: Vec<String> = (0..200).map(|i| format!("({i}, 'v{i}')")).collect();
    exec(&db, &format!("INSERT INTO s (id, v) VALUES {}", values.join(", "))).await;
    let t = table(&db, "s");
    let indexes = point_indexes_when_ready(&t, 1).await;
    let id = indexes[0][0].clone();
    let key = |v: i64, clause: &str| PointKey {
        table: "s".into(),
        where_clause: clause.into(),
        levels: smallvec::smallvec![(id.0, id.1.clone(), smallvec::SmallVec::from_slice(&v.to_le_bytes()))],
        names: smallvec::smallvec![id.2.clone()],
    };

    // `build_candidates` itself, in scope.
    let hits = point_key::HITS.load(Ordering::Relaxed);
    let in_scope = point_key::scope(key(42, "id = 42"), async { t.build_candidates("id = 42", None).await }).await;
    assert!(point_key::HITS.load(Ordering::Relaxed) > hits, "the key was not taken");
    let plain = t.build_candidates("id = 42", None).await;
    assert_eq!(slots(&in_scope.unwrap().unwrap()), slots(&plain.unwrap().unwrap()));
    // Another clause in the same scope: its own text.
    let other = point_key::scope(key(42, "id = 42"), async { t.build_candidates("id = 43", None).await }).await;
    assert_eq!(slots(&other.unwrap().unwrap()), slots(&t.build_candidates("id = 43", None).await.unwrap().unwrap()));
    // Another table's name: its own text.
    let mut elsewhere = key(42, "id = 43");
    elsewhere.table = "not_s".into();
    let other = point_key::scope(elsewhere, async { t.build_candidates("id = 43", None).await }).await;
    assert_eq!(slots(&other.unwrap().unwrap()), slots(&t.build_candidates("id = 43", None).await.unwrap().unwrap()));

    // A whole statement in scope answers as it does without: whether the
    // scan was handed the clause as written is what `HITS` tells.
    let sql = "SELECT id, v FROM s WHERE id = 77";
    let without = exec(&db, sql).await;
    let hits = point_key::HITS.load(Ordering::Relaxed);
    let with = point_key::scope(key(77, "id = 77"), exec(&db, sql)).await;
    assert_eq!(with, without);
    eprintln!("a statement in scope took the key: {}", point_key::HITS.load(Ordering::Relaxed) > hits);
    drop(t);
    close(dir.path(), db);
}
