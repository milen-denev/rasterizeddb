//! Vacuum collects the slots whose fingerprints it may clear as `(first,
//! count)` runs and clears them after its outcome barrier. A run that spans a
//! gap clears the live rows in the gap, and the filter then prunes them from
//! every filtered query: silently missing rows. Each case compares filtered
//! queries with a whole-table scan of the same table.

use std::path::Path;

use rclite::Arc;

use crate::core::database::Database;

/// Over the filter's default `min_table_rows` (4096), so the filter is used.
const ROWS: i32 = 4400;
/// Deleted rows: a run of one, a gap of one, a run of three, a long gap, and a
/// run of one again.
const DELETED: [i32; 5] = [10, 12, 13, 14, 40];

fn path_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

fn label(id: i32) -> String {
    format!("L{id:05}")
}

async fn run(db: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = crate::core::rql::lexer_s1::recognize_query_purpose(sql)
        .unwrap_or_else(|| panic!("could not parse {sql}"));
    let out = crate::core::rql::executor::execute_with_sync(purpose, db.clone(), true).await;
    assert!(
        !out.is_empty() && out[0] != 3,
        "{sql} failed: {}",
        String::from_utf8_lossy(&out)
    );
    out
}

fn labels_in(out: &[u8]) -> Vec<String> {
    let mut found = Vec::new();
    let mut i = 0;
    while i + 6 <= out.len() {
        if out[i] == b'L'
            && out[i + 1..i + 6].iter().all(u8::is_ascii_digit)
            && out.get(i + 6).is_none_or(|b| !b.is_ascii_digit())
        {
            found.push(String::from_utf8_lossy(&out[i..i + 6]).into_owned());
            i += 6;
        } else {
            i += 1;
        }
    }
    found.sort();
    found
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_clears_exactly_the_reclaimed_slots_across_gaps() {
    let _ = crate::core::processor::concurrent_processor::ENABLE_CACHE.set(false);
    let base = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::new_named(&path_str(base.path()), false, "clearruns").await);
    run(&db, "CREATE TABLE vr (id INTEGER, k INTEGER, label TEXT)").await;
    let mut id = 0;
    while id < ROWS {
        let end = (id + 200).min(ROWS);
        let values: Vec<String> = (id..end)
            .map(|i| format!("({i}, {}, '{}')", i * 10, label(i)))
            .collect();
        run(&db, &format!("INSERT INTO vr (id, k, label) VALUES {}", values.join(", "))).await;
        id = end;
    }
    let t = db.tables.get("vr").unwrap().clone();
    t.wait_filters_ready().await.unwrap();
    let list: Vec<String> = DELETED.iter().map(i32::to_string).collect();
    run(&db, &format!("DELETE FROM vr WHERE id IN ({})", list.join(", "))).await;

    let report = t.vacuum_now().await.expect("vacuum failed");
    assert_eq!(
        report.fingerprints_cleared,
        DELETED.len() as u64,
        "vacuum cleared a different number of slots than it reclaimed"
    );
    drop(t);

    let all = labels_in(&run(&db, "SELECT label FROM vr").await);
    // Every surviving row between and around the deleted ones, plus a far one.
    let survivors = (9..=41).filter(|i| !DELETED.contains(i)).chain([3000]);
    for i in survivors {
        assert!(all.contains(&label(i)), "the whole-table scan lost row {i}");
        for predicate in [format!("k = {}", i * 10), format!("label = '{}'", label(i))] {
            let got = labels_in(&run(&db, &format!("SELECT label FROM vr WHERE {predicate}")).await);
            assert_eq!(got, vec![label(i)], "`WHERE {predicate}` disagrees with the whole-table scan");
        }
    }
    for &i in &DELETED {
        assert!(!all.contains(&label(i)), "deleted row {i} is still visible");
    }

    drop(db);
    crate::core::wal::remove_for(&path_str(base.path()));
}
