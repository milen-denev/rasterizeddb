//! A disabled filter must be observationally identical.
//!
//! This is the differential the whole design rests on: the filter decides what
//! not to *fetch*, never what to *return*. A separate test binary is what makes
//! it testable — the switch is process-wide, so it has to be set before any
//! table in this process exists.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row::row::{ColumnWritePayload, RowWrite};
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

fn disable() {
    // Must win the race with every other test in this binary, hence `ctor`-like
    // placement at the top of each test rather than a shared harness.
    rasterizeddb_core::filter_settings().set_enabled(false);
    init();
}

const N: u64 = STD_N;

#[tokio::test(flavor = "multi_thread")]
async fn no_filters_are_built_at_all() {
    disable();
    let (_d, t) = std_table("dis_none").await;
    assert!(
        t.filters().unwrap().is_none(),
        "FILTER_ENABLED = false must build no filter catalog"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_predicate_shape_returns_the_same_rows() {
    disable();
    let (_d, t) = std_table("dis_shapes").await;

    // Exactly the expectations the enabled suite asserts.
    for (clause, expected) in [
        ("id = 1234", vec![1234u64]),
        ("id = 0", vec![0]),
        ("id = 1999", vec![1999]),
        ("id = 99999", vec![]),
        ("label = 'l777'", vec![777]),
        ("label = 'nope'", vec![]),
        ("id = 5 OR id = 9", vec![5, 9]),
        ("id = 7 OR id = 1234 AND cat = 18", vec![7, 1234]),
        ("id = 7 OR id = 1234 AND cat = 19", vec![7]),
        ("(id = 4 OR id = 68) AND cat = 4", vec![4, 68]),
        ("id = 1234 AND cat = 18", vec![1234]),
        ("id = 1234 AND cat = 19", vec![]),
    ] {
        assert_eq!(u64s(&run(&t, clause).await, 0), expected, "`{}`", clause);
    }

    assert_eq!(u64s(&run(&t, "id > 1996").await, 0), vec![1997, 1998, 1999]);
    assert_eq!(u64s(&run(&t, "id < 3").await, 0), vec![0, 1, 2]);
    assert_eq!(run(&t, "").await.len() as u64, N);
    assert_eq!(run(&t, "id != 5").await.len() as u64, N - 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn desugared_forms_return_the_same_rows() {
    disable();
    let (_d, t) = std_table("dis_desugar").await;

    assert_eq!(u64s(&run_rewritten(&t, "id IN (5, 9, 1999)").await, 0), vec![5, 9, 1999]);
    assert_eq!(
        u64s(&run_rewritten(&t, "id BETWEEN 10 AND 20").await, 0),
        (10..=20).collect::<Vec<_>>()
    );
    assert_eq!(
        u64s(&run_rewritten(&t, "id NOT BETWEEN 10 AND 20").await, 0),
        (0..N).filter(|i| !(10..=20).contains(i)).collect::<Vec<_>>()
    );
    assert_eq!(
        u64s(&run_rewritten(&t, "cat = 4 AND id IN (4, 68, 5)").await, 0),
        vec![4, 68]
    );
    assert_eq!(
        u64s(&run_rewritten(&t, "id IN (4, 68, 5) AND cat = 4").await, 0),
        vec![4, 68],
        "operand order must not change the answer"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mutations_behave_the_same_without_filters() {
    disable();
    let (_d, t) = std_table("dis_mutate").await;

    assert_eq!(
        t.delete_rows("id = 1234", full_fetch(&t.schema.fields), None).await.unwrap(),
        1
    );
    assert!(run(&t, "id = 1234").await.is_empty());
    assert_eq!(run(&t, "").await.len() as u64, N - 1);

    let updates: SmallVec<[ColumnWritePayload; 32]> =
        smallvec::smallvec![payload(&424242u64.to_le_bytes(), 0, DbType::U64)];
    assert_eq!(
        t.update_rows("id = 900", full_fetch(&t.schema.fields), updates, None)
            .await
            .unwrap(),
        1
    );
    assert!(run(&t, "id = 900").await.is_empty());
    assert_eq!(run(&t, "id = 424242").await.len(), 1);

    for i in N..N + 50 {
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(&((i % 64) as i32).to_le_bytes(), 1, DbType::I32));
        rw.columns_writing_data
            .push(payload(format!("l{}", i).as_bytes(), 2, DbType::STRING));
        rw.columns_writing_data
            .push(payload(&(i as f64 / 4.0).to_le_bytes(), 3, DbType::F64));
        t.insert_row(rw).await.unwrap();
    }
    assert_eq!(u64s(&run(&t, &format!("id = {}", N + 10)).await, 0), vec![N + 10]);
}

#[tokio::test(flavor = "multi_thread")]
async fn no_sidecar_is_written_when_disabled() {
    disable();
    let dir = tempfile::tempdir().unwrap();
    let t = open_table(dir.path(), "dis_sidecar", &std_columns()).await;
    fill_std(&t).await;
    t.checkpoint_filters().await.unwrap();

    let path = dir.path().join("dis_sidecar_flt.db");
    let len = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(len, 0, "a disabled filter must not write a checkpoint");
}

#[tokio::test(flavor = "multi_thread")]
async fn reopening_without_filters_is_correct() {
    disable();
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "dis_reopen", &std_columns()).await;
        fill_std(&t).await;
    }
    let t = open_table(dir.path(), "dis_reopen", &std_columns()).await;
    t.wait_filters_ready().await.unwrap();
    assert!(t.filters().unwrap().is_none());
    for target in [0u64, 1234, N - 1] {
        assert_eq!(u64s(&run(&t, &format!("id = {}", target)).await, 0), vec![target]);
    }
    assert_eq!(run(&t, "").await.len() as u64, N);
}

#[tokio::test(flavor = "multi_thread")]
async fn random_queries_match_brute_force_without_filters() {
    disable();
    let (_d, t) = std_table("dis_random").await;

    for _ in 0..40 {
        let (clause, want): (String, Vec<u64>) = match fastrand::usize(0..4) {
            0 => {
                let v = fastrand::u64(0..N + 50);
                (format!("id = {}", v), (0..N).filter(|i| *i == v).collect())
            }
            1 => {
                let v = fastrand::i32(0..70);
                (format!("cat = {}", v), (0..N).filter(|i| (i % 64) as i32 == v).collect())
            }
            2 => {
                let v = fastrand::u64(0..N);
                (format!("id > {}", v), (0..N).filter(|i| *i > v).collect())
            }
            _ => {
                let (a, b) = (fastrand::u64(0..N), fastrand::u64(0..N));
                (
                    format!("id = {} OR id = {}", a, b),
                    (0..N).filter(|i| *i == a || *i == b).collect(),
                )
            }
        };
        assert_eq!(u64s(&run(&t, &clause).await, 0), want, "`{}`", clause);
    }
}


/// Clauses whose answers must be identical with and without filtering.
///
/// The expected values are what the **evaluator** currently produces, which for
/// the mixed AND/OR shapes is not the SQL-correct answer: the engine flattens
/// boolean structure into a `(bool, Option<Next>)` chain and drops parentheses,
/// so `A AND (B OR C)` is evaluated as `(A AND B) OR C`.
///
/// This suite does not assert that behaviour is *right* — it asserts the filter
/// does not *change* it. `sarg::extract` declines exactly these shapes for that
/// reason. When the evaluator learns to honour parentheses, the values below
/// and the mirror in `end_to_end.rs` should both be updated to the SQL-correct
/// answers, together.
pub const PAREN_SHAPES: &[(&str, &[u64])] = &[
    // Three-element OR groups in both operand orders. Two-element groups pass
    // under flattening by coincidence, which is how the original bug survived
    // its first review — so every group here has at least three members or a
    // sibling group.
    ("cat = 4 AND (id = 4 OR id = 68 OR id = 5)", &[4, 68]),
    ("(id = 4 OR id = 68 OR id = 5) AND cat = 4", &[4, 68]),
    ("cat = 4 AND (id = 5 OR id = 6 OR id = 7 OR id = 68)", &[68]),
    ("cat = 4 AND (id = 4 OR id = 5)", &[4]),
    ("cat = 4 AND (id = 5 OR id = 4)", &[4]),
    ("(id = 5 OR id = 4) AND cat = 4", &[4]),
    // Sibling groups: depth alone cannot tell these apart from one flat run.
    ("(id = 1 OR id = 2) AND (cat = 1 OR cat = 2)", &[1, 2]),
    ("(cat = 4 AND id = 4) OR (cat = 5 AND id = 5)", &[4, 5]),
    ("cat = 4 AND ((id = 4 OR id = 5) OR id = 68)", &[4, 68]),
    // No parens: the flat walk, which was always correct.
    ("cat = 4 AND id = 4 OR id = 68 OR id = 5", &[4, 5, 68]),
    // Whole-clause and AND-only groups.
    ("(id = 1 OR id = 2)", &[1, 2]),
    ("(id >= 10 AND id <= 12)", &[10, 11, 12]),
    ("cat = 4 AND (id >= 4 AND id <= 68)", &[4, 68]),
];

#[tokio::test(flavor = "multi_thread")]
async fn paren_shapes_without_filters() {
    disable();
    let (_d, t) = std_table("dis_paren").await;
    for (q, want) in PAREN_SHAPES {
        assert_eq!(u64s(&run(&t, q).await, 0), want.to_vec(), "`{}`", q);
    }
}
