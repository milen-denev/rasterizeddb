//! Randomised property tests.
//!
//! The single property that matters: **the mask is always a superset of the
//! rows that actually match**. Everything else the filter does is an
//! optimisation; this is the part that can return a wrong answer.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::column::{ColumnFilter, bitmap_words};
use rasterizeddb_core::core::filter::hash::HashSeed;
use rasterizeddb_core::core::filter::sarg;
use rasterizeddb_core::core::filter::zonemap::SargOp;
use rasterizeddb_core::core::filter::{FilterConfig, TableFilters};
use rasterizeddb_core::core::index_store::gate::{Proof, test_mint};
use rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize;
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

#[derive(Clone)]
enum Expr {
    IdEq(u64),
    CatEq(i32),
    CatGt(i32),
    CatLe(i32),
    /// Deliberately unfilterable, so unknown branches are exercised.
    Opaque,
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}

fn build(depth: u32) -> Expr {
    if depth == 0 || fastrand::u32(0..100) < 35 {
        return match fastrand::usize(0..5) {
            0 => Expr::IdEq(fastrand::u64(0..1500)),
            1 => Expr::CatEq(fastrand::i32(0..40)),
            2 => Expr::CatGt(fastrand::i32(0..40)),
            3 => Expr::CatLe(fastrand::i32(0..40)),
            _ => Expr::Opaque,
        };
    }
    let (l, r) = (Box::new(build(depth - 1)), Box::new(build(depth - 1)));
    if fastrand::bool() { Expr::And(l, r) } else { Expr::Or(l, r) }
}

/// Render with explicit parens around every compound subexpression, so the text
/// means exactly what the tree means regardless of precedence.
fn render(e: &Expr, top: bool) -> String {
    match e {
        Expr::IdEq(v) => format!("id = {}", v),
        Expr::CatEq(v) => format!("cat = {}", v),
        Expr::CatGt(v) => format!("cat > {}", v),
        Expr::CatLe(v) => format!("cat <= {}", v),
        Expr::Opaque => "cat != 999".to_string(),
        Expr::And(l, r) | Expr::Or(l, r) => {
            let op = if matches!(e, Expr::And(..)) { "AND" } else { "OR" };
            let inner = format!("{} {} {}", render(l, false), op, render(r, false));
            if top { inner } else { format!("({})", inner) }
        }
    }
}

fn truth(e: &Expr, id: u64, cat: i32) -> bool {
    match e {
        Expr::IdEq(v) => id == *v,
        Expr::CatEq(v) => cat == *v,
        Expr::CatGt(v) => cat > *v,
        Expr::CatLe(v) => cat <= *v,
        Expr::Opaque => cat != 999,
        Expr::And(l, r) => truth(l, id, cat) && truth(r, id, cat),
        Expr::Or(l, r) => truth(l, id, cat) || truth(r, id, cat),
    }
}

#[test]
fn masks_are_supersets_of_truth_over_random_boolean_trees() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let fields = fields_of(&[("id", DbType::U64), ("cat", DbType::I32)]);
    let f = TableFilters::new(&fields, FilterConfig::default());

    let n = 4000u64;
    let ids: Vec<u64> = (0..n).map(|i| i * 3 % 1500).collect();
    let cats: Vec<i32> = (0..n).map(|i| (i % 40) as i32).collect();
    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..n as usize)
        .map(|i| {
            vec![
                (0u32, ids[i].to_le_bytes().to_vec()),
                (1u32, cats[i].to_le_bytes().to_vec()),
            ]
        })
        .collect();
    backfill_all(proof, &f, &rows);

    let mut filtered_any = false;
    for _ in 0..600 {
        let e = build(3);
        let text = render(&e, true);
        let Some(program) = sarg::extract(&tokenize(&text, &fields), &f) else { continue };
        let Some(bm) = f.evaluate(&program, n) else { continue };
        filtered_any = true;

        for slot in 0..n {
            if truth(&e, ids[slot as usize], cats[slot as usize]) {
                assert!(bm.get(slot), "slot {} pruned but matches `{}`", slot, text);
            }
        }
    }
    assert!(filtered_any, "no generated expression was filterable; the test proved nothing");
}

#[test]
fn masks_survive_a_random_mutation_workload() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    // Interleaved inserts, updates and deletes, then every live value must
    // still be findable and every deleted one must be gone from its slot.
    let fields = fields_of(&[("id", DbType::U64)]);
    let f = TableFilters::new(&fields, FilterConfig::default());

    let n = 6000usize;
    let mut live: Vec<Option<u64>> = vec![None; n];

    for slot in 0..n {
        let v = fastrand::u64(..);
        let sv: SmallVec<[(u32, Vec<u8>); 32]> =
            smallvec::smallvec![(0u32, v.to_le_bytes().to_vec())];
        f.backfill_row(proof, slot as u64, &sv);
        live[slot] = Some(v);
    }

    for _ in 0..4000 {
        let slot = fastrand::usize(0..n);
        match fastrand::usize(0..3) {
            0 => {
                f.on_delete(proof, slot as u64);
                live[slot] = None;
            }
            _ => {
                let v = fastrand::u64(..);
                let sv: SmallVec<[(u32, Vec<u8>); 32]> =
                    smallvec::smallvec![(0u32, v.to_le_bytes().to_vec())];
                f.backfill_row(proof, slot as u64, &sv);
                live[slot] = Some(v);
            }
        }
    }

    let total = n as u64;
    for (slot, v) in live.iter().enumerate() {
        let Some(v) = v else { continue };
        let q = format!("id = {}", v);
        let p = sarg::extract(&tokenize(&q, &fields), &f).expect("sargable");
        let bm = f.evaluate(&p, total).expect("bitmap");
        assert!(bm.get(slot as u64), "slot {} lost value {}", slot, v);
    }
}

#[test]
fn range_masks_are_supersets_for_every_ordered_type() {
    for ty in INDEXABLE {
        if *ty == DbType::STRING {
            continue;
        }
        let mut c = ColumnFilter::new(0, ty.clone(), HashSeed::default(), FilterConfig::default());
        let vals = sample_values(ty, 900);
        let n = vals.len() as u64;
        for (slot, v) in vals.iter().enumerate() {
            c.set(slot as u64, Some(v));
        }
        let mut bm = vec![0u64; bitmap_words(n)];

        for _ in 0..40 {
            let pivot = vals[fastrand::usize(0..vals.len())].clone();
            for (op, cmp) in [
                (SargOp::Gt, rasterizeddb_core::core::processor::transformer::ComparerOperation::Greater),
                (SargOp::Ge, rasterizeddb_core::core::processor::transformer::ComparerOperation::GreaterOrEquals),
                (SargOp::Lt, rasterizeddb_core::core::processor::transformer::ComparerOperation::Less),
                (SargOp::Le, rasterizeddb_core::core::processor::transformer::ComparerOperation::LessOrEquals),
                (SargOp::Eq, rasterizeddb_core::core::processor::transformer::ComparerOperation::Equals),
            ] {
                c.probe_pred_into(op, &pivot, n, &mut bm);
                for (slot, v) in vals.iter().enumerate() {
                    let truth =
                        rasterizeddb_core::core::row::logical::perform_comparison_operation(
                            v, &pivot, ty, &cmp,
                        )
                        .unwrap_or(true);
                    if truth {
                        let s = slot as u64;
                        assert!(
                            (bm[(s / 64) as usize] >> (s % 64)) & 1 == 1,
                            "{:?}: slot {} pruned for {:?}",
                            ty,
                            slot,
                            op
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn checkpoint_round_trips_under_random_workloads() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    for _ in 0..12 {
        let fields = fields_of(&[("id", DbType::U64), ("cat", DbType::I32)]);
        let f = TableFilters::new(&fields, FilterConfig::default());
        let n = fastrand::u64(600..5000);

        let mut live: Vec<Option<(u64, i32)>> = vec![None; n as usize];
        for slot in 0..n as usize {
            let (a, b) = (fastrand::u64(..), fastrand::i32(..));
            let sv: SmallVec<[(u32, Vec<u8>); 32]> = smallvec::smallvec![
                (0u32, a.to_le_bytes().to_vec()),
                (1u32, b.to_le_bytes().to_vec())
            ];
            f.backfill_row(proof, slot as u64, &sv);
            live[slot] = Some((a, b));
        }

        let bytes = f.checkpoint_bytes(n);
        let restored =
            TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n).unwrap();

        for (slot, v) in live.iter().enumerate() {
            let Some((a, _)) = v else { continue };
            let q = format!("id = {}", a);
            let p = sarg::extract(&tokenize(&q, &fields), &restored).expect("sargable");
            let bm = restored.evaluate(&p, n).expect("bitmap");
            assert!(bm.get(slot as u64), "slot {} lost across checkpoint", slot);
        }
    }
}

#[test]
fn arbitrary_byte_values_never_panic_a_probe() {
    // Values come from user queries; nothing about them should be able to take
    // the filter down.
    for ty in INDEXABLE {
        let mut c = ColumnFilter::new(0, ty.clone(), HashSeed::default(), FilterConfig::default());
        for (slot, v) in sample_values(ty, 60).iter().enumerate() {
            c.set(slot as u64, Some(v));
        }
        let n = 200u64;
        let mut bm = vec![0u64; bitmap_words(n)];
        for _ in 0..300 {
            let len = fastrand::usize(0..40);
            let probe: Vec<u8> = (0..len).map(|_| fastrand::u8(..)).collect();
            for op in [SargOp::Eq, SargOp::Gt, SargOp::Ge, SargOp::Lt, SargOp::Le] {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    c.probe_pred_into(op, &probe, n, &mut bm);
                }));
                assert!(r.is_ok(), "{:?}: probe of {} bytes with {:?} panicked", ty, len, op);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn end_to_end_results_match_brute_force_over_random_queries() {
    let (_d, t) = std_table("prop_e2e").await;

    for _ in 0..60 {
        let (clause, want): (String, Vec<u64>) = match fastrand::usize(0..6) {
            0 => {
                let v = fastrand::u64(0..STD_N + 50);
                (format!("id = {}", v), (0..STD_N).filter(|i| *i == v).collect())
            }
            1 => {
                let v = fastrand::i32(0..70);
                (format!("cat = {}", v), (0..STD_N).filter(|i| (i % 64) as i32 == v).collect())
            }
            2 => {
                let v = fastrand::u64(0..STD_N);
                (format!("id > {}", v), (0..STD_N).filter(|i| *i > v).collect())
            }
            3 => {
                let v = fastrand::u64(0..STD_N);
                (format!("id <= {}", v), (0..STD_N).filter(|i| *i <= v).collect())
            }
            4 => {
                let (a, b) = (fastrand::u64(0..STD_N), fastrand::u64(0..STD_N));
                (
                    format!("id = {} OR id = {}", a, b),
                    (0..STD_N).filter(|i| *i == a || *i == b).collect(),
                )
            }
            _ => {
                let v = fastrand::u64(0..STD_N);
                let c = (v % 64) as i32;
                (
                    format!("id = {} AND cat = {}", v, c),
                    (0..STD_N).filter(|i| *i == v && (i % 64) as i32 == c).collect(),
                )
            }
        };
        let rows = run(&t, &clause).await;
        assert_eq!(u64s(&rows, 0), want, "`{}`", clause);
    }
}

/// Random **parenthesised** expressions evaluated by the engine, compared to
/// brute force.
///
/// This is the test the earlier suite was missing. The existing property test
/// compares the *mask* against truth, so it could never catch an evaluator that
/// misread grouping — both sides came from the same parse. This one goes
/// through `Table::query_row`, so a disagreement between what the filter thinks
/// a clause means and what the engine computes shows up as a wrong result set.
///
/// Groups are generated with three or more members wherever possible, because a
/// two-element OR group survives boolean flattening by coincidence — which is
/// how exactly this bug once passed review.
#[tokio::test(flavor = "multi_thread")]
async fn engine_results_match_brute_force_over_random_parenthesised_clauses() {
    let (_d, t) = std_table("prop_paren").await;

    #[derive(Clone)]
    enum E {
        IdEq(u64),
        CatEq(i32),
        IdGt(u64),
        Group(Box<E>),
        And(Box<E>, Box<E>),
        Or(Box<E>, Box<E>),
    }

    fn leaf() -> E {
        match fastrand::usize(0..3) {
            0 => E::IdEq(fastrand::u64(0..STD_N)),
            1 => E::CatEq(fastrand::i32(0..64)),
            _ => E::IdGt(fastrand::u64(0..STD_N)),
        }
    }

    /// Build an n-ary OR/AND chain of `k` leaves, then wrap it in parens.
    fn chain(k: usize, or: bool) -> E {
        let mut e = leaf();
        for _ in 1..k {
            let r = Box::new(leaf());
            e = if or { E::Or(Box::new(e), r) } else { E::And(Box::new(e), r) };
        }
        E::Group(Box::new(e))
    }

    fn build(depth: u32) -> E {
        if depth == 0 {
            return leaf();
        }
        match fastrand::usize(0..4) {
            // `X AND (a OR b OR c)` — the shape that exposed the bug.
            0 => E::And(Box::new(leaf()), Box::new(chain(2 + fastrand::usize(0..3), true))),
            // Same, reversed: order must not change the answer.
            1 => E::And(Box::new(chain(2 + fastrand::usize(0..3), true)), Box::new(leaf())),
            // Sibling groups: depth alone cannot distinguish these.
            2 => E::And(
                Box::new(chain(2 + fastrand::usize(0..2), true)),
                Box::new(chain(2 + fastrand::usize(0..2), true)),
            ),
            _ => {
                let (l, r) = (Box::new(build(depth - 1)), Box::new(build(depth - 1)));
                if fastrand::bool() { E::Or(l, r) } else { E::And(l, r) }
            }
        }
    }

    /// Every compound subexpression is parenthesised, so the rendered text
    /// means exactly what the tree means. Without this, `And(Or(a, b), c)`
    /// prints as `a OR b AND c`, which SQL precedence reads as
    /// `a OR (b AND c)` — a different predicate, and the test would be
    /// comparing the engine against a tree the text never described.
    fn render(e: &E, top: bool) -> String {
        match e {
            E::IdEq(v) => format!("id = {}", v),
            E::CatEq(v) => format!("cat = {}", v),
            E::IdGt(v) => format!("id > {}", v),
            E::Group(i) => format!("({})", render(i, true)),
            E::And(l, r) | E::Or(l, r) => {
                let op = if matches!(e, E::And(..)) { "AND" } else { "OR" };
                let inner = format!("{} {} {}", render(l, false), op, render(r, false));
                if top { inner } else { format!("({})", inner) }
            }
        }
    }

    fn truth(e: &E, id: u64, cat: i32) -> bool {
        match e {
            E::IdEq(v) => id == *v,
            E::CatEq(v) => cat == *v,
            E::IdGt(v) => id > *v,
            E::Group(i) => truth(i, id, cat),
            E::And(l, r) => truth(l, id, cat) && truth(r, id, cat),
            E::Or(l, r) => truth(l, id, cat) || truth(r, id, cat),
        }
    }

    for _ in 0..80 {
        let e = build(2);
        let clause = render(&e, true);
        let expected: Vec<u64> =
            (0..STD_N).filter(|i| truth(&e, *i, (*i % 64) as i32)).collect();
        let got = u64s(&run(&t, &clause).await, 0);
        assert_eq!(got, expected, "`{}`", clause);
    }
}

/// Operand order must never change the answer.
///
/// `A AND (B OR C)` and `(B OR C) AND A` are the same predicate. Under boolean
/// flattening they were not, and one order was right while the other was wrong
/// — which is how the bug hid.
#[tokio::test(flavor = "multi_thread")]
async fn operand_order_does_not_change_results() {
    let (_d, t) = std_table("prop_order").await;

    for _ in 0..40 {
        let c = fastrand::i32(0..64);
        let k = 2 + fastrand::usize(0..4);
        let members: Vec<u64> = (0..k).map(|_| fastrand::u64(0..STD_N)).collect();
        let group = members
            .iter()
            .map(|m| format!("id = {}", m))
            .collect::<Vec<_>>()
            .join(" OR ");

        let forward = format!("cat = {} AND ({})", c, group);
        let reversed = format!("({}) AND cat = {}", group, c);

        let a = u64s(&run(&t, &forward).await, 0);
        let b = u64s(&run(&t, &reversed).await, 0);
        assert_eq!(a, b, "`{}` vs `{}`", forward, reversed);

        let expected: Vec<u64> = (0..STD_N)
            .filter(|i| (i % 64) as i32 == c && members.contains(i))
            .collect();
        assert_eq!(a, expected, "`{}`", forward);
    }
}
