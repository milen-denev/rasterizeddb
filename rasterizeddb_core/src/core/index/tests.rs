//! The planner never returns less than the true matches.
//!
//! Random rows go into B-tree, hash and composite indexes. Random predicate
//! trees (equalities, ranges, `AND`, `OR`) are planned, and every slot that
//! truly matches by the engine's own comparison must be among the
//! candidates. Extra candidates are allowed; a missing one is the bug this
//! module exists to prevent.

use std::sync::Arc;

use super::*;
use crate::core::filter::SargOp;
use crate::core::filter::sarg::{MaskExpr, SargPredicate};
use crate::core::index_store::gate::{Proof, test_mint};
use crate::core::processor::transformer::ComparerOperation;
use crate::core::row::logical::perform_comparison_operation;
use crate::core::row::schema::SchemaField;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Columns: 0 = I64 (small range, many duplicates), 1 = STRING (short, with
/// prefixes and NUL bytes), 2 = F64 (with NaN and -0).
fn fields() -> Vec<SchemaField> {
    vec![
        SchemaField::new("n".into(), DbType::I64, 8, 0, 0, false),
        SchemaField::new("s".into(), DbType::STRING, 12, 8, 1, false),
        SchemaField::new("f".into(), DbType::F64, 8, 20, 2, false),
    ]
}

fn value(r: &mut Rng, col: u32) -> Vec<u8> {
    match col {
        0 => ((r.below(41) as i64) - 20).to_le_bytes().to_vec(),
        1 => {
            let n = r.below(4) as usize;
            (0..n).map(|_| [b'a', b'b', 0u8, 0xFF][r.below(4) as usize]).collect()
        }
        _ => {
            let specials = [0.0f64, -0.0, f64::NAN, 1.5, -1.5, 2.0, f64::INFINITY];
            specials[r.below(specials.len() as u64) as usize].to_le_bytes().to_vec()
        }
    }
}

fn ty(col: u32) -> DbType {
    match col {
        0 => DbType::I64,
        1 => DbType::STRING,
        _ => DbType::F64,
    }
}

fn matches(row: &[Vec<u8>], p: &SargPredicate) -> bool {
    let op = match p.op {
        SargOp::Eq => ComparerOperation::Equals,
        SargOp::Gt => ComparerOperation::Greater,
        SargOp::Ge => ComparerOperation::GreaterOrEquals,
        SargOp::Lt => ComparerOperation::Less,
        SargOp::Le => ComparerOperation::LessOrEquals,
    };
    perform_comparison_operation(&row[p.write_order as usize], &p.value, &ty(p.write_order), &op).unwrap()
}

fn eval(row: &[Vec<u8>], e: &MaskExpr) -> bool {
    match e {
        MaskExpr::All => true,
        MaskExpr::Pred(p) => matches(row, p),
        MaskExpr::And(v) => v.iter().all(|c| eval(row, c)),
        MaskExpr::Or(v) => v.iter().any(|c| eval(row, c)),
    }
}

fn pred(r: &mut Rng) -> MaskExpr {
    let col = r.below(3) as u32;
    let op = [SargOp::Eq, SargOp::Eq, SargOp::Gt, SargOp::Ge, SargOp::Lt, SargOp::Le][r.below(6) as usize];
    MaskExpr::Pred(SargPredicate {
        write_order: col,
        op,
        value: value(r, col),
    })
}

fn expr(r: &mut Rng, depth: u32) -> MaskExpr {
    if depth == 0 || r.below(3) == 0 {
        return pred(r);
    }
    let n = 2 + r.below(2) as usize;
    let kids = (0..n).map(|_| expr(r, depth - 1)).collect();
    if r.below(2) == 0 { MaskExpr::And(kids) } else { MaskExpr::Or(kids) }
}

fn index(name: &str, method: Method, columns: &[u32]) -> Arc<LiveIndex> {
    Arc::new(LiveIndex::new(
        IndexDecl {
            name: name.into(),
            method,
            unique: false,
            columns: columns.to_vec(),
            is_deleted: false,
        },
        columns.iter().map(|c| (*c, ty(*c))).collect(),
        false,
    ))
}

#[test]
fn candidates_are_always_a_superset_of_the_matches() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let schema: Vec<SchemaField> = fields();
    let mut r = Rng(0xDEAD_BEEF);
    // Several index layouts, so every planner branch is reached: whole-key
    // hash, single-column B-trees, composite B-trees led by each type.
    let layouts: Vec<Vec<Arc<LiveIndex>>> = vec![
        vec![index("n_bt", Method::BTree, &[0])],
        vec![index("s_bt", Method::BTree, &[1]), index("f_h", Method::Hash, &[2])],
        vec![index("ns_bt", Method::BTree, &[0, 1])],
        vec![index("sn_bt", Method::BTree, &[1, 0]), index("n_h", Method::Hash, &[0])],
        vec![index("fs_bt", Method::BTree, &[2, 1]), index("nsf_h", Method::Hash, &[0, 1, 2])],
        // Whole keys on every column, so a conjunction's answer from one is
        // filtered by the others (step 3).
        vec![
            index("n_h", Method::Hash, &[0]),
            index("s_bt", Method::BTree, &[1]),
            index("f_h", Method::Hash, &[2]),
            index("n_bt", Method::BTree, &[0]),
        ],
    ];
    for layout in layouts {
        let indexes = TableIndexes::new(layout);
        let rows: Vec<Vec<Vec<u8>>> = (0..400).map(|_| (0..3).map(|c| value(&mut r, c)).collect()).collect();
        // Slots in a shuffled order, as concurrent writers deliver them, and
        // one in twenty unkeyed in each index: a slot whose key could not be
        // built must come back from every lookup of that index.
        let mut order: Vec<usize> = (0..rows.len()).collect();
        for i in (1..order.len()).rev() {
            order.swap(i, r.below(i as u64 + 1) as usize);
        }
        for &slot in &order {
            let row = &rows[slot];
            for ix in &indexes.list {
                let key = if r.below(20) == 0 { None } else { ix.key_of(|wo| Some(row[wo as usize].as_slice())) };
                ix.insert(proof, slot as u64, key);
            }
        }
        let mut used = 0;
        for _ in 0..3_000 {
            let e = expr(&mut r, 3);
            // A small cap as well: a broad key declined in step 1 is still a
            // filter in step 3.
            let cap = if r.below(3) == 0 { 60 } else { usize::MAX };
            let Some(candidates) = indexes.plan(&e, &schema, cap) else {
                continue;
            };
            used += 1;
            for (slot, row) in rows.iter().enumerate() {
                if eval(row, &e) {
                    assert!(
                        candidates.binary_search(&(slot as u64)).is_ok(),
                        "slot {slot} matches {e:?} but the index missed it (row {row:?})"
                    );
                }
            }
        }
        assert!(used > 100, "the planner answered too few queries to test ({used})");
    }
}

#[test]
fn removal_and_replacement_keep_the_superset() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let ix = index("n", Method::BTree, &[0]);
    let indexes = TableIndexes::new(vec![ix.clone()]);
    let key = |v: i64| {
        let bytes = v.to_le_bytes();
        ix.key_of(|_| Some(&bytes[..])).unwrap()
    };
    let eq = |v: i64| {
        MaskExpr::Pred(SargPredicate {
            write_order: 0,
            op: SargOp::Eq,
            value: v.to_le_bytes().to_vec(),
        })
    };
    let schema = fields();
    ix.insert(proof, 1, Some(key(5)));
    ix.insert(proof, 2, Some(key(5)));
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![1, 2]));
    // In place, 5 -> 7: slot 1 moves.
    ix.insert(proof, 1, Some(key(7)));
    ix.remove(proof, 1, &key(5));
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![2]));
    assert_eq!(indexes.plan(&eq(7), &schema, 100), Some(vec![1]));
    // Removing a pair that is not there is a no-op (a late, repeated forget).
    ix.remove(proof, 2, &key(7));
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![2]));
    // Vacuum forgets a reclaimed slot entirely.
    assert!(ix.holds(2));
    ix.forget(proof, 2, Some(&key(5)));
    assert!(!ix.holds(2));
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![]));
    // A version with no key is a candidate for every lookup.
    ix.insert(proof, 9, None);
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![9]));
    assert_eq!(indexes.plan(&eq(7), &schema, 100), Some(vec![1, 9]));
}

#[test]
fn a_broad_answer_is_declined() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let ix = index("n", Method::BTree, &[0]);
    let indexes = TableIndexes::new(vec![ix.clone()]);
    for slot in 0..1000u64 {
        let bytes = (slot as i64).to_le_bytes();
        ix.insert(proof, slot, ix.key_of(|_| Some(&bytes[..])));
    }
    let gt = MaskExpr::Pred(SargPredicate {
        write_order: 0,
        op: SargOp::Gt,
        value: 10i64.to_le_bytes().to_vec(),
    });
    assert!(indexes.plan(&gt, &fields(), 100).is_none(), "990 candidates over a cap of 100");
    assert_eq!(indexes.plan(&gt, &fields(), 10_000).map(|v| v.len()), Some(989));
}

#[test]
fn hash_answers_only_equality_on_its_whole_key() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let ix = index("ns", Method::Hash, &[0, 1]);
    let indexes = TableIndexes::new(vec![ix.clone()]);
    ix.insert(proof, 3, ix.key_of(|wo| Some(if wo == 0 { &[1, 0, 0, 0, 0, 0, 0, 0][..] } else { b"x" })));
    let p = |wo: u32, op: SargOp, v: &[u8]| {
        MaskExpr::Pred(SargPredicate {
            write_order: wo,
            op,
            value: v.to_vec(),
        })
    };
    let one = 1i64.to_le_bytes();
    // Only one column constrained: a hash cannot help.
    assert!(indexes.plan(&p(0, SargOp::Eq, &one), &fields(), 100).is_none());
    // A range: a hash cannot help.
    let both_range = MaskExpr::And(vec![p(0, SargOp::Gt, &one), p(1, SargOp::Eq, b"x")]);
    assert!(indexes.plan(&both_range, &fields(), 100).is_none());
    // The whole key by equality: one probe.
    let both = MaskExpr::And(vec![p(0, SargOp::Eq, &one), p(1, SargOp::Eq, b"x")]);
    assert_eq!(indexes.plan(&both, &fields(), 100), Some(vec![3]));
}

#[test]
fn method_choice() {
    assert_eq!(choose_method(None, &[DbType::I64]), Ok(Method::BTree));
    assert_eq!(choose_method(None, &[DbType::STRING, DbType::I32]), Ok(Method::BTree));
    assert_eq!(choose_method(None, &[DbType::UUID]), Ok(Method::Hash));
    assert_eq!(choose_method(None, &[DbType::JSONB]), Ok(Method::Hash));
    // A UUID next to an orderable column: a B-tree, since a range on the other
    // column may be asked.
    assert_eq!(choose_method(None, &[DbType::UUID, DbType::DATE]), Ok(Method::BTree));
    assert!(choose_method(Some(Method::BTree), &[DbType::JSONB]).is_err());
    assert!(choose_method(Some(Method::Hash), &[DbType::INTERVAL]).is_err());
    assert!(choose_method(None, &[DbType::INTERVAL]).is_err());
}

#[test]
fn a_composite_btree_ranges_inside_its_equality_prefix() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let strings = ["a", "b", "b\0", "c", "d"];
    let slot_of = |n: i64, j: usize| (n as u64) * strings.len() as u64 + j as u64;
    for (name, cols) in [("ns", [0u32, 1]), ("sn", [1, 0])] {
        let ix = index(name, Method::BTree, &cols);
        let indexes = TableIndexes::new(vec![ix.clone()]);
        for n in 0..10i64 {
            for (j, s) in strings.iter().enumerate() {
                let nb = n.to_le_bytes();
                let key = ix.key_of(|wo| Some(if wo == 0 { &nb[..] } else { s.as_bytes() }));
                ix.insert(proof, slot_of(n, j), key);
            }
        }
        let p = |wo: u32, op: SargOp, v: &[u8]| {
            MaskExpr::Pred(SargPredicate {
                write_order: wo,
                op,
                value: v.to_vec(),
            })
        };
        let three = 3i64.to_le_bytes();
        let five = 5i64.to_le_bytes();
        // Leading column fixed, a range on the next: only that slice.
        let (expr, want): (MaskExpr, Vec<u64>) = if cols[0] == 0 {
            (
                MaskExpr::And(vec![p(0, SargOp::Eq, &three), p(1, SargOp::Gt, b"b")]),
                vec![slot_of(3, 2), slot_of(3, 3), slot_of(3, 4)],
            )
        } else {
            (
                MaskExpr::And(vec![p(1, SargOp::Eq, b"b"), p(0, SargOp::Lt, &five)]),
                (0..5).map(|n| slot_of(n, 1)).collect(),
            )
        };
        assert_eq!(indexes.plan(&expr, &fields(), usize::MAX), Some(want), "{name}");
        // The prefix alone still narrows to it.
        let only_prefix = if cols[0] == 0 { p(0, SargOp::Eq, &three) } else { p(1, SargOp::Eq, b"b") };
        let got = indexes.plan(&only_prefix, &fields(), usize::MAX).unwrap();
        assert_eq!(got.len(), if cols[0] == 0 { strings.len() } else { 10 }, "{name}");
    }
}

/// `doc->>'status' = 'paid' AND id < 5`: the equality is a whole key, but a
/// fifth of the table, and the range on another index is five rows. The
/// smaller answer wins, whether or not the broad one fits under the cap.
#[test]
fn a_broad_whole_key_loses_to_a_narrow_range() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let by_n = index("n", Method::BTree, &[0]);
    let by_s = index("s", Method::BTree, &[1]);
    let indexes = TableIndexes::new(vec![by_n.clone(), by_s.clone()]);
    for slot in 0..1000u64 {
        let nb = (slot as i64).to_le_bytes();
        by_n.insert(proof, slot, by_n.key_of(|_| Some(&nb[..])));
        let s: &[u8] = if slot % 5 == 0 { b"paid" } else { b"new" };
        by_s.insert(proof, slot, by_s.key_of(|_| Some(s)));
    }
    let p = |wo: u32, op: SargOp, v: &[u8]| {
        MaskExpr::Pred(SargPredicate {
            write_order: wo,
            op,
            value: v.to_vec(),
        })
    };
    let fifty = 50i64.to_le_bytes();
    let both = MaskExpr::And(vec![p(1, SargOp::Eq, b"paid"), p(0, SargOp::Lt, &fifty)]);
    // The range, filtered by the broad key without reading its list: slots
    // under fifty that are 'paid'.
    let want: Vec<u64> = (0..50).filter(|s| s % 5 == 0).collect();
    // The broad key fits: still the range.
    assert_eq!(indexes.plan(&both, &fields(), usize::MAX), Some(want.clone()));
    // The broad key is over the cap: the range.
    assert_eq!(indexes.plan(&both, &fields(), 100), Some(want.clone()));
    // A slot whose key could not be built may hold anything: the filter
    // keeps it.
    by_s.insert(proof, 7, None);
    let mut with_unkeyed = want.clone();
    with_unkeyed.push(7);
    with_unkeyed.sort_unstable();
    assert_eq!(indexes.plan(&both, &fields(), 100), Some(with_unkeyed));
    // A selective key beats a broad range.
    let wide = 900i64.to_le_bytes();
    let rare = MaskExpr::And(vec![p(1, SargOp::Eq, b"paid"), p(0, SargOp::Lt, &wide)]);
    // 200 'paid' slots, and slot 7, unkeyed above.
    assert_eq!(indexes.plan(&rare, &fields(), usize::MAX).map(|v| v.len()), Some(201));
    // The key alone, over the cap: declined, not copied and then refused.
    let mut out = Vec::new();
    let key = by_s.key_of(|_| Some(&b"paid"[..])).unwrap();
    assert!(!by_s.lookup_eq(&key, &mut out, 100));
    assert!(out.is_empty(), "{} slots copied before the cap was checked", out.len());
}

/// A posting list stays sorted and distinct whatever order slots arrive in,
/// and a removal finds its slot by search.
#[test]
fn posting_lists_stay_sorted_and_distinct() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let ix = index("n", Method::BTree, &[0]);
    let hx = index("h", Method::Hash, &[0]);
    let indexes = TableIndexes::new(vec![ix.clone()]);
    let hashes = TableIndexes::new(vec![hx.clone()]);
    let seven = 7i64.to_le_bytes();
    let mut r = Rng(0x5107);
    let mut model = std::collections::BTreeSet::new();
    for _ in 0..2000 {
        let slot = r.below(500);
        if r.below(4) == 0 {
            ix.remove(proof, slot, &ix.key_of(|_| Some(&seven[..])).unwrap());
            hx.remove(proof, slot, &hx.key_of(|_| Some(&seven[..])).unwrap());
            model.remove(&slot);
        } else {
            ix.insert(proof, slot, ix.key_of(|_| Some(&seven[..])));
            hx.insert(proof, slot, hx.key_of(|_| Some(&seven[..])));
            model.insert(slot);
        }
    }
    let eq = MaskExpr::Pred(SargPredicate {
        write_order: 0,
        op: SargOp::Eq,
        value: seven.to_vec(),
    });
    let want: Vec<u64> = model.into_iter().collect();
    for (name, ix, set) in [("btree", &ix, &indexes), ("hash", &hx, &hashes)] {
        let mut raw = Vec::new();
        assert!(ix.lookup_eq(&ix.key_of(|_| Some(&seven[..])).unwrap(), &mut raw, usize::MAX));
        assert_eq!(raw, want, "{name}: the list itself, unsorted by the planner");
        assert_eq!(set.plan(&eq, &fields(), usize::MAX), Some(want.clone()), "{name}");
    }
}

/// `gallop_to` is `partition_point` from a starting position, for every
/// start at or before the answer.
#[test]
fn galloping_finds_what_a_binary_search_finds() {
    let mut r = Rng(0x6a11);
    for _ in 0..2_000 {
        let n = r.below(80) as usize;
        let mut v: Vec<u64> = (0..n).map(|_| r.below(200)).collect();
        v.sort_unstable();
        v.dedup();
        let target = r.below(210);
        let want = v.partition_point(|x| *x < target);
        for from in 0..=want {
            assert_eq!(gallop_to(&v, from, target), want, "{v:?} from {from} to {target}");
        }
    }
}

fn gin_pair() -> (Arc<gin::GinIndex>, Arc<gin::GinIndex>, TableIndexes) {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let decl = |name: &str, method| IndexDecl {
        name: name.into(),
        method,
        unique: false,
        columns: vec![0],
        is_deleted: false,
    };
    let ops = Arc::new(gin::GinIndex::new(decl("ops", Method::GinOps), 0, crate::core::jsonb::GinOpclass::Ops));
    let path = Arc::new(gin::GinIndex::new(decl("path", Method::GinPathOps), 0, crate::core::jsonb::GinOpclass::PathOps));
    // `jsonb_ops` declared first, so a stable sort on the estimate alone
    // would ask it first on a tie.
    let indexes = TableIndexes::with_gin(Vec::new(), vec![ops.clone(), path.clone()]);
    indexes.set_ready(proof, true);
    (ops, path, indexes)
}

fn contains_leaf(q: &str) -> gin::JsonExpr {
    gin::JsonExpr::Leaf {
        column: 0,
        op: gin::JsonOp::Contains(crate::core::jsonb::encode_text(q).unwrap()),
    }
}

/// `{"status":"paid"}` bounds both answers at the `paid` list, but under
/// `jsonb_ops` the walk also has the `status` key list every document is
/// on. Between equal bounds the index with less to read is asked first.
#[test]
fn ties_go_to_the_index_with_less_to_read() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let (_, _, indexes) = gin_pair();
    for slot in 0..500u64 {
        let d = format!(r#"{{"status":"{}","n":{}}}"#, ["paid", "new", "shipped"][(slot % 3) as usize], slot);
        let bytes = crate::core::jsonb::encode_text(&d).unwrap();
        indexes.index_row(proof, slot, &|_| Some(&bytes[..]));
    }
    let gin::JsonExpr::Leaf { op, .. } = contains_leaf(r#"{"status":"paid"}"#) else { unreachable!() };
    let order = indexes.ask_order(0, &op);
    assert_eq!(order[0].0.0, order[1].0.0, "the bounds tie");
    assert_eq!(order[0].1.decl.name, "path", "{:?}", order.iter().map(|o| (o.0, &o.1.decl.name)).collect::<Vec<_>>());
}

/// An answer too broad for a slot list comes back as a bitmap holding the
/// same slots, for one operator and for operators `AND`ed together, and is
/// declined when it would name more than its limit, or when the clause's
/// shape is one the bitmap path does not take.
#[test]
fn broad_gin_answers_come_back_as_bitmaps() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let (_, _, indexes) = gin_pair();
    let n = 2_000u64;
    let mut docs = Vec::new();
    for slot in 0..n {
        let d = match slot % 50 {
            0 => None,
            _ => Some(format!(
                r#"{{"status":"s{}","tags":["t{}"],"n":{}}}"#,
                slot % 5,
                slot % 3,
                slot
            )),
        };
        match &d {
            Some(d) => {
                let bytes = crate::core::jsonb::encode_text(d).unwrap();
                indexes.index_row(proof, slot, &|_| Some(&bytes[..]));
            }
            None => indexes.index_unreadable(proof, slot),
        }
        docs.push(d);
    }
    let bits = |words: &[u64]| -> Vec<u64> {
        let mut out = Vec::new();
        for (w, word) in words.iter().enumerate() {
            let mut b = *word;
            while b != 0 {
                out.push(w as u64 * 64 + b.trailing_zeros() as u64);
                b &= b - 1;
            }
        }
        out
    };
    let contains = |slot: u64, q: &str| -> bool {
        match &docs[slot as usize] {
            None => true, // unreadable: always a candidate
            Some(d) => crate::core::jsonb::contains(
                &crate::core::jsonb::encode_text(d).unwrap(),
                &crate::core::jsonb::encode_text(q).unwrap(),
            )
            .unwrap(),
        }
    };
    let paid = contains_leaf(r#"{"status":"s1"}"#);
    let tagged = contains_leaf(r#"{"tags":["t2"]}"#);
    let cap = 100;
    let cases: Vec<(gin::JsonExpr, Box<dyn Fn(u64) -> bool + '_>)> = vec![
        (paid.clone(), Box::new(|s| contains(s, r#"{"status":"s1"}"#))),
        (
            gin::JsonExpr::And(vec![paid.clone(), tagged.clone()]),
            Box::new(|s| contains(s, r#"{"status":"s1"}"#) && contains(s, r#"{"tags":["t2"]}"#)),
        ),
        (
            gin::JsonExpr::And(vec![gin::JsonExpr::Opaque, tagged.clone()]),
            Box::new(|s| contains(s, r#"{"tags":["t2"]}"#)),
        ),
        // As the rewriter's `NULL` guards leave it.
        (
            gin::JsonExpr::And(vec![
                gin::JsonExpr::And(vec![gin::JsonExpr::Opaque, paid.clone()]),
                gin::JsonExpr::And(vec![gin::JsonExpr::Opaque, tagged.clone()]),
            ]),
            Box::new(|s| contains(s, r#"{"status":"s1"}"#) && contains(s, r#"{"tags":["t2"]}"#)),
        ),
    ];
    for (expr, matches) in cases {
        assert_eq!(indexes.plan_json(&expr, cap), None, "{expr:?} is past the slot-list cap");
        let words = indexes.plan_json_dense(&expr, n, dense_cap(n)).expect("answered as a bitmap");
        let got = bits(&words);
        let all = indexes.plan_json(&expr, usize::MAX).unwrap();
        assert_eq!(got, all, "{expr:?}: the bitmap holds what the slot list holds");
        for slot in 0..n {
            if matches(slot) {
                assert!(got.contains(&slot), "{expr:?} dropped slot {slot}");
            }
        }
    }
    // Most of the table: declined, the scan reads it about as cheaply.
    let most = contains_leaf(r#"{"tags":[]}"#);
    assert!(indexes.plan_json(&most, usize::MAX).is_none_or(|a| a.len() as u64 > n / 2));
    assert_eq!(indexes.plan_json_dense(&most, n, dense_cap(n)), None);
    // A union is not taken as a bitmap.
    let either = gin::JsonExpr::Or(vec![paid.clone(), tagged.clone()]);
    assert_eq!(indexes.plan_json_dense(&either, n, dense_cap(n)), None);
}

/// An index over a `#>` path that can name either a key or a position is
/// built. Its declaration used to be stored under a key that did not read back
/// as a path, so it was accepted and never built; a record still in that form
/// is upgraded when loaded.
#[test]
fn a_path_index_over_an_either_step_is_built() {
    use crate::core::rql::path_index::PathIndex;
    let built = |stored: &str| {
        let mut schema = crate::core::row::schema::TableSchema::new("t".into(), false);
        schema.fields.push(SchemaField::new("doc".into(), DbType::JSONB, 8, 0, 0, false));
        let record = PathIndex { name: "ix".into(), path: stored.into(), is_deleted: false };
        schema.path_indexes.push(PathIndex::from_vec(&record.into_vec()).unwrap());
        for_schema(&schema).list.len()
    };
    let key = crate::core::jsonb::parse_path_expression("doc#>>'{a,2}'").unwrap().index_key();
    assert_eq!(built(&key), 1, "declared as {}", key);
    assert_eq!(built("doc->'a'#>'2'"), 1, "a record in the old form");
}

/// A table with one index of every kind the shape covers: an implicit
/// primary key, an implicit unique column, a NUMERIC(10,2) column under a
/// composite B-tree, a hash index, both gin operator classes, and paths with
/// `Key` and `Index` steps. A second document column has no index, for a gin
/// index to move onto. No `#>>` path: since `for_schema` builds one (see
/// [`a_path_index_over_an_either_step_is_built`]) it could be declared here,
/// but the pinned value predates that, and `Either` is covered by
/// [`the_shape_hash_sees_an_either_steps_text`] and
/// [`a_built_either_path_has_the_shape_of_its_steps`] instead.
fn shape_schema() -> crate::core::row::schema::TableSchema {
    use crate::core::row::schema::TableSchema;
    let mut s = TableSchema::new("orders".into(), false);
    let mut price = SchemaField::new("price".into(), DbType::DECIMAL, 16, 8, 1, false);
    price.scale = Some(2);
    s.fields.push(SchemaField::new("id".into(), DbType::I64, 8, 0, 0, false));
    s.fields.push(price);
    s.fields.push(SchemaField::new("email".into(), DbType::STRING, 12, 24, 2, true));
    s.fields.push(SchemaField::new("token".into(), DbType::UUID, 16, 36, 3, false));
    s.fields.push(SchemaField::new("doc".into(), DbType::JSONB, 12, 52, 4, false));
    s.fields.push(SchemaField::new("doc2".into(), DbType::JSONB, 12, 64, 5, false));
    s.primary_key = Some("id".into());
    let decl = |name: &str, method: Method, columns: &[u32]| IndexDecl {
        name: name.into(),
        method,
        unique: false,
        columns: columns.to_vec(),
        is_deleted: false,
    };
    s.secondary_indexes = vec![
        decl("by_price_email", Method::BTree, &[1, 2]),
        decl("by_token", Method::Hash, &[3]),
        decl("doc_ops", Method::GinOps, &[4]),
        decl("doc_path_ops", Method::GinPathOps, &[4]),
    ];
    let path = |name: &str, expr: &str| crate::core::rql::path_index::PathIndex {
        name: name.into(),
        path: crate::core::jsonb::parse_path_expression(expr).unwrap().index_key(),
        is_deleted: false,
    };
    s.path_indexes = vec![
        path("by_k", "doc->'a'->>'k'"),
        path("by_i", "doc->'a'->>1"),
    ];
    s
}

fn shape(s: &crate::core::row::schema::TableSchema) -> u64 {
    shape_of(s)
}

fn path_steps(expr: &str) -> String {
    crate::core::jsonb::parse_path_expression(expr).unwrap().index_key()
}

/// The shape is persisted beside a snapshot, so its value is frozen: a
/// change here is a format change.
#[test]
fn the_shape_hash_is_pinned() {
    let s = shape_schema();
    let built = for_schema(&s);
    assert_eq!((built.list.len(), built.gin.len()), (6, 2), "every kind is in the shape");
    let steps: Vec<_> = built.list.iter().filter_map(|i| i.path.as_ref()).flat_map(|p| p.1.clone()).collect();
    use crate::core::jsonb::Step;
    assert!(steps.iter().any(|s| matches!(s, Step::Key(_))), "{steps:?}");
    assert!(steps.iter().any(|s| matches!(s, Step::Index(_))), "{steps:?}");
    assert_eq!(shape(&s), 0x7dd7_75ac_cf81_1956, "shape_hash moved");
    assert_eq!(either_shape(Step::Either("2".into())), 0x09d8_3f82_d852_b42f, "shape_hash moved");
}

/// The shape of one path index over `doc->'a'` then `last`, built directly so
/// the step's kind can be chosen freely; a declared `#>>` path builds the same
/// index (see [`a_built_either_path_has_the_shape_of_its_steps`]).
fn either_shape(last: crate::core::jsonb::Step) -> u64 {
    let decl = IndexDecl {
        name: "p".into(),
        method: Method::BTree,
        unique: false,
        columns: vec![FIRST_PATH_COLUMN],
        is_deleted: false,
    };
    let steps = vec![crate::core::jsonb::Step::Key("a".into()), last];
    let index = LiveIndex::over_path(decl, FIRST_PATH_COLUMN, 4, steps);
    TableIndexes::new(vec![Arc::new(index)]).shape_hash(&shape_schema().fields)
}

/// A declared `#>>` path is built, and with the shape of the same steps built
/// directly: an `Either` step reaches the shape through `for_schema` too.
#[test]
fn a_built_either_path_has_the_shape_of_its_steps() {
    use crate::core::jsonb::Step;
    let mut s = shape_schema();
    s.primary_key = None;
    s.fields[2].is_unique = false;
    s.secondary_indexes.clear();
    s.path_indexes = vec![crate::core::rql::path_index::PathIndex {
        name: "p".into(),
        path: path_steps("doc#>>'{a,2}'"),
        is_deleted: false,
    }];
    let built = for_schema(&s);
    assert_eq!((built.list.len(), built.gin.len()), (1, 0), "only the path index is built");
    assert_eq!(shape(&s), either_shape(Step::Either("2".into())));
}

/// An `Either` step's text decides which value is keyed, and so does its
/// kind: `#>>'{a,2}'` is neither `->'a'->>'2'` nor `->'a'->>2`.
#[test]
fn the_shape_hash_sees_an_either_steps_text() {
    use crate::core::jsonb::Step;
    let base = either_shape(Step::Either("2".into()));
    for (what, other) in [
        ("either text", Step::Either("3".into())),
        ("key of the same text", Step::Key("2".into())),
        ("index of the same value", Step::Index(2)),
    ] {
        assert_ne!(either_shape(other), base, "{what} did not move the shape");
    }
}

/// Everything that decides an index's entries moves the shape; a name, which
/// decides none, does not.
#[test]
fn the_shape_hash_sees_what_keys_rows_and_no_name() {
    let base = shape(&shape_schema());
    let changed = |what: &str, edit: &dyn Fn(&mut crate::core::row::schema::TableSchema)| {
        let mut s = shape_schema();
        edit(&mut s);
        (what.to_string(), shape(&s))
    };
    let same = [
        changed("table renamed", &|s| s.name = "renamed".into()),
        changed("index renamed", &|s| s.secondary_indexes[1].name = "other".into()),
        changed("path index renamed", &|s| s.path_indexes[0].name = "other".into()),
        changed("column renamed with its key", &|s| {
            s.fields[0].name = "ident".into();
            s.primary_key = Some("ident".into());
        }),
        changed("unindexed column added", &|s| {
            s.fields.push(SchemaField::new("note".into(), DbType::STRING, 12, 76, 6, false))
        }),
    ];
    for (what, h) in same {
        assert_eq!(h, base, "{what} moved the shape");
    }
    let differ = [
        changed("scale", &|s| s.fields[1].scale = Some(3)),
        changed("scale to default", &|s| s.fields[1].scale = None),
        changed("type", &|s| s.fields[0].db_type = DbType::I32),
        changed("method", &|s| s.secondary_indexes[0].method = Method::Hash),
        changed("unique", &|s| s.secondary_indexes[0].unique = true),
        changed("column", &|s| s.secondary_indexes[0].columns = vec![1, 0]),
        changed("order", &|s| s.secondary_indexes.swap(0, 1)),
        changed("opclass", &|s| s.secondary_indexes[3].method = Method::GinOps),
        changed("path step", &|s| s.path_indexes[0].path = path_steps("doc->'a'->>'j'")),
        changed("path step kind", &|s| s.path_indexes[0].path = path_steps("doc->'a'->>0")),
        // `doc->'a'->>1` against `->>0` above: an index's value, not only
        // its kind.
        changed("path index value", &|s| s.path_indexes[1].path = path_steps("doc->'a'->>2")),
        changed("path document column", &|s| s.path_indexes[0].path = path_steps("doc2->'a'->>'k'")),
        changed("gin column", &|s| s.secondary_indexes[2].columns = vec![5]),
        changed("implicit unique column", &|s| s.fields[2].is_unique = false),
        // A declared index over the key column survives the dedup and the
        // implicit one does not: same columns and method, not implicit, not
        // unique.
        changed("dedup survivor", &|s| {
            s.secondary_indexes.insert(
                0,
                IndexDecl {
                    name: "by_id".into(),
                    method: Method::BTree,
                    unique: false,
                    columns: vec![0],
                    is_deleted: false,
                },
            )
        }),
        changed("dropped index", &|s| {
            s.secondary_indexes.remove(1);
        }),
    ];
    let mut seen = vec![base];
    for (what, h) in differ {
        assert!(!seen.contains(&h), "{what} did not move the shape");
        seen.push(h);
    }
}

/// `KEY_LOOKUPS` counts a lookup a column index answered, beside the path
/// and gin counters, so a test can tell an index was consulted.
#[test]
fn a_column_index_answer_is_counted() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let indexes = TableIndexes::new(vec![index("n_bt", Method::BTree, &[0]), index("s_hash", Method::Hash, &[1])]);
    for slot in 0..64u64 {
        let row = [(slot as i64).to_le_bytes().to_vec(), vec![b'a' + (slot % 7) as u8]];
        indexes.index_row(proof, slot, &|wo| row.get(wo as usize).map(|v| v.as_slice()));
    }
    indexes.set_ready(proof, true);
    let pred = |write_order: u32, op: SargOp, value: Vec<u8>| MaskExpr::Pred(SargPredicate { write_order, op, value });
    let cases = [
        (pred(0, SargOp::Eq, 5i64.to_le_bytes().to_vec()), vec![5]),
        (pred(1, SargOp::Eq, vec![b'a']), (0..64).step_by(7).collect()),
        (pred(0, SargOp::Lt, 3i64.to_le_bytes().to_vec()), vec![0, 1, 2]),
    ];
    for (expr, want) in cases {
        let before = KEY_LOOKUPS.load(Ordering::Relaxed);
        let here = KEY_LOOKUPS_HERE.with(|n| n.get());
        assert_eq!(indexes.plan(&expr, &fields(), 4096), Some(want), "{expr:?}");
        // The process-wide count is shared with every other test; this
        // thread's shadow of it sees only this lookup.
        assert_eq!(KEY_LOOKUPS_HERE.with(|n| n.get()), here + 1, "{expr:?} was not counted once");
        assert!(KEY_LOOKUPS.load(Ordering::Relaxed) > before, "{expr:?} missed the process-wide count");
    }
}

// ---------------------------------------------------------------------------
// The sorted stream: what a snapshot captures (hook C3) and loads (hook C9)
// ---------------------------------------------------------------------------

/// What `for_each_sorted` gave, with the stream's order checked on the way:
/// every entry, then the unkeyed slots once, then the presence words once.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Captured {
    entries: Vec<(Vec<u8>, Vec<u64>)>,
    unkeyed: Vec<u64>,
    present: Vec<u64>,
}

fn capture(ix: &LiveIndex) -> Captured {
    let mut c = Captured { entries: Vec::new(), unkeyed: Vec::new(), present: Vec::new() };
    let mut parts = 0; // closing parts seen
    ix.for_each_sorted(|part| -> Result<(), ()> {
        match part {
            Sorted::Entry(key, slots) => {
                assert_eq!(parts, 0, "an entry after the unkeyed slots");
                c.entries.push((key.to_vec(), slots.to_vec()));
            }
            Sorted::Unkeyed(slots) => {
                assert_eq!(parts, 0, "unkeyed slots twice, or after the presence bits");
                c.unkeyed = slots.to_vec();
                parts = 1;
            }
            Sorted::Present(words) => {
                assert_eq!(parts, 1, "presence bits before the unkeyed slots, or twice");
                c.present = words.to_vec();
                parts = 2;
            }
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(parts, 2, "the stream did not end with the presence bits");
    c
}

/// Presence words for a set of slots, without trailing zero words.
fn words_of(slots: impl IntoIterator<Item = u64>) -> Vec<u64> {
    let mut words = Vec::new();
    for s in slots {
        let w = (s / 64) as usize;
        if words.len() <= w {
            words.resize(w + 1, 0);
        }
        words[w] |= 1 << (s % 64);
    }
    words
}

/// What an index should list, kept beside it through every mutator.
#[derive(Default, Clone)]
struct Model {
    entries: std::collections::BTreeMap<Vec<u8>, std::collections::BTreeSet<u64>>,
    unkeyed: std::collections::BTreeSet<u64>,
    present: std::collections::BTreeSet<u64>,
}

impl Model {
    fn captured(&self) -> Captured {
        Captured {
            entries: self.entries.iter().map(|(k, s)| (k.clone(), s.iter().copied().collect())).collect(),
            unkeyed: self.unkeyed.iter().copied().collect(),
            present: words_of(self.present.iter().copied()),
        }
    }
}

/// Rows of an `I64` (0), a short `STRING` (1) and a document (3), with
/// enough distinct values that a stream spans several of `for_each_sorted`'s
/// blocks, and duplicates; a document's `k` is missing or JSON null now and
/// then, which a path index keeps under the empty key.
fn stream_rows(r: &mut Rng, n: usize) -> Vec<Vec<Vec<u8>>> {
    (0..n)
        .map(|_| {
            let doc = match r.below(10) {
                0 => r#"{"other":1}"#.to_string(),
                1 => r#"{"k":null}"#.to_string(),
                _ => format!(r#"{{"k":"v{}"}}"#, r.below(2000)),
            };
            vec![
                ((r.below(2500) as i64) - 1000).to_le_bytes().to_vec(),
                value(r, 1),
                Vec::new(),
                crate::core::jsonb::encode_text(&doc).unwrap(),
            ]
        })
        .collect()
}

/// One index of each kind a snapshot's INDEX section holds: B-tree, hash,
/// composite, and a path into a document.
fn stream_indexes() -> Vec<Arc<LiveIndex>> {
    let path = crate::core::jsonb::parse_path_expression("doc->>'k'").unwrap();
    let decl = IndexDecl {
        name: "k_path".into(),
        method: Method::BTree,
        unique: false,
        columns: vec![FIRST_PATH_COLUMN],
        is_deleted: false,
    };
    vec![
        index("n_bt", Method::BTree, &[0]),
        index("n_h", Method::Hash, &[0]),
        index("ns_bt", Method::BTree, &[0, 1]),
        index("s_h", Method::Hash, &[1]),
        Arc::new(LiveIndex::over_path(decl, FIRST_PATH_COLUMN, 3, path.steps)),
    ]
}

/// Fills `ix` from `rows` through every mutator a table calls, in a
/// shuffled order, keeping `model` beside it: inserts (one in twenty
/// unkeyed), in-place moves to a new key, and forgets with and without the
/// key read back. A forget without its key leaves the entry listed with its
/// presence bit clear, which the stream must still carry.
fn churn(ix: &LiveIndex, rows: &[Vec<Vec<u8>>], r: &mut Rng, proof: Proof<'_>) -> Model {
    let key = |row: &Vec<Vec<u8>>| ix.key_of(|wo| row.get(wo as usize).map(|v| v.as_slice()));
    let mut model = Model::default();
    let mut keys: Vec<Option<KeyBuf>> = vec![None; rows.len()];
    let mut order: Vec<usize> = (0..rows.len()).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, r.below(i as u64 + 1) as usize);
    }
    for &slot in &order {
        let k = if r.below(20) == 0 { None } else { key(&rows[slot]) };
        ix.insert(proof, slot as u64, k.clone());
        match &k {
            Some(k) => model.entries.entry(k.to_vec()).or_default().insert(slot as u64),
            None => model.unkeyed.insert(slot as u64),
        };
        model.present.insert(slot as u64);
        keys[slot] = k;
    }
    for _ in 0..rows.len() / 10 {
        let slot = r.below(rows.len() as u64);
        let s = slot as usize;
        if !model.present.contains(&slot) {
            continue;
        }
        match r.below(3) {
            // In place: the new key first, then the old one goes, as
            // `on_replace` orders it.
            0 => {
                let Some(old) = keys[s].clone() else { continue };
                let new = key(&rows[r.below(rows.len() as u64) as usize]);
                let Some(new) = new.filter(|n| *n != old) else { continue };
                ix.insert(proof, slot, Some(new.clone()));
                ix.remove(proof, slot, &old);
                model.entries.entry(new.to_vec()).or_default().insert(slot);
                let emptied = model.entries.get_mut(old.as_slice()).is_some_and(|set| {
                    set.remove(&slot);
                    set.is_empty()
                });
                if emptied {
                    model.entries.remove(old.as_slice());
                }
                keys[s] = Some(new);
            }
            // Vacuum, with the key read back.
            1 => {
                ix.forget(proof, slot, keys[s].as_deref());
                if let Some(k) = &keys[s] {
                    let emptied = model.entries.get_mut(k.as_slice()).is_some_and(|set| {
                        set.remove(&slot);
                        set.is_empty()
                    });
                    if emptied {
                        model.entries.remove(k.as_slice());
                    }
                }
                model.unkeyed.remove(&slot);
                model.present.remove(&slot);
            }
            // Vacuum, the row unreadable: the entry stays.
            _ => {
                ix.forget(proof, slot, None);
                model.unkeyed.remove(&slot);
                model.present.remove(&slot);
            }
        }
    }
    model
}

/// C3 emits every entry the index lists, in key order and slot order, then
/// the unkeyed slots and the presence bits, over B-tree, hash, composite
/// and path indexes, across many blocks; and filters nothing by presence.
#[test]
fn the_sorted_stream_is_every_entry_in_order() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let mut r = Rng(0x5eed_c3c3);
    let rows = stream_rows(&mut r, 3000);
    for ix in stream_indexes() {
        let model = churn(&ix, &rows, &mut r, proof);
        let got = capture(&ix);
        let name = &ix.decl.name;
        assert!(got.entries.windows(2).all(|w| w[0].0 < w[1].0), "{name}: keys not strictly ascending");
        assert!(
            got.entries.iter().all(|(_, s)| !s.is_empty() && s.windows(2).all(|w| w[0] < w[1])),
            "{name}: a posting list empty, unsorted or repeated"
        );
        assert_eq!(got, model.captured(), "{name}");
        for slot in 0..rows.len() as u64 + 70 {
            let bit = got.present.get((slot / 64) as usize).is_some_and(|w| w & (1 << (slot % 64)) != 0);
            assert_eq!(bit, ix.holds(slot), "{name}: presence of slot {slot}");
        }
        // A forget without its key keeps the entry, and it goes out.
        let unread = got
            .entries
            .iter()
            .flat_map(|(_, s)| s.iter())
            .filter(|s| !ix.holds(**s))
            .count();
        assert!(unread > 0, "{name}: no entry of a slot forgotten without its key was streamed");
        if ix.columns[0].0 == 0 {
            assert!(got.entries.len() > sorted::SORTED_BLOCK, "{name}: too few keys to span blocks");
        }
    }
}

/// Equal contents give equal streams, whatever order they arrived in and
/// whatever hasher a hash index drew: a hash index comes out by key bytes.
/// And a small stream, pinned.
#[test]
fn the_sorted_stream_depends_only_on_the_contents() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let mut r = Rng(0xde7e_c3c3);
    let rows = stream_rows(&mut r, 2500);
    for (ix, again) in stream_indexes().into_iter().zip(stream_indexes()) {
        let model = churn(&ix, &rows, &mut r, proof);
        // The same contents, entries in reverse key and slot order.
        for (key, slots) in model.entries.iter().rev() {
            for slot in slots.iter().rev() {
                again.insert(proof, *slot, Some(KeyBuf::from_slice(key)));
            }
        }
        for slot in model.unkeyed.iter().rev() {
            again.insert(proof, *slot, None);
        }
        for (_, slots) in &model.entries {
            for slot in slots.iter().filter(|s| !model.present.contains(s)) {
                again.forget(proof, *slot, None);
            }
        }
        assert_eq!(capture(&again), capture(&ix), "{}", ix.decl.name);
    }

    for method in [Method::BTree, Method::Hash] {
        let ix = index("pinned", method, &[0]);
        let key = |v: i64| {
            let bytes = v.to_le_bytes();
            ix.key_of(|_| Some(&bytes[..]))
        };
        ix.insert(proof, 3, key(5));
        ix.insert(proof, 1, key(5));
        ix.insert(proof, 2, key(-1));
        ix.insert(proof, 70, None);
        ix.insert(proof, 4, key(9));
        ix.forget(proof, 4, None); // unreadable: listed, not held
        ix.insert(proof, 300, key(9));
        ix.forget(proof, 300, key(9).as_deref()); // gone everywhere
        let hex = |h: &str| (0..h.len()).step_by(2).map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap()).collect();
        let want = Captured {
            entries: match method {
                // Ordered: sign-flipped big-endian.
                Method::BTree => vec![
                    (hex("7fffffffffffffff"), vec![2]),
                    (hex("8000000000000005"), vec![1, 3]),
                    (hex("8000000000000009"), vec![4]),
                ],
                // Canonical: the length, then the value's own bytes, so by
                // those bytes -1 sorts last.
                _ => vec![
                    (hex("080000000500000000000000"), vec![1, 3]),
                    (hex("080000000900000000000000"), vec![4]),
                    (hex("08000000ffffffffffffffff"), vec![2]),
                ],
            },
            unkeyed: vec![70],
            // Slots 1, 2, 3 and 70; nothing of 300's word survives.
            present: vec![0b1110, 1 << 6],
        };
        assert_eq!(capture(&ix), want, "{method:?}");
    }
}

/// The stream stops at the first error and hands it back.
#[test]
fn the_sorted_stream_stops_at_an_error() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    for method in [Method::BTree, Method::Hash] {
        let ix = index("e", method, &[0]);
        for slot in 0..3000u64 {
            let bytes = (slot as i64).to_le_bytes();
            ix.insert(proof, slot, ix.key_of(|_| Some(&bytes[..])));
        }
        let mut seen = 0;
        let got = ix.for_each_sorted(|_| {
            seen += 1;
            if seen == 1500 { Err("stop") } else { Ok(()) }
        });
        assert_eq!((got, seen), (Err("stop"), 1500), "{method:?}");
    }
}
