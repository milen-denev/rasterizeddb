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
    ];
    for layout in layouts {
        let indexes = TableIndexes::new(layout);
        let rows: Vec<Vec<Vec<u8>>> = (0..400).map(|_| (0..3).map(|c| value(&mut r, c)).collect()).collect();
        for (slot, row) in rows.iter().enumerate() {
            for ix in &indexes.list {
                let key = ix.key_of(|wo| Some(row[wo as usize].as_slice()));
                ix.insert(slot as u64, key);
            }
        }
        let mut used = 0;
        for _ in 0..3_000 {
            let e = expr(&mut r, 3);
            let Some(candidates) = indexes.plan(&e, &schema, usize::MAX) else {
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
    ix.insert(1, Some(key(5)));
    ix.insert(2, Some(key(5)));
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![1, 2]));
    // In place, 5 -> 7: slot 1 moves.
    ix.insert(1, Some(key(7)));
    ix.remove(1, &key(5));
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![2]));
    assert_eq!(indexes.plan(&eq(7), &schema, 100), Some(vec![1]));
    // Removing a pair that is not there is a no-op (a late, repeated forget).
    ix.remove(2, &key(7));
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![2]));
    // Vacuum forgets a reclaimed slot entirely.
    assert!(ix.holds(2));
    ix.forget(2, Some(&key(5)));
    assert!(!ix.holds(2));
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![]));
    // A version with no key is a candidate for every lookup.
    ix.insert(9, None);
    assert_eq!(indexes.plan(&eq(5), &schema, 100), Some(vec![9]));
    assert_eq!(indexes.plan(&eq(7), &schema, 100), Some(vec![1, 9]));
}

#[test]
fn a_broad_answer_is_declined() {
    let ix = index("n", Method::BTree, &[0]);
    let indexes = TableIndexes::new(vec![ix.clone()]);
    for slot in 0..1000u64 {
        let bytes = (slot as i64).to_le_bytes();
        ix.insert(slot, ix.key_of(|_| Some(&bytes[..])));
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
    let ix = index("ns", Method::Hash, &[0, 1]);
    let indexes = TableIndexes::new(vec![ix.clone()]);
    ix.insert(3, ix.key_of(|wo| Some(if wo == 0 { &[1, 0, 0, 0, 0, 0, 0, 0][..] } else { b"x" })));
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
