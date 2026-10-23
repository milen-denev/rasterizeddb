//! Fuzzing the predicate extractor.
//!
//! Token streams come from user SQL, so `extract` must survive anything: no
//! panic, no hang, and — most importantly — no mask that is narrower than the
//! truth. A random stream that happens to parse must still be sound.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::sarg;
use rasterizeddb_core::core::filter::zonemap::SargOp;
use rasterizeddb_core::core::filter::{FilterConfig, TableFilters};
use rasterizeddb_core::core::index_store::gate::{Proof, test_mint};
use rasterizeddb_core::core::tokenizer::query_tokenizer::{NumericValue, Token};
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

fn catalog() -> (SmallVec<[rasterizeddb_core::core::row::schema::SchemaField; 20]>, TableFilters) {
    filters_for(&[
        ("id", DbType::U64),
        ("cat", DbType::I32),
        ("name", DbType::STRING),
    ])
}

fn random_token() -> Token {
    match fastrand::usize(0..9) {
        0 => Token::Ident(("id".to_string(), DbType::U64, 0)),
        1 => Token::Ident(("cat".to_string(), DbType::I32, 1)),
        2 => Token::Ident(("name".to_string(), DbType::STRING, 2)),
        3 => Token::Number(NumericValue::U64(fastrand::u64(0..100))),
        4 => Token::Number(NumericValue::I32(fastrand::i32(-100..100))),
        5 => Token::StringLit("x".repeat(fastrand::usize(0..5))),
        6 => Token::Op(
            ["=", "!=", ">", ">=", "<", "<=", "+", "*", "IEQUALS", "CONTAINS"]
                [fastrand::usize(0..10)]
                .to_string(),
        ),
        7 => Token::Next(if fastrand::bool() { "AND" } else { "OR" }.to_string()),
        _ => {
            if fastrand::bool() {
                Token::LPar
            } else {
                Token::RPar
            }
        }
    }
}

#[test]
fn random_token_streams_never_panic() {
    let (_fields, f) = catalog();
    for _ in 0..20_000 {
        let len = fastrand::usize(0..14);
        let toks: Vec<Token> = (0..len).map(|_| random_token()).collect();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = sarg::extract(&toks, &f);
        }));
        assert!(r.is_ok(), "extract panicked on {:?}", toks);
    }
}

#[test]
fn random_token_streams_never_produce_an_unsound_mask() {
    // Whatever a random stream parses to, every predicate it yields must be one
    // of the five sound operators, on a column the catalog actually indexes,
    // with a literal at the column's exact width.
    let (fields, f) = catalog();
    for _ in 0..20_000 {
        let len = fastrand::usize(0..14);
        let toks: Vec<Token> = (0..len).map(|_| random_token()).collect();
        let Some(p) = sarg::extract(&toks, &f) else { continue };
        for pred in p.all_preds() {
            assert!(
                f.indexes(pred.write_order),
                "predicate on unindexed column {} from {:?}",
                pred.write_order,
                toks
            );
            assert!(
                matches!(
                    pred.op,
                    SargOp::Eq | SargOp::Gt | SargOp::Ge | SargOp::Lt | SargOp::Le
                ),
                "unsound operator {:?}",
                pred.op
            );
            let ty = &fields
                .iter()
                .find(|x| x.write_order as u32 == pred.write_order)
                .unwrap()
                .db_type;
            if *ty != DbType::STRING {
                assert_eq!(
                    pred.value.len(),
                    ty.get_size() as usize,
                    "literal width mismatch for {:?} from {:?}",
                    ty,
                    toks
                );
            }
        }
    }
}

#[test]
fn deeply_nested_parens_are_handled_without_stack_trouble() {
    let (fields, f) = catalog();
    for depth in [1usize, 8, 32, 64] {
        let inner = "id = 5";
        let q = format!("{}{}{}", "(".repeat(depth), inner, ")".repeat(depth));
        let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize(&q, &fields);
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sarg::extract(&toks, &f)));
        assert!(r.is_ok(), "depth {} panicked", depth);
        if let Ok(Some(p)) = r {
            assert_eq!(p.all_preds().len(), 1, "depth {}", depth);
        }
    }
}

#[test]
fn very_wide_disjunctions_are_handled() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    // An ORM `IN` list can be long; the desugared form is a wide OR.
    let (fields, f) = catalog();
    for width in [2usize, 16, 128, 512] {
        let q = (0..width)
            .map(|i| format!("id = {}", i))
            .collect::<Vec<_>>()
            .join(" OR ");
        let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize(&q, &fields);
        let p = sarg::extract(&toks, &f)
            .unwrap_or_else(|| panic!("width {} should be sargable", width));
        assert_eq!(p.all_preds().len(), width);

        let n = 4096u64;
        let rows: Vec<Vec<(u32, Vec<u8>)>> =
            (0..n as usize).map(|i| vec![(0u32, (i as u64).to_le_bytes().to_vec())]).collect();
        let f2 = TableFilters::new(&fields, FilterConfig::default());
        backfill_all(proof, &f2, &rows);
        let p2 = sarg::extract(&toks, &f2).unwrap();
        let bm = f2.evaluate(&p2, n).expect("bitmap");
        for i in 0..width as u64 {
            assert!(bm.get(i), "width {}: member {} pruned", width, i);
        }
    }
}

#[test]
fn very_long_conjunctions_are_handled() {
    let (fields, f) = catalog();
    for width in [2usize, 16, 128] {
        let q = (0..width)
            .map(|i| format!("id > {}", i))
            .collect::<Vec<_>>()
            .join(" AND ");
        let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize(&q, &fields);
        let p = sarg::extract(&toks, &f)
            .unwrap_or_else(|| panic!("width {} should be sargable", width));
        assert_eq!(p.all_preds().len(), width);
    }
}

#[test]
fn unbalanced_paren_streams_always_decline() {
    let (_fields, f) = catalog();
    let ident = Token::Ident(("id".to_string(), DbType::U64, 0));
    let eq = Token::Op("=".to_string());
    let num = Token::Number(NumericValue::U64(1));

    for extra_open in 0..4usize {
        for extra_close in 0..4usize {
            if extra_open == extra_close {
                continue;
            }
            let mut toks = Vec::new();
            for _ in 0..extra_open {
                toks.push(Token::LPar);
            }
            toks.extend([ident.clone(), eq.clone(), num.clone()]);
            for _ in 0..extra_close {
                toks.push(Token::RPar);
            }
            assert!(
                sarg::extract(&toks, &f).is_none(),
                "unbalanced ({} open, {} close) must decline",
                extra_open,
                extra_close
            );
        }
    }
}

#[test]
fn arbitrary_literal_bytes_never_break_extraction() {
    let (_fields, f) = catalog();
    for _ in 0..4000 {
        let len = fastrand::usize(0..300);
        let s: String = (0..len).map(|_| fastrand::char('\u{1}'..='\u{7f}')).collect();
        let toks = vec![
            Token::Ident(("name".to_string(), DbType::STRING, 2)),
            Token::Op("=".to_string()),
            Token::StringLit(s.clone()),
        ];
        let p = sarg::extract(&toks, &f).expect("string equality is always sargable");
        assert_eq!(p.all_preds()[0].value, s.as_bytes().to_vec());
    }
}

#[test]
fn extraction_is_deterministic() {
    // Same tokens must yield the same program, or a cached plan could diverge
    // from a fresh one.
    let (fields, f) = catalog();
    for q in [
        "id = 5",
        "id = 5 AND cat = 1",
        "id = 1 OR id = 2 AND cat = 3",
    ] {
        let toks = rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize(q, &fields);
        let a = sarg::extract(&toks, &f).unwrap();
        let b = sarg::extract(&toks, &f).unwrap();
        let av: Vec<_> = a.all_preds().iter().map(|p| (p.write_order, p.op, p.value.clone())).collect();
        let bv: Vec<_> = b.all_preds().iter().map(|p| (p.write_order, p.op, p.value.clone())).collect();
        assert_eq!(av, bv, "`{}` extracted differently across calls", q);
    }
}
