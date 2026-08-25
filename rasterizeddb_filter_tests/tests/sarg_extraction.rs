//! Predicate extraction: which shapes are acted on, which are refused, and why
//! refusing is the safe default.
//!
//! A mask must be a **superset** of true matches, so anything not understood
//! has to contribute "everything", never "nothing".

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::sarg::{self, MaskExpr};
use rasterizeddb_core::core::filter::zonemap::SargOp;
use rasterizeddb_core::core::filter::TableFilters;
use rasterizeddb_core::core::tokenizer::predicate_rewrite::rewrite_where;
use rasterizeddb_core::core::tokenizer::query_tokenizer::{NumericValue, Token, tokenize};
use rasterizeddb_core::core::value_codec;
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

fn setup() -> (SmallVec<[rasterizeddb_core::core::row::schema::SchemaField; 20]>, TableFilters) {
    filters_for(&[
        ("id", DbType::U64),
        ("cat", DbType::I32),
        ("label", DbType::STRING),
        ("amount", DbType::DECIMAL),
        ("uid", DbType::UUID),
        ("ip", DbType::INET),
        ("ts", DbType::DATETIME),
    ])
}

fn extract(q: &str) -> Option<sarg::SargProgram> {
    let (fields, f) = setup();
    sarg::extract(&tokenize(q, &fields), &f)
}

fn extract_rewritten(q: &str) -> Option<sarg::SargProgram> {
    let (fields, f) = setup();
    let r = rewrite_where(q, &fields).unwrap_or_else(|e| panic!("rewrite `{}`: {}", q, e));
    sarg::extract(&tokenize(&r, &fields), &f)
}

// ---------------------------------------------------------------------------
// Accepted shapes
// ---------------------------------------------------------------------------

#[test]
fn every_supported_operator_is_extracted() {
    for (q, want) in [
        ("id = 1", SargOp::Eq),
        ("id > 1", SargOp::Gt),
        ("id >= 1", SargOp::Ge),
        ("id < 1", SargOp::Lt),
        ("id <= 1", SargOp::Le),
    ] {
        let p = extract(q).unwrap_or_else(|| panic!("`{}` should be sargable", q));
        assert_eq!(p.all_preds()[0].op, want, "{}", q);
    }
}

#[test]
fn reversed_operands_are_mirrored_not_copied() {
    // `1990 < id` is `id > 1990`. Mirroring it the wrong way would prune
    // precisely the rows that match.
    for (q, want) in [
        ("1 < id", SargOp::Gt),
        ("1 <= id", SargOp::Ge),
        ("1 > id", SargOp::Lt),
        ("1 >= id", SargOp::Le),
        ("1 = id", SargOp::Eq),
    ] {
        let p = extract(q).unwrap_or_else(|| panic!("`{}` should be sargable", q));
        assert_eq!(p.all_preds()[0].op, want, "{}", q);
    }
}

#[test]
fn cross_typed_literals_are_coerced_not_rejected() {
    // The tokenizer types a literal from the *preceding* column, so a literal
    // rarely arrives matching the column it meets. Rejecting on mismatch left
    // these correct but entirely unfiltered — the silent no-pruning failure.
    for q in [
        "1990 < id",                                      // I32 literal, U64 column
        "amount > 200",                                   // I32 literal, DECIMAL column
        "uid = '11111111-2222-3333-4444-555555555555'",   // text literal, UUID column
        "ip = '192.168.0.1/24'",                          // text literal, INET column
    ] {
        assert!(extract(q).is_some(), "`{}` should be sargable", q);
    }
}

#[test]
fn coerced_bytes_match_the_value_codec_exactly() {
    // If extraction encoded a literal differently from the comparison path, the
    // fingerprint would not match and the row would be pruned.
    let cases: Vec<(&str, DbType, Vec<u8>)> = vec![
        (
            "uid = '11111111-2222-3333-4444-555555555555'",
            DbType::UUID,
            value_codec::encode_literal(&DbType::UUID, "11111111-2222-3333-4444-555555555555")
                .expect("uuid text encodes"),
        ),
        (
            "amount > 200",
            DbType::DECIMAL,
            value_codec::coerce_literal(&200i32.to_le_bytes(), &DbType::I32, &DbType::DECIMAL)
                .expect("i32 coerces to DECIMAL"),
        ),
        ("1990 < id", DbType::U64, 1990u64.to_le_bytes().to_vec()),
    ];
    for (q, ty, expected) in cases {
        let p = extract(q).unwrap();
        assert_eq!(p.all_preds()[0].value, expected, "`{}` encoded differently", q);
        assert_eq!(
            p.all_preds()[0].value.len(),
            ty.get_size() as usize,
            "`{}`: coerced literal is the wrong width",
            q
        );
    }
}

#[test]
fn uncoercible_literals_are_declined_not_guessed() {
    // Hashing garbage would prune real rows.
    for q in ["uid = 'not-a-uuid'", "ip = 'not-an-ip'"] {
        assert!(extract(q).is_none(), "`{}` should be declined", q);
    }
}

#[test]
fn string_equality_extracts() {
    let p = extract("label = 'bob'").unwrap();
    assert_eq!(p.all_preds()[0].value, b"bob".to_vec());
    assert_eq!(p.all_preds()[0].op, SargOp::Eq);
}

// ---------------------------------------------------------------------------
// Refused shapes
// ---------------------------------------------------------------------------

#[test]
fn inequality_is_refused() {
    // probe(v) over-approximates `= v`, so its complement UNDER-approximates
    // `!= v` and would drop real matches. No filter can answer it.
    for q in ["id != 5", "label != 'x'", "5 != id"] {
        assert!(extract(q).is_none(), "`{}` must be refused", q);
    }
}

#[test]
fn unknown_operators_are_opaque() {
    // The allowlist is what makes adding a case-insensitive operator elsewhere
    // safe: `ILIKE` breaks the hash invariant, and being unrecognised it
    // collapses to "everything" rather than losing rows.
    let (_fields, f) = setup();
    let ident = Token::Ident(("label".to_string(), DbType::STRING, 2));
    let lit = Token::StringLit("acme".to_string());
    for op in ["ILIKE", "LIKE", "~", "~*", "SIMILAR TO", "IS DISTINCT FROM", "@>", "&&", "!="] {
        let toks = vec![ident.clone(), Token::Op(op.to_string()), lit.clone()];
        assert!(
            sarg::extract(&toks, &f).is_none(),
            "`{}` must be opaque to the filter",
            op
        );
    }
}

#[test]
fn string_ranges_are_extracted_as_ranges() {
    // STRING carries truncated bounds, so a range on it is a real predicate
    // rather than something extraction has to give up on.
    for (q, want) in [
        ("label >= 'm'", SargOp::Ge),
        ("label > 'm'", SargOp::Gt),
        ("label < 'm'", SargOp::Lt),
        ("label <= 'm'", SargOp::Le),
    ] {
        let p = extract(q).unwrap_or_else(|| panic!("`{}` should be sargable", q));
        assert_eq!(p.all_preds()[0].op, want, "{}", q);
        assert_eq!(p.all_preds()[0].value, b"m".to_vec(), "{}", q);
    }

    // ANDed with something filterable, both halves contribute.
    let (fields, f) = setup();
    let p = sarg::extract(&tokenize("id = 5 AND label >= 'm'", &fields), &f)
        .expect("both halves should be sargable");
    assert_eq!(p.all_preds().len(), 2);
    let bm = f.evaluate(&p, 1000).expect("the id half should prune");
    assert!(bm.count() <= 1000);
}

// ---------------------------------------------------------------------------
// Prefix matching
// ---------------------------------------------------------------------------

#[test]
fn startswith_lowers_to_a_half_open_range() {
    // `LIKE 'ab%'` desugars to `label STARTSWITH 'ab'`, and this is the only
    // way a prefix match ever prunes: a fingerprint hashes the whole value, so
    // it cannot answer a question about part of one.
    let p = extract_rewritten("label LIKE 'ab%'").expect("a prefix LIKE should be sargable");
    let preds = p.all_preds();
    assert_eq!(preds.len(), 2, "one bound each side");
    let lo = preds.iter().find(|x| x.op == SargOp::Ge).expect("a lower bound");
    let hi = preds.iter().find(|x| x.op == SargOp::Lt).expect("a strict upper bound");
    assert_eq!(lo.value, b"ab".to_vec());
    assert_eq!(hi.value, b"ac".to_vec());
    assert_eq!(lo.write_order, hi.write_order);
}

#[test]
fn startswith_is_written_directly_too() {
    let p = extract("label STARTSWITH 'ab'").expect("STARTSWITH should be sargable");
    assert_eq!(p.all_preds().len(), 2);
}

#[test]
fn a_high_byte_prefix_still_gets_both_bounds() {
    // A SQL literal is a `String`, so the all-`0xFF` prefix that has no
    // successor is unreachable from a query — valid UTF-8 tops out at `0xF4`.
    // The `successor` fallback still exists for stored bytes (asserted directly
    // in the `zonemap` suite); what matters here is that a prefix ending on a
    // high byte carries rather than truncating.
    let p = extract("label STARTSWITH 'aÿ'").expect("STARTSWITH should be sargable");
    let preds = p.all_preds();
    assert_eq!(preds.len(), 2);
    let lo = preds.iter().find(|x| x.op == SargOp::Ge).unwrap();
    let hi = preds.iter().find(|x| x.op == SargOp::Lt).unwrap();
    assert_eq!(lo.value, "aÿ".as_bytes().to_vec());
    assert!(hi.value > lo.value, "the upper bound must be above the lower one");
    // Every string with this prefix falls inside the range.
    for tail in ["", "a", "\u{10FFFF}"] {
        let s = format!("aÿ{}", tail).into_bytes();
        assert!(s >= lo.value, "{:?} below the lower bound", s);
        assert!(s < hi.value, "{:?} at or above the upper bound", s);
    }
}

#[test]
fn an_empty_prefix_narrows_nothing() {
    // Every string starts with `''`, so the predicate carries no information —
    // which is different from being one we failed to parse, but reaches the
    // same place.
    assert!(extract("label STARTSWITH ''").is_none());
}

#[test]
fn case_insensitive_and_unanchored_matches_are_refused() {
    // `ISTARTSWITH` (from `ILIKE 'ab%'`) folds case, which no byte range can
    // express: `'a'` and `'A'` are 32 apart and every range holding both holds
    // most of ASCII besides. `CONTAINS` and `ENDSWITH` are not bounded by any
    // prefix of the value at all.
    for q in [
        "label ISTARTSWITH 'ab'",
        "label CONTAINS 'ab'",
        "label ENDSWITH 'ab'",
        "label ICONTAINS 'ab'",
        "label IENDSWITH 'ab'",
    ] {
        assert!(extract(q).is_none(), "`{}` must be refused", q);
    }
    assert!(
        extract_rewritten("label ILIKE 'ab%'").is_none(),
        "ILIKE lowers to the case-insensitive operator and must stay opaque"
    );
}

#[test]
fn startswith_on_a_non_string_column_is_refused() {
    // `perform_comparison_operation` returns `UnsupportedType` for a prefix test
    // on a number, which fails the query. Pruning on that basis would hide a
    // query error behind a short result set.
    for q in ["id STARTSWITH 'ab'", "amount STARTSWITH '1'"] {
        assert!(extract(q).is_none(), "`{}` must be refused", q);
    }
}

#[test]
fn unbalanced_parens_are_declined() {
    for q in ["(id = 1 OR id = 2", "id = 1)", "((id = 1)"] {
        assert!(extract(q).is_none(), "`{}` must be declined", q);
    }
}

#[test]
fn arithmetic_operands_are_opaque_not_misparsed() {
    // `(a + 1) = 5` also starts with `(`, but the group is an operand rather
    // than a boolean sub-expression.
    for q in ["(id + 1) = 5", "(cat * 2) > 4"] {
        assert!(extract(q).is_none(), "`{}` should be opaque", q);
    }
    // And it must not corrupt a sibling conjunct.
    let p = extract("id = 7 AND (cat + 1) = 5");
    if let Some(p) = p {
        assert_eq!(p.all_preds().len(), 1, "only `id = 7` should be extracted");
        assert_eq!(p.all_preds()[0].value, 7u64.to_le_bytes().to_vec());
    }
}

#[test]
fn an_or_branch_that_narrows_nothing_declines_the_whole_program() {
    // A union with an unknown branch is itself unknown, so building a bitmap
    // would be wasted work.
    for q in ["id = 1 OR id != 2", "id != 1 OR id = 2", "id = 1 OR (id != 2 AND id != 3)"] {
        assert!(extract(q).is_none(), "`{}` should decline", q);
    }
}

// ---------------------------------------------------------------------------
// Boolean structure
// ---------------------------------------------------------------------------

#[test]
fn precedence_matches_the_engine() {
    // `A AND B OR C AND D` is (A∧B) ∨ (C∧D). Folding left-to-right would give
    // a mask that is not a superset.
    match extract("id = 1 AND cat = 2 OR id = 3 AND cat = 4").unwrap().root {
        MaskExpr::Or(v) => {
            assert_eq!(v.len(), 2);
            for b in &v {
                assert!(matches!(b, MaskExpr::And(i) if i.len() == 2));
            }
        }
        other => panic!("expected Or, got {:?}", other),
    }

    match extract("id = 1 OR id = 2 AND cat = 3").unwrap().root {
        MaskExpr::Or(v) => {
            assert_eq!(v.len(), 2);
            assert!(matches!(v[0], MaskExpr::Pred(_)));
            assert!(matches!(&v[1], MaskExpr::And(i) if i.len() == 2));
        }
        other => panic!("expected Or, got {:?}", other),
    }
}

/// Parentheses are honoured by the evaluator, so extraction acts on them too.
///
/// It did not always: while the evaluator flattened boolean structure, acting
/// on the parens here made the mask narrower than what the evaluator returned,
/// and the filter changed results rather than only their cost. Extraction had
/// to decline the shape until the evaluator learned to reconstruct bracketing.
#[test]
fn or_groups_combined_with_other_terms_are_filterable() {
    for q in [
        "id = 1 AND (id = 2 OR cat = 3)",
        "(id = 1 OR cat = 3) AND id = 2",
        "(id = 1 AND (cat = 2 OR cat = 3))",
        "((id = 1 OR id = 2) AND (cat = 3 OR cat = 4))",
        "(id = 1 OR id = 2) OR cat = 3",
        "id = 1 AND (id = 2 OR id = 3 OR id = 4)",
    ] {
        assert!(extract(q).is_some(), "`{}` should be sargable", q);
    }
}

#[test]
fn safe_paren_shapes_stay_filterable() {
    // A whole-clause group, and AND-only groups anywhere, both flatten exactly.
    for q in [
        "(id = 1 OR id = 2)",
        "((id = 1))",
        "((id = 1 OR id = 2))",
        "(id >= 1 AND id <= 9)",
        "cat = 3 AND (id >= 1 AND id <= 9)",
        "(id >= 1 AND id <= 9) AND cat = 3",
    ] {
        assert!(extract(q).is_some(), "`{}` should stay sargable", q);
    }
}

#[test]
fn an_unknown_conjunct_leaves_the_rest_filterable() {
    // `A AND unknown` -> mask(A), still a superset.
    for q in [
        "id = 1 AND id != 2",
        "id = 1 AND (cat + 1) = 5",
        "id = 1 AND label != 'x'",
    ] {
        let p = extract(q).unwrap_or_else(|| panic!("`{}`: the filterable half should survive", q));
        assert_eq!(p.all_preds().len(), 1, "{}", q);
        assert_eq!(p.all_preds()[0].value, 1u64.to_le_bytes().to_vec(), "{}", q);
    }
}

#[test]
fn empty_and_trivial_inputs_decline() {
    assert!(extract("").is_none());
    let (_f, filters) = setup();
    assert!(sarg::extract(&[], &filters).is_none());
}

#[test]
fn predicates_on_unindexed_columns_contribute_nothing() {
    // `BYTEA` has no comparator, so it is not indexed, and a predicate on it
    // must not invalidate the sibling it is ANDed with.
    let (fields, f) = filters_for(&[("id", DbType::U64), ("blob", DbType::BYTEA)]);
    assert!(!f.indexes(1), "BYTEA should not be indexed");
    let p = sarg::extract(&tokenize("id = 3 AND blob = 1", &fields), &f);
    if let Some(p) = p {
        assert_eq!(p.all_preds().len(), 1);
    }
}

#[test]
fn a_bool_predicate_is_extracted_alongside_its_sibling() {
    // BOOL is indexed (for bounds), so both conjuncts are predicates now. This
    // is the shape `predicate_rewrite` produces for every comparison against a
    // nullable column: `(__null_c = 0 AND c = 'BG')`.
    let (fields, f) = filters_for(&[("id", DbType::U64), ("flag", DbType::BOOL)]);
    assert!(f.indexes(1), "BOOL is indexed for its zone bounds");
    let p = sarg::extract(&tokenize("id = 3 AND flag = 1", &fields), &f)
        .expect("both conjuncts are sargable");
    assert_eq!(p.all_preds().len(), 2);
    let flag = p.all_preds().into_iter().find(|x| x.write_order == 1).unwrap();
    assert_eq!(flag.op, SargOp::Eq);
    assert_eq!(flag.value, vec![1u8], "the literal is coerced to a one-byte BOOL");
}

// ---------------------------------------------------------------------------
// Desugared forms
// ---------------------------------------------------------------------------

#[test]
fn in_lists_become_indexable_equalities() {
    let p = extract_rewritten("id IN (1, 2, 3)").expect("IN should be sargable");
    assert_eq!(p.all_preds().len(), 3);
    assert!(p.all_preds().iter().all(|x| x.op == SargOp::Eq));
}

#[test]
fn between_becomes_two_bounds() {
    let p = extract_rewritten("id BETWEEN 10 AND 20").expect("BETWEEN should be sargable");
    assert_eq!(p.all_preds().len(), 2);
    let ops: Vec<SargOp> = p.all_preds().iter().map(|x| x.op).collect();
    assert!(ops.contains(&SargOp::Ge) && ops.contains(&SargOp::Le));
}

#[test]
fn not_between_becomes_two_ranges_and_stays_filterable() {
    let p = extract_rewritten("id NOT BETWEEN 10 AND 20").expect("NOT BETWEEN is two ranges");
    assert_eq!(p.all_preds().len(), 2);
    let ops: Vec<SargOp> = p.all_preds().iter().map(|x| x.op).collect();
    assert!(ops.contains(&SargOp::Lt) && ops.contains(&SargOp::Gt));
}

#[test]
fn not_in_declines_because_every_branch_is_an_inequality() {
    assert!(extract_rewritten("id NOT IN (1, 2, 3)").is_none());
}

#[test]
fn in_combined_with_another_predicate_keeps_both() {
    let p = extract_rewritten("cat = 1 AND id IN (4, 5)").expect("should be sargable");
    assert_eq!(p.all_preds().len(), 3);
    let p = extract_rewritten("id IN (4, 5, 6) AND cat = 1").expect("should be sargable");
    assert_eq!(p.all_preds().len(), 4);
}

#[test]
fn not_in_combined_keeps_the_filterable_half() {
    // `NOT IN` desugars to a parenthesised AND-only group, which flattens
    // exactly, so the `cat = 1` half still applies.
    let p = extract_rewritten("cat = 1 AND id NOT IN (4, 5)").expect("should be sargable");
    assert_eq!(p.all_preds().len(), 1);
    assert_eq!(p.all_preds()[0].op, SargOp::Eq);
}

#[test]
fn qualified_column_names_resolve() {
    // ORMs write `"users"."id"`; the rewrite strips the qualifier.
    for q in ["t.id = 5", "\"t\".\"id\" = 5"] {
        let (fields, f) = setup();
        if let Ok(r) = rewrite_where(q, &fields) {
            assert!(
                sarg::extract(&tokenize(&r, &fields), &f).is_some(),
                "`{}` -> `{}` should be sargable",
                q,
                r
            );
        }
    }
}

#[test]
fn numeric_literal_variants_all_coerce_to_the_column() {
    // Whatever the tokenizer picked for the literal, extraction must land on
    // the column's width.
    let (_fields, f) = setup();
    let variants = [
        NumericValue::I8(7),
        NumericValue::I16(7),
        NumericValue::I32(7),
        NumericValue::I64(7),
        NumericValue::U8(7),
        NumericValue::U16(7),
        NumericValue::U32(7),
        NumericValue::U64(7),
    ];
    for nv in variants {
        let toks = vec![
            Token::Ident(("id".to_string(), DbType::U64, 0)),
            Token::Op("=".to_string()),
            Token::Number(nv.clone()),
        ];
        let p = sarg::extract(&toks, &f)
            .unwrap_or_else(|| panic!("{:?} should coerce to U64", nv));
        assert_eq!(p.all_preds()[0].value, 7u64.to_le_bytes().to_vec(), "{:?}", nv);
    }
}

/// A parenthesised group that is an **operand** rather than a boolean
/// sub-expression must stay opaque, whatever is inside it.
///
/// `(a + 1) = 5` is the case that already existed. A correlated scalar
/// subquery — `(SELECT COUNT(*) FROM b WHERE b.x = a.y) = 1` — is the same
/// shape with unfamiliar tokens in the middle, and it is arriving: another
/// session is implementing it now.
///
/// The hazard is specific. The parser sees `(`, parses the inside as a boolean
/// expression, and finds `b.x = a.y` in there. If it acted on that group it
/// would build a mask for the *subquery's* predicate and apply it to the
/// **outer** table's slots — a mask that is not a superset of the outer match
/// set, which drops rows. What stops it is the operator after the `)`, which
/// proves the group was an operand; this pins that guard against a syntax it
/// has never seen.
#[test]
fn a_subquery_shaped_operand_is_opaque_not_misparsed() {
    let (_fields, f) = setup();

    // The essential shape: a group whose contents are a *perfectly valid*
    // predicate on an indexed column, used as an operand. Acting on it would
    // build a mask for the subquery's predicate and apply it to the outer
    // table's slots.
    let group = vec![
        Token::LPar,
        Token::Ident(("cat".to_string(), DbType::I32, 1)),
        Token::Op("=".to_string()),
        Token::Number(NumericValue::I32(3)),
        Token::RPar,
        Token::Op("=".to_string()),
        Token::Number(NumericValue::I32(1)),
    ];
    assert!(
        sarg::extract(&group, &f).is_none(),
        "a valid predicate used as an operand must narrow nothing"
    );

    // Control: the same group *without* the trailing operator is a boolean
    // sub-expression and must still extract, or the guard is too greedy.
    assert!(
        sarg::extract(&group[..5], &f).is_some(),
        "`(cat = 3)` on its own is a boolean group and should still extract"
    );

    // Standing alone, the case above is refused by the trailing-token check
    // rather than by the operand guard — two independent defences, and only one
    // of them is exercised there. Mid-stream is where the guard alone decides:
    // the sibling `id = 7` must survive *and* be the only predicate. Assert
    // success rather than absence, or removing the guard passes here too by
    // failing the whole extraction instead.
    let mut mid = vec![
        Token::Ident(("id".to_string(), DbType::U64, 0)),
        Token::Op("=".to_string()),
        Token::Number(NumericValue::U64(7)),
        Token::Next("AND".to_string()),
    ];
    mid.extend(group.iter().cloned());
    let p = sarg::extract(&mid, &f)
        .expect("the `id = 7` half must survive an operand group beside it");
    assert_eq!(p.all_preds().len(), 1, "only `id = 7` may be extracted");
    assert_eq!(p.all_preds()[0].write_order, 0);
    assert_eq!(p.all_preds()[0].value, 7u64.to_le_bytes().to_vec());

    // And the realistic token soup a subquery actually produces.
    let inner = vec![
        Token::Ident(("SELECT".to_string(), DbType::STRING, 0)),
        Token::Ident(("COUNT".to_string(), DbType::STRING, 0)),
        Token::Ident(("cat".to_string(), DbType::I32, 1)),
        Token::Op("=".to_string()),
        Token::Number(NumericValue::I32(3)),
    ];

    let mut toks = vec![Token::LPar];
    toks.extend(inner.iter().cloned());
    toks.push(Token::RPar);
    toks.push(Token::Op("=".to_string()));
    toks.push(Token::Number(NumericValue::I32(1)));
    assert!(
        sarg::extract(&toks, &f).is_none(),
        "a subquery used as an operand must narrow nothing"
    );

    // And the same, mid-stream, with the token soup a real subquery produces.
    let mut toks = vec![
        Token::Ident(("id".to_string(), DbType::U64, 0)),
        Token::Op("=".to_string()),
        Token::Number(NumericValue::U64(7)),
        Token::Next("AND".to_string()),
        Token::LPar,
    ];
    toks.extend(inner.iter().cloned());
    toks.push(Token::RPar);
    toks.push(Token::Op("=".to_string()));
    toks.push(Token::Number(NumericValue::I32(1)));

    let p = sarg::extract(&toks, &f).expect("the `id = 7` half must survive");
    assert_eq!(p.all_preds().len(), 1, "only `id = 7` may be extracted");
    assert_eq!(p.all_preds()[0].value, 7u64.to_le_bytes().to_vec());
}
