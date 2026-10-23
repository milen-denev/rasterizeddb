//! The native types the engine gained recently: `UUID`, `INET` and `DATETIME`.
//!
//! All three are fixed-width and bytewise-comparable by construction, so they
//! use the same fingerprint path as the integers and additionally carry zone
//! bounds — `memcmp` over `UUID`/`INET` octets is a total order, and `DATETIME`
//! compares as `i64`.
//!
//! Whether the bounds *earn their keep* depends entirely on the data:
//! append-ordered keys (timestamps, UUIDv7/ULID) prune hard, random ones
//! (UUIDv4) cannot prune at all. Both are asserted below.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::column::{ColumnFilter, bitmap_words};
use rasterizeddb_core::core::filter::hash::{HashSeed, canonical_hash, is_indexable};
use rasterizeddb_core::core::filter::sarg;
use rasterizeddb_core::core::filter::zonemap::{SargOp, ordered_width};
use rasterizeddb_core::core::filter::{FilterConfig, TableFilters};
use rasterizeddb_core::core::index_store::gate::{Proof, test_mint};
use rasterizeddb_core::core::row::row::RowWrite;
use rasterizeddb_core::core::tokenizer::query_tokenizer::tokenize;
use rasterizeddb_core::core::value_codec;
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

const NATIVE: &[DbType] = &[DbType::UUID, DbType::INET, DbType::DATETIME];

fn col(ty: DbType) -> ColumnFilter {
    ColumnFilter::new(0, ty, HashSeed::default(), FilterConfig::default())
}

fn live(bm: &[u64]) -> u64 {
    bm.iter().map(|w| w.count_ones() as u64).sum()
}

#[test]
fn all_three_are_indexed_and_carry_bounds() {
    for ty in NATIVE {
        assert!(is_indexable(ty), "{:?} must be indexable", ty);
        assert_eq!(
            ordered_width(ty),
            Some(ty.get_size() as usize),
            "{:?} must carry zone bounds at its full width",
            ty
        );
        assert!(col(ty.clone()).has_zonemap(), "{:?}", ty);
    }
    assert_eq!(DbType::UUID.get_size() as usize, value_codec::UUID_LEN);
    assert_eq!(DbType::INET.get_size() as usize, value_codec::INET_LEN);
    assert_eq!(DbType::DATETIME.get_size(), 8);
}

#[test]
fn text_literals_encode_to_the_stored_form() {
    // This is how a UUID or an IP is written in SQL, so extraction has to
    // reach the same bytes the comparison path will see.
    let seed = HashSeed::default();
    for (ty, text) in [
        (DbType::UUID, "11111111-2222-3333-4444-555555555555"),
        (DbType::INET, "192.168.0.1/24"),
    ] {
        let encoded = value_codec::encode_literal(&ty, text)
            .unwrap_or_else(|| panic!("{:?}: {} should encode", ty, text));
        assert_eq!(encoded.len(), ty.get_size() as usize);
        assert!(canonical_hash(&ty, &encoded, seed).is_some());

        // And going through the coercion path lands identically.
        let coerced =
            value_codec::coerce_literal(text.as_bytes(), &DbType::STRING, &ty).unwrap();
        assert_eq!(coerced, encoded, "{:?}: coerce and encode disagree", ty);
    }
}

#[test]
fn uuid_point_lookups_are_tight() {
    let n = 20_000u64;
    let mut c = col(DbType::UUID);
    let vals: Vec<Vec<u8>> = (0..n).map(|_| uuid_v4()).collect();
    for slot in 0..n {
        c.set(slot, Some(&vals[slot as usize]));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in (0..n).step_by(97) {
        c.probe_pred_into(SargOp::Eq, &vals[slot as usize], n, &mut bm);
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "slot {} lost its own UUID",
            slot
        );
        assert!(live(&bm) < n / 50, "UUID lookup left {} of {} candidates", live(&bm), n);
    }
}

#[test]
fn uuid_ranges_prune_only_when_the_keys_are_ordered() {
    // The practical consequence: UUIDv7/ULID gets range pruning, UUIDv4 does
    // not, and no amount of filter tuning changes that.
    let n = 20_000u64;

    let mut ordered = col(DbType::UUID);
    for slot in 0..n {
        ordered.set(slot, Some(&uuid_ordered(slot)));
    }
    let mut random = col(DbType::UUID);
    for slot in 0..n {
        random.set(slot, Some(&uuid_v4()));
    }

    let pivot = uuid_ordered(n * 9 / 10);
    let mut bm = vec![0u64; bitmap_words(n)];

    ordered.probe_pred_into(SargOp::Gt, &pivot, n, &mut bm);
    let ord = live(&bm);
    random.probe_pred_into(SargOp::Gt, &pivot, n, &mut bm);
    let rnd = live(&bm);

    assert!(ord < n / 4, "time-ordered UUIDs should prune, {} of {} left", ord, n);
    assert!(rnd > ord, "random UUIDs cannot prune by bounds ({} vs {})", rnd, ord);
}

#[test]
fn inet_distinguishes_family_address_and_prefix() {
    // Layout is [family(1)][address(16)][prefix(1)]. Two values differing only
    // in the prefix are distinct values and must not collide.
    let seed = HashSeed::default();
    let samples = inet_samples();
    for a in &samples {
        for b in &samples {
            if a == b {
                continue;
            }
            assert_ne!(
                canonical_hash(&DbType::INET, a, seed),
                canonical_hash(&DbType::INET, b, seed),
                "distinct INET values collided: {:?} vs {:?}",
                a,
                b
            );
        }
    }
}

#[test]
fn inet_lookups_find_their_own_value() {
    let n = 4000u64;
    let mut c = col(DbType::INET);
    let vals: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            let mut b = vec![0u8; 18];
            b[0] = if i % 2 == 0 { 4 } else { 6 };
            b[1..5].copy_from_slice(&(i as u32).to_be_bytes());
            b[17] = if i % 2 == 0 { 24 } else { 64 };
            b
        })
        .collect();
    for slot in 0..n {
        c.set(slot, Some(&vals[slot as usize]));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for slot in (0..n).step_by(31) {
        c.probe_pred_into(SargOp::Eq, &vals[slot as usize], n, &mut bm);
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "slot {} lost its own address",
            slot
        );
    }
}

#[test]
fn inet_bounds_sort_by_family_then_address() {
    // `memcmp` over the layout gives family-major ordering, so a range over one
    // family does not silently pull in the other.
    let n = 4000u64;
    let mut c = col(DbType::INET);
    for slot in 0..n {
        let mut b = vec![0u8; 18];
        b[0] = if slot < n / 2 { 4 } else { 6 };
        b[1..5].copy_from_slice(&(slot as u32).to_be_bytes());
        c.set(slot, Some(&b));
    }
    let mut pivot = vec![0u8; 18];
    pivot[0] = 6;
    let mut bm = vec![0u64; bitmap_words(n)];
    c.probe_pred_into(SargOp::Ge, &pivot, n, &mut bm);
    // Every v6 row must survive; the mask is a superset so v4 rows may too, but
    // it should still have narrowed something.
    for slot in (n / 2)..n {
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "v6 slot {} pruned from a `>= v6` range",
            slot
        );
    }
    assert!(live(&bm) < n, "family-major ordering should prune the v4 half");
}

#[test]
fn datetime_is_the_strongest_zone_map_candidate() {
    // Timestamps are append-ordered in practice, which is exactly the shape
    // bounds are good at.
    let n = 20_000u64;
    let base: i64 = 1_700_000_000_000;

    let mut c = col(DbType::DATETIME);
    for slot in 0..n {
        c.set(slot, Some(&(base + slot as i64 * 1000).to_le_bytes()));
    }

    let pivot = base + (n as i64 * 9 / 10) * 1000;
    let mut bm = vec![0u64; bitmap_words(n)];
    c.probe_pred_into(SargOp::Gt, &pivot.to_le_bytes(), n, &mut bm);

    let survivors = live(&bm);
    assert!(
        survivors < n / 5,
        "append-ordered timestamps should prune to ~10%, {} of {} left",
        survivors,
        n
    );
    for slot in (n * 9 / 10 + 1)..n {
        assert!(
            (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
            "slot {} lost from a timestamp range",
            slot
        );
    }
}

#[test]
fn datetime_ranges_have_no_false_negatives_out_of_order() {
    // Even with timestamps arriving shuffled, bounds must still bracket them.
    let n = 6000u64;
    let base: i64 = 1_700_000_000_000;
    let mut c = col(DbType::DATETIME);
    let vals: Vec<i64> = (0..n)
        .map(|_| base + fastrand::i64(-5_000_000..5_000_000))
        .collect();
    for slot in 0..n {
        c.set(slot, Some(&vals[slot as usize].to_le_bytes()));
    }
    let mut bm = vec![0u64; bitmap_words(n)];
    for _ in 0..60 {
        let pivot = base + fastrand::i64(-6_000_000..6_000_000);
        for (op, f) in [
            (SargOp::Gt, (|a: i64, b: i64| a > b) as fn(i64, i64) -> bool),
            (SargOp::Ge, |a, b| a >= b),
            (SargOp::Lt, |a, b| a < b),
            (SargOp::Le, |a, b| a <= b),
        ] {
            c.probe_pred_into(op, &pivot.to_le_bytes(), n, &mut bm);
            for slot in 0..n {
                if f(vals[slot as usize], pivot) {
                    assert!(
                        (bm[(slot / 64) as usize] >> (slot % 64)) & 1 == 1,
                        "slot {} pruned for {:?}",
                        slot,
                        op
                    );
                }
            }
        }
    }
}

#[test]
fn native_types_survive_a_checkpoint() {
    let ticket = test_mint();
    let proof = Proof::from(&ticket);
    let n = 5000u64;
    let fields = fields_of(&[
        ("uid", DbType::UUID),
        ("ip", DbType::INET),
        ("ts", DbType::DATETIME),
    ]);
    let f = TableFilters::new(&fields, FilterConfig::default());

    let uids: Vec<Vec<u8>> = (0..n).map(|_| uuid_v4()).collect();
    let base: i64 = 1_700_000_000_000;
    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..n as usize)
        .map(|i| {
            let mut ip = vec![0u8; 18];
            ip[0] = 4;
            ip[1..5].copy_from_slice(&(i as u32).to_be_bytes());
            ip[17] = 32;
            vec![
                (0u32, uids[i].clone()),
                (1u32, ip),
                (2u32, (base + i as i64 * 1000).to_le_bytes().to_vec()),
            ]
        })
        .collect();
    backfill_all(proof, &f, &rows);

    let bytes = f.checkpoint_bytes(n);
    let restored =
        TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n).expect("round trip");

    // Fingerprints survive.
    for slot in (0..n).step_by(53) {
        let hex = uids[slot as usize]
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();
        let text = format!(
            "{}-{}-{}-{}-{}",
            &hex[0..8],
            &hex[8..12],
            &hex[12..16],
            &hex[16..20],
            &hex[20..32]
        );
        let q = format!("uid = '{}'", text);
        let p = sarg::extract(&tokenize(&q, &fields), &restored)
            .unwrap_or_else(|| panic!("`{}` should be sargable", q));
        let bm = restored.evaluate(&p, n).expect("bitmap");
        assert!(bm.get(slot), "UUID at slot {} lost across checkpoint", slot);
    }

    // And so do the timestamp bounds.
    let q = format!("ts > {}", base + (n as i64 * 9 / 10) * 1000);
    let p = sarg::extract(&tokenize(&q, &fields), &restored).expect("range sargable");
    let bm = restored.evaluate(&p, n).expect("bitmap");
    for slot in (n * 9 / 10 + 1)..n {
        assert!(bm.get(slot), "timestamp slot {} lost across checkpoint", slot);
    }
    assert!(bm.count() < n / 2, "timestamp bounds lost selectivity");
}

#[tokio::test(flavor = "multi_thread")]
async fn native_types_work_end_to_end_through_a_real_table() {
    let dir = tempfile::tempdir().unwrap();
    let cols = vec![
        ("id", DbType::U64),
        ("uid", DbType::UUID),
        ("ip", DbType::INET),
        ("ts", DbType::DATETIME),
    ];
    let t = open_table(dir.path(), "native", &cols).await;

    let n = 1500u64;
    let base: i64 = 1_700_000_000_000;
    let uids: Vec<Vec<u8>> = (0..n).map(|_| uuid_v4()).collect();

    for i in 0..n {
        let mut ip = vec![0u8; 18];
        ip[0] = 4;
        ip[1..5].copy_from_slice(&(i as u32).to_be_bytes());
        ip[17] = 32;

        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data.push(payload(&uids[i as usize], 1, DbType::UUID));
        rw.columns_writing_data.push(payload(&ip, 2, DbType::INET));
        rw.columns_writing_data
            .push(payload(&(base + i as i64 * 1000).to_le_bytes(), 3, DbType::DATETIME));
        t.insert_row(rw).await.unwrap();
    }

    // A timestamp range must be exact.
    let cut = base + 1400 * 1000;
    let rows = run(&t, &format!("ts > {}", cut)).await;
    assert_eq!(u64s(&rows, 0), (1401..n).collect::<Vec<_>>(), "timestamp range");

    let rows = run(&t, &format!("ts <= {}", base + 4 * 1000)).await;
    assert_eq!(u64s(&rows, 0), (0..=4).collect::<Vec<_>>(), "timestamp range");

    // And the timestamp range must actually prune, not just answer correctly.
    let filters = t.filters().unwrap().as_ref().unwrap();
    let p = sarg::extract(&tokenize(&format!("ts > {}", cut), &t.schema.fields), filters)
        .expect("timestamp range should be sargable");
    let bm = filters.evaluate(&p, n).expect("timestamp range should prune");
    assert!(bm.count() < n / 2, "timestamp range left {} of {}", bm.count(), n);
    for slot in 1401..n {
        assert!(bm.get(slot), "slot {} pruned from a timestamp range", slot);
    }
}

// ---------------------------------------------------------------------------
// Case-insensitive operators
// ---------------------------------------------------------------------------

/// `IEQUALS` and friends break the hash invariant outright: `'ACME'` and
/// `'acme'` compare equal but hash differently, so a mask built from one would
/// prune rows holding the other.
///
/// They are safe only because `op_of` is an allowlist — these arrive as their
/// own operator strings and are never mapped to `SargOp::Eq`. This test uses
/// the **real** operator names rather than hypothetical ones, so it fails if a
/// future change routes a case-insensitive comparison through the equality
/// path.
#[test]
fn case_insensitive_operators_are_opaque_to_the_filter() {
    use rasterizeddb_core::core::tokenizer::query_tokenizer::Token;

    let (_fields, f) = filters_for(&[("id", DbType::U64), ("name", DbType::STRING)]);
    let ident = Token::Ident(("name".to_string(), DbType::STRING, 1));
    let lit = Token::StringLit("ACME".to_string());

    for op in ["IEQUALS", "ICONTAINS", "ISTARTSWITH", "IENDSWITH", "CONTAINS", "STARTSWITH", "ENDSWITH"] {
        let toks = vec![ident.clone(), Token::Op(op.to_string()), lit.clone()];
        assert!(
            sarg::extract(&toks, &f).is_none(),
            "`{}` must be opaque to the filter; mapping it to equality would \
             prune rows that match",
            op
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn case_insensitive_equality_keeps_all_its_rows() {
    // The end-to-end statement of the same property: a case-insensitive match
    // must return rows whose stored case differs from the literal's.
    let dir = tempfile::tempdir().unwrap();
    let cols = vec![("id", DbType::U64), ("name", DbType::STRING)];
    let t = open_table(dir.path(), "ci_equals", &cols).await;

    let names = ["acme", "ACME", "AcMe", "beta", "BETA"];
    for (i, n) in names.iter().enumerate() {
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&(i as u64).to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data.push(payload(n.as_bytes(), 1, DbType::STRING));
        t.insert_row(rw).await.unwrap();
    }

    // Case-sensitive equality: exactly one row.
    assert_eq!(u64s(&run(&t, "name = 'acme'").await, 0), vec![0]);

    // Case-insensitive: all three spellings, none pruned by the filter.
    let rows = run(&t, "name IEQUALS 'acme'").await;
    assert_eq!(
        u64s(&rows, 0),
        vec![0, 1, 2],
        "case-insensitive equality lost rows — the filter must not act on it"
    );
    let rows = run(&t, "name IEQUALS 'ACME'").await;
    assert_eq!(u64s(&rows, 0), vec![0, 1, 2]);
}

#[tokio::test(flavor = "multi_thread")]
async fn string_ordering_no_longer_panics_and_is_exact() {
    // `WHERE name < 'x'` used to panic a query worker. Now it is ordinary SQL,
    // and the filter leaves it alone because STRING carries no ordered bounds.
    let dir = tempfile::tempdir().unwrap();
    let cols = vec![("id", DbType::U64), ("name", DbType::STRING)];
    let t = open_table(dir.path(), "str_order", &cols).await;

    let names = ["alpha", "bravo", "charlie", "delta", "echo"];
    for (i, n) in names.iter().enumerate() {
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&(i as u64).to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data.push(payload(n.as_bytes(), 1, DbType::STRING));
        t.insert_row(rw).await.unwrap();
    }

    assert_eq!(u64s(&run(&t, "name < 'charlie'").await, 0), vec![0, 1]);
    assert_eq!(u64s(&run(&t, "name <= 'charlie'").await, 0), vec![0, 1, 2]);
    assert_eq!(u64s(&run(&t, "name > 'charlie'").await, 0), vec![3, 4]);
    assert_eq!(u64s(&run(&t, "name >= 'charlie'").await, 0), vec![2, 3, 4]);
}
