//! Checkpoints: round-tripping, and every path that must reject a sidecar.
//!
//! The sidecar is derived data. Every rejection costs one rebuild scan and can
//! never cost an answer — so the tests here care that rejection *happens*, not
//! that it is avoided.

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::filter::persist::{self, LoadReject};
use rasterizeddb_core::core::filter::sarg;
use rasterizeddb_core::core::filter::zonemap::SargOp;
use rasterizeddb_core::core::filter::{FilterConfig, TableFilters};
use rasterizeddb_core::core::index_store::gate::{Proof, test_mint};
use rasterizeddb_core::core::row::schema::SchemaField;
use rasterizeddb_core::core::tokenizer::query_tokenizer::{Token, tokenize};
use rasterizeddb_filter_tests::*;
use smallvec::SmallVec;

fn spec() -> Vec<(&'static str, DbType)> {
    vec![
        ("id", DbType::U64),
        ("label", DbType::STRING),
        ("cat", DbType::I32),
        ("uid", DbType::UUID),
    ]
}

fn filled(n: u64) -> (SmallVec<[SchemaField; 20]>, TableFilters, Vec<u64>, Vec<Vec<u8>>) {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let (fields, f) = filters_for(&spec());
    let ids: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    let uids: Vec<Vec<u8>> = (0..n).map(|_| uuid_v4()).collect();
    let rows: Vec<Vec<(u32, Vec<u8>)>> = (0..n as usize)
        .map(|i| {
            vec![
                (0u32, ids[i].to_le_bytes().to_vec()),
                (1u32, format!("l{}", i).into_bytes()),
                (2u32, ((i % 97) as i32).to_le_bytes().to_vec()),
                (3u32, uids[i].clone()),
            ]
        })
        .collect();
    backfill_all(proof, &f, &rows);
    (fields, f, ids, uids)
}

fn probe(f: &TableFilters, fields: &SmallVec<[SchemaField; 20]>, q: &str, n: u64) -> Option<Vec<bool>> {
    let p = sarg::extract(&tokenize(q, fields), f)?;
    let bm = f.evaluate(&p, n)?;
    Some((0..n).map(|s| bm.get(s)).collect())
}

#[test]
fn round_trip_loses_no_rows() {
    let n = 4000u64;
    let (fields, f, ids, _uids) = filled(n);
    let bytes = f.checkpoint_bytes(n);
    let restored =
        TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n).expect("round trip");

    for slot in 0..n {
        let q = format!("id = {}", ids[slot as usize]);
        let bits = probe(&restored, &fields, &q, n).expect("sargable");
        assert!(bits[slot as usize], "slot {} lost across a checkpoint", slot);
    }
}

#[test]
fn round_trip_preserves_pruning_power() {
    let n = 4000u64;
    let (fields, f, ids, _) = filled(n);
    let before = probe(&f, &fields, &format!("id = {}", ids[1234]), n).unwrap();
    let bytes = f.checkpoint_bytes(n);
    let restored =
        TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n).unwrap();
    let after = probe(&restored, &fields, &format!("id = {}", ids[1234]), n).unwrap();

    let (b, a) = (
        before.iter().filter(|x| **x).count(),
        after.iter().filter(|x| **x).count(),
    );
    assert_eq!(b, a, "restored mask differs from the live one");
    assert!(a < (n / 10) as usize, "restored filter barely prunes: {}", a);
    assert_eq!(restored.resume_slot(), n, "checkpoint should cover every row");
}

#[test]
fn round_trip_preserves_zone_bounds() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    // Bounds live alongside the fingerprints and must survive too.
    let n = 6000u64;
    let (fields, f) = filters_for(&[("id", DbType::U64)]);
    let rows: Vec<Vec<(u32, Vec<u8>)>> =
        (0..n as usize).map(|i| vec![(0u32, (i as u64).to_le_bytes().to_vec())]).collect();
    backfill_all(proof, &f, &rows);

    let bytes = f.checkpoint_bytes(n);
    let restored =
        TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n).unwrap();

    let bits = probe(&restored, &fields, "id > 5800", n).expect("range should be sargable");
    for slot in 5801..n {
        assert!(bits[slot as usize], "slot {} lost across checkpoint", slot);
    }
    let live = bits.iter().filter(|x| **x).count();
    assert!(live < (n / 2) as usize, "bounds lost selectivity: {}", live);
}

#[test]
fn round_trip_survives_generation_boundaries() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    let cfg = FilterConfig { min_f: 4, default_f: 4, max_f: 16, ..Default::default() };
    let fields = fields_of(&[("id", DbType::U64)]);
    let f = TableFilters::new(&fields, cfg);
    let n = 30_000u64;
    let ids: Vec<u64> = (0..n).map(|_| fastrand::u64(..)).collect();
    let rows: Vec<Vec<(u32, Vec<u8>)>> =
        (0..n as usize).map(|i| vec![(0u32, ids[i].to_le_bytes().to_vec())]).collect();
    backfill_all(proof, &f, &rows);
    assert!(f.stats()[0].1 > 4, "f should have grown");

    let bytes = f.checkpoint_bytes(n);
    let restored = TableFilters::from_checkpoint(&bytes, &fields, cfg, n).unwrap();
    for slot in (0..n).step_by(11) {
        let bits = probe(&restored, &fields, &format!("id = {}", ids[slot as usize]), n).unwrap();
        assert!(bits[slot as usize], "slot {} lost across generations", slot);
    }
}

#[test]
fn a_modification_after_a_checkpoint_pulls_the_frontier_back() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    // The checkpoint's bytes for that region could be stale, so it must not
    // claim to cover it.
    let n = 4000u64;
    let (fields, f, _, _) = filled(n);
    let _ = f.checkpoint_bytes(n);
    f.on_delete(proof, 1500);

    let bytes = f.checkpoint_bytes(n);
    let restored =
        TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n).unwrap();
    assert!(
        restored.resume_slot() <= 1500,
        "frontier {} still covers a slot modified after the checkpoint",
        restored.resume_slot()
    );
}

#[test]
fn the_dirty_byte_rejects_the_whole_checkpoint() {
    let n = 1000u64;
    let (fields, f, _, _) = filled(n);
    let mut bytes = f.checkpoint_bytes(n);
    bytes[persist::DIRTY_OFFSET as usize] = 1;
    assert_eq!(
        TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n)
            .err()
            .unwrap(),
        LoadReject::Dirty
    );
}

#[test]
fn header_corruption_is_rejected() {
    let n = 1000u64;
    let (fields, f, _, _) = filled(n);
    let good = f.checkpoint_bytes(n);
    let cfg = FilterConfig::default();
    let load = |b: &[u8]| TableFilters::from_checkpoint(b, &fields, cfg, n).err().unwrap();

    assert_eq!(load(&[]), LoadReject::Empty);

    let mut bad = good.clone();
    bad[0] = b'X';
    assert_eq!(load(&bad), LoadReject::BadMagic);

    let mut bad = good.clone();
    bad[8..10].copy_from_slice(&99u16.to_le_bytes());
    assert_eq!(load(&bad), LoadReject::Version(99));

    // Zone geometry: slot-to-zone arithmetic would differ.
    let mut bad = good.clone();
    bad[10..12].copy_from_slice(&256u16.to_le_bytes());
    assert_eq!(load(&bad), LoadReject::Geometry);

    // Pointer record length: `slot = offset / len`, so every fingerprint would
    // describe a different row. Perturb rather than hardcode, because 21 and 92
    // swap roles with `enable_long_row`.
    let mut bad = good.clone();
    bad[12] = bad[12].wrapping_add(1);
    assert_eq!(load(&bad), LoadReject::RecordLen);

    // A different seed makes every fingerprint meaningless.
    let mut bad = good.clone();
    bad[16..24].copy_from_slice(&0xDEAD_BEEFu64.to_le_bytes());
    assert_eq!(load(&bad), LoadReject::Seed);
    let mut bad = good.clone();
    bad[24..32].copy_from_slice(&0xDEAD_BEEFu64.to_le_bytes());
    assert_eq!(load(&bad), LoadReject::Seed);
}

#[test]
fn truncation_at_any_length_is_rejected() {
    let n = 1200u64;
    let (fields, f, _, _) = filled(n);
    let good = f.checkpoint_bytes(n);
    let cfg = FilterConfig::default();

    let mut cuts: Vec<usize> = vec![1, 2, 7, 8, 16, 31, 39, 40, 63, 64, 65, 100, 300];
    cuts.push(good.len() / 3);
    cuts.push(good.len() / 2);
    cuts.push(good.len() * 3 / 4);
    cuts.push(good.len() - 1);

    for cut in cuts {
        if cut >= good.len() {
            continue;
        }
        let r = TableFilters::from_checkpoint(&good[..cut], &fields, cfg, n);
        assert!(r.is_err(), "a checkpoint truncated to {} bytes was accepted", cut);
    }
}

#[test]
fn random_body_corruption_never_panics() {
    // Body bytes are lengths and plane words; a corrupt length must not drive a
    // huge allocation or an out-of-bounds read.
    let n = 800u64;
    let (fields, f, _, _) = filled(n);
    let good = f.checkpoint_bytes(n);
    let cfg = FilterConfig::default();

    for _ in 0..400 {
        let mut bad = good.clone();
        // Leave the superblock alone so we exercise the body's own validation.
        let i = fastrand::usize(persist::SUPERBLOCK_LEN..bad.len());
        bad[i] ^= 1 << fastrand::u32(0..8);
        let r = std::panic::catch_unwind(|| {
            let _ = TableFilters::from_checkpoint(&bad, &fields, cfg, n);
        });
        assert!(r.is_ok(), "corrupting byte {} panicked", i);
    }
}

#[test]
fn schema_changes_are_rejected() {
    let n = 500u64;
    let (_fields, f, _, _) = filled(n);
    let bytes = f.checkpoint_bytes(n);
    let cfg = FilterConfig::default();

    // A column removed.
    let narrower = fields_of(&[("id", DbType::U64), ("label", DbType::STRING)]);
    assert_eq!(
        TableFilters::from_checkpoint(&bytes, &narrower, cfg, n).err().unwrap(),
        LoadReject::SchemaMismatch
    );

    // A column added.
    let wider = fields_of(&[
        ("id", DbType::U64),
        ("label", DbType::STRING),
        ("cat", DbType::I32),
        ("uid", DbType::UUID),
        ("extra", DbType::I64),
    ]);
    assert_eq!(
        TableFilters::from_checkpoint(&bytes, &wider, cfg, n).err().unwrap(),
        LoadReject::SchemaMismatch
    );

    // A column retyped: `canonical_hash` is type-dispatched, so every stored
    // fingerprint would be from a different hash function.
    let retyped = fields_of(&[
        ("id", DbType::U64),
        ("label", DbType::STRING),
        ("cat", DbType::I64),
        ("uid", DbType::UUID),
    ]);
    assert_eq!(
        TableFilters::from_checkpoint(&bytes, &retyped, cfg, n).err().unwrap(),
        LoadReject::SchemaMismatch
    );
}

#[test]
fn checkpoint_size_stays_proportional() {
    // Catches format bloat regressions.
    let n = 8192u64;
    let (_fields, f, _, _) = filled(n);
    let bytes = f.checkpoint_bytes(n);
    // Four columns, at most 2 bytes/row of fingerprints plus bounds and headers.
    let ceiling = (n as usize * 4 * 2) + (n as usize / 512) * 4 * 64 + 8192;
    assert!(
        bytes.len() < ceiling,
        "checkpoint is {} bytes for {} rows across 4 columns (ceiling {})",
        bytes.len(),
        n,
        ceiling
    );
}

#[test]
fn an_empty_catalog_round_trips() {
    let (fields, f) = filters_for(&[("id", DbType::U64)]);
    let bytes = f.checkpoint_bytes(0);
    let restored =
        TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), 0).unwrap();
    assert_eq!(restored.resume_slot(), 0);
}

#[test]
fn stats_survive_the_round_trip() {
    let n = 6000u64;
    let (fields, f, _, _) = filled(n);
    let before = f.stats();
    let bytes = f.checkpoint_bytes(n);
    let after = TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n)
        .unwrap()
        .stats();
    assert_eq!(before.len(), after.len());
    for (b, a) in before.iter().zip(after.iter()) {
        assert_eq!(b.0, a.0, "write_order changed");
        assert_eq!(b.1, a.1, "fingerprint width changed");
        assert_eq!(b.2, a.2, "cardinality estimate changed");
    }
}

#[test]
fn evaluate_declines_when_a_predicate_names_an_unknown_column() {
    let n = 100u64;
    let (fields, f, _, _) = filled(n);
    let toks = vec![
        Token::Ident(("ghost".to_string(), DbType::U64, 99)),
        Token::Op("=".to_string()),
        Token::Number(rasterizeddb_core::core::tokenizer::query_tokenizer::NumericValue::U64(1)),
    ];
    assert!(sarg::extract(&toks, &f).is_none());
    let _ = fields;
}

#[test]
fn candidate_bitmap_basics() {
    use rasterizeddb_core::core::filter::CandidateBitmap;
    for n in [1u64, 63, 64, 65, 512, 1000] {
        let bm = CandidateBitmap::all_candidates(n);
        assert_eq!(bm.count(), n, "n={}: tail bits past n_slots must be clear", n);
        assert_eq!(bm.n_slots(), n);
        for s in 0..n {
            assert!(bm.get(s));
        }
        // Past the end the bitmap must answer conservatively.
        assert!(bm.get(n + 10_000));
    }
}

#[test]
fn probe_after_restore_matches_probe_before() {
    // Byte-for-byte agreement on the mask, not just on the surviving rows.
    let n = 3000u64;
    let (fields, f, ids, _) = filled(n);
    let bytes = f.checkpoint_bytes(n);
    let restored =
        TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n).unwrap();

    for i in (0..n).step_by(97) {
        let q = format!("id = {}", ids[i as usize]);
        let a = probe(&f, &fields, &q, n).unwrap();
        let b = probe(&restored, &fields, &q, n).unwrap();
        assert_eq!(a, b, "mask differs after restore for `{}`", q);
    }
    let _ = SargOp::Eq;
}


#[test]
fn a_checkpoint_claiming_more_slots_than_exist_is_rejected() {
    // A pointer file only grows between checkpoints — deletes tombstone in
    // place, and compaction re-checkpoints after truncating. So a checkpoint
    // describing more slots than the file holds cannot be describing that file:
    // it was dropped and recreated behind our back, which is what
    // `ALTER TABLE ... ADD COLUMN` does when it rewrites rows.
    //
    // The schema check catches a rewrite that changes the *indexable* columns.
    // Adding a column the filter does not index (`BOOL`, `BYTEA`, `ARRAY`)
    // leaves them identical, and file removal on drop is best-effort, so this
    // is the check that does not depend on either.
    let n = 3000u64;
    let (fields, f, _, _) = filled(n);
    let bytes = f.checkpoint_bytes(n);
    let cfg = FilterConfig::default();

    // Same size, or larger: accepted. Rows appended since a checkpoint sit
    // above its frontier and are backfilled.
    assert!(TableFilters::from_checkpoint(&bytes, &fields, cfg, n).is_ok());
    assert!(TableFilters::from_checkpoint(&bytes, &fields, cfg, n + 500).is_ok());

    // Fewer slots than claimed: rejected.
    for actual in [0u64, 1, n / 2, n - 1] {
        match TableFilters::from_checkpoint(&bytes, &fields, cfg, actual) {
            Err(LoadReject::StaleSlotCount { persisted, actual: got }) => {
                assert_eq!(persisted, n);
                assert_eq!(got, actual);
            }
            other => panic!("actual={} should have been rejected, got {:?}", actual, other.is_ok()),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recreated_table_does_not_inherit_the_old_checkpoint() {
    // End-to-end version: write a checkpoint, then replace the pointer file
    // with a shorter one as a table rewrite would, and confirm the reopened
    // table rebuilds rather than indexing rows by their former addresses.
    let dir = tempfile::tempdir().unwrap();
    {
        let t = open_table(dir.path(), "recreated", &std_columns()).await;
        fill_std(&t).await;
        t.checkpoint_filters().await;
    }

    // Truncate the pointer file to a third of its rows, leaving the sidecar
    // describing a table that no longer exists.
    let ptr = dir.path().join("recreated_pointers.db");
    let bytes = std::fs::read(&ptr).unwrap();
    let rec = rasterizeddb_core::core::row::row_pointer::ROW_POINTER_RECORD_LEN;
    let keep = (bytes.len() / rec / 3) * rec;
    std::fs::write(&ptr, &bytes[..keep]).unwrap();

    let t = open_table(dir.path(), "recreated", &std_columns()).await;
    t.wait_filters_ready().await;

    let rows = run(&t, "").await;
    assert_eq!(rows.len(), keep / rec, "row count should match the truncated file");
    // Every surviving row must still be findable — a stale index would prune.
    for id in u64s(&rows, 0).into_iter().step_by(29) {
        assert_eq!(
            u64s(&run(&t, &format!("id = {}", id)).await, 0),
            vec![id],
            "id {} lost after the pointer file was replaced",
            id
        );
    }
}

// ---------------------------------------------------------------------------
// Incremental checkpoints
// ---------------------------------------------------------------------------
//
// The sidecar is a base image followed by an append log. A checkpoint that only
// has to describe the zones written since the last one costs the writes rather
// than the table. These tests care about two things: that the append actually
// happens, and that every way the log can go wrong still ends at a sound
// checkpoint rather than a wrong one.

use rasterizeddb_core::core::row::row::RowWrite;
use rasterizeddb_core::core::row::table::Table;
use rasterizeddb_core::core::storage_providers::file_sync::LocalStorageProvider;

async fn append_rows(t: &Table<LocalStorageProvider>, from: u64, to: u64) {
    for i in from..to {
        let mut rw = RowWrite { columns_writing_data: SmallVec::new() };
        rw.columns_writing_data.push(payload(&i.to_le_bytes(), 0, DbType::U64));
        rw.columns_writing_data
            .push(payload(format!("l{}", i).as_bytes(), 1, DbType::STRING));
        rw.columns_writing_data
            .push(payload(&((i % 97) as i32).to_le_bytes(), 2, DbType::I32));
        rw.columns_writing_data.push(payload(&uuid_ordered(i), 3, DbType::UUID));
        t.insert_row(rw).await.unwrap();
    }
}

fn sidecar_bytes(dir: &std::path::Path, name: &str) -> Vec<u8> {
    std::fs::read(dir.join(format!("{}_flt.db", name))).expect("sidecar on disk")
}

/// A table whose columns match [`spec`], so the reload can be checked against
/// the same schema.
async fn incremental_table(
    dir: &std::path::Path,
    name: &str,
) -> Table<LocalStorageProvider> {
    open_table(dir, name, &spec()).await
}

fn every_id_survives(bytes: &[u8], fields: &SmallVec<[SchemaField; 20]>, n: u64) {
    let restored = TableFilters::from_checkpoint(bytes, fields, FilterConfig::default(), n)
        .expect("the sidecar should load");
    for id in 0..n {
        let q = format!("id = {}", id);
        let bits = probe(&restored, fields, &q, n).expect("sargable");
        assert!(bits[id as usize], "slot {} lost after reload", id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_checkpoint_appends_instead_of_rewriting() {
    let dir = tempfile::tempdir().unwrap();
    let t = incremental_table(dir.path(), "inc_append").await;
    let f = t.filters.clone().expect("filters active");

    append_rows(&t, 0, 8000).await;
    t.checkpoint_filters().await;
    let base_len = f.sidecar_len();
    assert_eq!(f.deltas_since_base(), 0, "the first write is a base image");
    assert!(base_len > 0);

    append_rows(&t, 8000, 8600).await;
    t.checkpoint_filters().await;
    assert_eq!(f.deltas_since_base(), 1, "the second write should be a delta");

    let grew = f.sidecar_len() - base_len;
    assert!(
        grew * 4 < base_len,
        "a 600-row delta wrote {} bytes against a {}-byte base",
        grew,
        base_len
    );

    every_id_survives(&sidecar_bytes(dir.path(), "inc_append"), &t.schema.fields, 8600);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chain_of_deltas_replays_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let t = incremental_table(dir.path(), "inc_chain").await;
    let f = t.filters.clone().expect("filters active");

    append_rows(&t, 0, 6000).await;
    t.checkpoint_filters().await;
    for round in 0..6u64 {
        append_rows(&t, 6000 + round * 600, 6600 + round * 600).await;
        t.checkpoint_filters().await;
    }
    assert_eq!(f.deltas_since_base(), 6, "each round should have appended");

    every_id_survives(&sidecar_bytes(dir.path(), "inc_chain"), &t.schema.fields, 9600);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_torn_delta_falls_back_to_the_state_before_it() {
    // The realistic failure: the process dies partway through the final append.
    // Everything before the torn record is a checkpoint of an earlier moment,
    // which is the same position an ordinary checkpoint leaves us in when rows
    // were appended after it — so it must load, not be rejected.
    let dir = tempfile::tempdir().unwrap();
    let t = incremental_table(dir.path(), "inc_torn").await;
    let f = t.filters.clone().expect("filters active");

    append_rows(&t, 0, 6000).await;
    t.checkpoint_filters().await;
    let after_base = f.sidecar_len();

    append_rows(&t, 6000, 6600).await;
    t.checkpoint_filters().await;
    let full = sidecar_bytes(dir.path(), "inc_torn");
    let after_delta = f.sidecar_len() as usize;

    for cut in [
        after_base as usize + 1,
        after_base as usize + 17,
        (after_base as usize + after_delta) / 2,
        after_delta - 1,
    ] {
        let restored =
            TableFilters::from_checkpoint(&full[..cut], &t.schema.fields, FilterConfig::default(), 6600)
                .unwrap_or_else(|e| panic!("cut at {}: {:?}", cut, e));
        // The base still covers its own 6000 rows exactly.
        for id in 0..6000u64 {
            let q = format!("id = {}", id);
            let bits = probe(&restored, &t.schema.fields, &q, 6600).expect("sargable");
            assert!(bits[id as usize], "cut {}: slot {} lost", cut, id);
        }
        // And the rows the torn delta would have described are above the
        // restored frontier, so they are candidates rather than pruned.
        let bits = probe(&restored, &t.schema.fields, "id = 999999", 6600).expect("sargable");
        for slot in 6000..6600usize {
            assert!(bits[slot], "cut {}: slot {} pruned without a checkpoint", cut, slot);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn garbage_after_the_base_stops_the_replay() {
    let dir = tempfile::tempdir().unwrap();
    let t = incremental_table(dir.path(), "inc_garbage").await;
    append_rows(&t, 0, 4000).await;
    t.checkpoint_filters().await;

    let mut bytes = sidecar_bytes(dir.path(), "inc_garbage");
    let base_len = t.filters.as_ref().unwrap().sidecar_len() as usize;
    bytes.truncate(base_len);
    for _ in 0..4096 {
        bytes.push(fastrand::u8(..));
    }

    let restored =
        TableFilters::from_checkpoint(&bytes, &t.schema.fields, FilterConfig::default(), 4000)
            .expect("the base must still load");
    for id in 0..4000u64 {
        let q = format!("id = {}", id);
        let bits = probe(&restored, &t.schema.fields, &q, 4000).expect("sargable");
        assert!(bits[id as usize], "slot {} lost", id);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_delete_rewrites_the_base_rather_than_appending() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    // A delete invalidates slots near the start, so the "tail" that would have
    // to be re-sent is the whole file. Folding it into a fresh base costs the
    // same and leaves nothing to replay.
    let dir = tempfile::tempdir().unwrap();
    let t = incremental_table(dir.path(), "inc_delete").await;
    let f = t.filters.clone().expect("filters active");

    append_rows(&t, 0, 6000).await;
    t.checkpoint_filters().await;
    assert_eq!(f.deltas_since_base(), 0);

    append_rows(&t, 6000, 6100).await;
    t.checkpoint_filters().await;
    assert_eq!(f.deltas_since_base(), 1, "an append is a delta");

    // Touching slot 3 drags the re-send point back to the first zone.
    f.on_delete(proof, 3);
    t.checkpoint_filters().await;
    assert_eq!(f.deltas_since_base(), 0, "a low-slot write forces a base rewrite");

    let bytes = sidecar_bytes(dir.path(), "inc_delete");
    let restored =
        TableFilters::from_checkpoint(&bytes, &t.schema.fields, FilterConfig::default(), 6100)
            .expect("the rewritten base should load");
    for id in (0..6100u64).filter(|i| *i != 3) {
        let q = format!("id = {}", id);
        let bits = probe(&restored, &t.schema.fields, &q, 6100).expect("sargable");
        assert!(bits[id as usize], "slot {} lost", id);
    }
}

#[test]
fn a_checkpoint_remembers_its_unindexed_prefix() {
    let ticket = test_mint();
    let proof = Proof::Ticket(&ticket);
    // Rows that predate the filter are candidates until a backfill indexes
    // them, and a checkpoint taken before that has to say so. Reloading one that
    // claimed the prefix was indexed would prune rows nothing had ever looked
    // at — a false negative, and a silent one. It is reachable whenever a
    // checkpoint lands before the backfill finishes, and permanently so with
    // `backfill_on_open = off`.
    let (fields, f) = filters_for(&spec());
    let n = 3000u64;
    let prefix = 1000u64;

    f.set_unindexed_prefix(proof, prefix);
    for slot in prefix..n {
        let cols: SmallVec<[(u32, Vec<u8>); 32]> = smallvec::smallvec![
            (0u32, slot.to_le_bytes().to_vec()),
            (1u32, format!("l{}", slot).into_bytes()),
            (2u32, ((slot % 97) as i32).to_le_bytes().to_vec()),
            (3u32, uuid_ordered(slot)),
        ];
        f.backfill_row(proof, slot, &cols);
    }

    let bytes = f.checkpoint_bytes(n);
    let restored = TableFilters::from_checkpoint(&bytes, &fields, FilterConfig::default(), n)
        .expect("round trip");

    // A value that exists nowhere: everything the filter can speak for is
    // pruned, and the un-indexed prefix is exactly what it cannot.
    let bits = probe(&restored, &fields, "id = 999999", n).expect("sargable");
    for slot in 0..prefix as usize {
        assert!(bits[slot], "slot {} was un-indexed but came back trusted", slot);
    }
    // The indexed part still prunes, or the test would pass by doing nothing.
    assert!(
        bits[prefix as usize..].iter().filter(|b| **b).count() < (n - prefix) as usize / 2,
        "the indexed region should still prune"
    );

    // And each indexed row is still findable.
    for slot in prefix..n {
        let q = format!("id = {}", slot);
        let hit = probe(&restored, &fields, &q, n).expect("sargable");
        assert!(hit[slot as usize], "slot {} lost", slot);
    }
}
