//! The startup reaper (S5, `docs/partitioning.md` §12, I15): it deletes the
//! `__pt_` files of uids no catalogue row holds and of partitions their map
//! never materialized, keeps every live table's files, touches nothing
//! outside the pattern, and deletes nothing at all when any live map cannot
//! be relied on or a plain table's name could look like a partition file.

use std::path::Path;

use super::relation_dispatch_tests::{
    acct, close, count, count_plain, create, files_in, insert, insert_plain, open, partitioned,
};
use crate::core::partition::files::{self, PartFile};

/// A uid no test table gets: `new_uid` picks at random, so a collision is a
/// 1 in 2^64 chance.
const DEAD: u64 = 0x0000_0000_dead_beef;

fn plant(root: &Path, name: &str) {
    std::fs::write(root.join(name), b"leftover bytes").unwrap();
}

/// Partition `pid`'s three files and their `.tmp`s.
fn plant_partition(root: &Path, uid: u64, pid: u32) -> Vec<String> {
    let mut names = Vec::new();
    for name in files::data_files(uid, pid) {
        for file in [name.clone(), format!("{name}.tmp")] {
            plant(root, &file);
            names.push(file);
        }
    }
    names
}

/// A dead uid's parent files and one partition.
fn plant_dead_uid(root: &Path) -> Vec<String> {
    let mut names: Vec<String> = files::parent_files(DEAD).into_iter().collect();
    names.push(format!("{}_FIELDS.db", files::schema_stem(DEAD)));
    for name in &names {
        plant(root, name);
    }
    names.extend(plant_partition(root, DEAD, 3));
    names
}

/// The partitions of `uid` with files in `root`.
fn data_pids(root: &Path, uid: u64) -> Vec<u32> {
    let mut pids: Vec<u32> = files_in(root)
        .iter()
        .filter_map(|n| match files::parse_name(n)? {
            PartFile::Data { uid: u, pid } if u == uid => Some(pid),
            _ => None,
        })
        .collect();
    pids.sort();
    pids.dedup();
    pids
}

fn present(root: &Path, names: &[String]) -> Vec<String> {
    names.iter().filter(|n| root.join(n).exists()).cloned().collect()
}

/// Names the pattern rejects, some under live or dead uids: never touched.
fn plant_unmatched(root: &Path, uid: u64) -> Vec<String> {
    let names = vec![
        format!("{}.bak", files::data_files(uid, 5)[0]),
        format!("{}{}", files::map_file(DEAD, 0), files::TORN),
        format!("__pt_{:016X}_00000001.db", DEAD),
        format!("__pt_{uid:016x}_map_00.db"),
        format!("__pt_{uid:016x}_schema_lower.db"),
        "notes.txt".to_string(),
    ];
    for name in &names {
        plant(root, name);
    }
    names
}

/// Reaped: a dead uid's files and an unmaterialized partition's. Kept: the
/// materialized partitions, the live table's parent files and every name
/// outside the pattern. Rows of the partitioned and the plain table are all
/// there after the reopen.
#[tokio::test(flavor = "multi_thread")]
async fn the_reaper_deletes_orphans_and_unmaterialized_partitions_only() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let uid = create(&db, "acct", 8).await;
    db.add_table(acct("side")).await.unwrap();
    let t = partitioned(&db, "acct");
    for tenant in 0..6 {
        insert(&t, tenant, 1).await;
    }
    for n in 0..5 {
        insert_plain(&db, "side", 1, n).await;
    }
    db.sync_durable().unwrap();
    drop(t);
    close(db, root);

    let materialized = data_pids(root, uid);
    assert!(!materialized.is_empty() && materialized.len() < 8, "{materialized:?}");
    let unmaterialized = (0..8u32).find(|p| !materialized.contains(p)).unwrap();
    let live_kept: Vec<String> = files_in(root)
        .into_iter()
        .filter(|n| files::parse_name(n).is_some_and(|f| f.uid() == uid))
        .collect();
    let dead = plant_dead_uid(root);
    let leftover = plant_partition(root, uid, unmaterialized);
    let unmatched = plant_unmatched(root, uid);
    let side_files: Vec<String> = files_in(root).into_iter().filter(|n| n.starts_with("side")).collect();

    let db = open(root).await;
    assert_eq!(present(root, &dead), Vec::<String>::new(), "a dead uid's files were kept");
    assert_eq!(present(root, &leftover), Vec::<String>::new(), "an unmaterialized partition's files were kept");
    assert_eq!(present(root, &live_kept), live_kept, "a live table's file was deleted");
    assert_eq!(present(root, &unmatched), unmatched, "a name outside the pattern was deleted");
    assert_eq!(present(root, &side_files), side_files, "a plain table's file was deleted");
    assert_eq!(data_pids(root, uid), materialized);
    let t = partitioned(&db, "acct");
    assert_eq!(t.broken(), None);
    assert_eq!(count(&t).await, 6);
    assert_eq!(count_plain(&db, "side").await, 5);
    drop(t);
    close(db, root);
}

/// I15: a live map that does not replay (a damaged record followed by an
/// intact one) turns reaping off for the start. Nothing is deleted anywhere,
/// orphans included; that table opens broken and the others read in full.
#[tokio::test(flavor = "multi_thread")]
async fn a_map_that_does_not_replay_turns_reaping_off() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let bad = create(&db, "bad", 4).await;
    // Eight partitions for five tenants: at least three stay unmaterialized.
    let good = create(&db, "good", 8).await;
    db.add_table(acct("side")).await.unwrap();
    for name in ["bad", "good"] {
        let t = partitioned(&db, name);
        for tenant in 0..5 {
            insert(&t, tenant, 1).await;
        }
    }
    for n in 0..3 {
        insert_plain(&db, "side", 1, n).await;
    }
    db.sync_durable().unwrap();
    // Without a checkpoint, recovery rewrites the map's records from the log
    // and heals the damage below before the reaper reads it.
    let checkpointed = crate::core::wal::wal_for(&super::relation_dispatch_tests::path_str(root))
        .expect("a file-backed database has a log")
        .checkpoint()
        .unwrap();
    assert!(checkpointed, "the checkpoint declined, so the damage would be healed and test nothing");
    close(db, root);

    plant_dead_uid(root);
    let unmaterialized = (0..8u32).find(|p| !data_pids(root, good).contains(p)).unwrap();
    plant_partition(root, good, unmaterialized);
    // Inside HEADER's payload: its CRC fails, and the records after it pass.
    let map = root.join(files::map_file(bad, 0));
    let mut bytes = std::fs::read(&map).unwrap();
    bytes[crate::core::partition::log::FRAME_LEN + 2] ^= 0xff;
    std::fs::write(&map, &bytes).unwrap();
    let before = files_in(root);

    let db = open(root).await;
    let after = files_in(root);
    let gone: Vec<&String> = before.iter().filter(|n| !after.contains(n)).collect();
    assert!(gone.is_empty(), "a start with an unreadable map deleted {gone:?}");
    assert!(partitioned(&db, "bad").broken().is_some(), "the damaged table opened");
    let t = partitioned(&db, "good");
    assert_eq!(t.broken(), None);
    assert_eq!(count(&t).await, 5);
    assert_eq!(count_plain(&db, "side").await, 3);
    assert!(!data_pids(root, bad).is_empty(), "the damaged table's partitions are gone");
    drop(t);
    close(db, root);
}

/// A torn map tail turns reaping off too, and so does the evidence file the
/// open that cuts the tail keeps, at every later start until it is removed.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_map_turns_reaping_off_until_examined() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let uid = create(&db, "acct", 4).await;
    close(db, root);

    let dead = plant_dead_uid(root);
    let map = root.join(files::map_file(uid, 0));
    let mut bytes = std::fs::read(&map).unwrap();
    // Half a frame header: a tear, with nothing intact after it.
    bytes.extend_from_slice(&[9, 0, 0]);
    std::fs::write(&map, &bytes).unwrap();

    let db = open(root).await;
    assert_eq!(present(root, &dead), dead, "a start with a torn map reaped");
    close(db, root);
    let torn = root.join(format!("{}{}", files::map_file(uid, 0), files::TORN));
    assert!(torn.exists(), "the open kept no evidence of the cut tail");

    let db = open(root).await;
    assert_eq!(present(root, &dead), dead, "a start with torn-tail evidence reaped");
    close(db, root);

    std::fs::remove_file(&torn).unwrap();
    let db = open(root).await;
    assert_eq!(present(root, &dead), Vec::<String>::new(), "reaping did not resume once the evidence went");
    assert_eq!(partitioned(&db, "acct").broken(), None);
    close(db, root);
}

/// A plain table an older build let take a `__pt_` name can own a file the
/// pattern reads as a partitioned table's (`__pt_<uid>_schema` has the rows
/// file `__pt_<uid>_schema.db`); with one in the catalogue nothing is
/// reaped, and its rows survive the start.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_table_under_the_prefix_turns_reaping_off() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let legacy = files::schema_stem(0xaa);
    let db = open(root).await;
    db.add_table_unguarded(acct(&legacy)).await.unwrap();
    for n in 0..4 {
        insert_plain(&db, &legacy, 1, n).await;
    }
    db.sync_durable().unwrap();
    close(db, root);
    assert!(
        files::parse_name(&format!("{legacy}.db")).is_some(),
        "the legacy table's rows file does not look like a partition file"
    );
    let dead = plant_dead_uid(root);

    let db = open(root).await;
    assert!(root.join(format!("{legacy}.db")).exists(), "the legacy table's rows file was reaped");
    assert_eq!(count_plain(&db, &legacy).await, 4);
    assert_eq!(present(root, &dead), dead, "reaping ran beside a plain table under the prefix");
    close(db, root);
}

/// A live uid's map generation other than 0 is a later version's: nothing
/// is reaped, since this version cannot tell its live generation.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_map_generation_turns_reaping_off() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let db = open(root).await;
    let uid = create(&db, "acct", 4).await;
    close(db, root);
    let dead = plant_dead_uid(root);
    let later = files::map_file(uid, 1);
    plant(root, &later);

    let db = open(root).await;
    assert_eq!(present(root, &dead), dead, "reaping ran beside a later map generation");
    assert!(root.join(&later).exists());
    close(db, root);
}
