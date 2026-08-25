//! Tests for the pool itself. Placement, persistence, and the parsing that
//! stands between an operator and their disks.
//!
//! The mirror and the move are exercised from `tests/storage_pools.rs`, which
//! can open a whole database; nothing here needs one.

use super::*;

fn temp_root(tag: &str) -> String {
    let base = std::env::temp_dir().join(format!(
        "rdb_pool_{}_{}_{}",
        tag,
        std::process::id(),
        // Distinct per call within a process, so two tests in the same run do
        // not share a directory and see each other's pool file.
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&base).unwrap();
    base.to_string_lossy().into_owned()
}

static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[test]
fn a_fresh_pool_holds_only_the_database_directory() {
    let root = temp_root("fresh");
    let pool = StorageManager::open(&root);

    assert!(!pool.is_active());
    assert_eq!(pool.policy(), PlacementPolicy::Primary);
    assert_eq!(pool.locations().len(), 1);
    assert!(pool.locations()[0].is_root);
    // No placement, so every table is a root table and nothing was written.
    assert!(pool.location_of("users").is_none());
    assert!(!std::path::Path::new(&root).join(POOL_FILE).exists());
}

#[test]
fn a_location_and_its_placements_survive_a_reopen() {
    let root = temp_root("persist");
    let second = format!("{}_second", root);

    {
        let pool = StorageManager::open(&root);
        pool.add_location(&second, LocationRole::Primary, 3, true).unwrap();
        pool.set_policy(PlacementPolicy::RoundRobin);
        pool.set_min_free_bytes(7 * 1024 * 1024);
        pool.pin("users", &second).unwrap();
    }

    let pool = StorageManager::open(&root);
    assert_eq!(pool.policy(), PlacementPolicy::RoundRobin);
    assert_eq!(pool.min_free_bytes(), 7 * 1024 * 1024);
    assert_eq!(pool.location_of("users").as_deref(), Some(second.as_str()));

    let locations = pool.locations();
    assert_eq!(locations.len(), 2);
    assert_eq!(locations[1].location.path, second);
    assert_eq!(locations[1].location.weight, 3);
    assert_eq!(locations[1].location.role, LocationRole::Primary);

    std::fs::remove_dir_all(&second).ok();
}

/// The failure this whole file format exists to prevent: a table placed on a
/// second disk must still resolve there after a restart. Losing the placement
/// does not report an error — the table opens from the database directory and
/// reads as empty.
#[test]
fn a_placement_is_never_recomputed_from_the_policy() {
    let root = temp_root("stable");
    let a = format!("{}_a", root);
    let b = format!("{}_b", root);

    {
        let pool = StorageManager::open(&root);
        pool.add_location(&a, LocationRole::Primary, 1, true).unwrap();
        pool.add_location(&b, LocationRole::Primary, 1, true).unwrap();
        pool.pin("users", &a).unwrap();
        // A policy change after the fact must not move anything.
        pool.set_policy(PlacementPolicy::FreeSpace);
    }

    let pool = StorageManager::open(&root);
    assert_eq!(pool.location_of("users").as_deref(), Some(a.as_str()));

    std::fs::remove_dir_all(&a).ok();
    std::fs::remove_dir_all(&b).ok();
}

#[test]
fn round_robin_fills_the_emptiest_primary() {
    let root = temp_root("rr");
    let a = format!("{}_a", root);
    let b = format!("{}_b", root);

    let pool = StorageManager::open(&root);
    pool.add_location(&a, LocationRole::Primary, 1, true).unwrap();
    pool.add_location(&b, LocationRole::Primary, 1, true).unwrap();
    pool.set_policy(PlacementPolicy::RoundRobin);

    // The root counts as empty, so it takes the first; then a, then b, then
    // back to the root once the others have one each.
    let first = pool.place("t1").unwrap();
    let second = pool.place("t2").unwrap();
    let third = pool.place("t3").unwrap();

    let mut chosen = vec![first, second, third];
    chosen.sort();
    let mut expected = vec![root.clone(), a.clone(), b.clone()];
    expected.sort();
    assert_eq!(chosen, expected, "each primary should take one before any takes two");

    std::fs::remove_dir_all(&a).ok();
    std::fs::remove_dir_all(&b).ok();
}

#[test]
fn a_backup_location_is_never_placed_into() {
    let root = temp_root("backup");
    let backup = format!("{}_backup", root);

    let pool = StorageManager::open(&root);
    pool.add_location(&backup, LocationRole::Backup, 1, true).unwrap();
    pool.set_policy(PlacementPolicy::FreeSpace);

    for table in ["a", "b", "c"] {
        assert_eq!(
            pool.place(table).as_deref(),
            Some(root.as_str()),
            "a backup location holds mirrors, never a table's only copy"
        );
    }
    assert_eq!(pool.mirrors(), vec![backup.clone()]);

    std::fs::remove_dir_all(&backup).ok();
}

#[test]
fn a_disabled_location_keeps_its_tables_and_takes_no_new_ones() {
    let root = temp_root("drain");
    let second = format!("{}_second", root);

    let pool = StorageManager::open(&root);
    pool.add_location(&second, LocationRole::Primary, 1, true).unwrap();
    pool.pin("users", &second).unwrap();
    pool.set_enabled(&second, false).unwrap();
    pool.set_policy(PlacementPolicy::RoundRobin);

    assert_eq!(pool.location_of("users").as_deref(), Some(second.as_str()));
    assert_eq!(pool.place("fresh").as_deref(), Some(root.as_str()));

    std::fs::remove_dir_all(&second).ok();
}

#[test]
fn a_location_still_holding_tables_cannot_be_dropped() {
    let root = temp_root("holding");
    let second = format!("{}_second", root);

    let pool = StorageManager::open(&root);
    pool.add_location(&second, LocationRole::Primary, 1, true).unwrap();
    pool.pin("users", &second).unwrap();

    let refused = pool.drop_location(&second).unwrap_err();
    assert!(refused.contains("users"), "the message should name what is in the way: {refused}");

    pool.pin("users", &root).unwrap();
    pool.drop_location(&second).unwrap();

    std::fs::remove_dir_all(&second).ok();
}

#[test]
fn the_database_directory_cannot_be_removed_or_made_a_backup() {
    let root = temp_root("rootguard");
    let pool = StorageManager::open(&root);

    assert!(pool.drop_location(&root).is_err());
    assert!(pool.set_role(&root, LocationRole::Backup).is_err());
}

#[test]
fn overlapping_locations_are_refused() {
    let root = temp_root("overlap");
    let outer = format!("{}_outer", root);
    let inner = format!("{}/inner", outer);

    let pool = StorageManager::open(&root);
    pool.add_location(&outer, LocationRole::Primary, 1, true).unwrap();

    let refused = pool.add_location(&inner, LocationRole::Backup, 1, true).unwrap_err();
    assert!(refused.contains("overlaps"), "{refused}");

    // A sibling whose name merely starts with the same characters is fine:
    // containment is per path component, not per byte.
    let sibling = format!("{}_outer2", root);
    pool.add_location(&sibling, LocationRole::Primary, 1, true).unwrap();

    std::fs::remove_dir_all(&outer).ok();
    std::fs::remove_dir_all(&sibling).ok();
}

#[test]
fn a_relative_location_is_refused() {
    let root = temp_root("relative");
    let pool = StorageManager::open(&root);
    let refused = pool.add_location("data/second", LocationRole::Primary, 1, true).unwrap_err();
    assert!(refused.contains("absolute"), "{refused}");
}

#[test]
fn the_same_directory_written_two_ways_is_one_location() {
    let root = temp_root("normalize");
    let second = format!("{}_second", root);

    let pool = StorageManager::open(&root);
    pool.add_location(&second, LocationRole::Primary, 1, true).unwrap();
    // A trailing separator must not create a second entry that placements
    // recorded against the first cannot be found under.
    let refused = pool
        .add_location(&format!("{}/", second), LocationRole::Primary, 1, true)
        .unwrap_err();
    assert!(refused.contains("already"), "{refused}");

    std::fs::remove_dir_all(&second).ok();
}

#[test]
fn a_corrupt_pool_file_degrades_to_one_directory() {
    let root = temp_root("corrupt");
    std::fs::write(
        std::path::Path::new(&root).join(POOL_FILE),
        b"this is not a pool file\n",
    )
    .unwrap();

    let pool = StorageManager::open(&root);
    assert_eq!(pool.locations().len(), 1);
    assert!(pool.locations()[0].is_root);
}

#[test]
fn a_path_with_a_space_survives_the_round_trip() {
    let root = temp_root("spaced");
    // A sibling, not a child: a location inside another is refused, and the
    // point of this test is the space in the name rather than the nesting.
    let second = format!("{} second disk", root);

    {
        let pool = StorageManager::open(&root);
        pool.add_location(&second, LocationRole::Primary, 1, true).unwrap();
        pool.pin("my table", &second).unwrap();
    }

    let pool = StorageManager::open(&root);
    assert_eq!(pool.location_of("my table").as_deref(), Some(second.as_str()));

    std::fs::remove_dir_all(&second).ok();
}

#[test]
fn sizes_parse_the_way_an_operator_writes_them() {
    assert_eq!(parse_bytes("512"), Some(512));
    assert_eq!(parse_bytes("512MB"), Some(512 * 1024 * 1024));
    assert_eq!(parse_bytes("2 GiB"), Some(2 * 1024 * 1024 * 1024));
    assert_eq!(parse_bytes("'1TB'"), Some(1024u64.pow(4)));
    assert_eq!(parse_bytes("plenty"), None);
    assert_eq!(parse_bytes("12 parsecs"), None);
}

#[test]
fn policy_and_role_names_accept_what_the_documentation_promises() {
    for spelling in ["free-space", "free_space", "FREE-SPACE", "balance"] {
        assert_eq!(PlacementPolicy::parse(spelling), Some(PlacementPolicy::FreeSpace), "{spelling}");
    }
    for spelling in ["round-robin", "ROUND_ROBIN", "rr"] {
        assert_eq!(PlacementPolicy::parse(spelling), Some(PlacementPolicy::RoundRobin), "{spelling}");
    }
    assert_eq!(LocationRole::parse("BACKUP"), Some(LocationRole::Backup));
    assert_eq!(LocationRole::parse("mirror"), Some(LocationRole::Backup));
    assert_eq!(LocationRole::parse("primary"), Some(LocationRole::Primary));
    assert_eq!(LocationRole::parse("elsewhere"), None);
}

#[test]
fn free_space_is_readable_for_a_directory_that_exists() {
    let root = temp_root("space");
    // Not asserting a value — this is a real filesystem — only that the probe
    // works at all, since a policy silently seeing `None` everywhere degrades
    // to picking the first location and would pass every other test here.
    assert!(available_bytes(&root).is_some(), "statvfs should answer for {root}");
    assert!(total_bytes(&root).unwrap_or(0) > 0);
    assert!(available_bytes("/definitely/not/a/directory/here").is_none());
}

#[test]
fn statement_recognition_claims_the_whole_phrase_and_no_more() {
    use super::sql::is_storage_statement;
    assert!(is_storage_statement("STORAGE ADD LOCATION '/mnt/b'"));
    assert!(is_storage_statement("  show storage locations;  "));
    assert!(is_storage_statement("SHOW STORAGE"));
    // Not ours: a table called `storage_events` must still reach the query
    // path, and a bare `SHOW TABLES` belongs to introspection.
    assert!(!is_storage_statement("SELECT * FROM storage_events"));
    assert!(!is_storage_statement("SHOW TABLES"));
    assert!(!is_storage_statement("SHOW STORAGES"));
    assert!(!is_storage_statement("STORAGES ADD LOCATION '/mnt/b'"));
}

#[test]
fn the_structured_forms_name_their_columns() {
    use super::sql::{is_structured, structured_fields};

    for (sql, first) in [
        ("SHOW STORAGE STATS", "metric"),
        ("SHOW STORAGE LOCATIONS", "location"),
        ("SHOW STORAGE PLACEMENTS", "table_name"),
    ] {
        assert!(is_structured(sql), "{sql} should be a structured form");
        let fields = structured_fields(sql).unwrap_or_else(|| panic!("no fields for {sql}"));
        assert_eq!(fields[0].0, first, "{sql}");
    }

    // The prose forms are not structured, and must fall back to the one-column
    // shape — a console that described them as columns and then received one
    // would be a protocol violation, not a cosmetic problem.
    for sql in ["SHOW STORAGE", "SHOW STORAGE POLICY", "SHOW STORAGE KEYS"] {
        assert!(!is_structured(sql), "{sql} should be a text form");
        assert!(structured_fields(sql).is_none(), "{sql}");
    }
}

/// Two directories on one filesystem report the same free space. Summing both
/// claims twice the room the machine has, and that sum is what an operator
/// reads when deciding whether a database will fit.
#[test]
fn two_directories_on_one_disk_are_one_filesystem() {
    let a = temp_root("fsid_a");
    let b = temp_root("fsid_b");
    assert_eq!(
        filesystem_id(&a),
        filesystem_id(&b),
        "two directories under the same temp root should be on one filesystem"
    );
    assert!(filesystem_id(&a).is_some(), "the probe should answer for a directory that exists");
    assert!(filesystem_id("/definitely/not/a/directory/here").is_none());
}


#[test]
fn repointing_moves_the_root_and_its_tables_to_the_restore() {
    let target = std::path::PathBuf::from(temp_root("repoint"));
    std::fs::write(
        target.join(POOL_FILE),
        "# comment\nversion\t1\npolicy\tprimary\nmin_free_bytes\t42\n\
         location\tprimary\t1\tenabled\t/srv/live\ntable\t/srv/live\torders\n",
    )
    .unwrap();

    let dropped = repoint_pool_config(&target, &target, Some("/srv/live")).unwrap();
    assert!(dropped.is_empty());

    let text = std::fs::read_to_string(target.join(POOL_FILE)).unwrap();
    assert!(!text.contains("/srv/live"), "the source path must be gone:\n{text}");
    let root = normalize(&target.to_string_lossy());
    assert!(text.contains(&format!("location\tprimary\t1\tenabled\t{root}")), "{text}");
    assert!(text.contains(&format!("table\t{root}\torders")), "{text}");
    // Everything else is carried through untouched.
    assert!(text.contains("policy\tprimary"), "{text}");
    assert!(text.contains("min_free_bytes\t42"), "{text}");
    assert!(text.contains("# comment"), "{text}");
}

#[test]
fn repointing_drops_locations_no_snapshot_could_have_copied() {
    let target = std::path::PathBuf::from(temp_root("repoint_other_disk"));
    std::fs::write(
        target.join(POOL_FILE),
        "version\t1\nlocation\tprimary\t1\tenabled\t/srv/live\n\
         location\tprimary\t1\tenabled\t/mnt/fast\n\
         table\t/srv/live\torders\ntable\t/mnt/fast\tevents\n",
    )
    .unwrap();

    let dropped = repoint_pool_config(&target, &target, Some("/srv/live")).unwrap();

    // A snapshot only ever walks the database directory, so `events` was never
    // in the backup. Saying so beats resolving it to the source's disk and
    // reading a live table.
    assert_eq!(dropped, vec!["events".to_string()]);
    let text = std::fs::read_to_string(target.join(POOL_FILE)).unwrap();
    assert!(!text.contains("/mnt/fast"), "{text}");
    assert!(text.contains("orders"), "{text}");
}

#[test]
fn an_in_place_restore_leaves_the_pool_alone() {
    // Restoring over the directory the backup came from: every path in the file
    // is still true, and rewriting would drop placements this host can serve.
    let target = std::path::PathBuf::from(temp_root("repoint_in_place"));
    let root = normalize(&target.to_string_lossy());
    let text = format!(
        "version\t1\nlocation\tprimary\t1\tenabled\t{root}\n\
         location\tprimary\t1\tenabled\t/mnt/fast\ntable\t/mnt/fast\tevents\n"
    );
    std::fs::write(target.join(POOL_FILE), &text).unwrap();

    let dropped = repoint_pool_config(&target, &target, Some(&root)).unwrap();

    assert!(dropped.is_empty());
    assert_eq!(std::fs::read_to_string(target.join(POOL_FILE)).unwrap(), text);
}

#[test]
fn without_a_recorded_root_the_first_location_stands_in() {
    let target = std::path::PathBuf::from(temp_root("repoint_no_hint"));
    std::fs::write(
        target.join(POOL_FILE),
        "version\t1\nlocation\tprimary\t1\tenabled\t/srv/live\ntable\t/srv/live\torders\n",
    )
    .unwrap();

    repoint_pool_config(&target, &target, None).unwrap();

    let text = std::fs::read_to_string(target.join(POOL_FILE)).unwrap();
    assert!(!text.contains("/srv/live"), "{text}");
    assert!(text.contains(&normalize(&target.to_string_lossy())), "{text}");
}

#[test]
fn a_database_with_no_pool_file_needs_no_repointing() {
    let target = std::path::PathBuf::from(temp_root("repoint_absent"));
    assert!(repoint_pool_config(&target, &target, Some("/srv/live")).unwrap().is_empty());
}

#[test]
fn a_restore_names_the_directory_it_will_be_published_into() {
    // A restore builds in a sibling and renames it into place, so the paths
    // written have to be the ones it will have *after* that rename. Naming the
    // staging directory left every table resolving to a path that no longer
    // existed, and the restored database came back empty.
    let staging = std::path::PathBuf::from(temp_root("repoint_staging"));
    let published = std::path::PathBuf::from(temp_root("repoint_published"));
    std::fs::write(
        staging.join(POOL_FILE),
        "version\t1\nlocation\tprimary\t1\tenabled\t/srv/live\ntable\t/srv/live\torders\n",
    )
    .unwrap();

    repoint_pool_config(&staging, &published, Some("/srv/live")).unwrap();

    let text = std::fs::read_to_string(staging.join(POOL_FILE)).unwrap();
    assert!(text.contains(&normalize(&published.to_string_lossy())), "{text}");
    assert!(!text.contains(&normalize(&staging.to_string_lossy())), "{text}");
}

#[test]
fn a_pool_file_copied_from_another_directory_is_still_read() {
    // The warning path: a mirrored directory opened as a database carries the
    // original's pool file. Opening must not fail — the copy is usable, and the
    // operator is told — but the placements it carries are the original's, so
    // this is the shape that needs the log line.
    let elsewhere = temp_root("copied_pool");
    let text = format!(
        "version\t1\nlocation\tprimary\t1\tenabled\t/srv/somewhere-else\n\
         table\t/srv/somewhere-else\torders\n"
    );
    std::fs::write(std::path::Path::new(&elsewhere).join(POOL_FILE), text).unwrap();

    let pool = StorageManager::open(&elsewhere);

    // Read, not refused: the placement is the original's, which is exactly what
    // the warning is about.
    assert_eq!(pool.location_of("orders").as_deref(), Some("/srv/somewhere-else"));
}


#[test]
fn concurrent_placements_leave_a_whole_configuration_on_disk() {
    // The production failure this guards: `place` releases the state lock
    // before persisting, so a table-creation storm had several persists in
    // flight at once, all writing the same fixed temporary path. One rename
    // moved it away and the next reported `No such file or directory` — and,
    // worse than the lost update, a rename could publish the other persist's
    // file while `fs::write` was still truncating and rewriting it.
    //
    // Timing-dependent by nature, so this asserts the invariant rather than the
    // interleaving: whatever order they run in, the file left behind is one
    // whole render that names every table, and no temporary file survives.
    use std::sync::Arc;

    let root = temp_root("concurrent_persist");
    let pool = Arc::new(StorageManager::open(&root));

    let threads: Vec<_> = (0..8)
        .map(|worker| {
            let pool = Arc::clone(&pool);
            std::thread::spawn(move || {
                for i in 0..25 {
                    pool.place(&format!("t_{worker}_{i}"));
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }

    assert!(
        !std::path::Path::new(&root).join(format!("{}.tmp", POOL_FILE)).exists(),
        "a temporary pool file was left behind"
    );

    // Read back through the parser: a truncated or interleaved file either
    // fails to parse or comes back missing placements, and `open` logs and
    // degrades rather than failing, so the assertion has to be on the contents.
    let reopened = StorageManager::open(&root);
    for worker in 0..8 {
        for i in 0..25 {
            let table = format!("t_{worker}_{i}");
            assert_eq!(
                reopened.location_of(&table).as_deref(),
                Some(normalize(&root).as_str()),
                "{table} did not survive the write"
            );
        }
    }
}
