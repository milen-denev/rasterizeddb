//! Moving tables between storage locations, one at a time or by policy.
//!
//! Placement decides where a table is *created*. This is what happens
//! afterwards: a disk fills up, a disk is added, a location is being drained
//! before it is removed. The engine will not move a table on its own — a
//! several-gigabyte copy is not something to start behind an operator's back —
//! so a rebalance is a statement, and it can be asked what it would do before
//! being asked to do it.
//!
//! # What a rebalance moves
//!
//! In order, and only what each rule names:
//!
//! 1. **Evacuation.** Any table sitting in a location that is disabled, or that
//!    is no longer a primary, is moved out. This is what makes
//!    `STORAGE DISABLE LOCATION` a drain rather than a freeze.
//! 2. **Policy.** Under `round-robin`, tables move from the fullest location to
//!    the emptiest until the counts differ by at most one. Under `free-space`,
//!    tables move off any location below the free-space floor, largest first,
//!    until it is above the floor. Under `primary`, nothing: that policy is a
//!    statement that placement is manual.
//!
//! Nothing is moved onto a location that would itself drop below the floor as a
//! result, so a rebalance cannot fill the disk it is moving to.

use rclite::Arc;

use crate::core::database::Database;

use super::{
    LocationRole, PlacementPolicy, StorageManager, TableBackend, available_bytes, human_bytes,
};

/// One move a rebalance intends.
struct Move {
    table: String,
    from: String,
    to: String,
    bytes: u64,
    /// Why this move is in the plan, for the line the operator reads.
    reason: &'static str,
}

/// `STORAGE MOVE TABLE <name> TO '<spec>'`, and the lines it reports.
///
/// A move to where the table already is is success, not an error — a rebalance
/// script that names every table should not fail on the ones the policy already
/// put in the right place — but it says so rather than reporting a copy of zero
/// bytes, which reads as a move that silently did nothing.
///
/// # It is a conversion, not only a relocation
///
/// `<spec>` is a backend, so this is also how a table changes what holds it:
/// a directory, `memory:`, or `kv:<store>` — and a store is a local directory
/// or an S3 bucket depending only on how it was defined. The copy itself is
/// backend-agnostic (see [`crate::core::database::Database::move_table_to_backend`]);
/// what is not agnostic, and what this reports, is which of the file-backed
/// features the table gains or loses on arrival. Saying nothing there is how an
/// operator ends up with a table that is no longer in any backup and no
/// indication that it changed.
pub async fn move_table(
    database: &Arc<Database>,
    table: &str,
    destination: &TableBackend,
) -> Result<Vec<String>, String> {
    let pool = &database.storage;
    let root = pool.root().ok_or("this database is in memory")?.to_string();
    // Before "already there": a partitioned table is in no placement, so the
    // root would read as where it is and the statement as done.
    database
        .refuse_partitioned(table, "STORAGE MOVE TABLE")
        .map_err(|e| e.to_string())?;

    let current = pool
        .backend_of(table)
        .unwrap_or(TableBackend::Directory(root));

    if current == *destination {
        return Ok(vec![format!(
            "{} is already on {}",
            table,
            where_it_is(destination)
        )]);
    }

    let bytes = database
        .move_table_to_backend(table, destination)
        .await
        .map_err(|e| e.to_string())?;

    let mut lines = vec![format!(
        "moved {} from {} to {} ({})",
        table,
        where_it_is(&current),
        where_it_is(destination),
        human_bytes(bytes)
    )];
    lines.extend(consequences(&current, destination));
    Ok(lines)
}

/// How a backend reads in a sentence an operator is looking at.
fn where_it_is(backend: &TableBackend) -> String {
    match backend {
        TableBackend::Directory(path) => path.clone(),
        TableBackend::Memory => "memory".to_string(),
        TableBackend::KeyValue(store) => format!("key/value store '{store}'"),
    }
}

/// What the table gained or lost by arriving where it did.
///
/// Backup, the write-ahead log, relocation and the free-space policy are
/// features *about files*. Crossing that line in either direction is the part
/// of a move that is invisible afterwards: the rows read back identically, and
/// the next snapshot is the first hint that the table is no longer in one.
fn consequences(from: &TableBackend, to: &TableBackend) -> Vec<String> {
    let mut lines = Vec::new();
    match to {
        TableBackend::Memory => lines.push(
            "it is now in this process only: it does not survive a restart, it is in no \
             backup, and it has no write-ahead log"
                .to_string(),
        ),
        TableBackend::KeyValue(_) => lines.push(
            "it is durable in the store, but it is no longer covered by backups or by \
             STORAGE REBALANCE, and it has no write-ahead log — its durability is \
             the delta object written at each commit"
                .to_string(),
        ),
        TableBackend::Directory(_) if !from.is_file_based() => lines.push(
            "it is a file-backed table again: backups include it, the write-ahead log \
             covers it, and the free-space policy counts it"
                .to_string(),
        ),
        TableBackend::Directory(_) => {}
    }
    lines
}

/// `STORAGE REBALANCE [DRY RUN]`.
pub async fn rebalance(database: &Arc<Database>, dry_run: bool) -> Result<Vec<String>, String> {
    let pool = &database.storage;
    if pool.root().is_none() {
        return Err("this database is in memory and has no storage locations".to_string());
    }

    let plan = plan(database, pool).await;

    if plan.is_empty() {
        return Ok(vec![
            "nothing to move: every table is where the policy would put it".to_string(),
        ]);
    }

    let total: u64 = plan.iter().map(|m| m.bytes).sum();

    if dry_run {
        let mut lines = vec![format!(
            "{} table(s), {} would move:",
            plan.len(),
            human_bytes(total)
        )];
        lines.extend(plan.iter().map(describe));
        lines.push("run STORAGE REBALANCE without DRY RUN to do it".to_string());
        return Ok(lines);
    }

    let mut lines = Vec::with_capacity(plan.len() + 1);
    let mut moved_bytes = 0u64;
    let mut moved = 0usize;

    for step in &plan {
        // Sequential, and a failure stops the run rather than carrying on. Each
        // move is individually atomic — see `Database::move_table_to` — so
        // stopping leaves a consistent database with part of the plan applied,
        // and the next `STORAGE REBALANCE` re-plans from where it got to.
        match database
            .move_table_to_backend(&step.table, &TableBackend::Directory(step.to.clone()))
            .await
        {
            Ok(bytes) => {
                moved += 1;
                moved_bytes = moved_bytes.saturating_add(bytes);
                lines.push(format!("moved {}", describe(step)));
            }
            Err(e) => {
                lines.push(format!("stopped at {}: {}", step.table, e));
                lines.push(format!(
                    "{} of {} table(s) moved ({}); run STORAGE REBALANCE again once that is fixed",
                    moved,
                    plan.len(),
                    human_bytes(moved_bytes)
                ));
                return Ok(lines);
            }
        }
    }

    lines.push(format!(
        "{} table(s) moved, {}",
        moved,
        human_bytes(moved_bytes)
    ));
    Ok(lines)
}

/// Works out what should move, without moving anything.
async fn plan(database: &Arc<Database>, pool: &StorageManager) -> Vec<Move> {
    let root = pool.root().unwrap_or("").to_string();
    let reports = pool.locations();

    // Where every live table currently is, and how big it is. Sizes are read
    // once: a plan that re-read them per candidate would spend more time in
    // `stat` than in the copy it is planning.
    let placements: std::collections::HashMap<String, String> =
        pool.placements().into_iter().collect();
    let mut tables: Vec<(String, String, u64)> = Vec::new();
    for (name, table) in database.each_table_unlocked() {
        // A partitioned table's shell has no files of its own, so it would
        // sort as the smallest table and be picked first, and its partitions
        // are not in the placement map. They rebalance with REBALANCE
        // PARTITIONS, a later version's.
        if database.is_partitioned(&name) || table.is_shell() {
            continue;
        }
        let at = match placements.get(&name) {
            // Only a directory placement is a rebalance candidate: the others
            // occupy no disk the policy is balancing and cannot be moved onto
            // one.
            Some(spec) => match super::TableBackend::parse(spec).directory() {
                Some(path) => path.to_string(),
                None => continue,
            },
            None => root.clone(),
        };
        let bytes = database.table_bytes(&name).await;
        tables.push((name, at, bytes));
    }
    // Deterministic: two runs of the same plan must produce the same moves, and
    // a `DRY RUN` that disagreed with the run after it would be worse than no
    // dry run at all.
    tables.sort_unstable();

    let usable: Vec<String> = reports
        .iter()
        .filter(|r| r.location.enabled && r.location.role == LocationRole::Primary)
        .map(|r| r.location.path.clone())
        .collect();

    if usable.is_empty() {
        // Nowhere to move anything to. Evacuating into a location that takes no
        // tables would be worse than leaving them.
        return Vec::new();
    }

    let floor = pool.min_free_bytes();
    // Free space is decremented as the plan grows, so two moves in one plan do
    // not both aim at the same headroom.
    let mut free: std::collections::HashMap<String, u64> = reports
        .iter()
        .map(|r| {
            (
                r.location.path.clone(),
                r.available_bytes.unwrap_or(u64::MAX),
            )
        })
        .collect();
    let mut counts: std::collections::HashMap<String, usize> = usable
        .iter()
        .map(|path| {
            (
                path.clone(),
                tables.iter().filter(|(_, at, _)| at == path).count(),
            )
        })
        .collect();

    let mut plan: Vec<Move> = Vec::new();
    let mut current: std::collections::HashMap<String, String> = tables
        .iter()
        .map(|(name, at, _)| (name.clone(), at.clone()))
        .collect();
    let size: std::collections::HashMap<String, u64> = tables
        .iter()
        .map(|(name, _, bytes)| (name.clone(), *bytes))
        .collect();

    // -- 1. evacuate ------------------------------------------------------
    for (name, at, bytes) in tables.iter() {
        if usable.iter().any(|path| path == at) {
            continue;
        }
        let Some(target) = roomiest(&usable, &free, *bytes, floor) else {
            continue;
        };
        apply(
            &mut plan,
            &mut free,
            &mut counts,
            &mut current,
            Move {
                table: name.clone(),
                from: at.clone(),
                to: target,
                bytes: *bytes,
                reason: "its location no longer takes tables",
            },
        );
    }

    // -- 2. policy --------------------------------------------------------
    match pool.policy() {
        PlacementPolicy::Primary => {}
        PlacementPolicy::RoundRobin => {
            // Greedy: repeatedly move one table from the fullest to the
            // emptiest until they are within one of each other. Bounded by the
            // table count so a pathological case cannot loop.
            for _ in 0..tables.len() {
                let Some(fullest) = usable
                    .iter()
                    .max_by_key(|p| counts.get(*p).copied().unwrap_or(0))
                else {
                    break;
                };
                let Some(emptiest) = usable
                    .iter()
                    .min_by_key(|p| counts.get(*p).copied().unwrap_or(0))
                else {
                    break;
                };
                let high = counts.get(fullest).copied().unwrap_or(0);
                let low = counts.get(emptiest).copied().unwrap_or(0);
                if high <= low + 1 {
                    break;
                }
                // The smallest table on the fullest location: evening out a
                // count should cost as little copying as it can.
                let Some(candidate) = current
                    .iter()
                    .filter(|(_, at)| *at == fullest)
                    .map(|(name, _)| name.clone())
                    .min_by_key(|name| (size.get(name).copied().unwrap_or(0), name.clone()))
                else {
                    break;
                };
                let bytes = size.get(&candidate).copied().unwrap_or(0);
                if !fits(emptiest, &free, bytes, floor) {
                    break;
                }
                let from = fullest.clone();
                let to = emptiest.clone();
                apply(
                    &mut plan,
                    &mut free,
                    &mut counts,
                    &mut current,
                    Move {
                        table: candidate,
                        from,
                        to,
                        bytes,
                        reason: "evening out the table count",
                    },
                );
            }
        }
        PlacementPolicy::FreeSpace => {
            for _ in 0..tables.len() {
                // A location genuinely short of space, largest table first: one
                // copy that frees the most is better than several that free the
                // same amount between them.
                let Some(tight) = usable
                    .iter()
                    .filter(|path| free.get(*path).copied().unwrap_or(u64::MAX) < floor)
                    .min_by_key(|path| free.get(*path).copied().unwrap_or(u64::MAX))
                    .cloned()
                else {
                    break;
                };
                let Some(candidate) = current
                    .iter()
                    .filter(|(_, at)| **at == tight)
                    .map(|(name, _)| name.clone())
                    .max_by_key(|name| (size.get(name).copied().unwrap_or(0), name.clone()))
                else {
                    break;
                };
                let bytes = size.get(&candidate).copied().unwrap_or(0);
                let elsewhere: Vec<String> = usable
                    .iter()
                    .filter(|path| **path != tight)
                    .cloned()
                    .collect();
                let Some(target) = roomiest(&elsewhere, &free, bytes, floor) else {
                    break;
                };
                apply(
                    &mut plan,
                    &mut free,
                    &mut counts,
                    &mut current,
                    Move {
                        table: candidate,
                        from: tight,
                        to: target,
                        bytes,
                        reason: "its location is below the free-space floor",
                    },
                );
            }
        }
    }

    plan
}

/// Records a move in the plan and in the running state the plan is built from.
fn apply(
    plan: &mut Vec<Move>,
    free: &mut std::collections::HashMap<String, u64>,
    counts: &mut std::collections::HashMap<String, usize>,
    current: &mut std::collections::HashMap<String, String>,
    step: Move,
) {
    if let Some(space) = free.get_mut(&step.to) {
        *space = space.saturating_sub(step.bytes);
    }
    if let Some(space) = free.get_mut(&step.from) {
        *space = space.saturating_add(step.bytes);
    }
    *counts.entry(step.to.clone()).or_insert(0) += 1;
    if let Some(count) = counts.get_mut(&step.from) {
        *count = count.saturating_sub(1);
    }
    current.insert(step.table.clone(), step.to.clone());
    plan.push(step);
}

/// The candidate with the most free space that still clears the floor with
/// `bytes` added to it.
fn roomiest(
    candidates: &[String],
    free: &std::collections::HashMap<String, u64>,
    bytes: u64,
    floor: u64,
) -> Option<String> {
    candidates
        .iter()
        .filter(|path| fits(path, free, bytes, floor))
        .max_by_key(|path| free.get(*path).copied().unwrap_or(u64::MAX))
        .cloned()
}

/// Whether `bytes` can go to `path` without taking it below the floor.
///
/// A location whose free space could not be determined is treated as roomy: the
/// alternative is refusing every move on a platform with no `statvfs`, which
/// would make the whole surface unusable there rather than merely unguided.
fn fits(path: &str, free: &std::collections::HashMap<String, u64>, bytes: u64, floor: u64) -> bool {
    match free.get(path) {
        Some(&space) if space == u64::MAX => true,
        Some(&space) => space.saturating_sub(bytes) >= floor,
        None => true,
    }
}

fn describe(step: &Move) -> String {
    format!(
        "{} ({}) {} -> {}: {}",
        step.table,
        human_bytes(step.bytes),
        step.from,
        step.to,
        step.reason
    )
}

/// Free space at `path` right now, for a caller that has no report to hand.
pub fn free_at(path: &str) -> Option<u64> {
    available_bytes(path)
}
