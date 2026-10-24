//! The table-lock core's tests: the matrix, the grant rule's two clauses,
//! mode sets, the fast path (exhaustively interleaved against a strong
//! request), the wait-for graph and its victims, and the wait loop's
//! `lock_timeout` and cancellation.

use super::*;

use TableMode::{AccessExclusive as AEm, AccessShare as ASm, RowExclusive as RXm, Share as Sm};

fn owner(id: TxId) -> LockOwner {
    LockOwner::block(id).expect("a real id")
}

fn granted(r: Request) -> bool {
    r.is_granted()
}

fn queued(r: Request) -> Wait {
    match r {
        Request::Queued(w) => w,
        Request::Granted => panic!("expected a queued request, got a grant"),
    }
}

/// PostgreSQL's table-lock conflict matrix (the documentation's table 13.2,
/// `lock.c`'s `LockConflicts`), in [`PgLockMode::ALL`]'s order: AccessShare,
/// RowShare, RowExclusive, ShareUpdateExclusive, Share, ShareRowExclusive,
/// Exclusive, AccessExclusive. Row i, column j: whether i conflicts with j.
const PG_CONFLICTS: [[bool; 8]; 8] = {
    const F: bool = false;
    const T: bool = true;
    [
        [F, F, F, F, F, F, F, T], // ACCESS SHARE
        [F, F, F, F, F, F, T, T], // ROW SHARE
        [F, F, F, F, T, T, T, T], // ROW EXCLUSIVE
        [F, F, F, T, T, T, T, T], // SHARE UPDATE EXCLUSIVE
        [F, F, T, T, F, T, T, T], // SHARE
        [F, F, T, T, T, T, T, T], // SHARE ROW EXCLUSIVE
        [F, T, T, T, T, T, T, T], // EXCLUSIVE
        [T, T, T, T, T, T, T, T], // ACCESS EXCLUSIVE
    ]
};

fn pg_index(m: PgLockMode) -> usize {
    PgLockMode::ALL.iter().position(|x| *x == m).unwrap()
}

/// The PostgreSQL mode each of ours is.
fn pg_of(m: TableMode) -> PgLockMode {
    match m {
        ASm => PgLockMode::AccessShare,
        RXm => PgLockMode::RowExclusive,
        Sm => PgLockMode::Share,
        AEm => PgLockMode::AccessExclusive,
    }
}

#[test]
fn the_four_modes_conflict_exactly_as_in_postgresql() {
    for a in TableMode::ALL {
        for b in TableMode::ALL {
            let pg = PG_CONFLICTS[pg_index(pg_of(a))][pg_index(pg_of(b))];
            assert_eq!(a.conflicts_with(b), pg, "{} vs {}", a.short(), b.short());
            assert_eq!(a.conflicts_with(b), b.conflicts_with(a), "the matrix is symmetric");
            assert_eq!(ModeSet::of(a).conflicts_with(b), pg);
        }
    }
    // The table §7 and the plan print.
    assert!(ASm.conflicts_with(AEm) && !ASm.conflicts_with(Sm) && !ASm.conflicts_with(RXm));
    assert!(RXm.conflicts_with(Sm) && !RXm.conflicts_with(RXm));
    assert!(!Sm.conflicts_with(Sm));
    assert!(TableMode::ALL.iter().all(|m| AEm.conflicts_with(*m)));
    assert!(Sm.is_strong() && AEm.is_strong() && !ASm.is_strong() && !RXm.is_strong());
}

#[test]
fn every_lock_table_mode_maps_to_one_never_weaker() {
    for a in PgLockMode::ALL {
        if let Some(own) = TableMode::ALL.iter().find(|m| pg_of(**m) == a) {
            assert_eq!(a.table_mode(), *own, "{a:?} has an exact match");
        }
        for b in PgLockMode::ALL {
            if PG_CONFLICTS[pg_index(a)][pg_index(b)] {
                assert!(
                    a.table_mode().conflicts_with(b.table_mode()),
                    "{a:?} conflicts with {b:?} in PostgreSQL but their modes here do not"
                );
            }
        }
    }
}

#[test]
fn the_grant_rule_follows_the_matrix_in_both_directions() {
    for held in TableMode::ALL {
        for asked in TableMode::ALL {
            for (first, second) in [(owner(10), owner(11)), (owner(11), owner(10))] {
                let locks = TableLocks::new();
                assert!(granted(locks.request(first, "t", held, WaitRole::Other)));
                let r = locks.request(second, "t", asked, WaitRole::Other);
                if held.conflicts_with(asked) {
                    let w = queued(r);
                    assert_eq!(&locks.blockers(second)[..], &[first]);
                    locks.release_owner(first);
                    assert_eq!(w.outcome(), WaitOutcome::Granted, "{} then {}", held.short(), asked.short());
                } else {
                    assert!(granted(r), "{} then {} must be granted", held.short(), asked.short());
                }
                assert!(locks.holds(second, "t").contains(asked));
                locks.release_owner(first);
                locks.release_owner(second);
                assert!(locks.main_is_empty());
                assert_eq!(locks.fast_path_len(), 0);
                assert_eq!(locks.strong_pending_for("t"), 0);
            }
        }
    }
}

#[test]
fn a_weak_request_takes_the_fast_path_and_a_strong_one_migrates_it() {
    let locks = TableLocks::new();
    let (a, b) = (owner(10), owner(11));
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    assert_eq!(locks.fast_path_len(), 1, "a weak request with no strong one pending stays off the main table");
    assert!(locks.main_is_empty());
    let w = queued(locks.request(b, "t", AEm, WaitRole::Ddl));
    assert_eq!(locks.fast_path_len(), 0, "the strong request moved it");
    assert_eq!(&locks.blockers(b)[..], &[a]);
    assert_eq!(locks.strong_pending_for("t"), 1);
    // A weak request now takes the main path, and waits behind the AE.
    let c = owner(12);
    let wc = queued(locks.request(c, "t", ASm, WaitRole::AutocommitReader));
    locks.release_owner(a);
    assert_eq!(w.outcome(), WaitOutcome::Granted);
    assert_eq!(wc.outcome(), WaitOutcome::Waiting);
    locks.release_owner(b);
    assert_eq!(wc.outcome(), WaitOutcome::Granted);
    assert_eq!(locks.strong_pending_for("t"), 0);
    locks.release_owner(c);
    assert!(locks.main_is_empty());
}

#[test]
fn a_strong_request_migrates_every_shard() {
    let locks = TableLocks::new();
    let holders: Vec<LockOwner> = (10..310).map(owner).collect();
    let shards: std::collections::HashSet<usize> = holders.iter().map(|o| shard_of(*o)).collect();
    assert_eq!(shards.len(), FAST_PATH_SHARDS, "the holders cover every shard");
    for o in &holders {
        assert!(granted(locks.request(*o, "t", RXm, WaitRole::Other)));
    }
    let ddl = owner(1000);
    let _w = queued(locks.request(ddl, "t", AEm, WaitRole::Ddl));
    assert_eq!(locks.fast_path_len(), 0, "an entry left in a shard is a holder the AE cannot see");
    let mut blockers = locks.blockers(ddl).to_vec();
    blockers.sort();
    assert_eq!(blockers, holders);
}

/// The loom test the plan names, run as an exhaustive interleaving instead
/// (loom is not available offline here): the weak request's two critical
/// sections (its shard section, then the main path if it fell through)
/// against every step of a strong request (raise the counter, migrate each
/// shard, the main path), at every relative position. Each step is one
/// mutex-guarded section of the real code, and each section's effects are
/// ordered by the mutexes, so these orders are every outcome the threads can
/// reach.
#[test]
fn no_interleaving_grants_ae_beside_a_fast_path_rx() {
    let strong_steps = 1 + FAST_PATH_SHARDS + 1;
    // Weak owners in the first and the last shard, so a migration that skips
    // either end is caught here as well as by `a_strong_request_migrates_every_shard`.
    let weak_owners: Vec<LockOwner> = [0, FAST_PATH_SHARDS - 1]
        .iter()
        .map(|s| (10..).map(owner).find(|o| shard_of(*o) == *s).unwrap())
        .collect();
    let ddl = owner(5);
    let mut runs = 0;
    for a in weak_owners {
        for first in 0..=strong_steps {
            for second in first..=strong_steps {
                let locks = TableLocks::new();
                let mut weak: Option<bool> = None;
                let mut fell_through: Option<ModeSet> = None;
                let mut strong: Option<Request> = None;
                for step in 0..=strong_steps {
                    if step == first {
                        match locks.fast_try(a, "t", RXm) {
                            Fast::Granted => weak = Some(true),
                            Fast::Main { fast } => fell_through = Some(fast),
                        }
                    }
                    if step == second
                        && let Some(fast) = fell_through.take()
                    {
                        weak = Some(match locks.decide(a, "t", RXm, WaitRole::Other, fast) {
                            Decided::Already | Decided::Granted => true,
                            Decided::Queued(_) => false,
                        });
                    }
                    if step == 0 {
                        locks.strong_begin("t");
                    } else if step <= FAST_PATH_SHARDS {
                        locks.migrate_shard(step - 1, "t");
                    } else if step == strong_steps - 1 {
                        strong = Some(locks.strong_finish(ddl, "t", AEm, WaitRole::Ddl));
                    }
                }
                let rx = weak.expect("the weak request ran");
                let ae = strong.expect("the strong request ran").is_granted();
                assert!(!(rx && ae), "AE granted beside a fast-path RX (owner {a:?}, weak at {first}/{second})");
                assert!(rx || ae, "with nothing else held one of them is granted");
                if rx {
                    assert_eq!(&locks.blockers(ddl)[..], &[a], "the AE waits for the RX it found");
                }
                runs += 1;
            }
        }
    }
    assert!(runs > 4000, "{runs} interleavings");
}

#[test]
fn rule_b_a_holder_goes_ahead_of_a_waiting_ae() {
    // R1's pattern: A holds a lock, B's AE queues behind it, and A's next
    // request on the table is granted at once, not queued behind B.
    let locks = TableLocks::new();
    let (a, b, c) = (owner(10), owner(11), owner(12));
    assert!(granted(locks.request(a, "t", ASm, WaitRole::Other)));
    let wb = queued(locks.request(b, "t", AEm, WaitRole::Ddl));
    assert!(
        granted(locks.request(a, "t", RXm, WaitRole::Other)),
        "A holds AS on t, so its RX passes the waiting AE (rule b)"
    );
    assert_eq!(locks.holds(a, "t"), ModeSet::of(ASm).union(ModeSet::of(RXm)));
    // C holds nothing, so rule (a) queues it behind the AE.
    let wc = queued(locks.request(c, "t", ASm, WaitRole::AutocommitReader));
    assert_eq!(&locks.blockers(c)[..], &[b]);
    locks.release_owner(a);
    assert_eq!(wb.outcome(), WaitOutcome::Granted);
    assert_eq!(wc.outcome(), WaitOutcome::Waiting);
}

#[test]
fn a_held_lock_re_requested_never_queues() {
    let locks = TableLocks::new();
    let (a, b) = (owner(10), owner(11));
    for mode in [RXm, Sm] {
        assert!(granted(locks.request(a, "t", mode, WaitRole::Other)));
        let wb = queued(locks.request(b, "t", AEm, WaitRole::Ddl));
        assert!(granted(locks.request(a, "t", mode, WaitRole::Other)));
        assert_eq!(locks.queued_on("t"), 1, "only B waits");
        locks.release_owner(a);
        assert_eq!(wb.outcome(), WaitOutcome::Granted);
        locks.release_owner(b);
        assert_eq!(locks.strong_pending_for("t"), 0, "a re-requested S counts once");
    }
}

#[test]
fn an_upgrade_waits_only_for_other_owners() {
    let locks = TableLocks::new();
    let (a, c) = (owner(10), owner(12));
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)), "its own RX does not block its AE");
    locks.release_owner(a);

    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    assert!(granted(locks.request(c, "t", ASm, WaitRole::Other)));
    let w = queued(locks.request(a, "t", AEm, WaitRole::Ddl));
    assert_eq!(&locks.blockers(a)[..], &[c], "the upgrade waits for C alone, never for itself");
    locks.release_owner(c);
    assert_eq!(w.outcome(), WaitOutcome::Granted);
    assert_eq!(locks.holds(a, "t"), ModeSet::of(RXm).union(ModeSet::of(AEm)));
}

#[test]
fn ending_an_ae_keeps_the_mode_held_before_it() {
    // Finding L5: a block holding RX runs a DDL (AE); the DDL's end gives back
    // the AE and the block keeps its RX.
    let locks = TableLocks::new();
    let (a, b) = (owner(10), owner(11));
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)));
    assert!(locks.release_mode(a, "t", AEm));
    assert_eq!(locks.holds(a, "t"), ModeSet::of(RXm), "the RX held before the AE survives it");
    assert_eq!(locks.strong_pending_for("t"), 0);
    let wb = queued(locks.request(b, "t", Sm, WaitRole::Other));
    assert_eq!(&locks.blockers(b)[..], &[a], "the kept RX still conflicts with S");
    locks.release_owner(a);
    assert_eq!(wb.outcome(), WaitOutcome::Granted);
    locks.release_owner(b);

    // The swap with no gap: AE held alone becomes RX.
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)));
    let wb = queued(locks.request(b, "t", Sm, WaitRole::Other));
    assert!(locks.downgrade(a, "t", AEm, RXm));
    assert_eq!(locks.holds(a, "t"), ModeSet::of(RXm));
    assert_eq!(wb.outcome(), WaitOutcome::Waiting, "S still conflicts with the RX it became");
    assert_eq!(locks.strong_pending_for("t"), 1, "B's waiting S, not A's AE");
    assert!(!locks.downgrade(a, "t", AEm, RXm), "nothing left to downgrade");
    locks.release_owner(a);
    assert_eq!(wb.outcome(), WaitOutcome::Granted);
    locks.release_owner(b);
    assert_eq!(locks.strong_pending_for("t"), 0);
    assert!(locks.main_is_empty());
}

#[test]
fn rule_a_queues_a_newcomer_behind_a_conflicting_strong_waiter_in_order() {
    let locks = TableLocks::new();
    let (a, b, c, d) = (owner(10), owner(11), owner(12), owner(13));
    assert!(granted(locks.request(a, "t", ASm, WaitRole::Other)));
    let wb = queued(locks.request(b, "t", AEm, WaitRole::Ddl));
    let wc = queued(locks.request(c, "t", ASm, WaitRole::AutocommitReader));
    let wd = queued(locks.request(d, "t", ASm, WaitRole::AutocommitReader));
    locks.release_owner(a);
    assert_eq!(wb.outcome(), WaitOutcome::Granted, "the AE that queued first goes first");
    assert_eq!((wc.outcome(), wd.outcome()), (WaitOutcome::Waiting, WaitOutcome::Waiting));
    locks.release_owner(b);
    assert_eq!((wc.outcome(), wd.outcome()), (WaitOutcome::Granted, WaitOutcome::Granted));
}

#[test]
fn rule_a_a_waiting_rx_holds_back_a_later_s() {
    // PostgreSQL's waitMask: a conflicting waiter of any mode queues a
    // newcomer, though S does not conflict with the S that is held.
    let locks = TableLocks::new();
    let (a, b, c) = (owner(10), owner(11), owner(12));
    assert!(granted(locks.request(a, "t", Sm, WaitRole::Other)));
    let wb = queued(locks.request(b, "t", RXm, WaitRole::Other));
    let wc = queued(locks.request(c, "t", Sm, WaitRole::Other));
    assert_eq!(&locks.blockers(c)[..], &[b], "C's edge is the RX ahead of it, not A's S");
    locks.release_owner(a);
    assert_eq!(wb.outcome(), WaitOutcome::Granted);
    assert_eq!(wc.outcome(), WaitOutcome::Waiting);
    assert_eq!(&locks.blockers(c)[..], &[b]);
    locks.release_owner(b);
    assert_eq!(wc.outcome(), WaitOutcome::Granted);
}

#[test]
fn a_stream_of_s_requests_does_not_starve_a_waiting_rx() {
    // CREATE INDEX-style S holders overlapping one another: under a rule
    // that counted only S and AE waiters, each new S passed the waiting RX
    // and the writer waited for as long as the stream lasted.
    let locks = TableLocks::new();
    let writer = owner(5);
    assert!(granted(locks.request(owner(10), "t", Sm, WaitRole::Other)));
    let w = queued(locks.request(writer, "t", RXm, WaitRole::Other));
    let mut later = Vec::new();
    for id in 11..20 {
        let r = locks.request(owner(id), "t", Sm, WaitRole::Other);
        assert!(!r.is_granted(), "S request {id} passed the waiting RX");
        later.push(queued(r));
    }
    // The S held when the RX queued ends: the RX goes next, before every S
    // that came after it.
    locks.release_owner(owner(10));
    assert_eq!(w.outcome(), WaitOutcome::Granted, "the RX is granted once the S it waited for ends");
    assert!(later.iter().all(|l| l.outcome() == WaitOutcome::Waiting));
    locks.release_owner(writer);
    assert!(later.iter().all(|l| l.outcome() == WaitOutcome::Granted), "the S requests share the table after it");
}

#[test]
fn a_stream_of_as_holders_re_requesting_does_not_starve_a_waiting_ae() {
    // Holders already block the AE, so their own next requests go ahead of
    // it (rule (b)), but a newcomer holding nothing queues behind it, and
    // once the holders end the AE is granted before any of them.
    let locks = TableLocks::new();
    let ddl = owner(5);
    let holders: Vec<LockOwner> = (10..14).map(owner).collect();
    for h in &holders {
        assert!(granted(locks.request(*h, "t", ASm, WaitRole::Other)));
    }
    let w = queued(locks.request(ddl, "t", AEm, WaitRole::Ddl));
    let mut newcomers = Vec::new();
    for (i, h) in holders.iter().enumerate() {
        assert!(granted(locks.request(*h, "t", ASm, WaitRole::Other)), "a held AS re-requested");
        assert!(granted(locks.request(*h, "t", RXm, WaitRole::Other)), "an AS holder blocks the AE, so it may pass it");
        newcomers.push(queued(locks.request(owner(100 + i as TxId), "t", ASm, WaitRole::AutocommitReader)));
        locks.release_owner(*h);
    }
    assert_eq!(w.outcome(), WaitOutcome::Granted, "the AE is granted once the holders end");
    assert!(newcomers.iter().all(|n| n.outcome() == WaitOutcome::Waiting));
    locks.release_owner(ddl);
    assert!(newcomers.iter().all(|n| n.outcome() == WaitOutcome::Granted));
}

#[test]
fn rule_b_a_holder_passes_only_the_waiters_it_blocks() {
    // X holds RX, so B's S waits. Readers holding AS then ask for RX: AS
    // does not conflict with S, so B does not wait for them and they may not
    // pass it. Under "holds any mode goes ahead" each would take RX beside
    // X's, and B would wait for as long as such readers kept coming.
    let locks = TableLocks::new();
    let (x, b) = (owner(5), owner(6));
    assert!(granted(locks.request(x, "t", RXm, WaitRole::Other)));
    let wb = queued(locks.request(b, "t", Sm, WaitRole::Other));
    let mut readers = Vec::new();
    for id in 10..14 {
        let r = owner(id);
        assert!(granted(locks.request(r, "t", ASm, WaitRole::Other)), "AS conflicts with neither X nor B");
        let got = locks.request(r, "t", RXm, WaitRole::Other);
        assert!(!got.is_granted(), "reader {id}'s RX passed the S it does not block");
        assert_eq!(&locks.blockers(r)[..], &[b]);
        readers.push((r, queued(got)));
    }
    locks.release_owner(x);
    assert_eq!(wb.outcome(), WaitOutcome::Granted, "the S is granted once X ends");
    assert!(readers.iter().all(|(_, w)| w.outcome() == WaitOutcome::Waiting));
    locks.release_owner(b);
    assert!(readers.iter().all(|(_, w)| w.outcome() == WaitOutcome::Granted));
}

#[test]
fn rule_b_a_holder_goes_ahead_of_everything_behind_the_first_waiter_it_blocks() {
    // A holds AS; the queue is [B: AE, C: S]. A's RX conflicts with both,
    // but A's place is just ahead of B (its AS blocks B), so C, behind B,
    // does not hold it back. Judged waiter by waiter, C would: A waits for
    // C, C for B (rule a), B for A's AS, a cycle PostgreSQL never forms.
    let locks = TableLocks::new();
    let (a, b, c, d) = (owner(10), owner(11), owner(12), owner(13));
    assert!(granted(locks.request(a, "t", ASm, WaitRole::Other)));
    let _wb = queued(locks.request(b, "t", AEm, WaitRole::Ddl));
    let _wc = queued(locks.request(c, "t", Sm, WaitRole::Other));
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    assert_eq!(locks.deadlock_check(c), None);
    // D holds nothing: its place is the end, behind both.
    let _wd = queued(locks.request(d, "t", RXm, WaitRole::Other));
    let mut bd = locks.blockers(d).to_vec();
    bd.sort();
    assert_eq!(bd, vec![b, c]);
}

#[test]
fn release_owner_drops_fast_path_main_and_queued_alike() {
    let locks = TableLocks::new();
    let (a, b, c) = (owner(10), owner(11), owner(12));
    assert!(granted(locks.request(a, "t1", RXm, WaitRole::Other)));
    assert!(granted(locks.request(a, "t2", ASm, WaitRole::Other)));
    assert!(granted(locks.request(a, "t3", AEm, WaitRole::Ddl)));
    assert!(granted(locks.request(a, "t3", Sm, WaitRole::Other)));
    assert!(granted(locks.request(b, "t4", AEm, WaitRole::Ddl)));
    let wa = queued(locks.request(a, "t4", ASm, WaitRole::Other));
    let wc = queued(locks.request(c, "t3", ASm, WaitRole::Other));
    locks.release_owner(a);
    for t in ["t1", "t2", "t3", "t4"] {
        assert!(locks.holds(a, t).is_empty(), "{t}");
    }
    assert_eq!(wa.outcome(), WaitOutcome::Abandoned);
    assert_eq!(wc.outcome(), WaitOutcome::Granted, "what A held is granted onward");
    assert_eq!(locks.strong_pending_for("t3"), 0, "both of A's strong modes are counted out");
    assert_eq!(locks.fast_path_len(), 0);
    locks.release_owner(b);
    locks.release_owner(c);
    assert!(locks.main_is_empty());
    for t in ["t1", "t2", "t3", "t4"] {
        assert_eq!(locks.strong_pending_for(t), 0, "{t}");
    }
}

#[test]
fn release_mode_reports_what_was_held() {
    let locks = TableLocks::new();
    let a = owner(10);
    assert!(!locks.release_mode(a, "t", RXm));
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    assert!(!locks.release_mode(a, "t", AEm));
    assert!(locks.release_mode(a, "t", RXm));
    assert!(!locks.release_mode(a, "t", RXm));
    assert_eq!(locks.fast_path_len(), 0);
}

#[test]
fn a_weak_mode_recorded_twice_is_released_from_both_places() {
    let locks = TableLocks::new();
    let (a, b) = (owner(10), owner(11));
    // RX on the fast path, migrated into the main table by B's AE…
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    let wb = queued(locks.request(b, "t", AEm, WaitRole::Ddl));
    assert_eq!(locks.abandon(&wb), WaitOutcome::Waiting);
    // …then recorded again on the fast path once nothing strong is pending.
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    assert_eq!(locks.fast_path_len(), 1);
    assert!(locks.release_mode(a, "t", RXm));
    assert!(locks.holds(a, "t").is_empty());
    assert!(granted(locks.request(b, "t", AEm, WaitRole::Ddl)));
}

#[test]
fn try_acquire_queues_nothing() {
    let locks = TableLocks::new();
    let (a, b) = (owner(10), owner(11));
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)));
    assert!(!locks.try_acquire(b, "t", ASm));
    assert_eq!(locks.queued_on("t"), 0);
    assert!(locks.blockers(b).is_empty());
    locks.release_owner(a);
    assert!(locks.try_acquire(b, "t", ASm));
}

#[test]
fn an_owner_is_never_frozen_tx() {
    use crate::core::mvcc::transaction::{FROZEN_TX, NO_TX};
    assert_eq!(LockOwner::block(FROZEN_TX), None);
    assert_eq!(LockOwner::autocommit(FROZEN_TX), None);
    assert_eq!(LockOwner::block(NO_TX), None);
    assert_eq!(LockOwner::autocommit(7).map(LockOwner::id), Some(7));
}

fn member(id: TxId, mode: Option<TableMode>, role: WaitRole) -> CycleMember {
    CycleMember {
        owner: owner(id),
        mode,
        role,
    }
}

#[test]
fn the_victim_is_chosen_by_role_before_age() {
    use WaitRole::*;
    // An older DDL goes before a younger writer; of two DDLs, the younger.
    let v = choose_victim(&[member(10, Some(AEm), Ddl), member(20, Some(RXm), Other)]);
    assert_eq!(v, Some(owner(10)));
    let v = choose_victim(&[member(10, Some(AEm), Ddl), member(30, Some(AEm), Ddl), member(20, Some(RXm), Other)]);
    assert_eq!(v, Some(owner(30)));
    // No DDL: an S or AE requester (LOCK TABLE), however old.
    let v = choose_victim(&[member(10, Some(Sm), Other), member(20, Some(RXm), Other), member(30, Some(ASm), AutocommitReader)]);
    assert_eq!(v, Some(owner(10)));
    // Neither: the youngest that is not an autocommit reader.
    let v = choose_victim(&[member(10, Some(RXm), Other), member(20, None, Other), member(30, Some(ASm), AutocommitReader)]);
    assert_eq!(v, Some(owner(20)));
    // Never an autocommit reader.
    assert_eq!(choose_victim(&[member(10, Some(ASm), AutocommitReader)]), None);
}

#[test]
fn an_upgrade_deadlock_fails_the_younger_ddl() {
    let locks = TableLocks::new();
    let (a, b) = (owner(10), owner(11));
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    assert!(granted(locks.request(b, "t", RXm, WaitRole::Other)));
    let wa = queued(locks.request(a, "t", AEm, WaitRole::Ddl));
    let wb = queued(locks.request(b, "t", AEm, WaitRole::Ddl));
    assert_eq!(locks.deadlock_check(a), Some(b), "the older member found the cycle; the younger DDL fails");
    assert_eq!(wb.outcome(), WaitOutcome::Deadlock);
    assert_eq!(wa.outcome(), WaitOutcome::Waiting);
    assert_eq!(locks.deadlock_check(a), None, "the cycle is gone");
    // The victim's block aborts, releasing what it held.
    locks.release_owner(b);
    assert_eq!(wa.outcome(), WaitOutcome::Granted);
    locks.release_owner(a);
    assert_eq!(locks.strong_pending_for("t"), 0);
}

#[test]
fn a_ddl_is_the_victim_over_a_younger_writer() {
    // W (young) holds RX on t1; D (old) holds AS on t2 and wants AE on t1; W
    // wants AE on t2. D is chosen though W is younger.
    let locks = TableLocks::new();
    let (d, w) = (owner(10), owner(20));
    assert!(granted(locks.request(w, "t1", RXm, WaitRole::Other)));
    assert!(granted(locks.request(d, "t2", ASm, WaitRole::Other)));
    let wd = queued(locks.request(d, "t1", AEm, WaitRole::Ddl));
    let ww = queued(locks.request(w, "t2", AEm, WaitRole::Other));
    assert_eq!(locks.deadlock_check(w), Some(d));
    assert_eq!(wd.outcome(), WaitOutcome::Deadlock);
    assert_eq!(ww.outcome(), WaitOutcome::Waiting);
}

#[test]
fn an_autocommit_reader_is_never_the_victim() {
    // R (youngest) is a reader past its restarts, holding AS on t1 and
    // waiting for AS on t2, where L holds AE (LOCK TABLE) and wants t1.
    let locks = TableLocks::new();
    let (l, r) = (owner(10), owner(20));
    assert!(granted(locks.request(r, "t1", ASm, WaitRole::AutocommitReader)));
    assert!(granted(locks.request(l, "t2", AEm, WaitRole::Other)));
    let wl = queued(locks.request(l, "t1", AEm, WaitRole::Other));
    let wr = queued(locks.request(r, "t2", ASm, WaitRole::AutocommitReader));
    assert_eq!(locks.deadlock_check(r), Some(l));
    assert_eq!(wl.outcome(), WaitOutcome::Deadlock);
    assert_eq!(wr.outcome(), WaitOutcome::Waiting);
}

#[test]
fn a_wait_on_no_cycle_finds_none() {
    let locks = TableLocks::new();
    let (a, b, c) = (owner(10), owner(11), owner(12));
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)));
    let _wb = queued(locks.request(b, "t", ASm, WaitRole::Other));
    let _wc = queued(locks.request(c, "t", ASm, WaitRole::Other));
    assert_eq!(locks.deadlock_check(b), None);
    assert_eq!(locks.deadlock_check(a), None, "A is not waiting");
}

fn quick() -> TableLockConfig {
    TableLockConfig {
        deadlock_timeout: Duration::from_millis(20),
        poll: Duration::from_millis(2),
    }
}

const NEVER: &(dyn Fn() -> bool + Sync) = &|| false;

#[test]
fn lock_timeout_zero_reads_as_no_bound() {
    assert_eq!(LockTimeout::from_setting_ms(0), LockTimeout::Unbounded);
    assert_eq!(LockTimeout::from_setting_ms(0).limit(), None);
    assert_eq!(LockTimeout::from_setting_ms(250).limit(), Some(Duration::from_millis(250)));
}

#[tokio::test]
async fn a_wait_under_lock_timeout_zero_outlasts_every_poll_and_deadline() {
    let locks = Arc::new(TableLocks::with_config(quick()));
    let (a, b) = (owner(10), owner(11));
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)));
    let l = locks.clone();
    let waiter = tokio::spawn(async move {
        l.acquire(b, "t", ASm, WaitRole::Other, LockTimeout::from_setting_ms(0), NEVER).await
    });
    // Ten deadlock timeouts and a hundred polls later it is still waiting.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiter.is_finished(), "lock_timeout = 0 must not bound the wait");
    locks.release_owner(a);
    let got = tokio::time::timeout(Duration::from_secs(5), waiter).await.expect("woken").unwrap();
    assert_eq!(got, Ok(()));
    assert!(locks.holds(b, "t").contains(ASm));
}

#[tokio::test]
async fn a_positive_lock_timeout_fails_with_55p03_and_leaves_nothing_queued() {
    let locks = TableLocks::with_config(quick());
    let (a, b) = (owner(10), owner(11));
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    let started = Instant::now();
    let got = locks.acquire(b, "t", AEm, WaitRole::Ddl, LockTimeout::After(Duration::from_millis(40)), NEVER).await;
    assert_eq!(got, Err(LockError::Timeout));
    assert_eq!(LockError::Timeout.sqlstate(), "55P03");
    assert!(started.elapsed() >= Duration::from_millis(40));
    assert_eq!(locks.queued_on("t"), 0);
    assert_eq!(locks.strong_pending_for("t"), 0, "the abandoned AE no longer diverts weak requests");
    assert!(locks.holds(b, "t").is_empty());
}

#[tokio::test]
async fn cancellation_ends_a_wait_with_57014() {
    let locks = TableLocks::with_config(quick());
    let (a, b) = (owner(10), owner(11));
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)));
    let flag = std::sync::atomic::AtomicBool::new(false);
    let cancel = || flag.load(std::sync::atomic::Ordering::SeqCst);
    let (got, ()) = tokio::join!(
        locks.acquire(b, "t", ASm, WaitRole::Other, LockTimeout::Unbounded, &cancel),
        async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    );
    assert_eq!(got, Err(LockError::Cancelled));
    assert_eq!(LockError::Cancelled.sqlstate(), "57014");
    assert_eq!(locks.queued_on("t"), 0);
}

#[tokio::test]
async fn the_wait_loop_breaks_a_deadlock_with_40p01_to_the_victim_only() {
    let locks = TableLocks::with_config(quick());
    let (a, b) = (owner(10), owner(11));
    assert!(granted(locks.request(a, "t", RXm, WaitRole::Other)));
    assert!(granted(locks.request(b, "t", RXm, WaitRole::Other)));
    let run = |o: LockOwner| {
        let locks = &locks;
        async move {
            let got = locks.acquire(o, "t", AEm, WaitRole::Ddl, LockTimeout::Unbounded, NEVER).await;
            if got.is_err() {
                // The victim's block aborts.
                locks.release_owner(o);
            }
            got
        }
    };
    let (ga, gb) = tokio::time::timeout(Duration::from_secs(10), async { tokio::join!(run(a), run(b)) })
        .await
        .expect("the deadlock was broken");
    assert_eq!(ga, Ok(()), "the older DDL survives");
    assert_eq!(gb, Err(LockError::Deadlock));
    assert_eq!(LockError::Deadlock.sqlstate(), "40P01");
    assert_eq!(locks.holds(a, "t"), ModeSet::of(RXm).union(ModeSet::of(AEm)));
}

/// The safety net for an owner that never released (B3b): a holder whose
/// transaction aborted is released by the first request it blocks, and what
/// waited behind it is granted; a live holder, and a committed one (whose
/// locks must outlive its commit until its release, finding L4), is not.
///
/// Mutation: `prune_dead` prunes nothing (the request queues behind the
/// dead holder for ever).
#[test]
fn an_aborted_holder_is_released_by_the_request_it_blocks() {
    let locks = TableLocks::new();
    let dead: Arc<std::sync::Mutex<std::collections::HashSet<TxId>>> = Arc::default();
    let marks = dead.clone();
    locks.set_liveness(move |id| !marks.lock().unwrap().contains(&id));
    let (a, b, c) = (owner(10), owner(11), owner(12));
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)));
    let w = queued(locks.request(b, "t", ASm, WaitRole::Other));
    // Alive: c queues too.
    let wc = queued(locks.request(c, "t", RXm, WaitRole::Other));
    assert_eq!(locks.abandon(&wc), WaitOutcome::Waiting);
    // Dead: the next request releases it, and the queue behind it moves.
    dead.lock().unwrap().insert(10);
    assert!(granted(locks.request(c, "t", RXm, WaitRole::Other)), "a dead holder still blocked");
    assert_eq!(w.outcome(), WaitOutcome::Granted, "the waiter behind the dead holder was not granted");
    assert!(locks.holds(a, "t").is_empty());
    assert_eq!(locks.strong_pending_for("t"), 0, "the dead AE's strong count was left behind");
}

/// A wait whose future is dropped (its task cancelled) leaves the queue: it
/// is not granted later behind the holder's release, so a request after
/// that release is not held back by it.
///
/// Mutation: `wait_for` without its drop guard (the dropped waiter is
/// granted AS on the holder's release and refuses the AE).
#[tokio::test]
async fn a_dropped_wait_leaves_the_queue() {
    let locks = Arc::new(TableLocks::with_config(quick()));
    let (a, b, c) = (owner(10), owner(11), owner(12));
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)));
    let l = locks.clone();
    let waiter = tokio::spawn(async move {
        l.acquire(b, "t", ASm, WaitRole::Other, LockTimeout::Unbounded, NEVER).await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(locks.queued_on("t"), 1);
    waiter.abort();
    let _ = waiter.await;
    assert_eq!(locks.queued_on("t"), 0, "the dropped wait is still queued");
    locks.release_owner(a);
    assert!(locks.holds(b, "t").is_empty(), "the dropped wait was granted");
    assert!(locks.try_acquire(c, "t", AEm));
}

/// `release_owner` skips the main mutex when nothing is on the main path,
/// and still releases a main-path holding when something is: the fast-path
/// statement end and the strong one share one function.
///
/// Mutation: the skip ignores `main_busy` (an AE is never released).
#[test]
fn release_owner_skips_the_main_path_only_when_it_is_empty() {
    let locks = TableLocks::new();
    let (a, b) = (owner(10), owner(11));
    assert!(granted(locks.request(a, "t", ASm, WaitRole::Other)));
    locks.release_owner(a);
    assert!(locks.main_is_empty() && locks.fast_path_len() == 0);
    assert!(granted(locks.request(a, "t", AEm, WaitRole::Ddl)));
    locks.release_owner(a);
    assert!(locks.holds(a, "t").is_empty(), "the AE outlived its owner's release");
    assert!(locks.try_acquire(b, "t", AEm));
    locks.release_owner(b);
    assert!(locks.main_is_empty());
}

/// What a statement's weak lock costs on the fast path, request and the
/// owner's release, for B3b's perf gate: run with `--release --ignored
/// fast_path_cost --nocapture`.
#[test]
#[ignore]
fn fast_path_cost() {
    let locks = TableLocks::new();
    let n = 1_000_000u64;
    let started = Instant::now();
    for i in 0..n {
        let o = owner(1_000 + i);
        assert!(granted(locks.request(o, "acc", ASm, WaitRole::AutocommitReader)));
        assert!(granted(locks.request(o, "acc", ASm, WaitRole::AutocommitReader)));
        locks.release_owner(o);
    }
    let per = started.elapsed().as_nanos() as f64 / n as f64;
    println!("{per:7.1} ns per statement: one AS, one confirmation, one release");
}
