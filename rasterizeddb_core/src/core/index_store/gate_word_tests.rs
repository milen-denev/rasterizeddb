//! The gate word's unit tests: the layout, each transition on its own, and
//! [`WriteGate`]'s slow path with its injected I/O. The interleavings are
//! `gate_word_model.rs`'s.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use super::*;

fn gate(state: GateState) -> Arc<WriteGate> {
    Arc::new(WriteGate::new(state))
}

#[test]
fn the_word_packs_count_state_and_flags_without_overlap() {
    let parts = [COUNT_MASK, STATE_MASK, SEALED, MANDATORY, PUBLISHING, CANCEL, RESERVED];
    for (i, a) in parts.iter().enumerate() {
        for b in &parts[i + 1..] {
            assert_eq!(a & b, 0, "{a:#x} overlaps {b:#x}");
        }
    }
    assert_eq!(parts.iter().fold(0, |acc, p| acc | p), u64::MAX, "every bit has one meaning");
    for s in [GateState::Clean, GateState::Dirty, GateState::Unknown] {
        let w = Word::new(s);
        assert_eq!((w.state(), w.count(), w.0 & FLAGS), (s, 0, 0));
    }
    // Taking a ticket is a plain add on the low bits.
    let w = Word(Word::new(GateState::Dirty).0 + 3);
    assert_eq!((w.state(), w.count()), (GateState::Dirty, 3));
    assert!(w.well_formed());
}

#[test]
fn a_ticket_on_a_dirty_word_is_one_rmw_and_its_drop_gives_it_back() {
    let g = gate(GateState::Dirty);
    let h = g.try_acquire().expect("held");
    assert_eq!(g.load().count(), 1);
    let h2 = g.try_acquire().expect("held");
    assert_eq!(g.load().count(), 2);
    drop(h);
    drop(h2);
    assert_eq!(g.load(), Word::new(GateState::Dirty));
}

#[test]
fn clean_and_unknown_send_a_ticket_to_the_slow_path_with_no_count_left() {
    for (state, reason) in [(GateState::Clean, SlowReason::Clean), (GateState::Unknown, SlowReason::Unknown)] {
        let g = gate(state);
        assert_eq!(g.try_acquire().unwrap_err(), reason);
        assert_eq!(g.load(), Word::new(state), "the back-out returned the count");
    }
}

#[test]
fn a_writer_preempts_an_optional_seal_which_then_cannot_publish() {
    let g = gate(GateState::Dirty);
    assert_eq!(g.seal(SealKind::Optional), SealStart::Sealed { outstanding: 0 });
    let h = g.try_acquire().expect("a writer does not wait for an optional seal");
    let w = g.load();
    assert!(w.sealed() && w.cancelled() && w.count() == 1);
    assert_eq!(g.try_publish(), PublishEnd::Cancelled);
    assert_eq!(g.unseal(SealEnd::Abandoned), GateState::Dirty);
    assert_eq!(g.load(), Word(Word::new(GateState::Dirty).0 + 1), "every flag cleared, the count kept");
    drop(h);
}

#[test]
fn an_optional_seal_finding_tickets_skips() {
    let g = gate(GateState::Dirty);
    let h = g.try_acquire().unwrap();
    assert_eq!(g.seal(SealKind::Optional), SealStart::Sealed { outstanding: 1 });
    assert_eq!(g.try_publish(), PublishEnd::Busy { count: 1 });
    g.unseal(SealEnd::Abandoned);
    drop(h);
    assert_eq!(g.load(), Word::new(GateState::Dirty));
}

#[test]
fn a_mandatory_seal_queues_tickets_and_publishes_once_drained() {
    let g = gate(GateState::Dirty);
    let h = g.try_acquire().unwrap();
    assert_eq!(g.seal(SealKind::Mandatory), SealStart::Sealed { outstanding: 1 });
    assert_eq!(g.try_acquire().unwrap_err(), SlowReason::SealRunning);
    assert_eq!(g.try_publish(), PublishEnd::Busy { count: 1 }, "PUBLISHING needs a count of zero");
    drop(h);
    assert_eq!(g.try_publish(), PublishEnd::Publishing);
    assert_eq!(g.try_acquire().unwrap_err(), SlowReason::Publishing);
    assert_eq!(g.cancel_seal(), CancelEnd::Publishing, "too late to cancel");
    assert_eq!(g.unseal(SealEnd::Published), GateState::Clean);
    assert_eq!(g.load(), Word::new(GateState::Clean));
    assert_eq!(g.try_acquire().unwrap_err(), SlowReason::Clean, "after CLEAN the next write needs DIRTY");
}

#[test]
fn a_failure_after_clean_was_issued_leaves_unknown() {
    let g = gate(GateState::Dirty);
    g.seal(SealKind::Optional);
    assert_eq!(g.try_publish(), PublishEnd::Publishing);
    assert_eq!(g.unseal(SealEnd::FailedAfterCleanIssued), GateState::Unknown);
    assert_eq!(g.try_acquire().unwrap_err(), SlowReason::Unknown);
}

#[test]
fn a_seal_starts_only_from_dirty() {
    for state in [GateState::Clean, GateState::Unknown] {
        let g = gate(state);
        assert_eq!(g.seal(SealKind::Mandatory), SealStart::NotDirty);
        assert_eq!(g.load(), Word::new(state), "nothing left sealed");
    }
}

#[test]
fn retirement_cancels_only_an_unpublished_seal() {
    let g = gate(GateState::Dirty);
    assert_eq!(g.cancel_seal(), CancelEnd::NoSeal);
    assert!(!g.load().cancelled(), "no CANCEL on an unsealed word");
    g.seal(SealKind::Mandatory);
    assert_eq!(g.cancel_seal(), CancelEnd::Cancelled);
    assert_eq!(g.cancel_seal(), CancelEnd::AlreadyCancelled);
    // A cancelled mandatory seal no longer queues tickets.
    let h = g.try_acquire().expect("held once the seal is cancelled");
    assert_eq!(g.try_publish(), PublishEnd::Cancelled);
    g.unseal(SealEnd::Abandoned);
    assert!(!g.load().cancelled() && !g.load().sealed());
    drop(h);
}

#[test]
fn a_gated_write_registers_on_the_same_count() {
    let g = gate(GateState::Dirty);
    let io = g.register_io().unwrap();
    assert!(!io.ticketless());
    assert_eq!(g.load().count(), 1);
    drop(io);
    // A seal with no ticket anywhere: the write is ticketless and cancels it.
    g.seal(SealKind::Mandatory);
    let io = g.register_io().unwrap();
    assert!(io.ticketless());
    assert!(g.load().cancelled());
    drop(io);
    g.unseal(SealEnd::Abandoned);
    // With a ticket counted, a write under a seal is issued with no verdict.
    let h = g.try_acquire().unwrap();
    g.seal(SealKind::Mandatory);
    let io = g.register_io().unwrap();
    assert!(!io.ticketless() && !g.load().cancelled());
    drop((io, h));
    assert_eq!(g.try_publish(), PublishEnd::Publishing);
    assert_eq!(g.register_io().unwrap_err(), SlowReason::Publishing);
    g.unseal(SealEnd::Published);
    assert_eq!(g.register_io().unwrap_err(), SlowReason::Clean);
    assert_eq!(g.load(), Word::new(GateState::Clean));
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "borrow into the state and flags")]
fn a_release_without_a_count_is_caught_before_it_borrows_into_the_state() {
    let word = AtomicU64::new(Word::new(GateState::Dirty).0);
    release(&word);
}

#[tokio::test]
async fn the_slow_path_runs_its_io_once_then_holds() {
    let g = gate(GateState::Clean);
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let slow = move |state: GateState| {
        let c = c.clone();
        async move {
            assert_eq!(state, GateState::Clean);
            c.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<(), std::io::Error>(())
        }
    };
    let (a, b) = tokio::join!(g.acquire(slow.clone()), g.acquire(slow));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "single flight: DIRTY is appended once");
    assert_eq!(g.load().state(), GateState::Dirty);
    assert_eq!(g.load().count(), 2);
    drop((a.unwrap(), b.unwrap()));
    assert_eq!(g.load(), Word::new(GateState::Dirty));
}

#[tokio::test]
async fn a_failed_slow_path_fails_the_ticket_and_leaves_the_state() {
    let g = gate(GateState::Unknown);
    let got = g
        .acquire(|state| async move {
            assert_eq!(state, GateState::Unknown);
            Err::<(), &str>("ENOSPC and the unlink failed")
        })
        .await;
    assert_eq!(got.unwrap_err(), "ENOSPC and the unlink failed");
    assert_eq!(g.load(), Word::new(GateState::Unknown), "the word leaves Unknown only after Ok");
}

#[tokio::test]
async fn a_writer_waits_out_a_mandatory_seal() {
    let g = gate(GateState::Dirty);
    g.seal(SealKind::Mandatory);
    let g2 = g.clone();
    let writer = tokio::spawn(async move {
        let h = g2.acquire(|_| async { Ok::<(), ()>(()) }).await.unwrap();
        let w = g2.load();
        drop(h);
        w
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!writer.is_finished(), "a mandatory seal queues new tickets");
    g.unseal(SealEnd::Abandoned);
    let seen = tokio::time::timeout(Duration::from_secs(5), writer).await.expect("woken").unwrap();
    assert_eq!((seen.state(), seen.sealed()), (GateState::Dirty, false));
}

#[tokio::test]
async fn a_mandatory_seal_waits_for_the_count_to_drain() {
    let g = gate(GateState::Dirty);
    let h = g.try_acquire().unwrap();
    g.seal(SealKind::Mandatory);
    let g2 = g.clone();
    let sealer = tokio::spawn(async move {
        g2.drained().await;
        g2.try_publish()
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!sealer.is_finished());
    drop(h);
    let end = tokio::time::timeout(Duration::from_secs(5), sealer).await.expect("woken").unwrap();
    assert_eq!(end, PublishEnd::Publishing);
}
