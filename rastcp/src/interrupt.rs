//! What can end a wait on a connection: the server stopping, or a deadline.
//!
//! Both used to be bought per request. Every read, handler call and write was
//! wrapped in its own `tokio::time::timeout`, which enters and leaves the
//! runtime's timer wheel under a lock, and every read was raced against a
//! `watch` receiver, which takes a shared read lock and then a shared mutex to
//! queue a waiter. A server answering small requests spent a tenth of its
//! cycles on that, all of it on memory every connection writes to.
//!
//! Here each connection pays once instead:
//!
//! - [`StopSignal`] is a flag plus a list of wakers. A connection registers its
//!   waker the first time it parks and then only reads the flag, which nothing
//!   writes until the server stops.
//! - [`Deadline`] is one timer per connection. A wait asks the clock for its
//!   own deadline only if it actually has to park, and the timer is moved only
//!   when it fires and finds the wait it was armed for already over. A busy
//!   connection therefore touches the timer wheel once per timeout period
//!   rather than three times per request.
//!
//! The timeouts mean what they did: a wait that is still pending `period`
//! after it first could not make progress is interrupted.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use cacheguard::CacheGuard;
use tokio::time::{Instant, Sleep};

/// Why a guarded wait ended before its future did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Interrupted {
    /// The server is stopping.
    Stopping,
    /// The wait outlived its deadline.
    TimedOut,
}

/// The wakers of everything waiting to hear that the server is stopping.
struct Waiters {
    slots: Vec<Option<Waker>>,
    free: Vec<usize>,
}

/// A stop request every connection can see without writing to shared memory.
pub(crate) struct StopSignal {
    /// Read on every wait and written once, so it gets a line of its own: the
    /// reference counts beside it in the `Arc` are written per connection.
    stopping: CacheGuard<AtomicBool>,
    waiters: Mutex<Waiters>,
}

impl StopSignal {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            stopping: CacheGuard::new(AtomicBool::new(false)),
            waiters: Mutex::new(Waiters {
                slots: Vec::new(),
                free: Vec::new(),
            }),
        })
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Acquire)
    }

    /// Raises the flag and wakes everything registered. Idempotent.
    pub(crate) fn stop(&self) {
        // The flag goes up before the list is read. A listener registers under
        // the same lock and looks at the flag again afterwards, so it either
        // is in the list read here or sees the flag itself.
        self.stopping.store(true, Ordering::SeqCst);

        let wakers: Vec<Waker> = {
            let mut waiters = self.lock();
            waiters.slots.iter_mut().filter_map(Option::take).collect()
        };

        // Woken outside the lock: a waker may run arbitrary scheduler code.
        for waker in wakers {
            waker.wake();
        }
    }

    pub(crate) fn listen(self: &Arc<Self>) -> StopListener {
        StopListener {
            signal: self.clone(),
            slot: None,
            waker: None,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Waiters> {
        // Nothing run under this lock can panic halfway through an update, so
        // a poisoned lock still guards a consistent list.
        self.waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(test)]
    fn registered(&self) -> usize {
        self.lock()
            .slots
            .iter()
            .filter(|slot| slot.is_some())
            .count()
    }

    #[cfg(test)]
    fn slots(&self) -> usize {
        self.lock().slots.len()
    }
}

/// One party's view of a [`StopSignal`].
pub(crate) struct StopListener {
    signal: Arc<StopSignal>,
    slot: Option<usize>,
    /// The waker currently in the signal's list, kept so the common case — the
    /// same task asking again — is a pointer comparison and not a lock.
    waker: Option<Waker>,
}

impl StopListener {
    pub(crate) fn is_stopping(&self) -> bool {
        self.signal.is_stopping()
    }

    /// Ready once the server is stopping; otherwise makes sure `cx`'s task is
    /// woken when it does.
    pub(crate) fn poll_stopped(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.signal.is_stopping() {
            return Poll::Ready(());
        }

        if self
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(cx.waker()))
        {
            return Poll::Pending;
        }

        {
            let mut waiters = self.signal.lock();
            let slot = match self.slot {
                Some(slot) => slot,
                None => {
                    let slot = match waiters.free.pop() {
                        Some(slot) => slot,
                        None => {
                            waiters.slots.push(None);
                            waiters.slots.len() - 1
                        }
                    };
                    self.slot = Some(slot);
                    slot
                }
            };
            waiters.slots[slot] = Some(cx.waker().clone());
        }
        self.waker = Some(cx.waker().clone());

        // A stop that read the list before the waker went in has already
        // raised the flag; see `StopSignal::stop`.
        if self.signal.is_stopping() {
            return Poll::Ready(());
        }

        Poll::Pending
    }

    /// Resolves once the server is stopping.
    pub(crate) async fn stopped(&mut self) {
        std::future::poll_fn(|cx| self.poll_stopped(cx)).await
    }
}

impl Drop for StopListener {
    fn drop(&mut self) {
        if let Some(slot) = self.slot {
            let mut waiters = self.signal.lock();
            waiters.slots[slot] = None;
            waiters.free.push(slot);
        }
    }
}

/// One timer that bounds every wait on a connection, one wait at a time.
pub(crate) struct Deadline {
    period: Duration,
    sleep: Pin<Box<Sleep>>,
    /// When the timer will next fire. Never later than the deadline of the
    /// wait in progress, so a firing can be early but never late.
    armed: Instant,
    /// The waker the timer holds, for the same reason as
    /// [`StopListener::waker`].
    waker: Option<Waker>,
}

impl Deadline {
    /// A deadline of `period` per wait. Must be called inside a Tokio runtime,
    /// and the result used on that runtime.
    pub(crate) fn new(period: Duration) -> Self {
        let armed = Instant::now() + period;

        Self {
            period,
            // Not registered with the timer wheel until first polled, so a
            // connection that never has to park never enters it.
            sleep: Box::pin(tokio::time::sleep_until(armed)),
            armed,
            waker: None,
        }
    }

    /// Called by a wait that is about to park. `expires` is that wait's own
    /// deadline, taken the first time it parks.
    ///
    /// Ready when the wait has outlived it. Otherwise the timer is known to be
    /// registered to wake `cx`'s task no later than `expires`.
    pub(crate) fn poll_expired(&mut self, cx: &mut Context<'_>, expires: Instant) -> Poll<()> {
        let listening = self
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(cx.waker()));

        if expires < self.armed {
            // Waits on one connection start in order with one period, so this
            // does not happen; it is here so the invariant on `armed` holds by
            // construction and not by how the callers happen to behave.
            self.sleep.as_mut().reset(expires);
            self.armed = expires;
        } else if listening && !self.sleep.is_elapsed() {
            return Poll::Pending;
        }

        loop {
            // A `Sleep` polled with no cooperative budget left returns pending
            // without registering anything, so only a poll that began with
            // budget is proof the timer holds this waker.
            let had_budget = tokio::task::coop::has_budget_remaining();

            match self.sleep.as_mut().poll(cx) {
                Poll::Pending => {
                    self.waker = had_budget.then(|| cx.waker().clone());
                    return Poll::Pending;
                }
                Poll::Ready(()) => {
                    if Instant::now() >= expires {
                        return Poll::Ready(());
                    }

                    // It fired for a wait that has since finished. Move it to
                    // the one in progress and go round to register the waker.
                    self.sleep.as_mut().reset(expires);
                    self.armed = expires;
                    self.waker = None;
                }
            }
        }
    }

    /// Runs `future` to completion unless it stays pending past the deadline.
    pub(crate) async fn run<F: Future>(&mut self, future: F) -> Result<F::Output, Interrupted> {
        let mut future = std::pin::pin!(future);
        let mut expires = None;

        std::future::poll_fn(|cx| {
            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                return Poll::Ready(Ok(output));
            }

            self.poll_wait(cx, &mut expires)
        })
        .await
    }

    /// The part of a guarded wait that runs once its future has returned
    /// pending.
    fn poll_wait<T>(
        &mut self,
        cx: &mut Context<'_>,
        expires: &mut Option<Instant>,
    ) -> Poll<Result<T, Interrupted>> {
        let expires = *expires.get_or_insert_with(|| Instant::now() + self.period);

        match self.poll_expired(cx, expires) {
            Poll::Ready(()) => Poll::Ready(Err(Interrupted::TimedOut)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Everything that can interrupt one served connection.
pub(crate) struct Guard {
    stop: StopListener,
    deadline: Deadline,
}

impl Guard {
    pub(crate) fn new(stop: StopListener, period: Duration) -> Self {
        Self {
            stop,
            deadline: Deadline::new(period),
        }
    }

    pub(crate) fn is_stopping(&self) -> bool {
        self.stop.is_stopping()
    }

    /// Runs `future` to completion with no deadline, unless — when
    /// `stoppable` — the server stops first, which is `None`.
    ///
    /// For a wait that is bounded elsewhere: a connection waiting on handlers
    /// that each carry their own timeout.
    pub(crate) async fn run_unbounded<F: Future>(
        &mut self,
        future: F,
        stoppable: bool,
    ) -> Option<F::Output> {
        let mut future = std::pin::pin!(future);

        std::future::poll_fn(|cx| {
            if stoppable && self.stop.is_stopping() {
                return Poll::Ready(None);
            }

            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                return Poll::Ready(Some(output));
            }

            if stoppable && self.stop.poll_stopped(cx).is_ready() {
                return Poll::Ready(None);
            }

            Poll::Pending
        })
        .await
    }

    /// Runs `future` to completion unless it stays pending past the deadline
    /// or — when `stoppable` — the server stops first.
    ///
    /// A stop that is already pending wins over a future that is ready, so a
    /// stopping server does not start on a request that arrived in the same
    /// instant.
    pub(crate) async fn run<F: Future>(
        &mut self,
        future: F,
        stoppable: bool,
    ) -> Result<F::Output, Interrupted> {
        let mut future = std::pin::pin!(future);
        let mut expires = None;

        std::future::poll_fn(|cx| {
            if stoppable && self.stop.is_stopping() {
                return Poll::Ready(Err(Interrupted::Stopping));
            }

            if let Poll::Ready(output) = future.as_mut().poll(cx) {
                return Poll::Ready(Ok(output));
            }

            if stoppable && self.stop.poll_stopped(cx).is_ready() {
                return Poll::Ready(Err(Interrupted::Stopping));
            }

            self.deadline.poll_wait(cx, &mut expires)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant as StdInstant;

    const PERIOD: Duration = Duration::from_millis(200);

    /// Well clear of scheduling jitter on a loaded machine, and well short of
    /// a second period.
    const SLACK: Duration = Duration::from_millis(120);

    /// Ends a test whose wait was never interrupted, so a deadline that fails
    /// to fire is a failure and not a test run that never returns.
    async fn bounded<F: Future>(wait: F) -> F::Output {
        tokio::time::timeout(PERIOD * 10, wait)
            .await
            .expect("the wait was neither finished nor interrupted")
    }

    #[tokio::test]
    async fn a_wait_that_never_finishes_is_interrupted_at_its_deadline() {
        let mut deadline = Deadline::new(PERIOD);
        let started = StdInstant::now();

        let outcome = bounded(deadline.run(std::future::pending::<()>())).await;

        assert_eq!(outcome, Err(Interrupted::TimedOut));
        let took = started.elapsed();
        assert!(took >= PERIOD, "interrupted early, after {took:?}");
        assert!(took < PERIOD + SLACK, "interrupted late, after {took:?}");
    }

    #[tokio::test]
    async fn each_wait_gets_a_whole_period_of_its_own() {
        // Twelve waits of a third of a period: four periods on one timer. If a
        // wait inherited the deadline of the one before it, the fourth would
        // be cut off.
        let mut deadline = Deadline::new(PERIOD);

        for round in 0..12 {
            let outcome = bounded(deadline.run(tokio::time::sleep(PERIOD / 3))).await;
            assert_eq!(outcome, Ok(()), "wait {round} was interrupted");
        }
    }

    #[tokio::test]
    async fn a_wait_begun_just_before_the_timer_fires_still_gets_its_period() {
        let mut deadline = Deadline::new(PERIOD);

        // Parks at 0 and arms the timer for one period.
        assert_eq!(
            deadline.run(tokio::time::sleep(PERIOD * 4 / 5)).await,
            Ok(())
        );

        // Parks at 0.8 and runs to 1.4: the timer fires in the middle of it,
        // for a wait that is over, and has to move rather than interrupt.
        assert_eq!(
            deadline.run(tokio::time::sleep(PERIOD * 3 / 5)).await,
            Ok(())
        );

        // And the moved timer is still a deadline: a wait parked now is
        // interrupted one period later, neither sooner nor never.
        let started = StdInstant::now();
        let outcome = bounded(deadline.run(std::future::pending::<()>())).await;

        assert_eq!(outcome, Err(Interrupted::TimedOut));
        let took = started.elapsed();
        assert!(took >= PERIOD, "interrupted early, after {took:?}");
        assert!(took < PERIOD + SLACK, "interrupted late, after {took:?}");
    }

    #[tokio::test]
    async fn a_deadline_follows_its_owner_to_another_task() {
        // A pooled client is driven by whichever task borrowed it. The timer
        // still holds the previous task's waker, and must take the new one.
        let mut deadline = Deadline::new(PERIOD);

        let mut deadline = tokio::spawn(async move {
            assert_eq!(deadline.run(tokio::time::sleep(PERIOD / 4)).await, Ok(()));
            deadline
        })
        .await
        .unwrap();

        let started = StdInstant::now();
        let outcome = bounded(tokio::spawn(async move {
            deadline.run(std::future::pending::<()>()).await
        }))
        .await
        .unwrap();

        assert_eq!(outcome, Err(Interrupted::TimedOut));
        let took = started.elapsed();
        assert!(took >= PERIOD, "interrupted early, after {took:?}");
        assert!(took < PERIOD + SLACK, "interrupted late, after {took:?}");
    }

    #[tokio::test]
    async fn a_wait_with_no_budget_left_is_still_interrupted() {
        // Spend the task's cooperative budget first: a `Sleep` polled without
        // any returns pending and registers nothing, and a deadline that took
        // that for a registration would never fire.
        let mut deadline = Deadline::new(PERIOD);

        while tokio::task::coop::has_budget_remaining() {
            tokio::task::coop::consume_budget().await;
        }

        let outcome = bounded(deadline.run(std::future::pending::<()>())).await;

        assert_eq!(outcome, Err(Interrupted::TimedOut));
    }

    #[tokio::test]
    async fn a_stop_ends_a_stoppable_wait_and_not_the_others() {
        let signal = StopSignal::new();
        let mut guard = Guard::new(signal.listen(), Duration::from_secs(30));

        let stopper = signal.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            stopper.stop();
        });

        // In flight: runs to its end although the stop lands halfway through.
        let outcome = guard
            .run(tokio::time::sleep(Duration::from_millis(150)), false)
            .await;
        assert_eq!(outcome, Ok(()));
        assert!(signal.is_stopping());

        // Between requests: ends at once, even though the future is ready.
        let outcome = guard.run(std::future::ready(()), true).await;
        assert_eq!(outcome, Err(Interrupted::Stopping));
    }

    #[tokio::test]
    async fn a_stop_wakes_a_wait_that_is_already_parked() {
        let signal = StopSignal::new();
        let mut guard = Guard::new(signal.listen(), Duration::from_secs(30));

        let waiting =
            tokio::spawn(async move { guard.run(std::future::pending::<()>(), true).await });

        while signal.registered() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        signal.stop();

        let outcome = tokio::time::timeout(Duration::from_secs(5), waiting).await;
        assert_eq!(outcome.unwrap().unwrap(), Err(Interrupted::Stopping));
    }

    #[tokio::test]
    async fn a_listener_made_after_the_stop_sees_it() {
        let signal = StopSignal::new();
        signal.stop();

        let mut listener = signal.listen();
        tokio::time::timeout(Duration::from_secs(5), listener.stopped())
            .await
            .expect("a stop raised before listening must not be missed");
    }

    #[tokio::test]
    async fn a_closed_connection_gives_its_slot_back() {
        let signal = StopSignal::new();

        for _ in 0..100 {
            let mut listener = signal.listen();
            let parked = std::future::poll_fn(|cx| Poll::Ready(listener.poll_stopped(cx))).await;
            assert!(parked.is_pending());
            assert_eq!(signal.registered(), 1);
        }

        assert_eq!(signal.registered(), 0);
        assert_eq!(
            signal.slots(),
            1,
            "a hundred connections in turn need one slot, not a hundred"
        );
    }
}
