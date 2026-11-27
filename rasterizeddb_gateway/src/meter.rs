//! Per-statement shard traffic, for an [`crate::hooks::Observer`]: a session
//! that is observed runs each statement with a [`Meter`] in scope, and every
//! call to a shard ([`crate::core::Link::call`]) adds to it, the parts a
//! fan-out runs on tasks of their own included ([`carry`]).

use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;

#[derive(Default, Debug)]
pub struct Meter {
    pub calls: Cell<u32>,
    pub out_plain: Cell<u64>,
    pub out_wire: Cell<u64>,
    pub in_plain: Cell<u64>,
    pub in_wire: Cell<u64>,
}

tokio::task_local! {
    static METER: Rc<Meter>;
}

/// One shard call's bytes, added to the statement's meter if one is in scope.
pub fn record(out_plain: usize, out_wire: usize, in_plain: usize, in_wire: usize) {
    let _ = METER.try_with(|m| {
        m.calls.set(m.calls.get() + 1);
        m.out_plain.set(m.out_plain.get() + out_plain as u64);
        m.out_wire.set(m.out_wire.get() + out_wire as u64);
        m.in_plain.set(m.in_plain.get() + in_plain as u64);
        m.in_wire.set(m.in_wire.get() + in_wire as u64);
    });
}

/// Runs `f` with `meter` in scope.
pub async fn scoped<F: Future>(meter: Rc<Meter>, f: F) -> F::Output {
    METER.scope(meter, f).await
}

/// `f` with the current meter (if any) carried into it: for a future handed
/// to `spawn_local`, which starts without the spawner's task-locals.
pub fn carry<F: Future>(f: F) -> impl Future<Output = F::Output> {
    let meter = METER.try_with(|m| m.clone()).ok();
    async move {
        match meter {
            Some(m) => METER.scope(m, f).await,
            None => f.await,
        }
    }
}
