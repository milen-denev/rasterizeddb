//! Awaiting several futures at once, with no executor or crate of its own:
//! a fan-out's shards answer concurrently on the caller's task.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A boxed future a fan-out waits on.
pub(crate) type Pending<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Every future's output, in the order given.
pub(crate) async fn all<'a, T: Send + 'a>(futures: Vec<Pending<'a, T>>) -> Vec<T> {
    let mut slots: Vec<(Option<Pending<'a, T>>, Option<T>)> =
        futures.into_iter().map(|f| (Some(f), None)).collect();
    std::future::poll_fn(move |cx: &mut Context<'_>| {
        let mut pending = false;
        for (fut, out) in slots.iter_mut() {
            if let Some(f) = fut {
                match f.as_mut().poll(cx) {
                    Poll::Ready(v) => {
                        *out = Some(v);
                        *fut = None;
                    }
                    Poll::Pending => pending = true,
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(slots.iter_mut().map(|(_, o)| o.take().expect("every future answered")).collect())
        }
    })
    .await
}
