//! One connection, many callers.
//!
//! [`crate::client::TcpClient`] carries one request at a time: whoever holds
//! it sends, waits and reads, and anyone else needs a connection of their own.
//! A hundred concurrent callers are then a hundred TLS sessions, and every
//! request is a system call in each direction on each side.
//!
//! [`MuxClient`] is a handle any number of tasks can clone and use at once.
//! It speaks `rastcp/2`, in which every frame carries an id, so all of their
//! requests share one connection and the server answers each when it is done,
//! in whatever order that is. One task owns the socket. It writes everything
//! callers have queued since it last looked in a single write, and hands each
//! response to the caller whose id it carries — so the busier the client, the
//! more requests each system call moves.
//!
//! What a caller can rely on:
//!
//! - A response is the response to its own request, or an error. Ids are not
//!   reused while the server might still answer the request they named.
//! - A request that times out fails alone. The connection is given up only
//!   when the server sent nothing at all for that long.
//! - A lost connection fails every request in flight on it, and the next
//!   request opens a new one. Nothing is sent twice: a request that failed may
//!   or may not have been carried out, and sending it again is the caller's
//!   decision.
//! - Requests from one task are not ordered relative to each other unless that
//!   task waits for each response before sending the next.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use log::{debug, warn};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_rustls::client::TlsStream;

use crate::client::Dialer;
use crate::error::RastcpError;
use crate::frame::{FrameReader, FrameWriter, KIND_DATA, KIND_FAILED, KIND_TIMED_OUT, Tag};
use crate::interrupt::Deadline;

/// How many responses the connection's task takes off the wire before it
/// stops to hand them out. Bounds what it holds, not what a connection can
/// carry.
const DELIVER_AT: usize = 256;

/// A client many tasks can use at once over one connection.
///
/// Cheap to clone; every clone is the same client. Built with
/// [`crate::client::TcpClientBuilder::build_multiplexed`]. The connection is
/// opened by the first request and reopened by the first request after it is
/// lost.
pub struct MuxClient {
    shared: Arc<Shared>,
}

struct Shared {
    dialer: Dialer,
    /// One permit per request in flight, so this side never asks the server to
    /// handle more at once than it will, and never queues without bound when
    /// the server falls behind.
    permits: Semaphore,
    /// Held while a connection is being opened, so callers that find it down
    /// together open one between them.
    opening: tokio::sync::Mutex<()>,
    state: Mutex<State>,
    /// Live [`MuxClient`] handles. The connection closes with the last.
    handles: AtomicUsize,
}

struct State {
    /// Counts connections opened and closed. The task that owns a connection
    /// carries the number it was started with; once the two differ the
    /// connection is no longer the client's, and the task closes it and
    /// leaves the state alone.
    epoch: u64,
    up: bool,
    /// Requests framed and waiting for the connection's task to write them.
    outbound: FrameWriter,
    /// The connection's task, when it had nothing to write.
    io_waker: Option<Waker>,
    slots: Vec<Slot>,
    free: Vec<u32>,
    /// Requests in flight in the order they were sent, which with one timeout
    /// is also the order in which they give up.
    deadlines: VecDeque<Expiry>,
    /// Frames received on this connection.
    received: u64,
}

/// One request id and what is known about the request that holds it.
struct Slot {
    /// Counts the requests that have held this id, so a record of an earlier
    /// one cannot be mistaken for the current one.
    turn: u32,
    caller: Caller,
    /// The server was sent this id and has not answered it. Until it does, or
    /// the connection is gone, the id cannot name another request: the answer
    /// would be taken for that one's.
    owed: bool,
}

enum Caller {
    /// The id is not in use.
    Free,
    /// The caller is waiting, and this wakes it.
    Waiting(Option<Waker>),
    /// The outcome is here for the caller to collect.
    Done(Result<Vec<u8>, RastcpError>),
    /// The caller has its outcome or stopped waiting for one.
    Gone,
}

/// When a request in flight gives up.
#[derive(Clone, Copy)]
struct Expiry {
    expires: Instant,
    id: u32,
    turn: u32,
    /// How many frames had been received when it was sent. Unchanged by the
    /// time it expires means the server has been silent for the whole timeout.
    received: u64,
}

/// An error to hand to each of several callers. The original cannot be shared
/// out — an I/O error is not `Clone` — so each gets one that reads the same.
fn replicate(err: &RastcpError) -> RastcpError {
    match err {
        RastcpError::Io(io) => RastcpError::Io(std::io::Error::new(io.kind(), io.to_string())),
        RastcpError::Tls(tls) => RastcpError::Tls(tls.clone()),
        RastcpError::ConnectionClosed => RastcpError::ConnectionClosed,
        RastcpError::ConnectionTimeout => RastcpError::ConnectionTimeout,
        RastcpError::MessageTooLarge { len, max } => RastcpError::MessageTooLarge {
            len: *len,
            max: *max,
        },
        RastcpError::ProtocolError(message) => RastcpError::ProtocolError(message.clone()),
        RastcpError::HandlerTimeout => RastcpError::HandlerTimeout,
        other => RastcpError::ProtocolError(other.to_string()),
    }
}

impl State {
    /// Gives `id` back if nothing can refer to it any more.
    fn release(&mut self, id: u32) {
        let slot = &mut self.slots[id as usize];

        if matches!(slot.caller, Caller::Gone) && !slot.owed {
            slot.caller = Caller::Free;
            self.free.push(id);
        }
    }

    /// Takes an id for a request and frames it for sending.
    fn enqueue(
        &mut self,
        request: &[u8],
        timeout: Option<std::time::Duration>,
    ) -> Result<u32, RastcpError> {
        let id = match self.free.pop() {
            Some(id) => id,
            None => {
                let id = u32::try_from(self.slots.len()).map_err(|_| {
                    RastcpError::ProtocolError("too many requests in flight".to_string())
                })?;
                self.slots.push(Slot {
                    turn: 0,
                    caller: Caller::Free,
                    owed: false,
                });
                id
            }
        };

        if let Err(err) = self.outbound.queue_tagged_copy(Tag::data(id), request) {
            self.free.push(id);
            return Err(err);
        }

        let received = self.received;
        let slot = &mut self.slots[id as usize];
        slot.turn = slot.turn.wrapping_add(1);
        slot.caller = Caller::Waiting(None);
        slot.owed = true;
        let turn = slot.turn;

        if let Some(timeout) = timeout {
            self.deadlines.push_back(Expiry {
                expires: Instant::now() + timeout,
                id,
                turn,
                received,
            });
        }

        Ok(id)
    }

    /// Records the server's answer to `tag.id`. An error means the server is
    /// not following the protocol, and the connection cannot be trusted.
    fn deliver(
        &mut self,
        tag: Tag,
        body: Vec<u8>,
        wakers: &mut Vec<Waker>,
    ) -> Result<(), RastcpError> {
        let owed = self
            .slots
            .get(tag.id as usize)
            .is_some_and(|slot| slot.owed);

        if !owed {
            return Err(RastcpError::ProtocolError(format!(
                "the server answered request {}, which is not in flight",
                tag.id
            )));
        }

        self.received += 1;

        let outcome = match tag.kind {
            KIND_DATA => Ok(body),
            KIND_TIMED_OUT => Err(RastcpError::HandlerTimeout),
            KIND_FAILED => Err(RastcpError::ProtocolError(format!(
                "the server could not handle the request: {}",
                String::from_utf8_lossy(&body)
            ))),
            kind => Err(RastcpError::ProtocolError(format!(
                "the server sent a frame of unknown kind {kind}"
            ))),
        };

        let slot = &mut self.slots[tag.id as usize];
        slot.owed = false;

        if let Caller::Waiting(waker) = &mut slot.caller {
            wakers.extend(waker.take());
            slot.caller = Caller::Done(outcome);
        }

        // Otherwise the request timed out here before the answer came, or its
        // caller stopped waiting. Either way the answer only frees the id.
        self.release(tag.id);

        Ok(())
    }

    /// The earliest request still waiting to give up, dropping records of
    /// those that no longer are.
    fn next_expiry(&mut self) -> Option<Expiry> {
        while let Some(expiry) = self.deadlines.front().copied() {
            let slot = &self.slots[expiry.id as usize];

            if slot.turn == expiry.turn && matches!(slot.caller, Caller::Waiting(_)) {
                return Some(expiry);
            }

            self.deadlines.pop_front();
        }

        None
    }

    /// Fails the request `expiry` names with a timeout, if it is still
    /// waiting. Its id stays taken until the server answers or the connection
    /// goes.
    fn expire(&mut self, expiry: Expiry, wakers: &mut Vec<Waker>) {
        let slot = &mut self.slots[expiry.id as usize];

        if slot.turn != expiry.turn {
            return;
        }

        if let Caller::Waiting(waker) = &mut slot.caller {
            wakers.extend(waker.take());
            slot.caller = Caller::Done(Err(RastcpError::ConnectionTimeout));
        }
    }

    /// The connection is gone: nothing sent on it will be answered.
    fn lose(&mut self, reason: &RastcpError, wakers: &mut Vec<Waker>) {
        self.up = false;
        self.io_waker = None;
        self.outbound.clear();
        self.deadlines.clear();
        self.received = 0;

        for id in 0..self.slots.len() {
            let slot = &mut self.slots[id];

            if !slot.owed {
                continue;
            }
            slot.owed = false;

            if let Caller::Waiting(waker) = &mut slot.caller {
                wakers.extend(waker.take());
                slot.caller = Caller::Done(Err(replicate(reason)));
            }

            self.release(id as u32);
        }
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // Every update under this lock is finished before anything that could
        // panic runs, so a poisoned lock still guards consistent state.
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Makes sure a connection is up, opening one if there is none.
    async fn open(self: &Arc<Self>) -> Result<(), RastcpError> {
        let _opening = self.opening.lock().await;

        if self.lock().up {
            return Ok(());
        }

        let stream = self.dialer.open().await?;

        let epoch = {
            let mut state = self.lock();
            state.epoch += 1;
            state.up = true;
            state.received = 0;
            state.epoch
        };

        tokio::spawn(drive(self.clone(), stream, epoch));

        Ok(())
    }

    /// Gives the connection up: its requests fail now, and its task is left
    /// to notice that the connection is no longer current and close it.
    fn close(&self) {
        let mut wakers = Vec::new();
        {
            let mut state = self.lock();
            if !state.up {
                return;
            }
            wakers.extend(state.io_waker.take());
            state.epoch += 1;
            state.lose(&RastcpError::ConnectionClosed, &mut wakers);
        }

        for waker in wakers {
            waker.wake();
        }
    }
}

/// The task that owns a connection: writes what callers queue, reads what the
/// server answers, and times out what it does not.
async fn drive(shared: Arc<Shared>, mut stream: TlsStream<TcpStream>, epoch: u64) {
    let mut link = Link {
        epoch,
        reader: FrameReader::tagged(),
        out: Vec::new(),
        written: 0,
        unflushed: false,
        inbox: Vec::new(),
        wakers: Vec::new(),
        deadline: shared.dialer.timeout.map(Deadline::new),
    };

    let end = std::future::poll_fn(|cx| link.poll(&shared, &mut stream, cx)).await;

    let reason = match end {
        Ok(()) => {
            debug!("Closing a multiplexed connection that was given up");
            // An orderly end: say so, but do not let a peer that is not
            // listening hold the task.
            let closing = std::time::Duration::from_secs(5);
            let _ = tokio::time::timeout(closing, stream.shutdown()).await;
            RastcpError::ConnectionClosed
        }
        Err(err) => {
            match err {
                RastcpError::ConnectionClosed => debug!("The multiplexed connection was closed"),
                ref err => warn!("The multiplexed connection was lost: {err}"),
            }
            err
        }
    };

    let mut wakers = Vec::new();
    {
        let mut state = shared.lock();
        // A newer connection may already be up; its state is not this
        // task's to clear.
        if state.epoch == epoch {
            state.lose(&reason, &mut wakers);
        }
    }
    for waker in wakers {
        waker.wake();
    }
}

/// What the connection's task keeps between polls.
struct Link {
    /// The connection this task owns; see [`State::epoch`].
    epoch: u64,
    reader: FrameReader,
    /// Requests taken from the shared queue, being written.
    out: Vec<u8>,
    written: usize,
    unflushed: bool,
    /// Responses read and not yet handed out, so one lock hands out many.
    inbox: Vec<(Tag, Vec<u8>)>,
    wakers: Vec<Waker>,
    deadline: Option<Deadline>,
}

impl Link {
    /// Moves the connection along until neither direction can go further.
    /// Ready when it is over: `Ok` if it was asked to close.
    fn poll(
        &mut self,
        shared: &Shared,
        stream: &mut TlsStream<TcpStream>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), RastcpError>> {
        loop {
            let mut progressed = false;

            // Out: whatever callers have queued since the last look.
            if self.written == self.out.len() {
                let mut state = shared.lock();

                if state.epoch != self.epoch {
                    return Poll::Ready(Ok(()));
                }

                if state.outbound.is_empty() {
                    // Registered under the lock a caller queues under, so a
                    // request cannot slip in unseen between the two.
                    if !state
                        .io_waker
                        .as_ref()
                        .is_some_and(|waker| waker.will_wake(cx.waker()))
                    {
                        state.io_waker = Some(cx.waker().clone());
                    }
                } else {
                    state.outbound.swap(&mut self.out);
                    self.written = 0;
                }
            }

            while self.written < self.out.len() {
                match Pin::new(&mut *stream).poll_write(cx, &self.out[self.written..]) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Err(std::io::Error::from(
                            std::io::ErrorKind::WriteZero,
                        )
                        .into()));
                    }
                    Poll::Ready(Ok(n)) => {
                        self.written += n;
                        self.unflushed = true;
                        progressed = true;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err.into())),
                    Poll::Pending => break,
                }
            }

            if self.written == self.out.len() && self.unflushed {
                match Pin::new(&mut *stream).poll_flush(cx) {
                    Poll::Ready(Ok(())) => self.unflushed = false,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err.into())),
                    Poll::Pending => {}
                }
            }

            // In: every response that has arrived.
            let mut broken = None;
            while self.inbox.len() < DELIVER_AT {
                match self
                    .reader
                    .poll_tagged(stream, cx, shared.dialer.max_message_size)
                {
                    Poll::Ready(Ok(frame)) => self.inbox.push(frame),
                    Poll::Ready(Err(err)) => {
                        broken = Some(err);
                        break;
                    }
                    Poll::Pending => break,
                }
            }

            // Responses read before the connection broke are still answers.
            let mut next = None;
            {
                let mut state = shared.lock();

                // Given up while this task was reading: what it read answers
                // requests that have already been failed.
                if state.epoch != self.epoch {
                    return Poll::Ready(Ok(()));
                }

                for (tag, body) in self.inbox.drain(..) {
                    progressed = true;
                    if let Err(err) = state.deliver(tag, body, &mut self.wakers) {
                        broken.get_or_insert(err);
                        break;
                    }
                }

                if broken.is_none() && self.deadline.is_some() {
                    next = state.next_expiry();
                }
            }
            self.inbox.clear();

            // Woken outside the lock: each of them wants it at once.
            for waker in self.wakers.drain(..) {
                waker.wake();
            }

            if let Some(err) = broken {
                return Poll::Ready(Err(err));
            }

            // Time: the request that has waited longest.
            if let (Some(expiry), Some(deadline)) = (next, self.deadline.as_mut()) {
                if deadline.poll_expired(cx, expiry.expires).is_ready() {
                    let mut state = shared.lock();

                    if state.epoch != self.epoch {
                        return Poll::Ready(Ok(()));
                    }

                    // Not one frame since it was sent, for a whole timeout:
                    // the server is not slow, it is gone.
                    if state.received == expiry.received {
                        return Poll::Ready(Err(RastcpError::ConnectionTimeout));
                    }

                    state.expire(expiry, &mut self.wakers);
                    drop(state);

                    for waker in self.wakers.drain(..) {
                        waker.wake();
                    }
                    progressed = true;
                }
            }

            if !progressed {
                return Poll::Pending;
            }
        }
    }
}

/// One caller's wait for its response.
struct Wait<'a> {
    shared: &'a Shared,
    id: u32,
    collected: bool,
}

impl Wait<'_> {
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<Vec<u8>, RastcpError>> {
        let mut state = self.shared.lock();
        let id = self.id as usize;

        match std::mem::replace(&mut state.slots[id].caller, Caller::Gone) {
            Caller::Done(outcome) => {
                self.collected = true;
                state.release(self.id);
                Poll::Ready(outcome)
            }
            Caller::Waiting(waker) => {
                let waker = match waker {
                    Some(waker) if waker.will_wake(cx.waker()) => waker,
                    _ => cx.waker().clone(),
                };
                state.slots[id].caller = Caller::Waiting(Some(waker));
                Poll::Pending
            }
            // Not reachable while this wait exists: only it moves the slot
            // out of the two states above. If it happens anyway the slot is
            // someone else's now, and is left exactly as it was found.
            other => {
                state.slots[id].caller = other;
                self.collected = true;
                Poll::Ready(Err(RastcpError::NotConnected))
            }
        }
    }
}

impl Drop for Wait<'_> {
    fn drop(&mut self) {
        if self.collected {
            return;
        }

        // The caller stopped waiting. The server may still answer, so the id
        // is given back only once it has, or the connection is gone.
        let mut state = self.shared.lock();
        state.slots[self.id as usize].caller = Caller::Gone;
        state.release(self.id);
    }
}

impl MuxClient {
    pub(crate) fn new(dialer: Dialer) -> Self {
        let max_in_flight = dialer.max_in_flight;

        Self {
            shared: Arc::new(Shared {
                dialer,
                permits: Semaphore::new(max_in_flight),
                opening: tokio::sync::Mutex::new(()),
                state: Mutex::new(State {
                    epoch: 0,
                    up: false,
                    outbound: FrameWriter::new(),
                    io_waker: None,
                    slots: Vec::new(),
                    free: Vec::new(),
                    deadlines: VecDeque::new(),
                    received: 0,
                }),
                handles: AtomicUsize::new(1),
            }),
        }
    }

    /// Opens the connection now instead of with the first request.
    pub async fn connect(&self) -> Result<(), RastcpError> {
        self.shared.open().await
    }

    pub fn is_connected(&self) -> bool {
        self.shared.lock().up
    }

    /// Sends a request and returns its response.
    ///
    /// Any number of these may be in flight at once, from any number of
    /// tasks; past the client's limit a caller waits its turn here. Dropping
    /// the returned future abandons the wait, not the request: it may already
    /// have reached the server.
    pub async fn send(&self, request: &[u8]) -> Result<Vec<u8>, RastcpError> {
        let _permit = self
            .shared
            .permits
            .acquire()
            .await
            .map_err(|_| RastcpError::NotConnected)?;

        let id = loop {
            {
                let mut state = self.shared.lock();

                if state.up {
                    let id = state.enqueue(request, self.shared.dialer.timeout)?;
                    let waker = state.io_waker.take();
                    drop(state);

                    if let Some(waker) = waker {
                        waker.wake();
                    }
                    break id;
                }
            }

            self.shared.open().await?;
        };

        let mut wait = Wait {
            shared: &self.shared,
            id,
            collected: false,
        };

        std::future::poll_fn(|cx| wait.poll(cx)).await
    }

    /// Closes the connection. Requests in flight fail; the next request opens
    /// a new connection.
    pub async fn close(&self) {
        self.shared.close();
    }
}

impl Clone for MuxClient {
    fn clone(&self) -> Self {
        self.shared.handles.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for MuxClient {
    fn drop(&mut self) {
        // The connection's task holds the shared state too, so it would never
        // be dropped on its own: the last handle has to tell the task to go.
        if self.shared.handles.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.close();
        }
    }
}
