//! The Redis protocol listener.
//!
//! One tokio task per connection, on the server's own runtime, so the
//! connections spread across every worker thread rather than one event loop.
//! A connection reads whatever the socket has, parses every complete command
//! in it, runs each straight into one output buffer, and writes that buffer
//! once. A pipelined batch of a hundred `GET`s is one read and one write.
//! Arguments are slices of the read buffer, never copied, and a reply is
//! encoded directly from the slot while the shard lock is held.
//!
//! # Authentication
//!
//! Against the database's own roles, the ones the PostgreSQL port
//! authenticates. `AUTH <user> <password>`, or `AUTH <password>` for the
//! configured default user, checked with the same SCRAM verifier. Until a
//! client authenticates it may send only `AUTH`, `HELLO`, `PING`, `QUIT` and
//! `RESET`, and its frames are capped (10 arguments, 16 KiB each), as Redis
//! caps unauthenticated clients. `--kv-auth trust` turns authentication off;
//! the listener then refuses to bind anything but loopback.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use log::{info, warn};
use smallvec::SmallVec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};

use super::commands::{self, BLOCKING, CONN, Cmd, NO_AUTH, NO_MULTI, SERVER, Spec, eq, parse_int};
use super::pubsub::{Message, QUEUE, Subscriber};
use super::reply::{Reply, Resp};
use super::store::{Direct, KvCache, Pinned, now_ms};
use super::{DATABASES, KvCacheHandle, REDIS_VERSION};
use crate::core::database::Database;

/// How clients prove who they are.
#[derive(Clone, Debug)]
pub enum RespAuth {
    /// Nobody is asked. Only allowed on a loopback address.
    Trust,
    /// The database's roles; `AUTH <password>` alone is checked as this user.
    Roles { default_user: String },
}

#[derive(Clone, Debug)]
pub struct RespConfig {
    pub addr: String,
    pub port: u16,
    pub auth: RespAuth,
    /// I/O threads; `None` is one per CPU this process may run on.
    pub threads: Option<usize>,
}

/// Arguments in one frame, and bytes in one argument, before `AUTH`.
const UNAUTH_ARGS: usize = 10;
const UNAUTH_BULK: usize = 16 * 1024;
/// Redis's `proto-max-bulk-len`.
const MAX_BULK: usize = 512 * 1024 * 1024;
/// Arguments in one frame after `AUTH`.
const MAX_ARGS: usize = 1024 * 1024;
/// A command that has not completed within this many buffered bytes is a
/// protocol error (Redis's `client-query-buffer-limit`).
const MAX_QUERY: usize = 1024 * 1024 * 1024;
/// Output past this is written out mid-batch rather than buffered further.
const FLUSH_AT: usize = 256 * 1024;
/// Commands parsed ahead of the one running, so their keys' memory can be
/// asked for together (see [`Conn::prefetch`]). The CPU keeps about a dozen
/// line requests in flight; sixteen commands keep that full without the first
/// lines going cold again before their command runs.
const WINDOW: usize = 16;
/// Arguments held for one window before it is cut short.
const WINDOW_ARGS: usize = 96;
/// Keys one window asks for: a long `MGET` is warmed up to here and no further.
const WINDOW_KEYS: usize = 32;

static NEXT_CLIENT: AtomicU64 = AtomicU64::new(1);

/// Binds the listener, refusing an unauthenticated one on a public address.
pub async fn bind(config: &RespConfig) -> io::Result<TcpListener> {
    if matches!(config.auth, RespAuth::Trust) {
        let loopback = config
            .addr
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(config.addr == "localhost");
        if !loopback {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "--kv-auth trust on {} would let anyone on the network read and write the store; \
                     bind 127.0.0.1 or authenticate with database roles",
                    config.addr
                ),
            ));
        }
    }
    TcpListener::bind((config.addr.as_str(), config.port)).await
}

/// Accepts connections until the listener fails.
pub async fn serve(
    listener: TcpListener,
    database: rclite::Arc<Database>,
    handle: KvCacheHandle,
    config: RespConfig,
) -> io::Result<()> {
    let port = listener.local_addr()?.port();
    handle.inner().port.store(port as u64, Ordering::Relaxed);
    info!(
        "kv cache: Redis protocol on {}:{} ({})",
        config.addr,
        port,
        match &config.auth {
            RespAuth::Trust => "no authentication".to_string(),
            RespAuth::Roles { default_user } => format!("database roles, default user {default_user}"),
        }
    );
    // Build database 0 now, so the sweeper is running before the first key.
    handle.db(0);
    let config = Arc::new(config);
    let threads = config
        .threads
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, |n| n.get()))
        .max(1);
    // Thread-per-core. Each I/O thread runs a single-threaded runtime with its
    // own epoll and owns the connections it is given outright. A readable
    // socket is noticed, read, answered and written by one thread, and no
    // futex hand-off between scheduler workers or cross-core wake-up stands
    // between a request and its reply. On a shared work-stealing runtime one
    // worker owns epoll and passes readiness to the others, which costs more
    // per unpipelined request than the command does. The accept loop deals
    // connections out round-robin. Work that must run on the main runtime
    // (`AUTH`'s role lookup, which runs SQL through the engine) is sent back
    // to it through `main`.
    let main = tokio::runtime::Handle::current();
    let mut workers = Vec::with_capacity(threads);
    for i in 0..threads {
        let (tx, mut rx) = mpsc::unbounded_channel::<(std::net::TcpStream, std::net::SocketAddr)>();
        let (database, handle, config, main) = (database.clone(), handle.clone(), config.clone(), main.clone());
        std::thread::Builder::new()
            .name(format!("kv-io-{i}"))
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    Ok(rt) => rt,
                    Err(e) => {
                        log::error!("kv cache: I/O thread {i} could not start: {e}");
                        return;
                    }
                };
                runtime.block_on(async move {
                    while let Some((socket, peer)) = rx.recv().await {
                        let Ok(socket) = TcpStream::from_std(socket) else {
                            continue;
                        };
                        let (database, handle, config, main) =
                            (database.clone(), handle.clone(), config.clone(), main.clone());
                        tokio::spawn(connection(socket, peer, database, handle, config, main));
                    }
                });
            })?;
        workers.push(tx);
    }
    let mut next = 0usize;
    loop {
        let (socket, peer) = listener.accept().await?;
        let _ = socket.set_nodelay(true);
        // Leaves this runtime's epoll; the worker registers it with its own.
        let socket = socket.into_std()?;
        if workers[next % threads].send((socket, peer)).is_err() {
            log::error!("kv cache: I/O thread {} is gone; refusing a connection from {peer}", next % threads);
        }
        next = next.wrapping_add(1);
    }
}

async fn connection(
    socket: TcpStream,
    peer: std::net::SocketAddr,
    database: rclite::Arc<Database>,
    handle: KvCacheHandle,
    config: Arc<RespConfig>,
    main: tokio::runtime::Handle,
) {
    let stats = &handle.inner().stats;
    stats.connected.fetch_add(1, Ordering::Relaxed);
    stats.connections_total.fetch_add(1, Ordering::Relaxed);
    let mut conn = Conn::new(handle.clone(), database, &config, peer.to_string(), main);
    if let Err(e) = conn.run(socket).await {
        if e.kind() != io::ErrorKind::ConnectionReset && e.kind() != io::ErrorKind::BrokenPipe {
            warn!("kv cache: connection from {peer} ended: {e}");
        }
    }
    conn.cleanup();
    handle.inner().stats.connected.fetch_sub(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub enum Frame {
    /// Not a whole command yet; `need` is a lower bound on the bytes the
    /// buffer must hold, so the reader can reserve for a large value at once.
    Incomplete { need: usize },
    /// A command whose arguments are now in `args`, spanning `consumed` bytes.
    Command { consumed: usize },
    /// Nothing but blank lines: skip them.
    Empty { consumed: usize },
    Error(String),
}

#[inline]
fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    memchr::memchr(b'\r', &buf[from..])
        .map(|i| from + i)
        .filter(|&i| buf.get(i + 1) == Some(&b'\n'))
        .or_else(|| {
            // A lone `\r` not followed by `\n` yet: keep looking past it.
            let mut at = from;
            while let Some(i) = memchr::memchr(b'\r', &buf[at..]) {
                let i = at + i;
                match buf.get(i + 1) {
                    Some(b'\n') => return Some(i),
                    Some(_) => at = i + 1,
                    None => return None,
                }
            }
            None
        })
}

fn parse_len(digits: &[u8]) -> Option<i64> {
    if digits.is_empty() || digits.len() > 19 {
        return None;
    }
    let (neg, d) = if digits[0] == b'-' { (true, &digits[1..]) } else { (false, digits) };
    if d.is_empty() {
        return None;
    }
    let mut v: i64 = 0;
    for &c in d {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as i64;
    }
    Some(if neg { -v } else { v })
}

/// Parses one command from the front of `buf` into `args`.
pub fn parse<'a>(buf: &'a [u8], args: &mut SmallVec<[&'a [u8]; 8]>, authed: bool) -> Frame {
    args.clear();
    parse_onto(buf, args, authed)
}

/// Parses one command from the front of `buf`, appending its arguments to
/// `args`. Anything but a whole command leaves `args` as it was.
pub fn parse_onto<'a, A>(buf: &'a [u8], args: &mut SmallVec<A>, authed: bool) -> Frame
where
    A: smallvec::Array<Item = &'a [u8]>,
{
    let start = args.len();
    let frame = parse_frame(buf, args, authed);
    if !matches!(frame, Frame::Command { .. }) {
        args.truncate(start);
    }
    frame
}

fn parse_frame<'a, A>(buf: &'a [u8], args: &mut SmallVec<A>, authed: bool) -> Frame
where
    A: smallvec::Array<Item = &'a [u8]>,
{
    if buf.is_empty() {
        return Frame::Incomplete { need: 1 };
    }
    if buf[0] != b'*' {
        return parse_inline(buf, args, authed);
    }
    let Some(eol) = find_crlf(buf, 1) else {
        return if buf.len() > 32 {
            Frame::Error("Protocol error: invalid multibulk length".into())
        } else {
            Frame::Incomplete { need: buf.len() + 1 }
        };
    };
    let Some(count) = parse_len(&buf[1..eol]) else {
        return Frame::Error("Protocol error: invalid multibulk length".into());
    };
    if count <= 0 {
        return Frame::Empty { consumed: eol + 2 };
    }
    let count = count as usize;
    if !authed && count > UNAUTH_ARGS {
        return Frame::Error("Protocol error: unauthenticated multibulk length".into());
    }
    if count > MAX_ARGS {
        return Frame::Error("Protocol error: invalid multibulk length".into());
    }
    let mut at = eol + 2;
    for _ in 0..count {
        if at >= buf.len() {
            return Frame::Incomplete { need: at + 1 };
        }
        if buf[at] != b'$' {
            return Frame::Error(format!(
                "Protocol error: expected '$', got '{}'",
                buf[at] as char
            ));
        }
        let Some(eol) = find_crlf(buf, at + 1) else {
            return if buf.len() - at > 32 {
                Frame::Error("Protocol error: invalid bulk length".into())
            } else {
                Frame::Incomplete { need: buf.len() + 1 }
            };
        };
        let len = match parse_len(&buf[at + 1..eol]) {
            Some(n) if n >= 0 => n as usize,
            _ => return Frame::Error("Protocol error: invalid bulk length".into()),
        };
        if !authed && len > UNAUTH_BULK {
            return Frame::Error("Protocol error: unauthenticated bulk length".into());
        }
        if len > MAX_BULK {
            return Frame::Error("Protocol error: invalid bulk length".into());
        }
        let start = eol + 2;
        let end = start + len;
        if end + 2 > buf.len() {
            return Frame::Incomplete { need: end + 2 };
        }
        if &buf[end..end + 2] != b"\r\n" {
            return Frame::Error("Protocol error: bulk string is not terminated by CRLF".into());
        }
        args.push(&buf[start..end]);
        at = end + 2;
    }
    Frame::Command { consumed: at }
}

/// The telnet form: one line of space-separated words, with `"…"` and `'…'`
/// quoting as `redis-cli` and Redis's `sdssplitargs` accept.
fn parse_inline<'a, A>(buf: &'a [u8], args: &mut SmallVec<A>, authed: bool) -> Frame
where
    A: smallvec::Array<Item = &'a [u8]>,
{
    let start = args.len();
    let Some(nl) = memchr::memchr(b'\n', buf) else {
        return if buf.len() > 64 * 1024 {
            Frame::Error("Protocol error: too big inline request".into())
        } else {
            Frame::Incomplete { need: buf.len() + 1 }
        };
    };
    let mut line = &buf[..nl];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    let mut i = 0;
    while i < line.len() {
        while i < line.len() && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= line.len() {
            break;
        }
        let quote = line[i];
        if quote == b'"' || quote == b'\'' {
            // Quoted: taken as-is up to the closing quote (escapes inside
            // quotes are not interpreted; a client wanting binary uses RESP).
            let start = i + 1;
            let Some(close) = line[start..].iter().position(|&c| c == quote) else {
                return Frame::Error("Protocol error: unbalanced quotes in request".into());
            };
            args.push(&line[start..start + close]);
            i = start + close + 1;
        } else {
            let start = i;
            while i < line.len() && !line[i].is_ascii_whitespace() {
                i += 1;
            }
            args.push(&line[start..i]);
        }
        if !authed && args.len() - start > UNAUTH_ARGS {
            return Frame::Error("Protocol error: unauthenticated multibulk length".into());
        }
    }
    if args.len() == start {
        Frame::Empty { consumed: nl + 1 }
    } else {
        Frame::Command { consumed: nl + 1 }
    }
}

// ---------------------------------------------------------------------------
// A connection
// ---------------------------------------------------------------------------

#[derive(PartialEq, Eq)]
enum Flow {
    Continue,
    Close,
}

/// `CLIENT REPLY`. After each command the connection applies the mode: `Off`
/// drops the reply; `SkipThis` drops it and turns replies back on;
/// `SkipNext` (set by `CLIENT REPLY SKIP`) drops this reply and the next.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplyMode {
    On,
    Off,
    SkipThis,
    SkipNext,
}

struct Watch {
    db: usize,
    key: Vec<u8>,
    version: Option<u64>,
    epoch: u64,
}

struct Conn {
    /// The server's main runtime, for work that runs SQL through the engine.
    main: tokio::runtime::Handle,
    handle: KvCacheHandle,
    database: rclite::Arc<Database>,
    auth: RespAuth,
    authed: bool,
    /// Whether the authenticated role is a superuser: the server-level
    /// commands (`CONFIG SET`, `SAVE`, `FLUSHALL`) need one, as their SQL
    /// counterparts do. Everyone is one under `trust`.
    superuser: bool,
    user: String,
    id: u64,
    peer: String,
    name: Option<Vec<u8>>,
    v3: bool,
    db: usize,
    store: Arc<KvCache>,
    reply_mode: ReplyMode,
    multi: Option<Vec<Vec<Vec<u8>>>>,
    multi_failed: bool,
    watches: Vec<Watch>,
    sub: Option<Subscriber>,
    rx: Option<mpsc::Receiver<Message>>,
    channels: Vec<Vec<u8>>,
    patterns: Vec<Vec<u8>>,
    /// The time the current batch runs at.
    now: u64,
}

impl Conn {
    fn new(
        handle: KvCacheHandle,
        database: rclite::Arc<Database>,
        config: &RespConfig,
        peer: String,
        main: tokio::runtime::Handle,
    ) -> Conn {
        let store = handle.db(0).clone();
        Conn {
            main,
            authed: matches!(config.auth, RespAuth::Trust),
            superuser: matches!(config.auth, RespAuth::Trust),
            auth: config.auth.clone(),
            user: match &config.auth {
                RespAuth::Trust => "default".into(),
                RespAuth::Roles { default_user } => default_user.clone(),
            },
            handle,
            database,
            id: NEXT_CLIENT.fetch_add(1, Ordering::Relaxed),
            peer,
            name: None,
            v3: false,
            db: 0,
            store,
            reply_mode: ReplyMode::On,
            multi: None,
            multi_failed: false,
            watches: Vec::new(),
            sub: None,
            rx: None,
            channels: Vec::new(),
            patterns: Vec::new(),
            now: now_ms(),
        }
    }

    fn subscribed(&self) -> bool {
        !self.channels.is_empty() || !self.patterns.is_empty()
    }

    fn cleanup(&mut self) {
        if let Some(sub) = &self.sub {
            self.handle
                .inner()
                .pubsub
                .forget(sub.id, &self.channels, &self.patterns);
        }
    }

    async fn run(&mut self, mut sock: TcpStream) -> io::Result<()> {
        let mut inbuf: Vec<u8> = Vec::with_capacity(16 * 1024);
        let mut out: Vec<u8> = Vec::with_capacity(16 * 1024);
        let mut need = 0usize;
        loop {
            // Room for what the parser said the next command needs, and
            // never less than a healthy read, so a large value arrives in
            // few reads and a small one costs no reallocation.
            let want = need.saturating_sub(inbuf.len()).max(16 * 1024);
            if inbuf.capacity() - inbuf.len() < want {
                inbuf.reserve(want);
            }
            let n = if self.subscribed() {
                let rx = self.rx.as_mut().expect("subscribed connections have a queue");
                tokio::select! {
                    biased;
                    r = sock.read_buf(&mut inbuf) => r?,
                    msg = rx.recv() => {
                        if let Some(msg) = msg {
                            encode_message(self.v3, &msg, &mut out);
                            // Drain what else is waiting into the same write.
                            while out.len() < FLUSH_AT {
                                match rx.try_recv() {
                                    Ok(m) => encode_message(self.v3, &m, &mut out),
                                    Err(_) => break,
                                }
                            }
                            sock.write_all(&out).await?;
                            out.clear();
                        }
                        if self.sub.as_ref().is_some_and(|s| s.overflowed.load(Ordering::Acquire)) {
                            return Ok(());
                        }
                        continue;
                    }
                }
            } else {
                sock.read_buf(&mut inbuf).await?
            };
            if n == 0 {
                return Ok(());
            }
            self.now = now_ms();
            let mut pos = 0usize;
            let mut commands_run = 0u64;
            let mut flow = Flow::Continue;
            {
                let mut held: SmallVec<[&[u8]; WINDOW_ARGS]> = SmallVec::new();
                let mut window: SmallVec<[Queued; WINDOW]> = SmallVec::new();
                'batch: loop {
                    // Parse a window of commands, then run it.
                    let ended =
                        fill_window(&inbuf, &mut pos, self.reads_ahead(), self.authed, &mut held, &mut window);
                    if window.len() > 1 {
                        self.prefetch(&window, &held);
                    }
                    for queued in &window {
                        need = 0;
                        commands_run += 1;
                        let mark = out.len();
                        flow = self
                            .execute(queued.spec, &held[queued.start..queued.end], &mut out, &mut sock)
                            .await?;
                        match self.reply_mode {
                            ReplyMode::On => {}
                            ReplyMode::Off => out.truncate(mark),
                            ReplyMode::SkipThis => {
                                out.truncate(mark);
                                self.reply_mode = ReplyMode::On;
                            }
                            ReplyMode::SkipNext => {
                                out.truncate(mark);
                                self.reply_mode = ReplyMode::SkipThis;
                            }
                        }
                        if flow == Flow::Close {
                            break 'batch;
                        }
                        if out.len() >= FLUSH_AT {
                            sock.write_all(&out).await?;
                            out.clear();
                        }
                    }
                    match ended {
                        // The window filled; there may be more to parse.
                        None => {}
                        Some(Frame::Incomplete { need: more }) => {
                            need = more;
                            break;
                        }
                        Some(Frame::Error(message)) => {
                            out.push(b'-');
                            out.extend_from_slice(b"ERR ");
                            out.extend_from_slice(message.as_bytes());
                            out.extend_from_slice(b"\r\n");
                            flow = Flow::Close;
                            break;
                        }
                        Some(Frame::Empty { .. } | Frame::Command { .. }) => {
                            unreachable!("the parse loop consumes these")
                        }
                    }
                }
            }
            if commands_run > 0 {
                self.handle.inner().stats.count_commands(commands_run);
            }
            if !out.is_empty() {
                sock.write_all(&out).await?;
                out.clear();
            }
            if flow == Flow::Close {
                let _ = sock.shutdown().await;
                return Ok(());
            }
            inbuf.drain(..pos);
            if inbuf.len() > MAX_QUERY {
                let _ = sock
                    .write_all(b"-ERR Protocol error: query buffer limit exceeded\r\n")
                    .await;
                return Ok(());
            }
            // Give back a buffer a large value grew, once it is idle.
            if inbuf.is_empty() && inbuf.capacity() > 1024 * 1024 {
                inbuf = Vec::with_capacity(16 * 1024);
            }
            if out.capacity() > 1024 * 1024 {
                out = Vec::with_capacity(16 * 1024);
            }
        }
    }

}

fn encode_message(v3: bool, msg: &Message, out: &mut Vec<u8>) {
    {
        let mut r = Resp { out, v3 };
        match msg {
            Message::Message { channel, data } => {
                push_header(&mut r, 3);
                r.bulk(b"message");
                r.bulk(channel);
                r.bulk(data);
            }
            Message::PMessage { pattern, channel, data } => {
                push_header(&mut r, 4);
                r.bulk(b"pmessage");
                r.bulk(pattern);
                r.bulk(channel);
                r.bulk(data);
            }
        }
    }
}

/// A parsed command waiting in the window: its spec, if the name is a
/// command, and where its arguments are in the window's argument list.
struct Queued {
    spec: Option<&'static Spec>,
    start: usize,
    end: usize,
}

/// Parses commands from `buf[*pos..]` into an emptied window, advancing
/// `pos` past each. Returns what the parser said when it was the parser that
/// stopped the window (the input ran out, or was malformed), and `None` when
/// the window closed on a command.
///
/// Reading ahead must not change what any command sees, and the one thing a
/// command can change about the parsing of the next is whether the client is
/// authenticated (the limits in [`parse_onto`]). So with `ahead` off the
/// window is a single command, which is the loop this replaced; and with it
/// on, the window closes on the first command that is not a plain data
/// command, since every command that can change the connection's state is a
/// connection command. That command runs last in its window, and whatever
/// follows it is parsed only after it has run.
fn fill_window<'a>(
    buf: &'a [u8],
    pos: &mut usize,
    ahead: bool,
    authed: bool,
    held: &mut SmallVec<[&'a [u8]; WINDOW_ARGS]>,
    window: &mut SmallVec<[Queued; WINDOW]>,
) -> Option<Frame> {
    held.clear();
    window.clear();
    loop {
        let start = held.len();
        match parse_onto(&buf[*pos..], held, authed) {
            Frame::Empty { consumed } => *pos += consumed,
            Frame::Command { consumed } => {
                *pos += consumed;
                let spec = commands::lookup(held[start]);
                window.push(Queued { spec, start, end: held.len() });
                let plain = spec.is_some_and(|s| !s.is(CONN | SERVER | BLOCKING));
                if !ahead || !plain || window.len() == WINDOW || held.len() >= WINDOW_ARGS {
                    return None;
                }
            }
            other => return Some(other),
        }
    }
}

impl Conn {
    /// Whether commands may be parsed ahead of the one running (see
    /// [`fill_window`]): only when authenticated, outside `MULTI` and not
    /// subscribed. Before authentication the next frame's limits depend on
    /// this one's outcome; in the other two states commands are queued or
    /// refused rather than run, and there is nothing to warm.
    fn reads_ahead(&self) -> bool {
        self.authed && self.multi.is_none() && !self.subscribed()
    }

    /// Asks for the memory the window's commands are about to read.
    ///
    /// A lookup in a keyspace larger than the cache is three or four memory
    /// waits in a row, and they were most of what a pipelined command cost.
    /// Run one command at a time, each waits alone. Here every key's table
    /// lines are asked for first, then every key's value, and only then do
    /// the commands run, so the waits of sixteen commands overlap.
    ///
    /// Advisory throughout: nothing learned here is passed to the commands,
    /// which hash and look up their keys again as if this had not run. A key
    /// position the spec gets wrong (the `numkeys` commands) warms a line
    /// nobody reads.
    fn prefetch(&self, window: &[Queued], held: &[&[u8]]) {
        let store = &*self.store;
        let mut keys: SmallVec<[(u64, usize); WINDOW_KEYS]> = SmallVec::new();
        'window: for queued in window {
            let Some(spec) = queued.spec else { continue };
            if spec.first <= 0 || spec.is(CONN | SERVER) {
                continue;
            }
            let n = (queued.end - queued.start) as i32;
            let last = if spec.last < 0 { n + spec.last } else { spec.last.min(n - 1) };
            let mut i = spec.first;
            while i <= last && i < n {
                if keys.len() == WINDOW_KEYS {
                    break 'window;
                }
                let at = queued.start + i as usize;
                let h = store.hash(held[at]);
                store.prefetch_table(h);
                keys.push((h, at));
                i += spec.step.max(1);
            }
        }
        for &(h, at) in &keys {
            store.prefetch_value(h, held[at]);
        }
    }

    async fn execute(
        &mut self,
        spec: Option<&'static Spec>,
        args: &[&[u8]],
        out: &mut Vec<u8>,
        sock: &mut TcpStream,
    ) -> io::Result<Flow> {
        let v3 = self.v3;
        let mut r = Resp { out, v3 };
        let Some(spec) = spec else {
            if self.multi.is_some() {
                self.multi_failed = true;
            }
            let mut msg = format!(
                "ERR unknown command '{}', with args beginning with: ",
                String::from_utf8_lossy(args[0])
            );
            for a in args.iter().skip(1).take(8) {
                msg.push_str(&format!("'{}' ", String::from_utf8_lossy(a)));
            }
            r.error(&msg);
            return Ok(Flow::Continue);
        };
        if !spec.arity_ok(args.len()) {
            if self.multi.is_some() {
                self.multi_failed = true;
            }
            r.error(&format!("ERR wrong number of arguments for '{}' command", spec.name));
            return Ok(Flow::Continue);
        }
        if !self.authed && !spec.is(NO_AUTH) {
            r.error("NOAUTH Authentication required.");
            return Ok(Flow::Continue);
        }
        if self.subscribed()
            && !self.v3
            && !matches!(
                spec.cmd,
                Cmd::Subscribe | Cmd::Unsubscribe | Cmd::PSubscribe | Cmd::PUnsubscribe | Cmd::Ping | Cmd::Quit | Cmd::Reset
            )
        {
            r.error(&format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                spec.name
            ));
            return Ok(Flow::Continue);
        }
        if !self.superuser && administers(spec, args) {
            r.error(&format!(
                "NOPERM this user has no permissions to run the '{}' command; it needs a superuser role",
                spec.name
            ));
            if self.multi.is_some() {
                self.multi_failed = true;
            }
            return Ok(Flow::Continue);
        }
        if self.multi.is_some() && !matches!(spec.cmd, Cmd::Exec | Cmd::Discard | Cmd::Multi | Cmd::Watch | Cmd::Quit | Cmd::Reset) {
            if spec.is(NO_MULTI) {
                self.multi_failed = true;
                r.error(&format!("ERR Command not allowed inside a transaction: '{}'", spec.name));
                return Ok(Flow::Continue);
            }
            self.multi
                .as_mut()
                .expect("in multi")
                .push(args.iter().map(|a| a.to_vec()).collect());
            r.simple("QUEUED");
            return Ok(Flow::Continue);
        }
        if spec.is(CONN) {
            return self.connection_command(spec, args, &mut r).await;
        }
        if spec.is(SERVER) {
            match spec.cmd {
                Cmd::Save => {
                    let handle = self.handle.clone();
                    match tokio::task::spawn_blocking(move || handle.save()).await {
                        Ok(Ok(_)) => r.ok(),
                        Ok(Err(e)) => r.err(&e),
                        Err(e) => r.error(&format!("ERR save panicked: {e}")),
                    }
                }
                Cmd::BgSave => {
                    let handle = self.handle.clone();
                    if handle.inner().saving.swap(true, Ordering::AcqRel) {
                        r.error("ERR Background save already in progress");
                    } else {
                        tokio::task::spawn_blocking(move || {
                            if let Err(e) = handle.save() {
                                log::error!("kv cache: BGSAVE failed: {e}");
                            }
                            handle.inner().saving.store(false, Ordering::Release);
                        });
                        r.simple("Background saving started");
                    }
                }
                _ => super::server::run(spec, &self.handle, self.db, args, &mut r),
            }
            return Ok(Flow::Continue);
        }
        if spec.is(BLOCKING) {
            return self.blocking(spec, args, out, sock).await;
        }
        let mut access = Direct { store: &self.store, now: self.now };
        commands::run(spec, &mut access, args, &mut r);
        Ok(Flow::Continue)
    }

    async fn connection_command(&mut self, spec: &Spec, args: &[&[u8]], r: &mut Resp<'_>) -> io::Result<Flow> {
        match spec.cmd {
            Cmd::Quit => {
                r.ok();
                return Ok(Flow::Close);
            }
            Cmd::Auth => {
                let (user, password) = match args.len() {
                    2 => (self.user.clone(), args[1]),
                    3 => (String::from_utf8_lossy(args[1]).to_string(), args[2]),
                    _ => {
                        r.error(commands::SYNTAX);
                        return Ok(Flow::Continue);
                    }
                };
                match self.authenticate(&user, password).await {
                    Ok(()) => r.ok(),
                    Err(e) => r.error(&e),
                }
            }
            Cmd::Hello => self.hello(args, r).await,
            Cmd::Select => match parse_int(args[1]) {
                Some(n) if (0..DATABASES as i64).contains(&n) => {
                    self.db = n as usize;
                    self.store = self.handle.db(self.db).clone();
                    r.ok();
                }
                _ => r.error("ERR DB index is out of range"),
            },
            Cmd::Reset => {
                self.multi = None;
                self.multi_failed = false;
                self.watches.clear();
                self.unsubscribe_all();
                self.db = 0;
                self.store = self.handle.db(0).clone();
                self.v3 = false;
                self.reply_mode = ReplyMode::On;
                self.name = None;
                if !matches!(self.auth, RespAuth::Trust) {
                    self.authed = false;
                    self.superuser = false;
                }
                r.simple("RESET");
            }
            Cmd::Client => self.client(args, r),
            Cmd::Multi => {
                if self.multi.is_some() {
                    r.error("ERR MULTI calls can not be nested");
                } else {
                    self.multi = Some(Vec::new());
                    self.multi_failed = false;
                    r.ok();
                }
            }
            Cmd::Discard => {
                if self.multi.take().is_some() {
                    self.watches.clear();
                    self.multi_failed = false;
                    r.ok();
                } else {
                    r.error("ERR DISCARD without MULTI");
                }
            }
            Cmd::Watch => {
                if self.multi.is_some() {
                    r.error("ERR WATCH inside MULTI is not allowed");
                } else {
                    for key in &args[1..] {
                        let h = self.store.hash(key);
                        let version = self.store.cell(h).lock.read().find(h, key).map(|s| s.version);
                        self.watches.push(Watch {
                            db: self.db,
                            key: key.to_vec(),
                            version,
                            epoch: self.store.flush_epoch.load(Ordering::Relaxed),
                        });
                    }
                    r.ok();
                }
            }
            Cmd::Unwatch => {
                self.watches.clear();
                r.ok();
            }
            Cmd::Exec => self.exec(r),
            Cmd::Publish => {
                let n = self.handle.inner().pubsub.publish(args[1], args[2]);
                r.int(n as i64);
            }
            Cmd::Subscribe | Cmd::PSubscribe => {
                let pattern = spec.cmd == Cmd::PSubscribe;
                let sub = self.subscriber();
                for ch in &args[1..] {
                    let pubsub = &self.handle.inner().pubsub;
                    let added = if pattern {
                        pubsub.psubscribe(ch, &sub)
                    } else {
                        pubsub.subscribe(ch, &sub)
                    };
                    if added {
                        if pattern {
                            self.patterns.push(ch.to_vec());
                        } else {
                            self.channels.push(ch.to_vec());
                        }
                    }
                    push_header(r, 3);
                    r.bulk(if pattern { b"psubscribe" } else { b"subscribe" });
                    r.bulk(ch);
                    r.int((self.channels.len() + self.patterns.len()) as i64);
                }
            }
            Cmd::Unsubscribe | Cmd::PUnsubscribe => {
                let pattern = spec.cmd == Cmd::PUnsubscribe;
                let kind: &[u8] = if pattern { b"punsubscribe" } else { b"unsubscribe" };
                let targets: Vec<Vec<u8>> = if args.len() > 1 {
                    args[1..].iter().map(|a| a.to_vec()).collect()
                } else if pattern {
                    self.patterns.clone()
                } else {
                    self.channels.clone()
                };
                if targets.is_empty() {
                    push_header(r, 3);
                    r.bulk(kind);
                    r.null();
                    r.int((self.channels.len() + self.patterns.len()) as i64);
                }
                for t in targets {
                    if let Some(sub) = &self.sub {
                        let pubsub = &self.handle.inner().pubsub;
                        if pattern {
                            pubsub.punsubscribe(&t, sub.id);
                            self.patterns.retain(|p| *p != t);
                        } else {
                            pubsub.unsubscribe(&t, sub.id);
                            self.channels.retain(|c| *c != t);
                        }
                    }
                    push_header(r, 3);
                    r.bulk(kind);
                    r.bulk(&t);
                    r.int((self.channels.len() + self.patterns.len()) as i64);
                }
            }
            Cmd::PubSubCmd => {
                let pubsub = &self.handle.inner().pubsub;
                let sub = args[1];
                if eq(sub, b"CHANNELS") {
                    let list = pubsub.channels(args.get(2).copied());
                    r.array(list.len());
                    for c in list {
                        r.bulk(&c);
                    }
                } else if eq(sub, b"NUMSUB") {
                    r.map(args.len() - 2);
                    for c in &args[2..] {
                        r.bulk(c);
                        r.int(pubsub.numsub(c) as i64);
                    }
                } else if eq(sub, b"NUMPAT") {
                    r.int(pubsub.numpat() as i64);
                } else {
                    r.error("ERR unknown subcommand for 'pubsub'");
                }
            }
            _ => r.error(&format!("ERR '{}' cannot run here", spec.name)),
        }
        Ok(Flow::Continue)
    }

    fn subscriber(&mut self) -> Subscriber {
        if self.sub.is_none() {
            let (tx, rx) = mpsc::channel(QUEUE);
            self.sub = Some(Subscriber {
                id: self.id,
                tx,
                overflowed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            });
            self.rx = Some(rx);
        }
        self.sub.clone().expect("just made")
    }

    fn unsubscribe_all(&mut self) {
        if let Some(sub) = &self.sub {
            self.handle
                .inner()
                .pubsub
                .forget(sub.id, &self.channels, &self.patterns);
        }
        self.channels.clear();
        self.patterns.clear();
    }

    async fn authenticate(&mut self, user: &str, password: &[u8]) -> Result<(), String> {
        match &self.auth {
            RespAuth::Trust => Err(
                "ERR AUTH <password> called without any password configured for the default user. \
                 Are you sure your configuration is correct?"
                    .into(),
            ),
            RespAuth::Roles { .. } => {
                let password = String::from_utf8_lossy(password).to_string();
                // The lookup runs SQL through the engine, which belongs on
                // the main runtime rather than this connection's I/O thread.
                let (database, name) = (self.database.clone(), user.to_string());
                let role = self
                    .main
                    .spawn(async move { crate::auth::lookup_role(&database, &name).await })
                    .await
                    .ok()
                    .flatten();
                let (ok, superuser) = match role {
                    Some(role) if role.can_login => (
                        role.verifier
                            .as_ref()
                            .is_some_and(|v| crate::auth::verify_password(v, user, &password)),
                        role.is_superuser,
                    ),
                    _ => (false, false),
                };
                if ok {
                    self.authed = true;
                    self.superuser = superuser;
                    self.user = user.to_string();
                    Ok(())
                } else {
                    Err("WRONGPASS invalid username-password pair or user is disabled.".into())
                }
            }
        }
    }

    async fn hello(&mut self, args: &[&[u8]], r: &mut Resp<'_>) {
        let mut i = 1;
        let mut proto = if self.v3 { 3 } else { 2 };
        if let Some(v) = args.get(1) {
            match parse_int(v) {
                Some(2) => proto = 2,
                Some(3) => proto = 3,
                _ => return r.error("NOPROTO unsupported protocol version"),
            }
            i = 2;
        }
        while i < args.len() {
            if eq(args[i], b"AUTH") && i + 2 < args.len() {
                let user = String::from_utf8_lossy(args[i + 1]).to_string();
                if let Err(e) = self.authenticate(&user, args[i + 2]).await {
                    return r.error(&e);
                }
                i += 3;
            } else if eq(args[i], b"SETNAME") && i + 1 < args.len() {
                self.name = Some(args[i + 1].to_vec());
                i += 2;
            } else {
                return r.error(commands::SYNTAX);
            }
        }
        if !self.authed {
            return r.error(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise the \
                 HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and \
                 select the RESP protocol version at the same time",
            );
        }
        self.v3 = proto == 3;
        r.v3 = self.v3;
        r.map(7);
        r.bulk(b"server");
        r.bulk(b"rasterizeddb");
        r.bulk(b"version");
        r.bulk(REDIS_VERSION.as_bytes());
        r.bulk(b"proto");
        r.int(proto);
        r.bulk(b"id");
        r.int(self.id as i64);
        r.bulk(b"mode");
        r.bulk(b"standalone");
        r.bulk(b"role");
        r.bulk(b"master");
        r.bulk(b"modules");
        r.array(0);
    }

    fn client_line(&self) -> String {
        format!(
            "id={} addr={} name={} db={} user={} resp={} multi={} sub={} psub={}",
            self.id,
            self.peer,
            self.name
                .as_ref()
                .map(|n| String::from_utf8_lossy(n).to_string())
                .unwrap_or_default(),
            self.db,
            self.user,
            if self.v3 { 3 } else { 2 },
            self.multi.as_ref().map_or(-1, |q| q.len() as i64),
            self.channels.len(),
            self.patterns.len(),
        )
    }

    fn client(&mut self, args: &[&[u8]], r: &mut Resp<'_>) {
        let sub = args[1];
        if eq(sub, b"ID") {
            r.int(self.id as i64)
        } else if eq(sub, b"SETNAME") && args.len() == 3 {
            if args[2].iter().any(|c| *c <= b' ' || *c > b'~') {
                return r.error("ERR Client names cannot contain spaces, newlines or special characters.");
            }
            self.name = (!args[2].is_empty()).then(|| args[2].to_vec());
            r.ok()
        } else if eq(sub, b"GETNAME") {
            match &self.name {
                Some(n) => r.bulk(n),
                None => r.null(),
            }
        } else if eq(sub, b"SETINFO") || eq(sub, b"NO-EVICT") || eq(sub, b"NO-TOUCH") {
            r.ok()
        } else if eq(sub, b"INFO") || eq(sub, b"LIST") {
            let line = self.client_line() + "\n";
            r.verbatim(&line)
        } else if eq(sub, b"REPLY") && args.len() == 3 {
            if eq(args[2], b"ON") {
                self.reply_mode = ReplyMode::On;
                r.ok()
            } else if eq(args[2], b"OFF") {
                self.reply_mode = ReplyMode::Off;
            } else if eq(args[2], b"SKIP") {
                // This command sends nothing, and neither does the next.
                self.reply_mode = ReplyMode::SkipNext;
            } else {
                r.error(commands::SYNTAX)
            }
        } else if eq(sub, b"GETREDIR") {
            r.int(-1)
        } else {
            r.error(&format!(
                "ERR CLIENT {} is not supported by this server",
                String::from_utf8_lossy(sub)
            ))
        }
    }

    fn exec(&mut self, r: &mut Resp<'_>) {
        let Some(queue) = self.multi.take() else {
            return r.error("ERR EXEC without MULTI");
        };
        let watches = std::mem::take(&mut self.watches);
        if self.multi_failed {
            self.multi_failed = false;
            return r.error("EXECABORT Transaction discarded because of previous errors.");
        }
        // Watches on other databases are checked first, without this
        // transaction's locks; the current database's under them.
        for w in watches.iter().filter(|w| w.db != self.db) {
            let store = self.handle.db(w.db);
            let h = store.hash(&w.key);
            let now = store.cell(h).lock.read().find(h, &w.key).map(|s| s.version);
            if now != w.version || store.flush_epoch.load(Ordering::Relaxed) != w.epoch {
                return r.null_array();
            }
        }
        let specs: Vec<&'static Spec> = queue
            .iter()
            .map(|args| commands::lookup(&args[0]).expect("queued commands were looked up"))
            .collect();
        let all = specs.iter().any(|s| s.is(commands::ALL));
        let store = self.store.clone();
        let mut hashes: Vec<u64> = Vec::new();
        if !all {
            for (spec, args) in specs.iter().zip(&queue) {
                let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
                for k in spec.keys(&refs) {
                    hashes.push(store.hash(k));
                }
            }
            for w in watches.iter().filter(|w| w.db == self.db) {
                hashes.push(store.hash(&w.key));
            }
        }
        let mut pinned = if all {
            Pinned::lock_all(&store)
        } else {
            Pinned::lock(&store, &hashes)
        };
        for w in watches.iter().filter(|w| w.db == self.db) {
            if pinned.version_of(&w.key) != w.version || store.flush_epoch.load(Ordering::Relaxed) != w.epoch {
                return r.null_array();
            }
        }
        r.array(queue.len());
        for (spec, args) in specs.iter().zip(&queue) {
            let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
            if spec.cmd == Cmd::FlushAll {
                // This database through the held locks; the others directly.
                for d in (0..DATABASES).filter(|&d| d != self.db) {
                    if let Some(other) = self.handle.existing(d) {
                        other.flush();
                    }
                }
                commands::run(
                    commands::lookup(b"flushdb").expect("flushdb"),
                    &mut pinned,
                    &[&b"flushdb"[..]],
                    r,
                );
            } else if spec.cmd == Cmd::Publish {
                let n = self.handle.inner().pubsub.publish(refs[1], refs[2]);
                r.int(n as i64);
            } else if spec.is(SERVER) {
                super::server::run(spec, &self.handle, self.db, &refs, r);
            } else if spec.is(CONN) {
                r.error(&format!("ERR '{}' cannot run inside a transaction", spec.name));
            } else {
                commands::run(spec, &mut pinned, &refs, r);
            }
        }
    }

    async fn blocking(&mut self, spec: &Spec, args: &[&[u8]], out: &mut Vec<u8>, sock: &mut TcpStream) -> io::Result<Flow> {
        let timeout_arg = args[args.len() - 1];
        let timeout = match commands::parse_float(timeout_arg) {
            Some(t) if t < 0.0 => {
                Resp { out, v3: self.v3 }.error("ERR timeout is negative");
                return Ok(Flow::Continue);
            }
            Some(t) if t.is_finite() => t,
            _ => {
                Resp { out, v3: self.v3 }.error("ERR timeout is not a float or out of range");
                return Ok(Flow::Continue);
            }
        };
        let keys: Vec<Vec<u8>> = match spec.cmd {
            Cmd::BLPop | Cmd::BRPop => args[1..args.len() - 1].iter().map(|k| k.to_vec()).collect(),
            _ => vec![args[1].to_vec()],
        };
        let deadline = (timeout > 0.0).then(|| tokio::time::Instant::now() + Duration::from_secs_f64(timeout));
        let notify = Arc::new(Notify::new());
        let waiters = &self.store.waiters;
        let mut registered = false;
        let mut peer_has_data = false;
        let stats = &self.handle.inner().stats;
        loop {
            {
                let mut access = Direct { store: &self.store, now: now_ms() };
                let mut r = Resp { out, v3: self.v3 };
                if commands::bpop_attempt(spec, &mut access, args, &mut r) {
                    break;
                }
            }
            if !registered {
                waiters.register(&keys, &notify);
                registered = true;
                stats.blocked.fetch_add(1, Ordering::Relaxed);
                // A push between the attempt and the registration would have
                // found nobody to wake: try once more now that we are listed.
                continue;
            }
            // Earlier replies of a pipeline go out before this one waits.
            if !out.is_empty() {
                sock.write_all(out).await?;
                out.clear();
            }
            let woke = tokio::select! {
                _ = notify.notified() => true,
                _ = async {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending::<()>().await,
                    }
                } => false,
                r = sock.readable(), if !peer_has_data => {
                    r?;
                    // Data or EOF. Data is the client's next command, which
                    // waits its turn; EOF means nobody is left to answer.
                    let mut peek = [0u8; 1];
                    match sock.peek(&mut peek).await {
                        Ok(0) => {
                            waiters.unregister(&keys, &notify);
                            stats.blocked.fetch_sub(1, Ordering::Relaxed);
                            return Ok(Flow::Close);
                        }
                        _ => peer_has_data = true,
                    }
                    true
                }
            };
            if !woke {
                waiters.unregister(&keys, &notify);
                stats.blocked.fetch_sub(1, Ordering::Relaxed);
                // A wake-up this waiter received but never used goes to the
                // next one.
                for k in &keys {
                    waiters.wake(k, 1);
                }
                let mut r = Resp { out, v3: self.v3 };
                match spec.cmd {
                    Cmd::BLPop | Cmd::BRPop => r.null_array(),
                    _ => r.null(),
                }
                return Ok(Flow::Continue);
            }
        }
        if registered {
            waiters.unregister(&keys, &notify);
            stats.blocked.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(Flow::Continue)
    }
}

/// Whether a command administers the server rather than the data: changes
/// its settings, writes files, or empties every database.
fn administers(spec: &Spec, args: &[&[u8]]) -> bool {
    match spec.cmd {
        Cmd::Save | Cmd::BgSave | Cmd::FlushAll => true,
        Cmd::Config => !args.get(1).is_some_and(|sub| eq(sub, b"GET")),
        _ => false,
    }
}

/// A pub/sub frame: a push in RESP3, an array in RESP2.
fn push_header(r: &mut Resp<'_>, n: usize) {
    if r.v3 {
        r.out.push(b'>');
        super::reply::write_i64(r.out, n as i64);
        r.out.extend_from_slice(b"\r\n");
    } else {
        r.array(n);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(buf: &[u8]) -> (Frame, Vec<Vec<u8>>) {
        let mut args = SmallVec::new();
        let f = parse(buf, &mut args, true);
        (f, args.iter().map(|a| a.to_vec()).collect())
    }

    #[test]
    fn multibulk_and_inline() {
        let (f, a) = parsed(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
        assert_eq!(f, Frame::Command { consumed: 20 });
        assert_eq!(a, vec![b"GET".to_vec(), b"k".to_vec()]);
        let (f, a) = parsed(b"SET k \"two words\"\r\n");
        assert!(matches!(f, Frame::Command { .. }));
        assert_eq!(a, vec![b"SET".to_vec(), b"k".to_vec(), b"two words".to_vec()]);
        // Binary-safe: a CR LF inside a bulk is data.
        let (f, a) = parsed(b"*1\r\n$4\r\na\r\nb\r\n");
        assert!(matches!(f, Frame::Command { .. }));
        assert_eq!(a, vec![b"a\r\nb".to_vec()]);
    }

    #[test]
    fn partial_frames_ask_for_more() {
        let full = b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\n";
        for cut in 1..full.len() {
            let (f, _) = parsed(&full[..cut]);
            assert!(matches!(f, Frame::Incomplete { .. }), "cut at {cut}: {f:?}");
        }
        let (f, _) = parsed(b"*1\r\n$100000\r\nab");
        assert_eq!(f, Frame::Incomplete { need: 4 + 9 + 100_000 + 2 });
    }

    /// One window, as (command names, what the parser ended it with).
    fn window_at(buf: &[u8], pos: &mut usize, ahead: bool, authed: bool) -> (Vec<String>, Option<Frame>) {
        let mut held = SmallVec::new();
        let mut window = SmallVec::new();
        let ended = fill_window(buf, pos, ahead, authed, &mut held, &mut window);
        let names = window
            .iter()
            .map(|q| {
                assert!(q.start < q.end && q.end <= held.len());
                String::from_utf8_lossy(held[q.start]).into_owned()
            })
            .collect();
        (names, ended)
    }

    fn frame(args: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n{a}\r\n", a.len()).as_bytes());
        }
        out
    }

    #[test]
    fn a_window_closes_on_the_command_that_changes_the_connection() {
        let mut buf = Vec::new();
        for args in [&["SET", "a", "1"][..], &["GET", "a"], &["SELECT", "1"], &["GET", "a"], &["NOPE"], &["GET", "a"]] {
            buf.extend(frame(args));
        }
        buf.extend_from_slice(b"*0\r\n\r\n");
        buf.extend(frame(&["BLPOP", "q", "0"]));
        buf.extend(frame(&["GET", "a"]));
        buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$5\r\nhal");
        let mut pos = 0;
        // SELECT changes the store the next command runs against: it runs
        // last in its window, and the GET after it is parsed afterwards.
        let (names, ended) = window_at(&buf, &mut pos, true, true);
        assert_eq!(names, ["SET", "GET", "SELECT"]);
        assert_eq!(ended, None);
        // An unknown name and a blocking command close a window too.
        assert_eq!(window_at(&buf, &mut pos, true, true).0, ["GET", "NOPE"]);
        assert_eq!(window_at(&buf, &mut pos, true, true).0, ["GET", "BLPOP"]);
        // The input running out mid-command ends the window with what was
        // whole, and leaves the position at the partial command.
        let before_partial = buf.len() - b"*2\r\n$3\r\nGET\r\n$5\r\nhal".len();
        let (names, ended) = window_at(&buf, &mut pos, true, true);
        assert_eq!(names, ["GET"]);
        assert!(matches!(ended, Some(Frame::Incomplete { .. })), "{ended:?}");
        assert_eq!(pos, before_partial);

        // Without read-ahead (before AUTH, inside MULTI, subscribed) every
        // window is one command, whatever follows it.
        let mut pos = 0;
        for expected in ["SET", "GET", "SELECT", "GET", "NOPE", "GET", "BLPOP", "GET"] {
            let (names, ended) = window_at(&buf, &mut pos, false, true);
            assert_eq!(names, [expected]);
            assert_eq!(ended, None);
        }
    }

    #[test]
    fn a_window_is_bounded_and_keeps_what_came_before_an_error() {
        let mut buf = Vec::new();
        for i in 0..WINDOW + 3 {
            buf.extend(frame(&["GET", &format!("k{i}")]));
        }
        buf.extend_from_slice(b"*1\r\n$-7\r\n");
        let mut pos = 0;
        let (names, ended) = window_at(&buf, &mut pos, true, true);
        assert_eq!((names.len(), ended), (WINDOW, None));
        // The malformed frame ends the next window, after the three commands
        // in front of it: they still run, and are answered, before the error.
        let (names, ended) = window_at(&buf, &mut pos, true, true);
        assert_eq!(names.len(), 3);
        assert!(matches!(ended, Some(Frame::Error(_))), "{ended:?}");

        // One command with more arguments than a window holds is a window.
        let many: Vec<String> = (0..WINDOW_ARGS * 2).map(|i| format!("k{i}")).collect();
        let mut args = vec!["MGET"];
        args.extend(many.iter().map(String::as_str));
        let mut buf = frame(&args);
        buf.extend(frame(&["GET", "a"]));
        let mut pos = 0;
        assert_eq!(window_at(&buf, &mut pos, true, true).0, ["MGET"]);
        assert_eq!(window_at(&buf, &mut pos, true, true).0, ["GET"]);

        // Before authentication the limits apply to each frame as it is
        // parsed, and a refused frame leaves no arguments behind.
        let mut buf = frame(&["PING"]);
        buf.extend(frame(&["AUTH", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]));
        let mut pos = 0;
        assert_eq!(window_at(&buf, &mut pos, false, false).0, ["PING"]);
        let (names, ended) = window_at(&buf, &mut pos, false, false);
        assert!(names.is_empty());
        assert!(matches!(ended, Some(Frame::Error(_))), "{ended:?}");
    }

    #[test]
    fn limits_before_auth() {
        let big = format!("*1\r\n${}\r\n", UNAUTH_BULK + 1);
        let frames: [(&[u8], bool); 4] = [
            (b"*11\r\n", false),
            (big.as_bytes(), false),
            (b"*1\r\n$-5\r\n", true),
            (b"*1\r\nX3\r\n", true),
        ];
        for (frame, authed) in frames {
            let mut args = SmallVec::new();
            assert!(matches!(parse(frame, &mut args, authed), Frame::Error(_)), "{frame:?}");
        }
    }
}
