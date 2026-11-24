//! Replicating the store: what a write ships, how it travels, how a peer
//! applies it.
//!
//! A write reaches the peers over the table data's own link (see
//! [`crate::replication`]): the same authentication, the same per-origin
//! sequence numbers with no gaps, the same replication log and catch-up for a
//! peer that was away. What travels is a record flagged
//! [`FLAG_KV`](crate::replication::protocol::FLAG_KV) whose statements are
//! chunks of *effects*, written in the format below.
//!
//! # Effects, not commands
//!
//! A command is shipped as what it did, by Redis's own propagation rules, so a
//! peer that applies it gets the same answer whenever it runs:
//!
//! - a relative lifetime (`EX`, `PX`, `EXPIRE`, `SETEX`, `GETEX` …) becomes
//!   the absolute one the write left on the key: `SET … PXAT`, `PEXPIREAT`;
//! - `SPOP` becomes `SREM` of the members it took, `ZPOPMIN`/`ZPOPMAX` a
//!   `ZREM`;
//! - `INCRBYFLOAT` becomes `SET` of its result, `HINCRBYFLOAT` an `HSET`;
//! - a blocking pop becomes the `LPOP`, `RPOP` or `LMOVE` that happened;
//! - `SETNX`, `MSETNX`, `HSETNX`, `RENAMENX` become the write they made;
//! - a command that wrote nothing (a failed `NX`, a `WRONGTYPE`, a `DEL` of
//!   nothing) ships nothing;
//! - `MULTI`/`EXEC` ships as one entry, applied under one set of locks;
//! - `FLUSHDB` and `FLUSHALL` ship. Expiry and eviction do not: every node
//!   expires a key at its absolute time itself, and evicts under its own
//!   memory limit.
//!
//! # Order
//!
//! A write is run under its keys' shard locks held together (as `EXEC` holds
//! them), and its effect is appended to the node's stream before they are let
//! go. Two writers of one key therefore append in the order the store applied
//! them: one stream per origin, in execution order. Many effects are numbered
//! into one record a moment later ([`Replication::kv_append`]).
//!
//! # Cost when not replicating
//!
//! None per read. A write pays one relaxed load of the replication gate, and
//! takes this path only when it is set.
//!
//! # Format
//!
//! A chunk is [`KV_FORMAT_V1`], then entries. All integers little-endian.
//!
//! ```text
//! entry   := 1 db:u8 command           -- one command
//!          | 2 db:u8 n:u32 command*n   -- one EXEC, applied atomically
//! command := argc:u32 (len:u32 bytes)*argc
//! ```

use std::sync::Arc;

use smallvec::SmallVec;

use super::commands::{self, ALL, Cmd, SERVER, Spec, WRITE, eq};
use super::reply::{Collect, Item, Reply, Resp};
use super::store::{Direct, KvCache, Pinned, now_ms};
use super::{DATABASES, KvCacheHandle};
use crate::replication::Replication;
use crate::replication::manager::KV_ENTRY_MAX;
use crate::replication::protocol::KV_FORMAT_V1;

const ENTRY_COMMAND: u8 = 1;
const ENTRY_GROUP: u8 = 2;

/// One command to ship: its name and arguments.
pub type Effect = Vec<Vec<u8>>;

// ---------------------------------------------------------------------------
// Reading back what a command answered
// ---------------------------------------------------------------------------

/// A reply that can be read back after a command wrote it: what the effect
/// of a write is derived from (did `SET … NX` set, what did `SPOP` take).
pub trait Observed: Reply {
    /// Where the next reply will start.
    fn mark(&self) -> usize;
    /// The reply written since `mark`.
    fn reply_since(&self, mark: usize) -> Option<Item>;
}

impl Observed for Resp<'_> {
    fn mark(&self) -> usize {
        self.out.len()
    }
    fn reply_since(&self, mark: usize) -> Option<Item> {
        parse_reply(self.out.get(mark..)?).map(|(item, _)| item)
    }
}

impl Observed for Collect {
    fn mark(&self) -> usize {
        self.done.len()
    }
    fn reply_since(&self, mark: usize) -> Option<Item> {
        self.done.get(mark).cloned()
    }
}

/// Parses one RESP2 or RESP3 reply, as [`Resp`] writes them, into an
/// [`Item`]; returns it and the bytes it took. `None` for an incomplete or
/// malformed one.
pub fn parse_reply(buf: &[u8]) -> Option<(Item, usize)> {
    let kind = *buf.first()?;
    let end = buf.windows(2).position(|w| w == b"\r\n")?;
    let line = &buf[1..end];
    let after = end + 2;
    let text = || std::str::from_utf8(line).ok();
    let count = || -> Option<i64> { text()?.parse().ok() };
    let items = |n: usize, mut at: usize| -> Option<(Vec<Item>, usize)> {
        let mut items = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            let (item, used) = parse_reply(buf.get(at..)?)?;
            items.push(item);
            at += used;
        }
        Some((items, at))
    };
    Some(match kind {
        b'+' => (Item::Simple(text()?.to_string()), after),
        b'-' => (Item::Error(String::from_utf8_lossy(line).into_owned()), after),
        b':' => (Item::Int(count()?), after),
        b'(' => (Item::Simple(text()?.to_string()), after),
        b'_' => (Item::Null, after),
        b'#' => (Item::Bool(line == b"t"), after),
        b',' => {
            let t = text()?;
            let f = match t {
                "inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                _ => t.parse().ok()?,
            };
            (Item::Double(f), after)
        }
        b'$' | b'=' => {
            let n = count()?;
            if n < 0 {
                return Some((Item::Null, after));
            }
            let n = n as usize;
            let body = buf.get(after..after + n)?;
            let body = if kind == b'=' && body.len() >= 4 { &body[4..] } else { body };
            (Item::Bulk(body.to_vec()), after + n + 2)
        }
        b'*' | b'~' | b'>' => {
            let n = count()?;
            if n < 0 {
                return Some((Item::NullArray, after));
            }
            let (items, at) = items(n as usize, after)?;
            (Item::Array(items), at)
        }
        b'%' => {
            let n = count()?.max(0) as usize;
            let (items, at) = items(n * 2, after)?;
            let mut pairs = Vec::with_capacity(n);
            let mut it = items.into_iter();
            while let (Some(k), Some(v)) = (it.next(), it.next()) {
                pairs.push((k, v));
            }
            (Item::Map(pairs), at)
        }
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// What a write did
// ---------------------------------------------------------------------------

fn owned(parts: &[&[u8]]) -> Effect {
    parts.iter().map(|p| p.to_vec()).collect()
}

fn named(name: &[u8], rest: &[&[u8]]) -> Effect {
    let mut effect = Vec::with_capacity(rest.len() + 1);
    effect.push(name.to_vec());
    effect.extend(rest.iter().map(|p| p.to_vec()));
    effect
}

fn decimal(n: u64) -> Vec<u8> {
    n.to_string().into_bytes()
}

/// `key` holds `value` now, with whatever lifetime the write left on it.
fn string_with_lifetime(key: &[u8], value: &[u8], ttl: &mut dyn FnMut(&[u8]) -> Option<u64>) -> Effect {
    match ttl(key) {
        // Written with a lifetime that has already run out: what is left is
        // no key.
        None => named(b"DEL", &[key]),
        Some(0) => named(b"SET", &[key, value]),
        Some(at) => named(b"SET", &[key, value, b"PXAT", &decimal(at)]),
    }
}

/// `key`'s lifetime is now whatever the write left on it.
fn lifetime_of(key: &[u8], ttl: &mut dyn FnMut(&[u8]) -> Option<u64>) -> Effect {
    match ttl(key) {
        None => named(b"DEL", &[key]),
        Some(0) => named(b"PERSIST", &[key]),
        Some(at) => named(b"PEXPIREAT", &[key, &decimal(at)]),
    }
}

/// The bulk strings in a reply that lists members: a flat list, or (RESP3,
/// with scores) one pair per member.
fn members(reply: &Item) -> Vec<Vec<u8>> {
    let Item::Array(items) = reply else {
        return match reply {
            Item::Bulk(b) => vec![b.clone()],
            _ => Vec::new(),
        };
    };
    let bulk = |item: &Item| match item {
        Item::Bulk(b) => Some(b.clone()),
        _ => None,
    };
    if matches!(items.first(), Some(Item::Array(_))) {
        items
            .iter()
            .filter_map(|pair| match pair {
                Item::Array(pair) => pair.first().and_then(bulk),
                _ => None,
            })
            .collect()
    } else {
        items.iter().filter_map(bulk).collect()
    }
}

/// A `ZPOPMIN`/`ZPOPMAX` reply's members: every other element of a flat
/// member/score list, or the first of each pair.
fn popped_members(reply: &Item) -> Vec<Vec<u8>> {
    let Item::Array(items) = reply else {
        return Vec::new();
    };
    if matches!(items.first(), Some(Item::Array(_))) {
        return members(reply);
    }
    items
        .iter()
        .step_by(2)
        .filter_map(|item| match item {
            Item::Bulk(b) => Some(b.clone()),
            _ => None,
        })
        .collect()
}

/// What a write command did, as the commands that make a peer do the same.
///
/// `reply` is what the command answered; `ttl` reads a key's lifetime as the
/// command left it, under the locks it ran under (see [`Pinned::live_expiry`]).
/// Empty when the command wrote nothing.
pub fn effects(spec: &Spec, args: &[&[u8]], reply: &Item, ttl: &mut dyn FnMut(&[u8]) -> Option<u64>) -> Vec<Effect> {
    use Cmd::*;
    if matches!(reply, Item::Error(_)) || !spec.is(WRITE) {
        return Vec::new();
    }
    let int = match reply {
        Item::Int(n) => Some(*n),
        Item::Bool(b) => Some(i64::from(*b)),
        _ => None,
    };
    let positive = int.is_some_and(|n| n > 0);
    let is_ok = matches!(reply, Item::Simple(_));
    let is_null = matches!(reply, Item::Null | Item::NullArray);
    let bulk = match reply {
        Item::Bulk(b) => Some(b.as_slice()),
        _ => None,
    };
    let verbatim = || vec![owned(args)];
    let none = Vec::new;

    match spec.cmd {
        Set => {
            let options = &args[3..];
            let has = |w: &[u8]| options.iter().any(|o| eq(o, w));
            let wrote = if has(b"GET") {
                if has(b"NX") {
                    is_null
                } else if has(b"XX") {
                    bulk.is_some()
                } else {
                    true
                }
            } else {
                is_ok
            };
            if wrote { vec![string_with_lifetime(args[1], args[2], ttl)] } else { none() }
        }
        SetNx if positive => vec![string_with_lifetime(args[1], args[2], ttl)],
        SetEx | PSetEx if is_ok => vec![string_with_lifetime(args[1], args[3], ttl)],
        GetSet => vec![string_with_lifetime(args[1], args[2], ttl)],
        GetDel if bulk.is_some() => vec![named(b"DEL", &[args[1]])],
        GetEx if args.len() > 2 && bulk.is_some() => vec![lifetime_of(args[1], ttl)],
        MSet if is_ok => verbatim(),
        MSetNx if positive => vec![named(b"MSET", &args[1..])],
        Incr | Decr | IncrBy | DecrBy | Append | SetBit if int.is_some() => verbatim(),
        SetRange if int.is_some() && !args[3].is_empty() => verbatim(),
        IncrByFloat => match bulk {
            Some(value) => vec![string_with_lifetime(args[1], value, ttl)],
            None => none(),
        },

        Del | Unlink if positive => vec![named(b"DEL", &args[1..])],
        Expire | PExpire | ExpireAt | PExpireAt if positive => vec![lifetime_of(args[1], ttl)],
        Persist if positive => vec![named(b"PERSIST", &[args[1]])],
        Rename if is_ok => vec![named(b"RENAME", &[args[1], args[2]])],
        RenameNx if positive => vec![named(b"RENAME", &[args[1], args[2]])],
        Copy if positive => vec![named(b"COPY", &[args[1], args[2], b"REPLACE"])],
        FlushDb if is_ok => vec![named(b"FLUSHDB", &[])],
        FlushAll if is_ok => vec![named(b"FLUSHALL", &[])],
        Move if positive => verbatim(),

        HSet | HMSet if !is_null => verbatim(),
        HSetNx if positive => vec![named(b"HSET", &[args[1], args[2], args[3]])],
        HDel if positive => verbatim(),
        HIncrBy if int.is_some() => verbatim(),
        HIncrByFloat => match bulk {
            Some(value) => vec![named(b"HSET", &[args[1], args[2], value])],
            None => none(),
        },

        LPush | RPush if int.is_some() => verbatim(),
        LPushX | RPushX | LRem | LInsert if positive => verbatim(),
        LSet | LTrim if is_ok => verbatim(),
        LPop | RPop => {
            let name: &[u8] = if spec.cmd == LPop { b"LPOP" } else { b"RPOP" };
            match reply {
                Item::Bulk(_) => vec![named(name, &[args[1]])],
                Item::Array(items) if !items.is_empty() => {
                    vec![named(name, &[args[1], &decimal(items.len() as u64)])]
                }
                _ => none(),
            }
        }
        LMove | BLMove if bulk.is_some() => vec![named(b"LMOVE", &[args[1], args[2], args[3], args[4]])],
        RPopLPush | BRPopLPush if bulk.is_some() => vec![named(b"LMOVE", &[args[1], args[2], b"RIGHT", b"LEFT"])],
        BLPop | BRPop => match reply {
            Item::Array(pair) => match pair.first() {
                Some(Item::Bulk(key)) => {
                    let name: &[u8] = if spec.cmd == BLPop { b"LPOP" } else { b"RPOP" };
                    vec![named(name, &[key])]
                }
                _ => none(),
            },
            _ => none(),
        },

        SAdd | SRem | SMove if positive => verbatim(),
        SPop => {
            let taken = members(reply);
            if taken.is_empty() {
                none()
            } else {
                let mut effect = vec![b"SREM".to_vec(), args[1].to_vec()];
                effect.extend(taken);
                vec![effect]
            }
        }
        SInterStore | SUnionStore | SDiffStore if int.is_some() => verbatim(),

        ZAdd if !is_null => verbatim(),
        ZIncrBy => verbatim(),
        ZRem | ZRemRangeByRank | ZRemRangeByScore | ZRemRangeByLex if positive => verbatim(),
        ZPopMin | ZPopMax => {
            let taken = popped_members(reply);
            if taken.is_empty() {
                none()
            } else {
                let mut effect = vec![b"ZREM".to_vec(), args[1].to_vec()];
                effect.extend(taken);
                vec![effect]
            }
        }
        ZUnionStore | ZInterStore | ZDiffStore if int.is_some() => verbatim(),

        // `SWAPDB` is refused by this server. Everything else above either
        // matched or wrote nothing.
        _ => none(),
    }
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

#[inline]
fn put_u32(out: &mut Vec<u8>, n: usize) {
    out.extend_from_slice(&(n as u32).to_le_bytes());
}

fn put_command<T: AsRef<[u8]>>(out: &mut Vec<u8>, args: &[T]) {
    put_u32(out, args.len());
    for arg in args {
        let arg = arg.as_ref();
        put_u32(out, arg.len());
        out.extend_from_slice(arg);
    }
}

/// Encodes `effects`, all on database `db`, as one entry: a command, or (two
/// or more, from one `EXEC`) a group applied atomically.
pub fn encode_entry(out: &mut Vec<u8>, db: usize, effects: &[Effect]) {
    match effects {
        [] => {}
        [one] => {
            out.push(ENTRY_COMMAND);
            out.push(db as u8);
            put_command(out, one);
        }
        many => {
            out.push(ENTRY_GROUP);
            out.push(db as u8);
            put_u32(out, many.len());
            for effect in many {
                put_command(out, effect);
            }
        }
    }
}

/// One decoded entry: commands on one database, and whether they are one
/// `EXEC` to be applied together.
#[derive(Debug, PartialEq, Eq)]
pub struct Entry<'a> {
    pub db: usize,
    pub atomic: bool,
    pub commands: Vec<Vec<&'a [u8]>>,
}

struct Cursor<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at.checked_add(n).filter(|&end| end <= self.buf.len());
        let end = end.ok_or_else(|| "a key/value chunk ends in the middle of an entry".to_string())?;
        let slice = &self.buf[self.at..end];
        self.at = end;
        Ok(slice)
    }
    fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<usize, String> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
    }
    fn command(&mut self) -> Result<Vec<&'a [u8]>, String> {
        let argc = self.u32()?;
        if argc == 0 {
            return Err("a key/value effect with no command name".into());
        }
        // Bounded by what is left: an argument is at least its length.
        let mut args = Vec::with_capacity(argc.min((self.buf.len() - self.at) / 4));
        for _ in 0..argc {
            let len = self.u32()?;
            args.push(self.take(len)?);
        }
        Ok(args)
    }
}

/// Decodes a chunk written by [`encode_entry`] behind [`KV_FORMAT_V1`].
pub fn decode_chunk(chunk: &[u8]) -> Result<Vec<Entry<'_>>, String> {
    let Some((&version, body)) = chunk.split_first() else {
        return Err("an empty key/value chunk".into());
    };
    if version != KV_FORMAT_V1 {
        return Err(format!(
            "key/value effects in format {version}, which this build does not know (it reads {KV_FORMAT_V1})"
        ));
    }
    let mut cursor = Cursor { buf: body, at: 0 };
    let mut entries = Vec::new();
    while cursor.at < body.len() {
        let tag = cursor.u8()?;
        let db = cursor.u8()? as usize;
        match tag {
            ENTRY_COMMAND => entries.push(Entry {
                db,
                atomic: false,
                commands: vec![cursor.command()?],
            }),
            ENTRY_GROUP => {
                let n = cursor.u32()?;
                let mut commands = Vec::with_capacity(n.min(body.len() / 8));
                for _ in 0..n {
                    commands.push(cursor.command()?);
                }
                entries.push(Entry { db, atomic: true, commands });
            }
            other => return Err(format!("unknown key/value entry kind {other}")),
        }
    }
    Ok(entries)
}

// ---------------------------------------------------------------------------
// Capture
// ---------------------------------------------------------------------------

/// The replication a write on this handle is recorded for, when it is on.
#[inline]
pub fn capturing(handle: &KvCacheHandle) -> Option<&Arc<Replication>> {
    handle.replication().filter(|r| r.is_active())
}

/// Whether a write's arguments alone are more than one record may carry.
fn too_large(args: &[&[u8]]) -> bool {
    args.iter().map(|a| a.len() + 4).sum::<usize>() + 64 > KV_ENTRY_MAX
}

const TOO_LARGE: &str = "ERR this write is larger than a replicated key/value write may be; \
                         split it, or write it on a node that is not replicating";

/// Appends `effects`, of one command or one `EXEC` on database `db`, to the
/// node's stream. The caller still holds the shards they wrote.
pub fn append(replication: &Arc<Replication>, db: usize, effects: &[Effect]) {
    if effects.is_empty() {
        return;
    }
    let size: usize = effects
        .iter()
        .map(|e| 4 + e.iter().map(|a| a.len() + 4).sum::<usize>())
        .sum::<usize>()
        + 8;
    if size > KV_ENTRY_MAX {
        replication.note_capture_failure(format!(
            "A key/value write on database {db} came to {size} bytes of effects, more than the \
             {KV_ENTRY_MAX} one replicated write may carry; it was applied here and not shipped"
        ));
        return;
    }
    let mut entry = Vec::with_capacity(size);
    encode_entry(&mut entry, db, effects);
    replication.kv_append(&entry);
}

/// Runs a data command with its keys' shards held together, and returns what
/// it did. The locks are let go when this returns, so a caller that ships the
/// effects must do so through [`run_write`], which appends them first.
fn run_locked<O: Observed>(
    store: &KvCache,
    spec: &Spec,
    args: &[&[u8]],
    out: &mut O,
    then: impl FnOnce(&[Effect]),
) {
    let hashes: SmallVec<[u64; 8]> = if spec.is(ALL) {
        SmallVec::new()
    } else {
        spec.keys(args).iter().map(|k| store.hash(k)).collect()
    };
    if !spec.is(ALL) && hashes.is_empty() {
        // A write with no keys has no shard to order against.
        let mark = out.mark();
        commands::run(spec, &mut Direct::new(store), args, out);
        if let Some(reply) = out.reply_since(mark) {
            then(&effects(spec, args, &reply, &mut |_| None));
        }
        return;
    }
    let mut pinned = if spec.is(ALL) {
        Pinned::lock_all(store)
    } else {
        Pinned::lock(store, &hashes)
    };
    let mark = out.mark();
    commands::run(spec, &mut pinned, args, out);
    if let Some(reply) = out.reply_since(mark) {
        let shipped = effects(spec, args, &reply, &mut |key| pinned.live_expiry(key));
        then(&shipped);
    }
    drop(pinned);
}

/// A data write on a replicating node: run under its keys' locks, and its
/// effect appended to the stream before they go.
// Out of line and cold: the connection loop that calls it is the hot
// path of a node that is not replicating, and must not grow by it.
#[cold]
#[inline(never)]
pub fn run_write<O: Observed>(
    replication: &Arc<Replication>,
    store: &KvCache,
    db: usize,
    spec: &Spec,
    args: &[&[u8]],
    out: &mut O,
) {
    if too_large(args) {
        return out.error(TOO_LARGE);
    }
    run_locked(store, spec, args, out, |effects| append(replication, db, effects));
}

/// One attempt of a blocking pop on a replicating node: what it took is
/// shipped as the non-blocking pop or move it was. Returns whether it
/// answered, as [`commands::bpop_attempt`] does.
// Out of line and cold: the connection loop that calls it is the hot
// path of a node that is not replicating, and must not grow by it.
#[cold]
#[inline(never)]
pub fn bpop_attempt<O: Observed>(
    replication: &Arc<Replication>,
    store: &KvCache,
    db: usize,
    spec: &Spec,
    args: &[&[u8]],
    out: &mut O,
) -> bool {
    let hashes: SmallVec<[u64; 8]> = spec.keys(args).iter().map(|k| store.hash(k)).collect();
    let mut pinned = Pinned::lock(store, &hashes);
    let mark = out.mark();
    let answered = commands::bpop_attempt(spec, &mut pinned, args, out);
    if answered {
        if let Some(reply) = out.reply_since(mark) {
            let shipped = effects(spec, args, &reply, &mut |key| pinned.live_expiry(key));
            append(replication, db, &shipped);
        }
    }
    answered
}

/// A server-level write on a replicating node: `FLUSHALL`, `MOVE`. Anything
/// else is passed to [`super::server::run`] unchanged.
// Out of line and cold: the connection loop that calls it is the hot
// path of a node that is not replicating, and must not grow by it.
#[cold]
#[inline(never)]
pub fn run_server_write<O: Observed>(
    replication: &Arc<Replication>,
    handle: &KvCacheHandle,
    db: usize,
    spec: &Spec,
    args: &[&[u8]],
    out: &mut O,
) {
    match spec.cmd {
        Cmd::FlushAll => {
            match args.get(1) {
                None => {}
                Some(m) if eq(m, b"ASYNC") || eq(m, b"SYNC") => {}
                Some(_) => return out.error(commands::SYNTAX),
            }
            // Database by database, each emptied and recorded under all of
            // its shards, so no write to it can be streamed on the wrong
            // side of its flush.
            let flushdb = commands::lookup(b"flushdb").expect("flushdb is a command");
            for d in 0..DATABASES {
                if let Some(store) = handle.existing(d) {
                    let mut pinned = Pinned::lock_all(store);
                    commands::run(flushdb, &mut pinned, &[&b"flushdb"[..]], &mut Collect::default());
                    append(replication, d, &[named(b"FLUSHDB", &[])]);
                }
            }
            out.ok()
        }
        Cmd::Move => {
            if too_large(args) {
                return out.error(TOO_LARGE);
            }
            super::server::move_key(handle, db, args, out, &mut || {
                append(replication, db, &[owned(args)]);
            })
        }
        _ => super::server::run(spec, handle, db, args, out),
    }
}

/// `EXEC`'s queue, all of it, against the arguments one record may carry.
pub fn queue_too_large(queue: &[Vec<Vec<u8>>]) -> bool {
    queue.iter().flatten().map(|a| a.len() + 4).sum::<usize>() + 64 > KV_ENTRY_MAX
}

/// The error a write too large to replicate is refused with.
pub fn too_large_error() -> &'static str {
    TOO_LARGE
}

// ---------------------------------------------------------------------------
// Apply
// ---------------------------------------------------------------------------

/// What applying one chunk came to.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ApplyReport {
    /// Commands that ran.
    pub applied: u64,
    /// Commands that failed, or could not be read; skipped.
    pub errors: u64,
    pub first_error: Option<String>,
}

impl ApplyReport {
    fn failed(&mut self, why: String) {
        self.errors += 1;
        if self.first_error.is_none() {
            self.first_error = Some(why);
        }
    }
}

/// The spec for a shipped command, if it is one a peer may apply: a write.
fn shipped_spec(args: &[&[u8]]) -> Result<&'static Spec, String> {
    let spec = commands::lookup(args[0])
        .ok_or_else(|| format!("unknown command '{}'", String::from_utf8_lossy(args[0])))?;
    if !spec.arity_ok(args.len()) {
        return Err(format!("wrong number of arguments for '{}'", spec.name));
    }
    if !spec.is(WRITE) {
        return Err(format!("'{}' is not a write", spec.name));
    }
    Ok(spec)
}

/// A reply that keeps nothing but the first error: what applying needs to
/// know of a command's answer, without building it.
#[derive(Default)]
struct Outcome {
    error: Option<String>,
}

impl Reply for Outcome {
    fn simple(&mut self, _: &str) {}
    fn error(&mut self, e: &str) {
        if self.error.is_none() {
            self.error = Some(e.to_string());
        }
    }
    fn int(&mut self, _: i64) {}
    fn bulk(&mut self, _: &[u8]) {}
    fn null(&mut self) {}
    fn null_array(&mut self) {}
    fn array(&mut self, _: usize) {}
    fn map(&mut self, _: usize) {}
    fn set(&mut self, _: usize) {}
    fn double(&mut self, _: f64) {}
    fn boolean(&mut self, _: bool) {}
    fn verbatim(&mut self, _: &str) {}
}

impl ApplyReport {
    fn note(&mut self, outcome: Outcome) {
        match outcome.error {
            Some(e) => self.failed(e),
            None => self.applied += 1,
        }
    }

    fn merge(&mut self, other: ApplyReport) {
        self.applied += other.applied;
        self.errors += other.errors;
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }
}

/// How many keys an invocation names, by its spec, without collecting them.
fn key_count(spec: &Spec, argc: usize) -> usize {
    use Cmd::*;
    if matches!(
        spec.cmd,
        ZUnionStore | ZInterStore | ZDiffStore | SInterCard | ZUnion | ZInter | ZDiff
    ) {
        return usize::MAX;
    }
    if spec.first <= 0 {
        return 0;
    }
    let n = argc as i32;
    let last = if spec.last < 0 { n + spec.last } else { spec.last.min(n - 1) };
    if last < spec.first {
        return 0;
    }
    ((last - spec.first) / spec.step.max(1) + 1) as usize
}

/// Lanes a large chunk's single-key commands are applied on at once.
const LANES: usize = 4;
/// Commands in a run below which one lane is cheaper than starting others.
const PARALLEL_FROM: usize = 512;

/// A single-key command waiting for its run: database, spec, arguments.
type Queued<'a> = (usize, &'static Spec, &'a [&'a [u8]]);

/// Applies a run of single-key commands. Each key's commands stay in order
/// on one lane (the lane is the key's shard's), so two commands on one key
/// are applied in the order they were shipped; commands on different keys
/// are independent, and a large run is spread over [`LANES`] threads, since
/// one thread applying what four wrote is how a peer falls behind.
fn run_single_keys(stores: &[Option<&KvCache>; DATABASES], run: &mut Vec<Queued<'_>>, now: u64, report: &mut ApplyReport) {
    let apply = |queued: &[&Queued<'_>], report: &mut ApplyReport| {
        for (db, spec, args) in queued {
            let store = stores[*db].expect("resolved before queueing");
            let mut out = Outcome::default();
            commands::run(spec, &mut Direct { store, now }, args, &mut out);
            report.note(out);
        }
    };
    if run.len() < PARALLEL_FROM {
        let all: Vec<&Queued<'_>> = run.iter().collect();
        apply(&all, report);
        run.clear();
        return;
    }
    let mut lanes: [Vec<&Queued<'_>>; LANES] = std::array::from_fn(|_| Vec::with_capacity(run.len() / LANES + 16));
    for queued in run.iter() {
        let (db, spec, args) = queued;
        let store = stores[*db].expect("resolved before queueing");
        let key = args[spec.first as usize];
        let lane = (super::store::shard_of(store.hash(key)) + db) % LANES;
        lanes[lane].push(queued);
    }
    let apply = &apply;
    let reports: Vec<ApplyReport> = std::thread::scope(|scope| {
        let (first, rest) = lanes.split_first().expect("LANES > 0");
        let handles: Vec<_> = rest
            .iter()
            .filter(|lane| !lane.is_empty())
            .map(|lane| {
                scope.spawn(move || {
                    let mut report = ApplyReport::default();
                    apply(lane, &mut report);
                    report
                })
            })
            .collect();
        let mut own = ApplyReport::default();
        apply(first, &mut own);
        let mut reports = vec![own];
        for handle in handles {
            match handle.join() {
                Ok(report) => reports.push(report),
                Err(_) => {
                    let mut failed = ApplyReport::default();
                    failed.failed("a key/value apply lane panicked".into());
                    reports.push(failed);
                }
            }
        }
        reports
    });
    for lane in reports {
        report.merge(lane);
    }
    run.clear();
}

/// Applies a chunk of a peer's effects to this node's stores, in order,
/// through the ordinary command code: memory accounting, `WATCH` versions
/// and blocked pops all see them as any write. Nothing here is recorded for
/// the peers again, so an applied write cannot loop back.
///
/// Per-key order is kept: single-key commands are applied in runs spread
/// over lanes by key (see [`run_single_keys`]), and anything that touches
/// several keys, every key, or another database (`MSET`, `RENAME`, `EXEC`'s
/// group, `FLUSHDB`, `MOVE`) waits for the run before it and runs alone.
///
/// A command that fails (`WRONGTYPE` after two nodes wrote one key
/// differently, `OOM`) is counted and skipped, never fatal: the store is a
/// cache.
pub fn apply_chunk(handle: &KvCacheHandle, chunk: &[u8]) -> ApplyReport {
    let mut report = ApplyReport::default();
    let entries = match decode_chunk(chunk) {
        Ok(entries) => entries,
        Err(e) => {
            report.failed(e);
            return report;
        }
    };
    let now = now_ms();
    let mut stores: [Option<&KvCache>; DATABASES] = [None; DATABASES];
    let mut run: Vec<Queued<'_>> = Vec::new();
    for entry in &entries {
        if entry.db >= DATABASES {
            for _ in &entry.commands {
                report.failed(format!("database {} is out of range", entry.db));
            }
            continue;
        }
        let store: &KvCache = *stores[entry.db].get_or_insert_with(|| handle.db(entry.db).as_ref());
        if entry.atomic {
            run_single_keys(&stores, &mut run, now, &mut report);
            apply_group(handle, store, entry.db, &entry.commands, &mut report);
            continue;
        }
        for args in &entry.commands {
            let spec = match shipped_spec(args) {
                Ok(spec) => spec,
                Err(e) => {
                    report.failed(e);
                    continue;
                }
            };
            if !spec.is(SERVER | ALL) && key_count(spec, args.len()) == 1 {
                run.push((entry.db, spec, args.as_slice()));
                continue;
            }
            run_single_keys(&stores, &mut run, now, &mut report);
            let mut out = Outcome::default();
            if spec.is(SERVER) {
                super::server::run(spec, handle, entry.db, args, &mut out);
            } else {
                commands::run(spec, &mut Direct { store, now }, args, &mut out);
            }
            report.note(out);
        }
    }
    run_single_keys(&stores, &mut run, now, &mut report);
    report
}

/// One `EXEC`'s effects, under every shard they touch at once, as `EXEC` ran
/// them.
fn apply_group(handle: &KvCacheHandle, store: &KvCache, db: usize, group: &[Vec<&[u8]>], report: &mut ApplyReport) {
    let mut runnable: Vec<(&'static Spec, &[&[u8]])> = Vec::with_capacity(group.len());
    for args in group {
        match shipped_spec(args) {
            // A server-level command takes its own locks, which the group's
            // would deadlock; `EXEC` refuses all of them but `FLUSHALL`.
            Ok(spec) if spec.is(SERVER) && spec.cmd != Cmd::FlushAll => {
                report.failed(format!("'{}' cannot be applied inside a transaction", spec.name))
            }
            Ok(spec) => runnable.push((spec, args.as_slice())),
            Err(e) => report.failed(e),
        }
    }
    let all = runnable.iter().any(|(spec, _)| spec.is(ALL));
    let mut pinned = if all {
        Pinned::lock_all(store)
    } else {
        let hashes: Vec<u64> = runnable
            .iter()
            .flat_map(|(spec, args)| spec.keys(args))
            .map(|k| store.hash(k))
            .collect();
        Pinned::lock(store, &hashes)
    };
    for (spec, args) in runnable {
        let mut out = Outcome::default();
        if spec.cmd == Cmd::FlushAll {
            for d in (0..DATABASES).filter(|&d| d != db) {
                if let Some(other) = handle.existing(d) {
                    other.flush();
                }
            }
            let flushdb = commands::lookup(b"flushdb").expect("flushdb is a command");
            commands::run(flushdb, &mut pinned, &[&b"flushdb"[..]], &mut out);
        } else {
            commands::run(spec, &mut pinned, args, &mut out);
        }
        report.note(out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> KvCache {
        KvCache::new(0)
    }

    /// The process-wide settings held at their defaults while a test writes
    /// (another test may be bounding `maxmemory`).
    fn settings() -> parking_lot::RwLockReadGuard<'static, ()> {
        crate::core::kv_cache::tests::default_settings()
    }

    /// Runs `args` the way a replicating node does, over RESP2 or RESP3, and
    /// returns the reply and what would ship.
    fn ship(store: &KvCache, args: &[&str], v3: bool) -> (Item, Vec<Effect>) {
        let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
        let spec = commands::lookup(args[0]).expect("a command");
        let mut bytes = Vec::new();
        let mut shipped = Vec::new();
        {
            let mut out = Resp { out: &mut bytes, v3 };
            if spec.is(commands::BLOCKING) {
                let hashes: Vec<u64> = spec.keys(&args).iter().map(|k| store.hash(k)).collect();
                let mut pinned = Pinned::lock(store, &hashes);
                let mark = out.mark();
                if commands::bpop_attempt(spec, &mut pinned, &args, &mut out) {
                    let reply = out.reply_since(mark).expect("a reply");
                    shipped = effects(spec, &args, &reply, &mut |k| pinned.live_expiry(k));
                }
            } else {
                run_locked(store, spec, &args, &mut out, |e| shipped = e.to_vec());
            }
        }
        let reply = parse_reply(&bytes).map(|(item, _)| item).unwrap_or(Item::Null);
        (reply, shipped)
    }

    fn strings(effect: &Effect) -> Vec<String> {
        effect.iter().map(|a| String::from_utf8_lossy(a).into_owned()).collect()
    }

    fn expiry_of(store: &KvCache, key: &str) -> Option<u64> {
        let mut pinned = Pinned::lock(store, &[store.hash(key.as_bytes())]);
        pinned.live_expiry(key.as_bytes())
    }

    #[test]
    fn a_relative_lifetime_ships_as_the_absolute_one_it_left() {
        let _settings = settings();
        let store = store();
        let before = now_ms();
        let (_, shipped) = ship(&store, &["SET", "k", "v", "EX", "100"], false);
        let at = expiry_of(&store, "k").expect("set with a lifetime");
        assert!(at >= before + 100_000 && at <= now_ms() + 100_000);
        assert_eq!(shipped.len(), 1);
        assert_eq!(strings(&shipped[0]), ["SET", "k", "v", "PXAT", &at.to_string()]);

        let (_, shipped) = ship(&store, &["SETEX", "s", "50", "w"], false);
        let at = expiry_of(&store, "s").unwrap();
        assert_eq!(strings(&shipped[0]), ["SET", "s", "w", "PXAT", &at.to_string()]);

        let (_, shipped) = ship(&store, &["EXPIRE", "k", "300"], false);
        let at = expiry_of(&store, "k").unwrap();
        assert_eq!(strings(&shipped[0]), ["PEXPIREAT", "k", &at.to_string()]);

        let (_, shipped) = ship(&store, &["GETEX", "k", "PX", "5000"], true);
        let at = expiry_of(&store, "k").unwrap();
        assert_eq!(strings(&shipped[0]), ["PEXPIREAT", "k", &at.to_string()]);

        let (_, shipped) = ship(&store, &["GETEX", "k", "PERSIST"], false);
        assert_eq!(strings(&shipped[0]), ["PERSIST", "k"]);
        // A plain GETEX reads; it ships nothing.
        assert!(ship(&store, &["GETEX", "k"], false).1.is_empty());

        // A lifetime in the past removes the key, and that is what ships.
        let (_, shipped) = ship(&store, &["EXPIRE", "k", "-1"], false);
        assert_eq!(strings(&shipped[0]), ["DEL", "k"]);

        // A plain SET clears a lifetime, and ships as the plain SET.
        ship(&store, &["SET", "p", "1", "PX", "100000"], false);
        let (_, shipped) = ship(&store, &["SET", "p", "2"], false);
        assert_eq!(strings(&shipped[0]), ["SET", "p", "2"]);
        // KEEPTTL ships the lifetime it kept, as a time.
        ship(&store, &["PEXPIRE", "p", "100000"], false);
        let (_, shipped) = ship(&store, &["SET", "p", "3", "KEEPTTL"], false);
        let at = expiry_of(&store, "p").unwrap();
        assert_eq!(strings(&shipped[0]), ["SET", "p", "3", "PXAT", &at.to_string()]);
    }

    #[test]
    fn spop_ships_the_srem_of_what_it_took() {
        let _settings = settings();
        for v3 in [false, true] {
            let store = store();
            ship(&store, &["SADD", "s", "a", "b", "c", "d"], v3);
            let (reply, shipped) = ship(&store, &["SPOP", "s", "2"], v3);
            let taken = members(&reply);
            assert_eq!(taken.len(), 2, "{reply:?}");
            assert_eq!(shipped.len(), 1);
            assert_eq!(shipped[0][0], b"SREM");
            assert_eq!(shipped[0][1], b"s");
            assert_eq!(&shipped[0][2..], &taken[..]);

            let (reply, shipped) = ship(&store, &["SPOP", "s"], v3);
            let Item::Bulk(one) = reply else { panic!("{reply:?}") };
            assert_eq!(shipped, vec![vec![b"SREM".to_vec(), b"s".to_vec(), one]]);

            ship(&store, &["SPOP", "s"], v3);
            // Empty now: nothing taken, nothing shipped.
            assert!(ship(&store, &["SPOP", "s"], v3).1.is_empty());
        }
    }

    #[test]
    fn incrbyfloat_ships_the_set_of_its_result() {
        let _settings = settings();
        let store = store();
        ship(&store, &["SET", "f", "10.5"], false);
        let (_, shipped) = ship(&store, &["INCRBYFLOAT", "f", "0.1"], false);
        assert_eq!(strings(&shipped[0]), ["SET", "f", "10.6"]);
        // With a lifetime, the lifetime goes with it.
        ship(&store, &["PEXPIRE", "f", "100000"], false);
        let (_, shipped) = ship(&store, &["INCRBYFLOAT", "f", "1"], true);
        let at = expiry_of(&store, "f").unwrap();
        assert_eq!(strings(&shipped[0]), ["SET", "f", "11.6", "PXAT", &at.to_string()]);

        let (_, shipped) = ship(&store, &["HINCRBYFLOAT", "h", "x", "2.5"], false);
        assert_eq!(strings(&shipped[0]), ["HSET", "h", "x", "2.5"]);
        // An integer increment is deterministic and ships as it was.
        let (_, shipped) = ship(&store, &["INCRBY", "n", "5"], false);
        assert_eq!(strings(&shipped[0]), ["INCRBY", "n", "5"]);
    }

    #[test]
    fn a_write_that_wrote_nothing_ships_nothing() {
        let _settings = settings();
        let store = store();
        ship(&store, &["SET", "k", "v"], false);
        assert!(ship(&store, &["SET", "k", "w", "NX"], false).1.is_empty(), "a failed NX");
        assert!(ship(&store, &["SET", "missing", "w", "XX"], false).1.is_empty(), "a failed XX");
        assert!(ship(&store, &["SET", "k", "w", "NX", "GET"], true).1.is_empty(), "NX GET on a key that exists");
        assert!(ship(&store, &["SETNX", "k", "w"], false).1.is_empty());
        assert!(ship(&store, &["DEL", "missing"], false).1.is_empty(), "a DEL of nothing");
        assert!(ship(&store, &["LPUSH", "k", "x"], false).1.is_empty(), "WRONGTYPE");
        assert!(ship(&store, &["EXPIRE", "missing", "10"], false).1.is_empty());
        assert!(ship(&store, &["HSETNX", "h", "f", "1"], false).1.len() == 1);
        assert!(ship(&store, &["HSETNX", "h", "f", "2"], false).1.is_empty());
        assert!(ship(&store, &["SADD", "s", "a"], false).1.len() == 1);
        assert!(ship(&store, &["SADD", "s", "a"], false).1.is_empty(), "already a member");
        assert!(ship(&store, &["LPOP", "nolist"], false).1.is_empty());
        assert!(ship(&store, &["GETDEL", "missing"], false).1.is_empty());
        assert!(ship(&store, &["RENAMENX", "k", "s"], false).1.is_empty());
        assert!(ship(&store, &["MSETNX", "k", "1", "new", "2"], false).1.is_empty());
        // And what did write, ships as the write it was.
        let (_, shipped) = ship(&store, &["SET", "k", "w", "XX", "GET"], false);
        assert_eq!(strings(&shipped[0]), ["SET", "k", "w"]);
        let (_, shipped) = ship(&store, &["SETNX", "fresh", "1"], false);
        assert_eq!(strings(&shipped[0]), ["SET", "fresh", "1"]);
        let (_, shipped) = ship(&store, &["DEL", "fresh", "missing"], false);
        assert_eq!(strings(&shipped[0]), ["DEL", "fresh", "missing"]);
    }

    #[test]
    fn pops_ship_what_they_took() {
        let _settings = settings();
        let store = store();
        ship(&store, &["RPUSH", "l", "a", "b", "c"], false);
        let (_, shipped) = ship(&store, &["BLPOP", "nothing", "l", "0"], false);
        assert_eq!(strings(&shipped[0]), ["LPOP", "l"]);
        let (_, shipped) = ship(&store, &["BRPOPLPUSH", "l", "m", "0"], false);
        assert_eq!(strings(&shipped[0]), ["LMOVE", "l", "m", "RIGHT", "LEFT"]);
        let (_, shipped) = ship(&store, &["LPOP", "m", "5"], true);
        assert_eq!(strings(&shipped[0]), ["LPOP", "m", "1"]);

        ship(&store, &["ZADD", "z", "1", "a", "2", "b", "3", "c"], false);
        for v3 in [false, true] {
            let (_, shipped) = ship(&store, &["ZPOPMIN", "z"], v3);
            assert_eq!(shipped[0][0], b"ZREM");
            assert_eq!(shipped[0].len(), 3);
        }
        ship(&store, &["ZADD", "y", "1", "a", "2", "b", "3", "c"], false);
        let (_, shipped) = ship(&store, &["ZPOPMAX", "y", "2"], true);
        assert_eq!(strings(&shipped[0]), ["ZREM", "y", "c", "b"]);
    }

    #[test]
    fn entries_round_trip_with_any_bytes() {
        let _settings = settings();
        let binary: Vec<u8> = (0..=255u8).collect();
        let single = vec![vec![b"SET".to_vec(), b"k\0\xff".to_vec(), binary.clone()]];
        let group = vec![
            vec![b"SET".to_vec(), b"a".to_vec(), Vec::new()],
            vec![b"INCR".to_vec(), b"n".to_vec()],
        ];
        let mut chunk = vec![KV_FORMAT_V1];
        encode_entry(&mut chunk, 3, &single);
        encode_entry(&mut chunk, 15, &group);
        let entries = decode_chunk(&chunk).expect("decodes");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].db, 3);
        assert!(!entries[0].atomic);
        assert_eq!(entries[0].commands, vec![vec![&b"SET"[..], b"k\0\xff", &binary]]);
        assert_eq!(entries[1].db, 15);
        assert!(entries[1].atomic);
        assert_eq!(entries[1].commands, vec![vec![&b"SET"[..], b"a", b""], vec![&b"INCR"[..], b"n"]]);

        // A cut chunk, or one in a format from the future, is refused rather
        // than misread.
        assert!(decode_chunk(&chunk[..chunk.len() - 1]).is_err());
        let mut future = chunk.clone();
        future[0] = KV_FORMAT_V1 + 1;
        assert!(decode_chunk(&future).is_err());
    }

    #[test]
    fn a_peer_applying_the_effects_holds_what_the_origin_holds() {
        let _settings = settings();
        let origin = store();
        let mut chunk = vec![KV_FORMAT_V1];
        let writes: &[&[&str]] = &[
            &["SET", "a", "1", "EX", "1000"],
            &["SET", "b", "x"],
            &["INCRBYFLOAT", "c", "1.5"],
            &["SADD", "s", "p", "q", "r"],
            &["SPOP", "s"],
            &["RPUSH", "l", "1", "2", "3"],
            &["LPOP", "l"],
            &["DEL", "b"],
            &["HSET", "h", "f", "v"],
        ];
        for args in writes {
            let (_, shipped) = ship(&origin, args, false);
            encode_entry(&mut chunk, 0, &shipped);
        }
        let peer = KvCacheHandle::new(None);
        let report = apply_chunk(&peer, &chunk);
        assert_eq!(report.errors, 0, "{report:?}");
        assert_eq!(report.applied, writes.len() as u64);

        let read = |store: &KvCache, args: &[&str]| -> Item {
            let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
            let mut out = Collect::default();
            commands::run(commands::lookup(args[0]).unwrap(), &mut Direct::new(store), &args, &mut out);
            out.take()
        };
        let peer_store = peer.db(0);
        for check in [
            &["GET", "a"][..],
            &["GET", "b"],
            &["GET", "c"],
            &["SMEMBERS", "s"],
            &["LRANGE", "l", "0", "-1"],
            &["HGETALL", "h"],
            &["PEXPIRETIME", "a"],
        ] {
            let (mut a, mut b) = (read(&origin, check), read(peer_store, check));
            if let (Item::Array(x), Item::Array(y)) = (&mut a, &mut b) {
                x.sort_by_key(|i| format!("{i:?}"));
                y.sort_by_key(|i| format!("{i:?}"));
            }
            assert_eq!(a, b, "{check:?}");
        }

        // A command that fails on the peer is counted and the rest goes on.
        let mut chunk = vec![KV_FORMAT_V1];
        encode_entry(&mut chunk, 0, &[vec![b"LPUSH".to_vec(), b"a".to_vec(), b"x".to_vec()]]);
        encode_entry(&mut chunk, 0, &[vec![b"SET".to_vec(), b"after".to_vec(), b"1".to_vec()]]);
        let report = apply_chunk(&peer, &chunk);
        assert_eq!((report.applied, report.errors), (1, 1));
        assert!(report.first_error.unwrap().starts_with("WRONGTYPE"));
        assert_eq!(read(peer_store, &["GET", "after"]), Item::Bulk(b"1".to_vec()));
    }

    /// A chunk large enough to be applied on several lanes keeps each key's
    /// commands in order, and a multi-key command waits for what came before.
    #[test]
    fn a_large_chunk_applied_in_lanes_keeps_per_key_order() {
        let _settings = settings();
        let mut chunk = vec![KV_FORMAT_V1];
        let command = |parts: &[&[u8]]| vec![parts.iter().map(|p| p.to_vec()).collect::<Effect>()];
        for i in 0..3000u32 {
            let key = format!("k{}", i % 97);
            encode_entry(&mut chunk, 0, &command(&[b"SET", key.as_bytes(), i.to_string().as_bytes()]));
            encode_entry(&mut chunk, 0, &command(&[b"APPEND", key.as_bytes(), b"!"]));
            encode_entry(&mut chunk, 1, &command(&[b"INCR", b"n"]));
            if i == 1500 {
                // Everything before it is in place when it runs.
                encode_entry(&mut chunk, 0, &command(&[b"MSET", b"k0", b"reset", b"other", b"x"]));
            }
        }
        let peer = KvCacheHandle::new(None);
        let report = apply_chunk(&peer, &chunk);
        assert_eq!((report.applied, report.errors), (9001, 0), "{report:?}");
        let get = |db: usize, key: &str| {
            let mut out = Collect::default();
            let args: [&[u8]; 2] = [b"GET", key.as_bytes()];
            commands::run(commands::lookup(b"GET").unwrap(), &mut Direct::new(peer.db(db)), &args, &mut out);
            out.take()
        };
        for k in 0..97u32 {
            let last = (0..3000u32).filter(|i| i % 97 == k).max().unwrap();
            assert_eq!(get(0, &format!("k{k}")), Item::Bulk(format!("{last}!").into_bytes()), "k{k}");
        }
        assert_eq!(get(1, "n"), Item::Bulk(b"3000".to_vec()));
        assert_eq!(get(0, "other"), Item::Bulk(b"x".to_vec()));
    }

    #[test]
    fn resp_replies_parse_back() {
        let _settings = settings();
        let mut bytes = Vec::new();
        let mut r = Resp { out: &mut bytes, v3: true };
        r.map(1);
        r.bulk(b"k");
        r.double(1.5);
        r.set(2);
        r.bulk(b"a");
        r.null();
        r.boolean(true);
        r.error("ERR no");
        let (first, used) = parse_reply(&bytes).unwrap();
        assert_eq!(first, Item::Map(vec![(Item::Bulk(b"k".to_vec()), Item::Double(1.5))]));
        let (second, used2) = parse_reply(&bytes[used..]).unwrap();
        assert_eq!(second, Item::Array(vec![Item::Bulk(b"a".to_vec()), Item::Null]));
        let (third, used3) = parse_reply(&bytes[used + used2..]).unwrap();
        assert_eq!(third, Item::Bool(true));
        let (fourth, _) = parse_reply(&bytes[used + used2 + used3..]).unwrap();
        assert_eq!(fourth, Item::Error("ERR no".into()));
    }
}
