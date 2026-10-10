//! The command set: lookup, arity, key positions, and dispatch.
//!
//! Every data command is written once, generic over [`Access`] (how it reaches
//! shards) and [`Reply`] (where its answer goes), and runs unchanged from the
//! RESP port, from `EXEC`, and from a `KV …` SQL statement. Connection-level
//! commands (`AUTH`, `SELECT`, `MULTI`, `SUBSCRIBE`, …) need the connection's
//! state and live in `resp.rs`, and are flagged [`CONN`] here so the
//! table still answers `COMMAND` for them.

mod hashes;
mod keys;
mod lists;
mod sets;
mod strings;
mod zsets;

use std::collections::HashMap;
use std::sync::OnceLock;

use super::KvError;
use super::reply::{Reply, write_i64};
use super::store::{Access, Shard, Slot, Value};

pub use lists::bpop_attempt;

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

pub const WRITE: u16 = 1 << 0;
pub const READONLY: u16 = 1 << 1;
/// May grow memory: refused under `noeviction` when the store is full.
pub const DENYOOM: u16 = 1 << 2;
pub const ADMIN: u16 = 1 << 3;
pub const PUBSUB: u16 = 1 << 4;
pub const BLOCKING: u16 = 1 << 5;
/// Answered by the connection, not by [`run`].
pub const CONN: u16 = 1 << 6;
/// Not allowed inside `MULTI`.
pub const NO_MULTI: u16 = 1 << 7;
pub const FAST: u16 = 1 << 8;
/// Needs the whole server (every database, config, the snapshot): answered
/// by `server.rs`.
pub const SERVER: u16 = 1 << 9;
/// Allowed before `AUTH`.
pub const NO_AUTH: u16 = 1 << 10;
/// Reads or writes every shard: `EXEC` must lock them all.
pub const ALL: u16 = 1 << 11;

/// One command: Redis's `COMMAND INFO` facts, and which code runs it.
pub struct Spec {
    pub name: &'static str,
    /// Positive: exactly this many arguments including the name; negative:
    /// at least that many.
    pub arity: i32,
    pub flags: u16,
    /// Key positions: first, last (negative counts from the end), step.
    pub first: i32,
    pub last: i32,
    pub step: i32,
    pub cmd: Cmd,
}

impl Spec {
    pub fn arity_ok(&self, argc: usize) -> bool {
        let argc = argc as i32;
        if self.arity >= 0 {
            argc == self.arity
        } else {
            argc >= -self.arity
        }
    }

    pub fn is(&self, flag: u16) -> bool {
        self.flags & flag != 0
    }

    /// The keys this invocation names, for `EXEC`'s locks and `COMMAND GETKEYS`.
    pub fn keys<'a>(&self, args: &[&'a [u8]]) -> Vec<&'a [u8]> {
        let n = args.len() as i32;
        match self.cmd {
            // `dest numkeys key …`
            Cmd::ZUnionStore | Cmd::ZInterStore | Cmd::ZDiffStore => {
                let count = args
                    .get(2)
                    .and_then(|b| parse_int(b))
                    .unwrap_or(0)
                    .clamp(0, (n - 3).max(0) as i64) as usize;
                let mut keys = vec![args[1]];
                keys.extend_from_slice(&args[3..3 + count]);
                return keys;
            }
            // `numkeys key …`
            Cmd::SInterCard | Cmd::ZUnion | Cmd::ZInter | Cmd::ZDiff => {
                let count = args
                    .get(1)
                    .and_then(|b| parse_int(b))
                    .unwrap_or(0)
                    .clamp(0, (n - 2).max(0) as i64) as usize;
                return args[2..2 + count].to_vec();
            }
            _ => {}
        }
        if self.first == 0 {
            return Vec::new();
        }
        let last = if self.last < 0 { n + self.last } else { self.last.min(n - 1) };
        let mut keys = Vec::new();
        let mut i = self.first;
        while i <= last && i < n {
            keys.push(args[i as usize]);
            i += self.step.max(1);
        }
        keys
    }
}

macro_rules! commands {
    ($( $cmd:ident = $name:literal, $arity:expr, $flags:expr, $first:expr, $last:expr, $step:expr; )*) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum Cmd { $( $cmd, )* }
        pub static TABLE: &[Spec] = &[
            $( Spec { name: $name, arity: $arity, flags: $flags, first: $first, last: $last, step: $step, cmd: Cmd::$cmd }, )*
        ];
    };
}

const W: u16 = WRITE;
const WD: u16 = WRITE | DENYOOM;
const WDF: u16 = WRITE | DENYOOM | FAST;
const WF: u16 = WRITE | FAST;
const R: u16 = READONLY;
const RF: u16 = READONLY | FAST;

commands! {
    // Strings
    Get = "get", 2, RF, 1, 1, 1;
    Set = "set", -3, WD, 1, 1, 1;
    SetNx = "setnx", 3, WDF, 1, 1, 1;
    SetEx = "setex", 4, WD, 1, 1, 1;
    PSetEx = "psetex", 4, WD, 1, 1, 1;
    GetSet = "getset", 3, WDF, 1, 1, 1;
    GetDel = "getdel", 2, WF, 1, 1, 1;
    GetEx = "getex", -2, WF, 1, 1, 1;
    MGet = "mget", -2, RF, 1, -1, 1;
    MSet = "mset", -3, WD, 1, -1, 2;
    MSetNx = "msetnx", -3, WD, 1, -1, 2;
    Incr = "incr", 2, WDF, 1, 1, 1;
    Decr = "decr", 2, WDF, 1, 1, 1;
    IncrBy = "incrby", 3, WDF, 1, 1, 1;
    DecrBy = "decrby", 3, WDF, 1, 1, 1;
    IncrByFloat = "incrbyfloat", 3, WDF, 1, 1, 1;
    Append = "append", 3, WDF, 1, 1, 1;
    StrLen = "strlen", 2, RF, 1, 1, 1;
    GetRange = "getrange", 4, R, 1, 1, 1;
    SubStr = "substr", 4, R, 1, 1, 1;
    SetRange = "setrange", 4, WD, 1, 1, 1;
    SetBit = "setbit", 4, WD, 1, 1, 1;
    GetBit = "getbit", 3, RF, 1, 1, 1;
    BitCount = "bitcount", -2, R, 1, 1, 1;
    // Keys
    Del = "del", -2, W, 1, -1, 1;
    Unlink = "unlink", -2, WF, 1, -1, 1;
    Exists = "exists", -2, RF, 1, -1, 1;
    Touch = "touch", -2, RF, 1, -1, 1;
    Expire = "expire", -3, WF, 1, 1, 1;
    PExpire = "pexpire", -3, WF, 1, 1, 1;
    ExpireAt = "expireat", -3, WF, 1, 1, 1;
    PExpireAt = "pexpireat", -3, WF, 1, 1, 1;
    Ttl = "ttl", 2, RF, 1, 1, 1;
    PTtl = "pttl", 2, RF, 1, 1, 1;
    ExpireTime = "expiretime", 2, RF, 1, 1, 1;
    PExpireTime = "pexpiretime", 2, RF, 1, 1, 1;
    Persist = "persist", 2, WF, 1, 1, 1;
    Type = "type", 2, RF, 1, 1, 1;
    Rename = "rename", 3, W, 1, 2, 1;
    RenameNx = "renamenx", 3, WF, 1, 2, 1;
    Copy = "copy", -3, WD, 1, 2, 1;
    Keys = "keys", 2, R | ALL, 0, 0, 0;
    Scan = "scan", -2, R | ALL, 0, 0, 0;
    DbSize = "dbsize", 1, RF | ALL, 0, 0, 0;
    RandomKey = "randomkey", 1, R | ALL, 0, 0, 0;
    Object = "object", -2, R, 2, 2, 1;
    Memory = "memory", -2, R, 2, 2, 1;
    // Hashes
    HSet = "hset", -4, WDF, 1, 1, 1;
    HSetNx = "hsetnx", 4, WDF, 1, 1, 1;
    HMSet = "hmset", -4, WDF, 1, 1, 1;
    HGet = "hget", 3, RF, 1, 1, 1;
    HMGet = "hmget", -3, RF, 1, 1, 1;
    HGetAll = "hgetall", 2, R, 1, 1, 1;
    HDel = "hdel", -3, WF, 1, 1, 1;
    HExists = "hexists", 3, RF, 1, 1, 1;
    HLen = "hlen", 2, RF, 1, 1, 1;
    HKeys = "hkeys", 2, R, 1, 1, 1;
    HVals = "hvals", 2, R, 1, 1, 1;
    HIncrBy = "hincrby", 4, WDF, 1, 1, 1;
    HIncrByFloat = "hincrbyfloat", 4, WDF, 1, 1, 1;
    HStrLen = "hstrlen", 3, RF, 1, 1, 1;
    HRandField = "hrandfield", -2, R, 1, 1, 1;
    HScan = "hscan", -3, R, 1, 1, 1;
    // Lists
    LPush = "lpush", -3, WDF, 1, 1, 1;
    RPush = "rpush", -3, WDF, 1, 1, 1;
    LPushX = "lpushx", -3, WDF, 1, 1, 1;
    RPushX = "rpushx", -3, WDF, 1, 1, 1;
    LPop = "lpop", -2, WF, 1, 1, 1;
    RPop = "rpop", -2, WF, 1, 1, 1;
    LLen = "llen", 2, RF, 1, 1, 1;
    LRange = "lrange", 4, R, 1, 1, 1;
    LIndex = "lindex", 3, R, 1, 1, 1;
    LSet = "lset", 4, WD, 1, 1, 1;
    LRem = "lrem", 4, W, 1, 1, 1;
    LTrim = "ltrim", 4, W, 1, 1, 1;
    LInsert = "linsert", 5, WD, 1, 1, 1;
    LPos = "lpos", -3, R, 1, 1, 1;
    LMove = "lmove", 5, WD, 1, 2, 1;
    RPopLPush = "rpoplpush", 3, WD, 1, 2, 1;
    BLPop = "blpop", -3, W | BLOCKING, 1, -2, 1;
    BRPop = "brpop", -3, W | BLOCKING, 1, -2, 1;
    BLMove = "blmove", 6, WD | BLOCKING, 1, 2, 1;
    BRPopLPush = "brpoplpush", 4, WD | BLOCKING, 1, 2, 1;
    // Sets
    SAdd = "sadd", -3, WDF, 1, 1, 1;
    SRem = "srem", -3, WF, 1, 1, 1;
    SMembers = "smembers", 2, R, 1, 1, 1;
    SIsMember = "sismember", 3, RF, 1, 1, 1;
    SMIsMember = "smismember", -3, RF, 1, 1, 1;
    SCard = "scard", 2, RF, 1, 1, 1;
    SPop = "spop", -2, WF, 1, 1, 1;
    SRandMember = "srandmember", -2, R, 1, 1, 1;
    SInter = "sinter", -2, R, 1, -1, 1;
    SInterStore = "sinterstore", -3, WD, 1, -1, 1;
    SInterCard = "sintercard", -3, R, 0, 0, 0;
    SUnion = "sunion", -2, R, 1, -1, 1;
    SUnionStore = "sunionstore", -3, WD, 1, -1, 1;
    SDiff = "sdiff", -2, R, 1, -1, 1;
    SDiffStore = "sdiffstore", -3, WD, 1, -1, 1;
    SMove = "smove", 4, WF, 1, 2, 1;
    SScan = "sscan", -3, R, 1, 1, 1;
    // Sorted sets
    ZAdd = "zadd", -4, WDF, 1, 1, 1;
    ZRem = "zrem", -3, WF, 1, 1, 1;
    ZScore = "zscore", 3, RF, 1, 1, 1;
    ZMScore = "zmscore", -3, RF, 1, 1, 1;
    ZIncrBy = "zincrby", 4, WDF, 1, 1, 1;
    ZCard = "zcard", 2, RF, 1, 1, 1;
    ZCount = "zcount", 4, RF, 1, 1, 1;
    ZLexCount = "zlexcount", 4, RF, 1, 1, 1;
    ZRank = "zrank", -3, RF, 1, 1, 1;
    ZRevRank = "zrevrank", -3, RF, 1, 1, 1;
    ZRange = "zrange", -4, R, 1, 1, 1;
    ZRevRange = "zrevrange", -4, R, 1, 1, 1;
    ZRangeByScore = "zrangebyscore", -4, R, 1, 1, 1;
    ZRevRangeByScore = "zrevrangebyscore", -4, R, 1, 1, 1;
    ZRangeByLex = "zrangebylex", -4, R, 1, 1, 1;
    ZRevRangeByLex = "zrevrangebylex", -4, R, 1, 1, 1;
    ZPopMin = "zpopmin", -2, WF, 1, 1, 1;
    ZPopMax = "zpopmax", -2, WF, 1, 1, 1;
    ZRemRangeByRank = "zremrangebyrank", 4, W, 1, 1, 1;
    ZRemRangeByScore = "zremrangebyscore", 4, W, 1, 1, 1;
    ZRemRangeByLex = "zremrangebylex", 4, W, 1, 1, 1;
    ZScan = "zscan", -3, R, 1, 1, 1;
    ZUnionStore = "zunionstore", -4, WD, 1, 1, 1;
    ZInterStore = "zinterstore", -4, WD, 1, 1, 1;
    ZDiffStore = "zdiffstore", -4, WD, 1, 1, 1;
    ZUnion = "zunion", -3, R, 0, 0, 0;
    ZInter = "zinter", -3, R, 0, 0, 0;
    ZDiff = "zdiff", -3, R, 0, 0, 0;
    // Pub/Sub
    Publish = "publish", 3, PUBSUB | FAST | CONN, 0, 0, 0;
    Subscribe = "subscribe", -2, PUBSUB | NO_MULTI | CONN, 0, 0, 0;
    Unsubscribe = "unsubscribe", -1, PUBSUB | NO_MULTI | CONN, 0, 0, 0;
    PSubscribe = "psubscribe", -2, PUBSUB | NO_MULTI | CONN, 0, 0, 0;
    PUnsubscribe = "punsubscribe", -1, PUBSUB | NO_MULTI | CONN, 0, 0, 0;
    PubSubCmd = "pubsub", -2, PUBSUB | CONN, 0, 0, 0;
    // Transactions
    Multi = "multi", 1, NO_MULTI | CONN | FAST, 0, 0, 0;
    Exec = "exec", 1, NO_MULTI | CONN, 0, 0, 0;
    Discard = "discard", 1, NO_MULTI | CONN | FAST, 0, 0, 0;
    Watch = "watch", -2, NO_MULTI | CONN | FAST, 1, -1, 1;
    Unwatch = "unwatch", 1, NO_MULTI | CONN | FAST, 0, 0, 0;
    // Connection
    Ping = "ping", -1, FAST, 0, 0, 0;
    Echo = "echo", 2, FAST, 0, 0, 0;
    Auth = "auth", -2, CONN | NO_AUTH | NO_MULTI | FAST, 0, 0, 0;
    Hello = "hello", -1, CONN | NO_AUTH | NO_MULTI | FAST, 0, 0, 0;
    Select = "select", 2, CONN | FAST, 0, 0, 0;
    Quit = "quit", -1, CONN | NO_AUTH | FAST, 0, 0, 0;
    Reset = "reset", 1, CONN | NO_AUTH | FAST, 0, 0, 0;
    Client = "client", -2, CONN, 0, 0, 0;
    // Server
    Info = "info", -1, SERVER | NO_MULTI, 0, 0, 0;
    Config = "config", -2, SERVER | ADMIN, 0, 0, 0;
    Command = "command", -1, SERVER, 0, 0, 0;
    Save = "save", 1, SERVER | ADMIN | NO_MULTI, 0, 0, 0;
    BgSave = "bgsave", -1, SERVER | ADMIN, 0, 0, 0;
    LastSave = "lastsave", 1, SERVER | FAST, 0, 0, 0;
    FlushDb = "flushdb", -1, WRITE | ALL, 0, 0, 0;
    FlushAll = "flushall", -1, WRITE | SERVER | ALL, 0, 0, 0;
    Time = "time", 1, FAST, 0, 0, 0;
    SwapDb = "swapdb", 3, WRITE | SERVER | NO_MULTI, 0, 0, 0;
    Move = "move", 3, WRITE | SERVER | NO_MULTI, 1, 1, 1;
}

/// The spec for a command name, case-insensitively.
pub fn lookup(name: &[u8]) -> Option<&'static Spec> {
    static INDEX: OnceLock<HashMap<&'static [u8], &'static Spec, ahash::RandomState>> = OnceLock::new();
    let index = INDEX.get_or_init(|| TABLE.iter().map(|s| (s.name.as_bytes(), s)).collect());
    if name.len() > 32 {
        return None;
    }
    let mut lower = [0u8; 32];
    for (d, s) in lower.iter_mut().zip(name) {
        *d = s.to_ascii_lowercase();
    }
    index.get(&lower[..name.len()]).copied()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// Runs a data command. The caller has checked arity and that the command is
/// not one of the connection- or server-level ones.
pub fn run<A: Access, O: Reply>(spec: &Spec, a: &mut A, args: &[&[u8]], out: &mut O) {
    use Cmd::*;
    match spec.cmd {
        Get => strings::get(a, args, out),
        Set => strings::set(a, args, out),
        SetNx => strings::setnx(a, args, out),
        SetEx => strings::setex(a, args, out, 1000),
        PSetEx => strings::setex(a, args, out, 1),
        GetSet => strings::getset(a, args, out),
        GetDel => strings::getdel(a, args, out),
        GetEx => strings::getex(a, args, out),
        MGet => strings::mget(a, args, out),
        MSet => strings::mset(a, args, out, false),
        MSetNx => strings::mset(a, args, out, true),
        Incr => strings::incr_by(a, args[1], Ok(1), out),
        Decr => strings::incr_by(a, args[1], Ok(-1), out),
        IncrBy => strings::incr_by(a, args[1], int_arg(args[2]), out),
        DecrBy => strings::incr_by(
            a,
            args[1],
            int_arg(args[2]).and_then(|n| n.checked_neg().ok_or(KvError::Overflow)),
            out,
        ),
        IncrByFloat => strings::incr_by_float(a, args, out),
        Append => strings::append(a, args, out),
        StrLen => strings::strlen(a, args, out),
        GetRange | SubStr => strings::getrange(a, args, out),
        SetRange => strings::setrange(a, args, out),
        SetBit => strings::setbit(a, args, out),
        GetBit => strings::getbit(a, args, out),
        BitCount => strings::bitcount(a, args, out),

        Del | Unlink => keys::del(a, args, out),
        Exists => keys::exists(a, args, out),
        Touch => keys::touch(a, args, out),
        Expire => keys::expire(a, args, out, 1000, false),
        PExpire => keys::expire(a, args, out, 1, false),
        ExpireAt => keys::expire(a, args, out, 1000, true),
        PExpireAt => keys::expire(a, args, out, 1, true),
        Ttl => keys::ttl(a, args, out, false, false),
        PTtl => keys::ttl(a, args, out, true, false),
        ExpireTime => keys::ttl(a, args, out, false, true),
        PExpireTime => keys::ttl(a, args, out, true, true),
        Persist => keys::persist(a, args, out),
        Type => keys::type_of(a, args, out),
        Rename => keys::rename(a, args, out, false),
        RenameNx => keys::rename(a, args, out, true),
        Copy => keys::copy(a, args, out),
        Keys => keys::keys(a, args, out),
        Scan => keys::scan(a, args, out),
        DbSize => keys::dbsize(a, out),
        FlushDb => keys::flushdb(a, args, out),
        RandomKey => keys::randomkey(a, out),
        Object => keys::object(a, args, out),
        Memory => keys::memory(a, args, out),

        HSet => hashes::hset(a, args, out, Hset::Count),
        HMSet => hashes::hset(a, args, out, Hset::Ok),
        HSetNx => hashes::hsetnx(a, args, out),
        HGet => hashes::hget(a, args, out),
        HMGet => hashes::hmget(a, args, out),
        HGetAll => hashes::hgetall(a, args, out, true, true),
        HKeys => hashes::hgetall(a, args, out, true, false),
        HVals => hashes::hgetall(a, args, out, false, true),
        HDel => hashes::hdel(a, args, out),
        HExists => hashes::hexists(a, args, out),
        HLen => hashes::hlen(a, args, out),
        HIncrBy => hashes::hincrby(a, args, out),
        HIncrByFloat => hashes::hincrbyfloat(a, args, out),
        HStrLen => hashes::hstrlen(a, args, out),
        HRandField => hashes::hrandfield(a, args, out),
        HScan => hashes::hscan(a, args, out),

        LPush => lists::push(a, args, out, true, false),
        RPush => lists::push(a, args, out, false, false),
        LPushX => lists::push(a, args, out, true, true),
        RPushX => lists::push(a, args, out, false, true),
        LPop => lists::pop(a, args, out, true),
        RPop => lists::pop(a, args, out, false),
        LLen => lists::llen(a, args, out),
        LRange => lists::lrange(a, args, out),
        LIndex => lists::lindex(a, args, out),
        LSet => lists::lset(a, args, out),
        LRem => lists::lrem(a, args, out),
        LTrim => lists::ltrim(a, args, out),
        LInsert => lists::linsert(a, args, out),
        LPos => lists::lpos(a, args, out),
        LMove | RPopLPush | BLPop | BRPop | BLMove | BRPopLPush => {
            // Non-blocking form: inside EXEC, or the first attempt of a
            // blocking one. A blocking command that finds nothing replies
            // with a null array here; the connection decides whether to wait.
            if !bpop_attempt(spec, a, args, out) {
                match spec.cmd {
                    LMove | RPopLPush | BLMove | BRPopLPush => out.null(),
                    _ => out.null_array(),
                }
            }
        }

        SAdd => sets::sadd(a, args, out),
        SRem => sets::srem(a, args, out),
        SMembers => sets::smembers(a, args, out),
        SIsMember => sets::sismember(a, args, out),
        SMIsMember => sets::smismember(a, args, out),
        SCard => sets::scard(a, args, out),
        SPop => sets::spop(a, args, out),
        SRandMember => sets::srandmember(a, args, out),
        SInter => sets::combine(a, args, out, sets::Op::Inter, None),
        SUnion => sets::combine(a, args, out, sets::Op::Union, None),
        SDiff => sets::combine(a, args, out, sets::Op::Diff, None),
        SInterStore => sets::combine(a, &args[1..], out, sets::Op::Inter, Some(args[1])),
        SUnionStore => sets::combine(a, &args[1..], out, sets::Op::Union, Some(args[1])),
        SDiffStore => sets::combine(a, &args[1..], out, sets::Op::Diff, Some(args[1])),
        SInterCard => sets::sintercard(a, args, out),
        SMove => sets::smove(a, args, out),
        SScan => sets::sscan(a, args, out),

        ZAdd => zsets::zadd(a, args, out),
        ZIncrBy => zsets::zincrby(a, args, out),
        ZRem => zsets::zrem(a, args, out),
        ZScore => zsets::zscore(a, args, out),
        ZMScore => zsets::zmscore(a, args, out),
        ZCard => zsets::zcard(a, args, out),
        ZCount => zsets::zcount(a, args, out, false),
        ZLexCount => zsets::zcount(a, args, out, true),
        ZRank => zsets::zrank(a, args, out, false),
        ZRevRank => zsets::zrank(a, args, out, true),
        ZRange => zsets::zrange(a, args, out, zsets::Form::Range),
        ZRevRange => zsets::zrange(a, args, out, zsets::Form::RevRange),
        ZRangeByScore => zsets::zrange(a, args, out, zsets::Form::ByScore),
        ZRevRangeByScore => zsets::zrange(a, args, out, zsets::Form::RevByScore),
        ZRangeByLex => zsets::zrange(a, args, out, zsets::Form::ByLex),
        ZRevRangeByLex => zsets::zrange(a, args, out, zsets::Form::RevByLex),
        ZPopMin => zsets::zpop(a, args, out, false),
        ZPopMax => zsets::zpop(a, args, out, true),
        ZRemRangeByRank => zsets::zremrange(a, args, out, zsets::RemBy::Rank),
        ZRemRangeByScore => zsets::zremrange(a, args, out, zsets::RemBy::Score),
        ZRemRangeByLex => zsets::zremrange(a, args, out, zsets::RemBy::Lex),
        ZScan => zsets::zscan(a, args, out),
        ZUnionStore => zsets::combine(a, args, out, zsets::SetOp::Union, true),
        ZInterStore => zsets::combine(a, args, out, zsets::SetOp::Inter, true),
        ZDiffStore => zsets::combine(a, args, out, zsets::SetOp::Diff, true),
        ZUnion => zsets::combine(a, args, out, zsets::SetOp::Union, false),
        ZInter => zsets::combine(a, args, out, zsets::SetOp::Inter, false),
        ZDiff => zsets::combine(a, args, out, zsets::SetOp::Diff, false),

        Ping => match args.get(1) {
            Some(msg) => out.bulk(msg),
            None => out.simple("PONG"),
        },
        Echo => out.bulk(args[1]),
        Time => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            out.array(2);
            bulk_int(out, now.as_secs() as i64);
            bulk_int(out, now.subsec_micros() as i64);
        }

        // Connection- and server-level commands are answered before `run`.
        _ => out.error(&format!(
            "ERR '{}' cannot run here",
            spec.name
        )),
    }
}

pub enum Hset {
    Count,
    Ok,
}

// ---------------------------------------------------------------------------
// Helpers shared by the command files
// ---------------------------------------------------------------------------

pub const SYNTAX: &str = "ERR syntax error";

#[inline]
pub fn eq(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// Redis's `string2ll`: an optional `-`, then digits with no leading zero
/// (`0` alone excepted), nothing else, within `i64`.
pub fn parse_int(b: &[u8]) -> Option<i64> {
    if b.is_empty() || b.len() > 20 {
        return None;
    }
    if b == b"0" {
        return Some(0);
    }
    let (negative, digits) = match b[0] {
        b'-' => (true, &b[1..]),
        _ => (false, b),
    };
    if digits.is_empty() || !(b'1'..=b'9').contains(&digits[0]) {
        return None;
    }
    let mut v: u64 = 0;
    for &c in digits {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)?.checked_add((c - b'0') as u64)?;
    }
    if negative {
        if v <= i64::MAX as u64 + 1 {
            Some((v as i128).wrapping_neg() as i64)
        } else {
            None
        }
    } else {
        i64::try_from(v).ok()
    }
}

pub fn int_arg(b: &[u8]) -> Result<i64, KvError> {
    parse_int(b).ok_or(KvError::NotInteger)
}

/// A non-negative integer argument, as Redis's `getPositiveLongFromObject`.
pub fn uint_arg(b: &[u8]) -> Result<u64, KvError> {
    match parse_int(b) {
        Some(n) if n >= 0 => Ok(n as u64),
        Some(_) => Err(KvError::Other("value is out of range, must be positive".into())),
        None => Err(KvError::NotInteger),
    }
}

/// Redis's float argument: a decimal or exponent form, `inf`/`-inf`, never
/// NaN, no surrounding space.
pub fn parse_float(b: &[u8]) -> Option<f64> {
    if b.is_empty() || b[0].is_ascii_whitespace() || b[b.len() - 1].is_ascii_whitespace() {
        return None;
    }
    let s = std::str::from_utf8(b).ok()?;
    let v: f64 = match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => f64::INFINITY,
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        lower => {
            if lower.contains("nan") || lower.contains("inf") {
                return None;
            }
            s.parse().ok()?
        }
    };
    (!v.is_nan()).then_some(v)
}

pub fn float_arg(b: &[u8]) -> Result<f64, KvError> {
    parse_float(b).ok_or(KvError::NotFloat)
}

/// An integer sent as a bulk string (what `TIME` and `INCRBYFLOAT` send).
pub fn bulk_int<O: Reply>(out: &mut O, n: i64) {
    let mut buf = Vec::with_capacity(20);
    write_i64(&mut buf, n);
    out.bulk(&buf);
}

/// Sends a string-typed slot's value; WRONGTYPE for a structure.
#[inline]
pub fn send_string<O: Reply>(slot: &Slot, out: &mut O) {
    match &slot.value {
        Value::Str => out.bulk(slot.str_bytes()),
        Value::Int(n) => bulk_int(out, *n),
        _ => out.err(&KvError::WrongType),
    }
}

/// Runs `f` on the live slot for `key` (or `None`) under a read lock, and
/// counts the hit or miss.
#[inline]
pub fn read_key<A: Access, O: Reply>(
    a: &mut A,
    key: &[u8],
    out: &mut O,
    f: impl FnOnce(Option<&Slot>, &mut O),
) {
    let h = a.hash(key);
    let now = a.now();
    let hit = a.read(h, |s| {
        let slot = s.live(h, key, now);
        if let Some(sl) = slot {
            sl.touch();
        }
        let hit = slot.is_some();
        f(slot, out);
        hit
    });
    a.count_read(h, hit);
}

/// Mutates the value at `key` with `f`, creating it with `make` first when
/// the key is absent. A created value that `f` leaves empty is not stored,
/// and an existing one `f` empties is deleted, as Redis never keeps an empty
/// structure.
pub fn modify<T>(
    s: &mut Shard,
    h: u64,
    key: &[u8],
    now: u64,
    make: impl FnOnce() -> Value,
    f: impl FnOnce(&mut Value) -> Result<T, KvError>,
) -> Result<T, KvError> {
    let mut f = Some(f);
    let existing = s.update(h, key, now, |slot| (f.take().expect("once"))(&mut slot.value));
    match existing {
        Some(result) => {
            remove_if_empty(s, h, key);
            result
        }
        None => {
            let mut value = make();
            let result = (f.take().expect("once"))(&mut value)?;
            if !is_empty_value(&value) {
                s.insert(h, Slot::with_value(key, value, 0));
            }
            Ok(result)
        }
    }
}

/// Mutates the value at `key` only if it exists. `Ok(None)` when absent.
pub fn modify_existing<T>(
    s: &mut Shard,
    h: u64,
    key: &[u8],
    now: u64,
    f: impl FnOnce(&mut Value) -> Result<T, KvError>,
) -> Result<Option<T>, KvError> {
    match s.update(h, key, now, |slot| f(&mut slot.value)) {
        Some(result) => {
            remove_if_empty(s, h, key);
            result.map(Some)
        }
        None => Ok(None),
    }
}

pub fn is_empty_value(v: &Value) -> bool {
    match v {
        Value::Hash(m) => m.is_empty(),
        Value::List(l) => l.is_empty(),
        Value::Set(x) => x.is_empty(),
        Value::ZSet(z) => z.is_empty(),
        Value::Str | Value::Int(_) => false,
    }
}

pub fn remove_if_empty(s: &mut Shard, h: u64, key: &[u8]) {
    if s.find(h, key).is_some_and(|slot| is_empty_value(&slot.value)) {
        s.remove(h, key);
    }
}

/// Makes room for `need` bytes in the shard by the configured policy.
#[inline]
pub fn room(s: &mut Shard, need: usize) -> Result<(), KvError> {
    let budget = super::max_memory() / super::store::SHARDS;
    if budget == 0 {
        return Ok(());
    }
    s.make_room(need, budget, super::eviction_policy())
}

/// A per-thread xorshift for read-only random picks (`SRANDMEMBER`, …),
/// which hold only a read lock and so cannot use the shard's generator.
pub fn random_u64() -> u64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = Cell::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x2545F4914F6CDD1D)
                | 1,
        );
    }
    STATE.with(|s| {
        let mut x = s.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        x
    })
}

/// Normalises Redis's inclusive, negative-from-the-end `start stop` against a
/// length: `None` when the range is empty.
pub fn range_of(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let mut start = if start < 0 { len + start } else { start };
    let mut stop = if stop < 0 { len + stop } else { stop };
    if start < 0 {
        start = 0;
    }
    if stop >= len {
        stop = len - 1;
    }
    if start > stop || start >= len || len == 0 {
        return None;
    }
    Some((start as usize, stop as usize))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_int_is_redis_strict() {
        assert_eq!(parse_int(b"0"), Some(0));
        assert_eq!(parse_int(b"-12"), Some(-12));
        assert_eq!(parse_int(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_int(b"-9223372036854775808"), Some(i64::MIN));
        for bad in [&b"01"[..], b"-0", b"+1", b" 1", b"1 ", b"", b"1.0", b"9223372036854775808", b"-"] {
            assert_eq!(parse_int(bad), None, "{:?}", std::str::from_utf8(bad));
        }
    }

    #[test]
    fn parse_float_refuses_nan_and_space() {
        assert_eq!(parse_float(b"1.5"), Some(1.5));
        assert_eq!(parse_float(b"-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_float(b"+inf"), Some(f64::INFINITY));
        assert_eq!(parse_float(b"1e3"), Some(1000.0));
        assert_eq!(parse_float(b"nan"), None);
        assert_eq!(parse_float(b" 1"), None);
        assert_eq!(parse_float(b""), None);
    }

    #[test]
    fn every_name_is_lowercase_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for spec in TABLE {
            assert_eq!(spec.name, spec.name.to_ascii_lowercase());
            assert!(seen.insert(spec.name), "duplicate {}", spec.name);
            assert!(lookup(spec.name.to_ascii_uppercase().as_bytes()).is_some());
        }
    }

    #[test]
    fn key_specs() {
        let mset = lookup(b"MSET").unwrap();
        assert_eq!(
            mset.keys(&[b"mset", b"a", b"1", b"b", b"2"]),
            vec![&b"a"[..], b"b"]
        );
        let blpop = lookup(b"blpop").unwrap();
        assert_eq!(blpop.keys(&[b"blpop", b"x", b"y", b"0"]), vec![&b"x"[..], b"y"]);
        let zu = lookup(b"zunionstore").unwrap();
        assert_eq!(
            zu.keys(&[b"zunionstore", b"d", b"2", b"a", b"b", b"WEIGHTS", b"1", b"2"]),
            vec![&b"d"[..], b"a", b"b"]
        );
    }
}
