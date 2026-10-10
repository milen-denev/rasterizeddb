//! The store and its commands, checked against the answers Redis gives.

use std::sync::Arc;

use super::commands::{self, ALL};
use super::reply::{Collect, Item};
use super::store::{Direct, KvCache, Pinned};
use super::{EvictionPolicy, KvCacheHandle};

fn run(store: &KvCache, args: &[&str]) -> Item {
    let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
    let spec = commands::lookup(args[0]).unwrap_or_else(|| panic!("no command {:?}", args[0]));
    assert!(spec.arity_ok(args.len()), "arity of {:?}", args);
    let mut out = Collect::default();
    commands::run(spec, &mut Direct::new(store), &args, &mut out);
    out.take()
}

/// The same command inside a transaction holding only the shards its key
/// spec names: a wrong spec panics here rather than in production.
fn run_pinned(store: &KvCache, args: &[&str]) -> Item {
    let args: Vec<&[u8]> = args.iter().map(|a| a.as_bytes()).collect();
    let spec = commands::lookup(args[0]).expect("command");
    let mut pinned = if spec.is(ALL) {
        Pinned::lock_all(store)
    } else {
        let hashes: Vec<u64> = spec.keys(&args).iter().map(|k| store.hash(k)).collect();
        Pinned::lock(store, &hashes)
    };
    let mut out = Collect::default();
    commands::run(spec, &mut pinned, &args, &mut out);
    out.take()
}

fn bulk(s: &str) -> Item {
    Item::Bulk(s.as_bytes().to_vec())
}

fn ok() -> Item {
    Item::Simple("OK".into())
}

fn bulks(items: &[&str]) -> Item {
    Item::Array(items.iter().map(|s| bulk(s)).collect())
}

fn is_error(item: &Item, prefix: &str) -> bool {
    matches!(item, Item::Error(e) if e.starts_with(prefix))
}

fn sorted(item: Item) -> Vec<Vec<u8>> {
    let Item::Array(items) = item else {
        panic!("not an array: {item:?}")
    };
    let mut v: Vec<Vec<u8>> = items
        .into_iter()
        .map(|i| match i {
            Item::Bulk(b) => b,
            other => panic!("not a bulk: {other:?}"),
        })
        .collect();
    v.sort();
    v
}

#[test]
fn strings() {
    let s = KvCache::new(0);
    assert_eq!(run(&s, &["GET", "k"]), Item::Null);
    assert_eq!(run(&s, &["SET", "k", "v"]), ok());
    assert_eq!(run(&s, &["GET", "k"]), bulk("v"));
    assert_eq!(run(&s, &["SET", "k", "w", "NX"]), Item::Null);
    assert_eq!(run(&s, &["SET", "k", "w", "XX", "GET"]), bulk("v"));
    assert_eq!(run(&s, &["GET", "k"]), bulk("w"));
    assert_eq!(run(&s, &["SET", "absent", "x", "XX"]), Item::Null);
    assert_eq!(run(&s, &["SETNX", "k", "z"]), Item::Int(0));
    assert_eq!(run(&s, &["APPEND", "k", "xyz"]), Item::Int(4));
    assert_eq!(run(&s, &["STRLEN", "k"]), Item::Int(4));
    assert_eq!(run(&s, &["GETRANGE", "k", "1", "-2"]), bulk("xy"));
    assert_eq!(run(&s, &["GETRANGE", "k", "10", "20"]), bulk(""));
    assert_eq!(run(&s, &["SETRANGE", "k", "6", "!!"]), Item::Int(8));
    assert_eq!(run(&s, &["GET", "k"]), Item::Bulk(b"wxyz\0\0!!".to_vec()));
    assert_eq!(run(&s, &["GETDEL", "k"]), Item::Bulk(b"wxyz\0\0!!".to_vec()));
    assert_eq!(run(&s, &["GET", "k"]), Item::Null);
    assert_eq!(run(&s, &["MSET", "a", "1", "b", "2"]), ok());
    assert_eq!(
        run(&s, &["MGET", "a", "nope", "b"]),
        Item::Array(vec![bulk("1"), Item::Null, bulk("2")])
    );
    assert_eq!(run(&s, &["MSETNX", "b", "9", "c", "3"]), Item::Int(0));
    assert_eq!(run(&s, &["GET", "c"]), Item::Null);
    assert_eq!(run(&s, &["MSETNX", "c", "3", "d", "4"]), Item::Int(1));
    assert!(is_error(&run(&s, &["MSET", "a", "1", "b"]), "ERR wrong number"));
    assert!(is_error(&run(&s, &["SET", "k", "v", "EX", "0"]), "ERR invalid expire"));
    assert!(is_error(&run(&s, &["SET", "k", "v", "BOGUS"]), "ERR syntax"));
}

#[test]
fn counters() {
    let s = KvCache::new(0);
    assert_eq!(run(&s, &["INCR", "n"]), Item::Int(1));
    assert_eq!(run(&s, &["INCRBY", "n", "41"]), Item::Int(42));
    assert_eq!(run(&s, &["DECR", "n"]), Item::Int(41));
    assert_eq!(run(&s, &["DECRBY", "n", "-9"]), Item::Int(50));
    assert_eq!(run(&s, &["GET", "n"]), bulk("50"));
    assert_eq!(run(&s, &["APPEND", "n", "0"]), Item::Int(3));
    assert_eq!(run(&s, &["INCR", "n"]), Item::Int(501));
    run(&s, &["SET", "big", &i64::MAX.to_string()]);
    assert!(is_error(&run(&s, &["INCR", "big"]), "ERR increment or decrement would overflow"));
    run(&s, &["SET", "word", "abc"]);
    assert!(is_error(&run(&s, &["INCR", "word"]), "ERR value is not an integer"));
    run(&s, &["SET", "lead", "01"]);
    assert!(is_error(&run(&s, &["INCR", "lead"]), "ERR value is not an integer"));
    assert!(is_error(&run(&s, &["DECRBY", "n", &i64::MIN.to_string()]), "ERR"));
    assert_eq!(run(&s, &["INCRBYFLOAT", "f", "10.5"]), bulk("10.5"));
    assert_eq!(run(&s, &["INCRBYFLOAT", "f", "-0.5"]), bulk("10"));
    assert!(is_error(&run(&s, &["INCRBYFLOAT", "f", "nan"]), "ERR value is not a valid float"));
    run(&s, &["HSET", "h", "f", "1"]);
    assert!(is_error(&run(&s, &["INCR", "h"]), "WRONGTYPE"));
}

#[test]
fn lifetimes() {
    let s = KvCache::new(0);
    assert_eq!(run(&s, &["TTL", "k"]), Item::Int(-2));
    run(&s, &["SET", "k", "v"]);
    assert_eq!(run(&s, &["TTL", "k"]), Item::Int(-1));
    assert_eq!(run(&s, &["EXPIRE", "k", "100"]), Item::Int(1));
    assert_eq!(run(&s, &["TTL", "k"]), Item::Int(100));
    assert_eq!(run(&s, &["EXPIRE", "k", "50", "GT"]), Item::Int(0));
    assert_eq!(run(&s, &["EXPIRE", "k", "50", "LT"]), Item::Int(1));
    assert_eq!(run(&s, &["EXPIRE", "k", "70", "NX"]), Item::Int(0));
    assert_eq!(run(&s, &["PERSIST", "k"]), Item::Int(1));
    assert_eq!(run(&s, &["PERSIST", "k"]), Item::Int(0));
    assert_eq!(run(&s, &["EXPIRE", "k", "70", "XX"]), Item::Int(0));
    // A plain SET clears a lifetime; KEEPTTL keeps it.
    run(&s, &["SET", "k", "v", "EX", "100"]);
    run(&s, &["SET", "k", "w", "KEEPTTL"]);
    assert_eq!(run(&s, &["TTL", "k"]), Item::Int(100));
    run(&s, &["SET", "k", "x"]);
    assert_eq!(run(&s, &["TTL", "k"]), Item::Int(-1));
    // A past time deletes.
    assert_eq!(run(&s, &["EXPIRE", "k", "-1"]), Item::Int(1));
    assert_eq!(run(&s, &["EXISTS", "k"]), Item::Int(0));
    // An expired key reads as absent before anything sweeps it.
    run(&s, &["SET", "short", "v", "PX", "1"]);
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert_eq!(run(&s, &["GET", "short"]), Item::Null);
    assert_eq!(run(&s, &["EXISTS", "short"]), Item::Int(0));
    // ... and the sweep reclaims it.
    run(&s, &["SET", "short2", "v", "PX", "1"]);
    std::thread::sleep(std::time::Duration::from_millis(5));
    let before = s.stats().keys;
    for _ in 0..64 {
        s.sweep(usize::MAX);
    }
    assert!(s.stats().keys < before, "sweep left expired keys");
    assert_eq!(run(&s, &["PEXPIREAT", "absent", "1"]), Item::Int(0));
    run(&s, &["SET", "at", "v", "EXAT", "4102444800"]);
    assert_eq!(run(&s, &["EXPIRETIME", "at"]), Item::Int(4102444800));
}

#[test]
fn a_budgeted_sweep_reaches_every_shard() {
    // Enough expired keys that one pass cannot finish; repeated passes must
    // still reach the last shard rather than restarting at the first.
    let s = KvCache::new(0);
    for i in 0..20_000 {
        run(&s, &["SET", &format!("t{i}"), "v", "PX", "1"]);
    }
    std::thread::sleep(std::time::Duration::from_millis(5));
    for _ in 0..1_000 {
        s.sweep(512);
        if s.stats().keys == 0 {
            break;
        }
    }
    assert_eq!(s.stats().keys, 0, "some shard was never swept");
}

#[test]
fn keys_and_rename() {
    let s = KvCache::new(0);
    for k in ["user:1", "user:2", "order:1"] {
        run(&s, &["SET", k, "x"]);
    }
    assert_eq!(sorted(run(&s, &["KEYS", "user:*"])), vec![b"user:1".to_vec(), b"user:2".to_vec()]);
    assert_eq!(run(&s, &["DBSIZE"]), Item::Int(3));
    assert_eq!(run(&s, &["DEL", "user:1", "user:1", "nope"]), Item::Int(1));
    assert_eq!(run(&s, &["EXISTS", "user:2", "user:2", "order:1"]), Item::Int(3));
    assert_eq!(run(&s, &["TYPE", "user:2"]), Item::Simple("string".into()));
    assert_eq!(run(&s, &["TYPE", "nope"]), Item::Simple("none".into()));
    assert!(is_error(&run(&s, &["RENAME", "nope", "x"]), "ERR no such key"));
    run(&s, &["EXPIRE", "user:2", "100"]);
    assert_eq!(run(&s, &["RENAME", "user:2", "u2"]), ok());
    assert_eq!(run(&s, &["TTL", "u2"]), Item::Int(100), "a rename keeps the lifetime");
    assert_eq!(run(&s, &["RENAMENX", "u2", "order:1"]), Item::Int(0));
    assert_eq!(run(&s, &["COPY", "u2", "u3"]), Item::Int(1));
    assert_eq!(run(&s, &["COPY", "u2", "u3"]), Item::Int(0));
    assert_eq!(run(&s, &["COPY", "u2", "u3", "REPLACE"]), Item::Int(1));
    // SCAN visits every key exactly once across its pages.
    for i in 0..500 {
        run(&s, &["SET", &format!("scan:{i}"), "v"]);
    }
    let mut seen = std::collections::HashSet::new();
    let mut cursor = "0".to_string();
    loop {
        let Item::Array(parts) = run(&s, &["SCAN", &cursor, "MATCH", "scan:*", "COUNT", "50"]) else {
            panic!()
        };
        let Item::Bulk(next) = &parts[0] else { panic!() };
        let Item::Array(keys) = &parts[1] else { panic!() };
        for k in keys {
            let Item::Bulk(k) = k else { panic!() };
            assert!(seen.insert(k.clone()), "SCAN returned a key twice");
        }
        cursor = String::from_utf8(next.clone()).unwrap();
        if cursor == "0" {
            break;
        }
    }
    assert_eq!(seen.len(), 500);
    assert!(matches!(run(&s, &["RANDOMKEY"]), Item::Bulk(_)));
    assert_eq!(run(&s, &["FLUSHDB"]), ok());
    assert_eq!(run(&s, &["RANDOMKEY"]), Item::Null);
}

#[test]
fn hashes() {
    let s = KvCache::new(0);
    assert_eq!(run(&s, &["HSET", "h", "a", "1", "b", "2"]), Item::Int(2));
    assert_eq!(run(&s, &["HSET", "h", "a", "9", "c", "3"]), Item::Int(1));
    assert_eq!(run(&s, &["HGET", "h", "a"]), bulk("9"));
    assert_eq!(run(&s, &["HMGET", "h", "a", "x"]), Item::Array(vec![bulk("9"), Item::Null]));
    assert_eq!(run(&s, &["HLEN", "h"]), Item::Int(3));
    assert_eq!(run(&s, &["HSETNX", "h", "a", "0"]), Item::Int(0));
    assert_eq!(run(&s, &["HINCRBY", "h", "b", "5"]), Item::Int(7));
    assert_eq!(run(&s, &["HINCRBYFLOAT", "h", "f", "1.5"]), bulk("1.5"));
    assert_eq!(run(&s, &["HSTRLEN", "h", "a"]), Item::Int(1));
    assert_eq!(sorted(run(&s, &["HKEYS", "h"])), vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"f".to_vec()]);
    let Item::Map(pairs) = run(&s, &["HGETALL", "h"]) else { panic!() };
    assert_eq!(pairs.len(), 4);
    assert_eq!(run(&s, &["HDEL", "h", "a", "b", "c", "f", "zz"]), Item::Int(4));
    assert_eq!(run(&s, &["EXISTS", "h"]), Item::Int(0), "an emptied hash is deleted");
    assert_eq!(run(&s, &["HMSET", "h2", "x", "1"]), ok());
    run(&s, &["SET", "str", "v"]);
    assert!(is_error(&run(&s, &["HSET", "str", "a", "1"]), "WRONGTYPE"));
    // Arity is checked before a command runs (the wire layer answers it).
    assert!(!commands::lookup(b"hset").unwrap().arity_ok(3));
}

#[test]
fn lists() {
    let s = KvCache::new(0);
    assert_eq!(run(&s, &["LPUSH", "l", "a", "b"]), Item::Int(2));
    assert_eq!(run(&s, &["RPUSH", "l", "c"]), Item::Int(3));
    assert_eq!(run(&s, &["LRANGE", "l", "0", "-1"]), bulks(&["b", "a", "c"]));
    assert_eq!(run(&s, &["LINDEX", "l", "-1"]), bulk("c"));
    assert_eq!(run(&s, &["LSET", "l", "1", "A"]), ok());
    assert!(is_error(&run(&s, &["LSET", "l", "9", "x"]), "ERR index out of range"));
    assert_eq!(run(&s, &["LINSERT", "l", "BEFORE", "c", "x"]), Item::Int(4));
    assert_eq!(run(&s, &["LINSERT", "l", "AFTER", "nope", "x"]), Item::Int(-1));
    assert_eq!(run(&s, &["LPOS", "l", "x"]), Item::Int(2));
    assert_eq!(run(&s, &["LREM", "l", "0", "x"]), Item::Int(1));
    assert_eq!(run(&s, &["LTRIM", "l", "0", "1"]), ok());
    assert_eq!(run(&s, &["LRANGE", "l", "0", "-1"]), bulks(&["b", "A"]));
    assert_eq!(run(&s, &["LPOP", "l"]), bulk("b"));
    assert_eq!(run(&s, &["RPOP", "l", "5"]), bulks(&["A"]));
    assert_eq!(run(&s, &["EXISTS", "l"]), Item::Int(0), "an emptied list is deleted");
    assert_eq!(run(&s, &["LPOP", "l"]), Item::Null);
    assert_eq!(run(&s, &["LPOP", "l", "2"]), Item::NullArray);
    assert_eq!(run(&s, &["LPUSHX", "l", "a"]), Item::Int(0));
    run(&s, &["RPUSH", "src", "1", "2", "3"]);
    assert_eq!(run(&s, &["LMOVE", "src", "dst", "LEFT", "RIGHT"]), bulk("1"));
    assert_eq!(run(&s, &["RPOPLPUSH", "src", "dst"]), bulk("3"));
    assert_eq!(run(&s, &["LRANGE", "dst", "0", "-1"]), bulks(&["3", "1"]));
    // Rotation onto itself.
    assert_eq!(run(&s, &["LMOVE", "dst", "dst", "LEFT", "RIGHT"]), bulk("3"));
    assert_eq!(run(&s, &["LRANGE", "dst", "0", "-1"]), bulks(&["1", "3"]));
    // A blocking pop outside a connection answers at once.
    assert_eq!(run(&s, &["BLPOP", "empty", "dst", "0"]), bulks(&["dst", "1"]));
    assert_eq!(run(&s, &["BLPOP", "empty", "0"]), Item::NullArray);
    run(&s, &["SET", "str", "v"]);
    assert!(is_error(&run(&s, &["LMOVE", "dst", "str", "LEFT", "LEFT"]), "WRONGTYPE"));
    assert_eq!(run(&s, &["LLEN", "dst"]), Item::Int(1), "a refused move moves nothing");
}

#[test]
fn sets() {
    let s = KvCache::new(0);
    assert_eq!(run(&s, &["SADD", "a", "1", "2", "3", "3"]), Item::Int(3));
    run(&s, &["SADD", "b", "2", "3", "4"]);
    assert_eq!(run(&s, &["SISMEMBER", "a", "2"]), Item::Int(1));
    assert_eq!(
        run(&s, &["SMISMEMBER", "a", "1", "9"]),
        Item::Array(vec![Item::Int(1), Item::Int(0)])
    );
    assert_eq!(sorted(run(&s, &["SINTER", "a", "b"])), vec![b"2".to_vec(), b"3".to_vec()]);
    assert_eq!(sorted(run(&s, &["SUNION", "a", "b"])).len(), 4);
    assert_eq!(sorted(run(&s, &["SDIFF", "a", "b"])), vec![b"1".to_vec()]);
    assert_eq!(run(&s, &["SINTERSTORE", "c", "a", "b"]), Item::Int(2));
    assert_eq!(run(&s, &["SCARD", "c"]), Item::Int(2));
    assert_eq!(run(&s, &["SINTERCARD", "2", "a", "b", "LIMIT", "1"]), Item::Int(1));
    assert_eq!(run(&s, &["SMOVE", "a", "b", "1"]), Item::Int(1));
    assert_eq!(run(&s, &["SMOVE", "a", "b", "1"]), Item::Int(0));
    let Item::Bulk(m) = run(&s, &["SPOP", "c"]) else { panic!() };
    assert!(m == b"2" || m == b"3");
    assert_eq!(run(&s, &["SCARD", "c"]), Item::Int(1));
    assert_eq!(sorted(run(&s, &["SRANDMEMBER", "b", "10"])).len(), 4);
    let Item::Array(rep) = run(&s, &["SRANDMEMBER", "b", "-10"]) else { panic!() };
    assert_eq!(rep.len(), 10, "a negative count may repeat");
    assert_eq!(run(&s, &["SREM", "c", "2", "3"]), Item::Int(1));
    assert_eq!(run(&s, &["EXISTS", "c"]), Item::Int(0));
    // SINTERSTORE with an empty result deletes the destination.
    run(&s, &["SADD", "dest", "x"]);
    assert_eq!(run(&s, &["SINTERSTORE", "dest", "a", "nothing"]), Item::Int(0));
    assert_eq!(run(&s, &["EXISTS", "dest"]), Item::Int(0));
}

#[test]
fn sorted_sets() {
    let s = KvCache::new(0);
    assert_eq!(run(&s, &["ZADD", "z", "1", "a", "2", "b", "3", "c"]), Item::Int(3));
    assert_eq!(run(&s, &["ZADD", "z", "XX", "CH", "5", "a", "9", "nope"]), Item::Int(1));
    assert_eq!(run(&s, &["ZADD", "z", "GT", "1", "a"]), Item::Int(0));
    assert_eq!(run(&s, &["ZSCORE", "z", "a"]), Item::Double(5.0));
    assert_eq!(run(&s, &["ZADD", "z", "INCR", "1", "a"]), Item::Double(6.0));
    assert_eq!(run(&s, &["ZINCRBY", "z", "-4", "a"]), Item::Double(2.0));
    assert_eq!(run(&s, &["ZRANGE", "z", "0", "-1"]), bulks(&["a", "b", "c"]), "ties order by member");
    assert_eq!(run(&s, &["ZREVRANGE", "z", "0", "0"]), bulks(&["c"]));
    assert_eq!(run(&s, &["ZRANK", "z", "c"]), Item::Int(2));
    assert_eq!(run(&s, &["ZREVRANK", "z", "c"]), Item::Int(0));
    assert_eq!(
        run(&s, &["ZRANK", "z", "b", "WITHSCORE"]),
        Item::Array(vec![Item::Int(1), Item::Double(2.0)])
    );
    assert_eq!(run(&s, &["ZCOUNT", "z", "(2", "+inf"]), Item::Int(1));
    assert_eq!(run(&s, &["ZRANGEBYSCORE", "z", "2", "3"]), bulks(&["a", "b", "c"]));
    assert_eq!(run(&s, &["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "1"]), bulks(&["b"]));
    assert_eq!(run(&s, &["ZREVRANGEBYSCORE", "z", "+inf", "(2"]), bulks(&["c"]));
    assert_eq!(run(&s, &["ZRANGE", "z", "(2", "3", "BYSCORE"]), bulks(&["c"]));
    assert_eq!(run(&s, &["ZRANGE", "z", "3", "2", "BYSCORE", "REV", "LIMIT", "0", "2"]), bulks(&["c", "b"]));
    assert_eq!(
        run(&s, &["ZRANGE", "z", "0", "0", "WITHSCORES"]),
        Item::Array(vec![bulk("a"), Item::Double(2.0)])
    );
    assert!(is_error(&run(&s, &["ZRANGE", "z", "0", "1", "LIMIT", "0", "1"]), "ERR syntax error, LIMIT"));
    // Lex ranges on equal scores.
    run(&s, &["ZADD", "lex", "0", "apple", "0", "banana", "0", "cherry"]);
    assert_eq!(run(&s, &["ZRANGEBYLEX", "lex", "[b", "+"]), bulks(&["banana", "cherry"]));
    assert_eq!(run(&s, &["ZLEXCOUNT", "lex", "-", "(c"]), Item::Int(2));
    assert_eq!(run(&s, &["ZREVRANGEBYLEX", "lex", "+", "[b"]), bulks(&["cherry", "banana"]));
    assert_eq!(run(&s, &["ZPOPMIN", "z"]), Item::Array(vec![bulk("a"), Item::Double(2.0)]));
    assert_eq!(run(&s, &["ZPOPMAX", "z", "1"]), Item::Array(vec![bulk("c"), Item::Double(3.0)]));
    assert_eq!(run(&s, &["ZCARD", "z"]), Item::Int(1));
    // Unions and intersections, with weights and aggregation.
    run(&s, &["ZADD", "x", "1", "a", "2", "b"]);
    run(&s, &["ZADD", "y", "10", "b", "20", "c"]);
    assert_eq!(run(&s, &["ZUNIONSTORE", "u", "2", "x", "y", "WEIGHTS", "1", "2"]), Item::Int(3));
    assert_eq!(run(&s, &["ZSCORE", "u", "b"]), Item::Double(22.0));
    assert_eq!(run(&s, &["ZINTERSTORE", "i", "2", "x", "y", "AGGREGATE", "MAX"]), Item::Int(1));
    assert_eq!(run(&s, &["ZSCORE", "i", "b"]), Item::Double(10.0));
    assert_eq!(run(&s, &["ZDIFF", "2", "x", "y"]), bulks(&["a"]));
    // A plain set takes part with score 1.
    run(&s, &["SADD", "plain", "a"]);
    assert_eq!(run(&s, &["ZUNIONSTORE", "u2", "2", "x", "plain"]), Item::Int(2));
    assert_eq!(run(&s, &["ZSCORE", "u2", "a"]), Item::Double(2.0));
    assert_eq!(run(&s, &["ZREMRANGEBYSCORE", "u", "-inf", "(20"]), Item::Int(1));
    assert_eq!(run(&s, &["ZREMRANGEBYRANK", "u", "0", "0"]), Item::Int(1));
    assert!(is_error(&run(&s, &["ZADD", "z", "nan", "m"]), "ERR value is not a valid float"));
    assert!(is_error(&run(&s, &["ZADD", "z", "NX", "XX", "1", "m"]), "ERR XX and NX"));
}

#[test]
fn every_command_runs_inside_a_transaction() {
    // Each needs only what its key spec locks; a wrong spec panics.
    let s = KvCache::new(0);
    let script: &[&[&str]] = &[
        &["SET", "a", "1"],
        &["MSET", "b", "2", "c", "3"],
        &["MGET", "a", "b", "c"],
        &["INCR", "n"],
        &["RENAME", "a", "a2"],
        &["COPY", "a2", "a3"],
        &["DEL", "a2", "a3", "zz"],
        &["EXISTS", "b", "c"],
        &["RPUSH", "l1", "x", "y"],
        &["LMOVE", "l1", "l2", "LEFT", "RIGHT"],
        &["RPOPLPUSH", "l1", "l2"],
        &["BLPOP", "l2", "l3", "0"],
        &["BLMOVE", "l2", "l4", "LEFT", "LEFT", "0"],
        &["SADD", "s1", "a", "b"],
        &["SADD", "s2", "b"],
        &["SINTERSTORE", "s3", "s1", "s2"],
        &["SUNION", "s1", "s2"],
        &["SMOVE", "s1", "s2", "a"],
        &["SINTERCARD", "2", "s1", "s2"],
        &["ZADD", "z1", "1", "a"],
        &["ZUNIONSTORE", "z2", "2", "z1", "s1"],
        &["ZINTER", "2", "z1", "z2"],
        &["HSET", "h", "f", "v"],
        &["KEYS", "*"],
        &["SCAN", "0"],
        &["DBSIZE"],
        &["RANDOMKEY"],
        &["OBJECT", "ENCODING", "h"],
        &["MEMORY", "USAGE", "h"],
        &["FLUSHDB"],
    ];
    for cmd in script {
        let item = run_pinned(&s, cmd);
        assert!(!matches!(item, Item::Error(_)), "{cmd:?} -> {item:?}");
    }
}

#[test]
fn eviction_keeps_the_store_under_its_bound() {
    // The settings are process-wide; this test owns them for its duration.
    let _guard = SETTINGS.lock();
    let s = KvCache::new(0);
    super::set_max_memory(4 * 1024 * 1024);
    super::set_eviction_policy(EvictionPolicy::AllKeysLru);
    let value = "x".repeat(512);
    for i in 0..50_000 {
        let item = run(&s, &["SET", &format!("k{i}"), &value]);
        assert_eq!(item, ok());
    }
    let stats = s.stats();
    assert!(stats.used <= 4 * 1024 * 1024 + 64 * 1024, "used {} over the bound", stats.used);
    assert!(stats.evicted > 0);
    // Under noeviction a full store refuses instead.
    super::set_eviction_policy(EvictionPolicy::NoEviction);
    let mut refused = 0;
    for i in 0..50_000 {
        if is_error(&run(&s, &["SET", &format!("again{i}"), &value]), "OOM") {
            refused += 1;
        }
    }
    assert!(refused > 0);
    super::set_max_memory(0);
}

static SETTINGS: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[test]
fn memory_accounting_returns_to_zero() {
    let s = KvCache::new(0);
    for i in 0..1000 {
        let k = format!("k{i}");
        run(&s, &["SET", &k, "value"]);
        run(&s, &["HSET", &format!("h{i}"), "a", "1", "b", "22"]);
        run(&s, &["RPUSH", &format!("l{i}"), "x", "y"]);
        run(&s, &["SADD", &format!("s{i}"), "x"]);
        run(&s, &["ZADD", &format!("z{i}"), "1", "x"]);
        run(&s, &["APPEND", &k, "more"]);
        run(&s, &["HDEL", &format!("h{i}"), "a"]);
        run(&s, &["LPOP", &format!("l{i}")]);
        run(&s, &["INCR", &format!("n{i}")]);
    }
    assert!(s.stats().used > 0);
    run(&s, &["FLUSHDB"]);
    assert_eq!(s.stats().used, 0);
    for i in 0..100 {
        run(&s, &["SET", &format!("k{i}"), "v"]);
        run(&s, &["ZADD", "z", &i.to_string(), &format!("m{i}")]);
    }
    for i in 0..100 {
        run(&s, &["DEL", &format!("k{i}")]);
        run(&s, &["ZREM", "z", &format!("m{i}")]);
    }
    assert_eq!(s.stats().used, 0, "deletes return every byte they charged");
}

#[test]
fn concurrent_increments_and_moves_are_atomic() {
    let s = Arc::new(KvCache::new(0));
    let threads = 8;
    let per = 20_000;
    run(&s, &["RPUSH", "ring", "0", "1", "2", "3", "4", "5", "6", "7"]);
    std::thread::scope(|scope| {
        for t in 0..threads {
            let s = s.clone();
            scope.spawn(move || {
                for i in 0..per {
                    run(&s, &["INCR", "counter"]);
                    run(&s, &["HINCRBY", "h", "f", "1"]);
                    // Atomic multi-key writes, read back atomically.
                    let v = (t * per + i).to_string();
                    run(&s, &["MSET", "pa", &v, "pb", &v]);
                    let Item::Array(both) = run(&s, &["MGET", "pa", "pb"]) else { panic!() };
                    assert_eq!(both[0], both[1], "MGET saw half an MSET");
                    // Elements move between lists but are never lost.
                    run(&s, &["LMOVE", "ring", "ring2", "LEFT", "RIGHT"]);
                    run(&s, &["LMOVE", "ring2", "ring", "LEFT", "RIGHT"]);
                }
            });
        }
    });
    assert_eq!(run(&s, &["GET", "counter"]), bulk(&(threads * per).to_string()));
    assert_eq!(run(&s, &["HGET", "h", "f"]), bulk(&(threads * per).to_string()));
    let Item::Int(a) = run(&s, &["LLEN", "ring"]) else { panic!() };
    let Item::Int(b) = run(&s, &["LLEN", "ring2"]) else { panic!() };
    assert_eq!(a + b, 8);
}

#[test]
fn snapshot_round_trips_every_type() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = KvCacheHandle::new(Some(dir.path().to_str().unwrap()));
    let s = handle.db(0).clone();
    run(&s, &["SET", "s", "v\r\n\0bin"]);
    run(&s, &["INCR", "n"]);
    run(&s, &["SET", "ttl", "v", "EX", "1000"]);
    run(&s, &["SET", "gone", "v", "PX", "1"]);
    run(&s, &["HSET", "h", "a", "1", "b", "2"]);
    run(&s, &["RPUSH", "l", "x", "y", "z"]);
    run(&s, &["SADD", "set", "p", "q"]);
    run(&s, &["ZADD", "z", "1.5", "a", "-2", "b"]);
    run(&handle.db(3).clone(), &["SET", "in3", "x"]);
    assert!(handle.is_dirty());
    let saved = handle.save().expect("save");
    assert!(saved >= 8);
    assert!(!handle.is_dirty());
    std::thread::sleep(std::time::Duration::from_millis(5));

    let reopened = KvCacheHandle::new(Some(dir.path().to_str().unwrap()));
    reopened.load_snapshot_if_present();
    let r = reopened.db(0).clone();
    assert_eq!(run(&r, &["GET", "s"]), Item::Bulk(b"v\r\n\0bin".to_vec()));
    assert_eq!(run(&r, &["INCR", "n"]), Item::Int(2));
    assert!(matches!(run(&r, &["TTL", "ttl"]), Item::Int(t) if t > 990));
    assert_eq!(run(&r, &["EXISTS", "gone"]), Item::Int(0));
    assert_eq!(run(&r, &["HGET", "h", "b"]), bulk("2"));
    assert_eq!(run(&r, &["LRANGE", "l", "0", "-1"]), bulks(&["x", "y", "z"]));
    assert_eq!(run(&r, &["SCARD", "set"]), Item::Int(2));
    assert_eq!(run(&r, &["ZRANGE", "z", "0", "-1"]), bulks(&["b", "a"]));
    assert_eq!(run(&reopened.db(3).clone(), &["GET", "in3"]), bulk("x"));

    // A torn file is refused whole, not loaded in part.
    let path = dir.path().join(super::SNAPSHOT_FILE);
    let mut bytes = std::fs::read(&path).unwrap();
    let n = bytes.len();
    bytes[n / 2] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();
    let torn = KvCacheHandle::new(Some(dir.path().to_str().unwrap()));
    assert!(super::snapshot::load(&torn, &path).is_err());
    assert!(torn.existing(0).is_none(), "nothing was loaded from a torn file");
}

#[test]
fn sql_rows_have_the_described_shape() {
    use super::sql::{Cell, rows_for_test};
    let rows = rows_for_test(
        Item::Map(vec![(bulk("f"), bulk("v"))]),
        &["HGETALL", "h"],
    )
    .unwrap();
    assert_eq!(rows, vec![vec![Cell::Text("f".into()), Cell::Text("v".into())]]);
    let rows = rows_for_test(Item::Null, &["GET", "k"]).unwrap();
    assert!(rows.is_empty(), "a missing key is no row");
    let rows = rows_for_test(Item::Array(vec![bulk("a"), Item::Double(1.5)]), &["ZPOPMIN", "z"]).unwrap();
    assert_eq!(rows, vec![vec![Cell::Text("a".into()), Cell::Float(1.5)]]);
    assert!(rows_for_test(Item::Bulk(vec![0xff, 0xfe]), &["GET", "k"]).is_err(), "not UTF-8 is refused");
    let rows = rows_for_test(
        Item::Array(vec![bulk("0"), Item::Array(vec![])]),
        &["SCAN", "0"],
    )
    .unwrap();
    assert_eq!(rows, vec![vec![Cell::Text("0".into()), Cell::Null]]);
}

// ---------------------------------------------------------------------------
// Over TCP
// ---------------------------------------------------------------------------

/// A minimal RESP client: sends commands, reads whole replies.
struct Client {
    sock: tokio::net::TcpStream,
    buf: Vec<u8>,
}

impl Client {
    async fn connect(port: u16) -> Client {
        Client {
            sock: tokio::net::TcpStream::connect(("127.0.0.1", port)).await.expect("connect"),
            buf: Vec::new(),
        }
    }

    fn encode(args: &[&str]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    async fn send(&mut self, args: &[&str]) {
        use tokio::io::AsyncWriteExt;
        self.sock.write_all(&Self::encode(args)).await.expect("write");
    }

    async fn send_raw(&mut self, bytes: &[u8]) {
        use tokio::io::AsyncWriteExt;
        self.sock.write_all(bytes).await.expect("write");
    }

    /// One whole reply, rendered as text for easy comparison.
    async fn read(&mut self) -> String {
        use tokio::io::AsyncReadExt;
        loop {
            if let Some((text, used)) = parse_reply(&self.buf) {
                self.buf.drain(..used);
                return text;
            }
            let mut chunk = [0u8; 4096];
            let n = tokio::time::timeout(std::time::Duration::from_secs(10), self.sock.read(&mut chunk))
                .await
                .expect("reply within 10s")
                .expect("read");
            if n == 0 {
                return "<closed>".into();
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    async fn call(&mut self, args: &[&str]) -> String {
        self.send(args).await;
        self.read().await
    }
}

fn parse_reply(buf: &[u8]) -> Option<(String, usize)> {
    let eol = buf.windows(2).position(|w| w == b"\r\n")?;
    let line = std::str::from_utf8(&buf[1..eol]).ok()?;
    let after = eol + 2;
    match buf.first()? {
        b'+' | b'-' | b':' | b',' | b'#' => Some((format!("{}{}", buf[0] as char, line), after)),
        b'_' => Some(("nil".into(), after)),
        b'$' => {
            let n: i64 = line.parse().ok()?;
            if n < 0 {
                return Some(("nil".into(), after));
            }
            let end = after + n as usize;
            if buf.len() < end + 2 {
                return None;
            }
            Some((String::from_utf8_lossy(&buf[after..end]).into(), end + 2))
        }
        b'*' | b'%' | b'~' | b'>' => {
            let n: i64 = line.parse().ok()?;
            if n < 0 {
                return Some(("nil".into(), after));
            }
            let n = if buf[0] == b'%' { n * 2 } else { n };
            let mut at = after;
            let mut parts = Vec::new();
            for _ in 0..n {
                let (text, used) = parse_reply(&buf[at..])?;
                parts.push(text);
                at += used;
            }
            Some((format!("[{}]", parts.join(",")), at))
        }
        _ => None,
    }
}

async fn start_server() -> (u16, KvCacheHandle) {
    let database = rclite::Arc::new(crate::core::database::Database::new_with_backend("kvtest", true).await);
    let handle = database.kv_cache.clone();
    let config = super::resp::RespConfig {
        addr: "127.0.0.1".into(),
        port: 0,
        auth: super::resp::RespAuth::Trust,
        threads: Some(2),
    };
    let listener = super::resp::bind(&config).await.expect("bind");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(super::resp::serve(listener, database, handle.clone(), config));
    (port, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_pipelining_transactions_pubsub_and_blocking() {
    let (port, _handle) = start_server().await;
    let mut c = Client::connect(port).await;
    assert_eq!(c.call(&["PING"]).await, "+PONG");

    // A pipeline: three commands in one write, three replies in order.
    let mut batch = Client::encode(&["SET", "a", "1"]);
    batch.extend(Client::encode(&["INCR", "a"]));
    batch.extend(Client::encode(&["GET", "a"]));
    c.send_raw(&batch).await;
    assert_eq!(c.read().await, "+OK");
    assert_eq!(c.read().await, ":2");
    assert_eq!(c.read().await, "2");

    // A command split across writes is reassembled.
    let whole = Client::encode(&["SET", "split", "value"]);
    c.send_raw(&whole[..7]).await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    c.send_raw(&whole[7..]).await;
    assert_eq!(c.read().await, "+OK");

    // Inline commands, as telnet sends them.
    c.send_raw(b"GET split\r\n").await;
    assert_eq!(c.read().await, "value");

    // MULTI/EXEC, and an error inside one.
    assert_eq!(c.call(&["MULTI"]).await, "+OK");
    assert_eq!(c.call(&["INCR", "a"]).await, "+QUEUED");
    assert_eq!(c.call(&["LPUSH", "a", "x"]).await, "+QUEUED");
    assert_eq!(c.call(&["INCR", "a"]).await, "+QUEUED");
    let exec = c.call(&["EXEC"]).await;
    assert!(exec.starts_with("[:3,-WRONGTYPE"), "{exec}");
    assert!(exec.ends_with(",:4]"), "{exec}");
    // A queued unknown command aborts the whole transaction.
    c.call(&["MULTI"]).await;
    assert!(c.call(&["NOPE"]).await.starts_with("-ERR unknown command"));
    assert!(c.call(&["EXEC"]).await.starts_with("-EXECABORT"));

    // WATCH: a write by another client between WATCH and EXEC aborts it.
    let mut other = Client::connect(port).await;
    assert_eq!(c.call(&["WATCH", "w"]).await, "+OK");
    assert_eq!(other.call(&["SET", "w", "changed"]).await, "+OK");
    c.call(&["MULTI"]).await;
    c.call(&["SET", "w", "mine"]).await;
    assert_eq!(c.call(&["EXEC"]).await, "nil");
    assert_eq!(c.call(&["GET", "w"]).await, "changed");
    // ... and without a conflicting write it commits.
    c.call(&["WATCH", "w"]).await;
    c.call(&["MULTI"]).await;
    c.call(&["SET", "w", "mine"]).await;
    assert_eq!(c.call(&["EXEC"]).await, "[+OK]");

    // Pub/sub, including a pattern.
    let mut sub = Client::connect(port).await;
    assert_eq!(sub.call(&["SUBSCRIBE", "news"]).await, "[subscribe,news,:1]");
    assert_eq!(sub.call(&["PSUBSCRIBE", "n*"]).await, "[psubscribe,n*,:2]");
    assert!(sub.call(&["GET", "a"]).await.starts_with("-ERR Can't execute 'get'"));
    assert_eq!(c.call(&["PUBLISH", "news", "hello"]).await, ":2");
    let mut got = vec![sub.read().await, sub.read().await];
    got.sort();
    assert_eq!(got, vec!["[message,news,hello]", "[pmessage,n*,news,hello]"]);
    assert_eq!(sub.call(&["UNSUBSCRIBE"]).await, "[unsubscribe,news,:1]");
    assert_eq!(sub.call(&["PUNSUBSCRIBE"]).await, "[punsubscribe,n*,:0]");
    assert_eq!(sub.call(&["PING"]).await, "+PONG");

    // A blocking pop waits for a push from another client.
    let mut waiter = Client::connect(port).await;
    waiter.send(&["BLPOP", "jobs", "5"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(c.call(&["RPUSH", "jobs", "job-1"]).await, ":1");
    assert_eq!(waiter.read().await, "[jobs,job-1]");
    // ... and times out with nil.
    assert_eq!(waiter.call(&["BLPOP", "nothing", "0.05"]).await, "nil");
    // Two waiters, two pushes: each gets one, none is lost.
    let mut w1 = Client::connect(port).await;
    let mut w2 = Client::connect(port).await;
    w1.send(&["BRPOP", "q", "5"]).await;
    w2.send(&["BRPOP", "q", "5"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    c.call(&["LPUSH", "q", "x", "y"]).await;
    let mut both = vec![w1.read().await, w2.read().await];
    both.sort();
    assert_eq!(both, vec!["[q,x]", "[q,y]"]);

    // RESP3 via HELLO.
    let hello = c.call(&["HELLO", "3"]).await;
    assert!(hello.contains("rasterizeddb"), "{hello}");
    assert_eq!(c.call(&["ZADD", "z", "1.5", "m"]).await, ":1");
    assert_eq!(c.call(&["ZSCORE", "z", "m"]).await, ",1.5");
    assert_eq!(c.call(&["GET", "missing"]).await, "nil");

    // CLIENT REPLY SKIP drops the next reply only.
    c.send(&["CLIENT", "REPLY", "SKIP"]).await;
    c.send(&["SET", "silent", "1"]).await;
    assert_eq!(c.call(&["GET", "silent"]).await, "1");

    // SELECT keeps databases apart.
    assert_eq!(c.call(&["SELECT", "2"]).await, "+OK");
    assert_eq!(c.call(&["GET", "a"]).await, "nil");
    assert_eq!(c.call(&["SELECT", "16"]).await, "-ERR DB index is out of range");

    // A protocol error closes the connection after saying why.
    let mut bad = Client::connect(port).await;
    bad.send_raw(b"*1\r\n$-7\r\n").await;
    assert!(bad.read().await.starts_with("-ERR Protocol error"));
    assert_eq!(bad.read().await, "<closed>");

    assert_eq!(c.call(&["QUIT"]).await, "+OK");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_large_values_and_many_clients() {
    let (port, _handle) = start_server().await;
    // A value larger than any one read.
    let mut c = Client::connect(port).await;
    let big = "v".repeat(3 * 1024 * 1024 + 17);
    assert_eq!(c.call(&["SET", "big", &big]).await, "+OK");
    assert_eq!(c.call(&["STRLEN", "big"]).await, format!(":{}", big.len()));
    assert_eq!(c.call(&["GET", "big"]).await.len(), big.len());
    // Many clients incrementing one key concurrently lose nothing.
    let mut tasks = Vec::new();
    for _ in 0..16 {
        tasks.push(tokio::spawn(async move {
            let mut c = Client::connect(port).await;
            let mut batch = Vec::new();
            for _ in 0..500 {
                batch.extend(Client::encode(&["INCR", "shared"]));
            }
            c.send_raw(&batch).await;
            for _ in 0..500 {
                assert!(c.read().await.starts_with(':'));
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(c.call(&["GET", "shared"]).await, "8000");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trust_is_refused_on_a_public_address() {
    let config = super::resp::RespConfig {
        addr: "0.0.0.0".into(),
        port: 0,
        auth: super::resp::RespAuth::Trust,
        threads: None,
    };
    assert!(super::resp::bind(&config).await.is_err());
}
