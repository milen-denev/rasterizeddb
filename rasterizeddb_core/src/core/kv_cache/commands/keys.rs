//! Commands about keys rather than values: delete, lifetimes, rename, the
//! keyspace walks.

use std::sync::atomic::Ordering;

use super::{SYNTAX, eq, int_arg, parse_int, read_key};
use crate::core::kv_cache::KvError;
use crate::core::kv_cache::glob;
use crate::core::kv_cache::reply::Reply;
use crate::core::kv_cache::store::{Access, SHARDS, Value};

pub fn del<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let keys = &args[1..];
    let now = a.now();
    if keys.len() == 1 {
        let key = keys[0];
        let h = a.hash(key);
        let n = a.write(h, |s| {
            let live = s.live_mut(h, key, now).is_some();
            if live {
                s.remove(h, key);
            }
            i64::from(live)
        });
        return out.int(n);
    }
    let hashes: Vec<u64> = keys.iter().map(|k| a.hash(k)).collect();
    let n = a.write_many(&hashes, |shards| {
        let mut n = 0;
        for (key, &h) in keys.iter().zip(&hashes) {
            let s = shards.get(h);
            if s.live_mut(h, key, now).is_some() {
                s.remove(h, key);
                n += 1;
            }
        }
        n
    });
    out.int(n);
}

pub fn exists<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let keys = &args[1..];
    let now = a.now();
    let hashes: Vec<u64> = keys.iter().map(|k| a.hash(k)).collect();
    let n = a.read_many(&hashes, |shards| {
        keys.iter()
            .zip(&hashes)
            .filter(|(k, h)| shards.get(**h).live(**h, k, now).is_some())
            .count()
    });
    out.int(n as i64);
}

pub fn touch<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let keys = &args[1..];
    let now = a.now();
    let hashes: Vec<u64> = keys.iter().map(|k| a.hash(k)).collect();
    let n = a.read_many(&hashes, |shards| {
        keys.iter()
            .zip(&hashes)
            .filter(|(k, h)| match shards.get(**h).live(**h, k, now) {
                Some(slot) => {
                    slot.touch();
                    true
                }
                None => false,
            })
            .count()
    });
    out.int(n as i64);
}

/// `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` with `NX|XX|GT|LT`.
pub fn expire<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, unit_ms: u64, absolute: bool) {
    let key = args[1];
    let now = a.now();
    let name = match (unit_ms, absolute) {
        (1000, false) => "expire",
        (1, false) => "pexpire",
        (1000, true) => "expireat",
        _ => "pexpireat",
    };
    let t = match int_arg(args[2]) {
        Ok(t) => t,
        Err(e) => return out.err(&e),
    };
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for o in &args[3..] {
        if eq(o, b"NX") {
            nx = true;
        } else if eq(o, b"XX") {
            xx = true;
        } else if eq(o, b"GT") {
            gt = true;
        } else if eq(o, b"LT") {
            lt = true;
        } else {
            return out.error(&format!("ERR Unsupported option {}", String::from_utf8_lossy(o)));
        }
    }
    if nx && (xx || gt || lt) {
        return out.error("ERR NX and XX, GT or LT options at the same time are not compatible");
    }
    if gt && lt {
        return out.error("ERR GT and LT options at the same time are not compatible");
    }
    let invalid = || KvError::Other(format!("invalid expire time in '{name}' command"));
    let at: i128 = if absolute {
        t as i128 * unit_ms as i128
    } else {
        now as i128 + t as i128 * unit_ms as i128
    };
    if at > i64::MAX as i128 || (!absolute && (t as i128 * unit_ms as i128).abs() > i64::MAX as i128) {
        return out.err(&invalid());
    }
    let h = a.hash(key);
    let n = a.write(h, |s| {
        let Some(slot) = s.live_mut(h, key, now) else {
            return 0;
        };
        let current = slot.expires_at;
        let allowed = if nx {
            current == 0
        } else if xx && current == 0 {
            false
        } else if gt {
            // No lifetime is an infinite one, which nothing is greater than.
            current != 0 && at > current as i128
        } else if lt {
            current == 0 || at < current as i128
        } else {
            true
        };
        if !allowed {
            return 0;
        }
        if at <= now as i128 {
            s.remove(h, key);
        } else {
            s.update(h, key, now, |slot| slot.expires_at = at as u64);
        }
        1
    });
    out.int(n);
}

pub fn ttl<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, millis: bool, absolute: bool) {
    let now = a.now();
    read_key(a, args[1], out, |slot, out| match slot {
        None => out.int(-2),
        Some(slot) if slot.expires_at == 0 => out.int(-1),
        Some(slot) => {
            let at = slot.expires_at;
            out.int(match (millis, absolute) {
                (true, true) => at as i64,
                (false, true) => (at / 1000) as i64,
                (true, false) => at.saturating_sub(now) as i64,
                (false, false) => (at.saturating_sub(now).saturating_add(500) / 1000) as i64,
            })
        }
    });
}

pub fn persist<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let now = a.now();
    let h = a.hash(key);
    let n = a.write(h, |s| {
        match s.live_mut(h, key, now) {
            Some(slot) if slot.expires_at != 0 => {}
            _ => return 0,
        }
        s.update(h, key, now, |slot| slot.expires_at = 0);
        1
    });
    out.int(n);
}

pub fn type_of<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    read_key(a, args[1], out, |slot, out| {
        out.simple(slot.map_or("none", |s| s.value.type_name()))
    });
}

pub fn rename<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, nx: bool) {
    let (from, to) = (args[1], args[2]);
    let now = a.now();
    let (hf, ht) = (a.hash(from), a.hash(to));
    let mut moved_list = false;
    a.write_many(&[hf, ht], |shards| {
        if shards.get(hf).live_mut(hf, from, now).is_none() {
            return out.error("ERR no such key");
        }
        if from == to {
            return if nx { out.int(0) } else { out.ok() };
        }
        if nx && shards.get(ht).live_mut(ht, to, now).is_some() {
            return out.int(0);
        }
        let slot = shards.get(hf).remove(hf, from).expect("live");
        moved_list = matches!(slot.value, Value::List(_));
        let renamed = slot.rekey(to);
        shards.get(ht).put(ht, renamed);
        if nx { out.int(1) } else { out.ok() }
    });
    if moved_list {
        a.store().waiters.wake(to, 1);
    }
}

pub fn copy<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (from, to) = (args[1], args[2]);
    let mut replace = false;
    let mut i = 3;
    while i < args.len() {
        if eq(args[i], b"REPLACE") {
            replace = true;
        } else if eq(args[i], b"DB") && i + 1 < args.len() {
            match parse_int(args[i + 1]) {
                Some(db) if db as usize == a.store().db => {}
                Some(_) => return out.error("ERR COPY to another database is not supported; use MOVE"),
                None => return out.err(&KvError::NotInteger),
            }
            i += 1;
        } else {
            return out.error(SYNTAX);
        }
        i += 1;
    }
    let now = a.now();
    let (hf, ht) = (a.hash(from), a.hash(to));
    let n = a.write_many(&[hf, ht], |shards| {
        let copied = match shards.get(hf).live(hf, from, now) {
            Some(slot) => slot.copy_as(to),
            None => return 0,
        };
        if from == to {
            return 0;
        }
        let dest = shards.get(ht);
        if dest.live_mut(ht, to, now).is_some() && !replace {
            return 0;
        }
        if let Err(_e) = super::room(dest, copied.cost()) {
            return -1;
        }
        dest.put(ht, copied);
        1
    });
    if n < 0 {
        out.err(&KvError::OutOfMemory)
    } else {
        out.int(n)
    }
}

pub fn keys<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let pattern = args[1];
    let all = pattern == b"*";
    let now = a.now();
    let mut found: Vec<Vec<u8>> = Vec::new();
    a.each_shard(|_, s| {
        for slot in s.iter() {
            if !slot.is_expired(now) && (all || glob::matches(pattern, slot.key())) {
                found.push(slot.key().to_vec());
            }
        }
        true
    });
    out.array(found.len());
    for k in &found {
        out.bulk(k);
    }
}

/// `SCAN cursor [MATCH p] [COUNT n] [TYPE t]`.
///
/// The cursor is a shard index, and a call returns whole shards until it has
/// at least `COUNT` keys. A shard is walked under one read lock, so the walk
/// can never miss a key that existed throughout, whatever the table does
/// between calls: growth or a rehash that moves keys between buckets cannot
/// move one out of a shard. The cost is that a call returns more keys than
/// `COUNT` when shards are large, which Redis's contract allows.
pub fn scan<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let Some(cursor) = std::str::from_utf8(args[1])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return out.error("ERR invalid cursor");
    };
    let (mut pattern, mut count, mut type_filter): (Option<&[u8]>, usize, Option<&[u8]>) = (None, 10, None);
    let mut i = 2;
    while i < args.len() {
        if i + 1 >= args.len() {
            return out.error(SYNTAX);
        }
        if eq(args[i], b"MATCH") {
            pattern = Some(args[i + 1]).filter(|p| *p != b"*");
        } else if eq(args[i], b"COUNT") {
            match parse_int(args[i + 1]) {
                Some(n) if n >= 1 => count = n as usize,
                Some(_) => return out.error(SYNTAX),
                None => return out.err(&KvError::NotInteger),
            }
        } else if eq(args[i], b"TYPE") {
            type_filter = Some(args[i + 1]);
        } else {
            return out.error(SYNTAX);
        }
        i += 2;
    }
    let now = a.now();
    let mut keys: Vec<Vec<u8>> = Vec::new();
    let mut next = 0u64;
    if (cursor as usize) < SHARDS {
        let start = cursor as usize;
        a.each_shard(|index, s| {
            if index < start {
                return true;
            }
            for slot in s.iter() {
                if slot.is_expired(now) {
                    continue;
                }
                if pattern.is_some_and(|p| !glob::matches(p, slot.key())) {
                    continue;
                }
                if type_filter.is_some_and(|t| !eq(t, slot.value.type_name().as_bytes())) {
                    continue;
                }
                keys.push(slot.key().to_vec());
            }
            if keys.len() >= count && index + 1 < SHARDS {
                next = (index + 1) as u64;
                return false;
            }
            true
        });
    }
    out.array(2);
    out.bulk(next.to_string().as_bytes());
    out.array(keys.len());
    for k in &keys {
        out.bulk(k);
    }
}

pub fn dbsize<A: Access, O: Reply>(a: &mut A, out: &mut O) {
    let mut n = 0usize;
    a.each_shard(|_, s| {
        n += s.len();
        true
    });
    out.int(n as i64);
}

pub fn flushdb<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    match args.get(1) {
        None => {}
        Some(m) if eq(m, b"ASYNC") || eq(m, b"SYNC") => {}
        Some(_) => return out.error(SYNTAX),
    }
    a.each_shard_mut(|_, s| {
        s.clear();
    });
    a.store().flush_epoch.fetch_add(1, Ordering::Relaxed);
    out.ok();
}

pub fn randomkey<A: Access, O: Reply>(a: &mut A, out: &mut O) {
    let now = a.now();
    let start = (super::random_u64() as usize) % SHARDS;
    let mut found: Option<Vec<u8>> = None;
    // Two passes over the ring of shards, starting at a random one: the
    // first from `start` to the end, the second from 0 back up to it.
    for pass in 0..2 {
        a.each_shard(|index, s| {
            if (pass == 0 && index < start) || (pass == 1 && index >= start) {
                return true;
            }
            if s.is_empty() {
                return true;
            }
            let buckets = s.num_buckets();
            let r = super::random_u64() as usize;
            for probe in 0..buckets.min(64) {
                if let Some(slot) = s.table.get_bucket((r + probe * 7919) % buckets) {
                    if !slot.is_expired(now) {
                        found = Some(slot.key().to_vec());
                        return false;
                    }
                }
            }
            if let Some(slot) = s.iter().find(|slot| !slot.is_expired(now)) {
                found = Some(slot.key().to_vec());
                return false;
            }
            true
        });
        if found.is_some() {
            break;
        }
    }
    match found {
        Some(k) => out.bulk(&k),
        None => out.null(),
    }
}

pub fn object<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let sub = args[1];
    if eq(sub, b"HELP") {
        out.array(4);
        out.simple("OBJECT ENCODING <key>");
        out.simple("OBJECT IDLETIME <key>");
        out.simple("OBJECT FREQ <key>");
        out.simple("OBJECT REFCOUNT <key>");
        return;
    }
    if args.len() != 3 {
        return out.error(SYNTAX);
    }
    let sub = sub.to_ascii_uppercase();
    read_key(a, args[2], out, |slot, out| {
        let Some(slot) = slot else { return out.null() };
        match sub.as_slice() {
            b"ENCODING" => out.bulk(match &slot.value {
                Value::Str if slot.str_bytes().len() <= 44 => b"embstr",
                Value::Str => b"raw",
                Value::Int(_) => b"int",
                Value::Hash(_) | Value::Set(_) => b"hashtable",
                Value::List(_) => b"quicklist",
                Value::ZSet(_) => b"skiplist",
            }),
            b"IDLETIME" => out.int(slot.idle_secs() as i64),
            b"REFCOUNT" => out.int(1),
            b"FREQ" => out.error(
                "ERR An LFU maxmemory policy is not selected, access frequency not tracked.",
            ),
            _ => out.error(SYNTAX),
        }
    });
}

pub fn memory<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let sub = args[1];
    if eq(sub, b"USAGE") && args.len() >= 3 {
        read_key(a, args[2], out, |slot, out| match slot {
            Some(slot) => out.int(slot.cost() as i64),
            None => out.null(),
        });
    } else if eq(sub, b"HELP") {
        out.array(1);
        out.simple("MEMORY USAGE <key> [SAMPLES <count>]");
    } else {
        out.error("ERR unknown subcommand or wrong number of arguments for 'memory' command");
    }
}
