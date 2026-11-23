//! Set commands.

use super::{SYNTAX, eq, modify, modify_existing, parse_int, random_u64, read_key, room};
use crate::core::kv_cache::KvError;
use crate::core::kv_cache::reply::Reply;
use crate::core::kv_cache::store::{Access, Slot, Value};
use crate::core::kv_cache::values::{ELEMENT_OVERHEAD, SetVal};

fn new_set() -> Value {
    Value::Set(Box::new(SetVal::new()))
}

fn as_set(v: &mut Value) -> Result<&mut SetVal, KvError> {
    match v {
        Value::Set(x) => Ok(x),
        _ => Err(KvError::WrongType),
    }
}

pub fn sadd<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, members) = (args[1], &args[2..]);
    let need: usize = members.iter().map(|m| m.len() + ELEMENT_OVERHEAD).sum();
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, need) {
            return out.err(&e);
        }
        match modify(s, h, key, now, new_set, |v| {
            let set = as_set(v)?;
            Ok(members.iter().filter(|m| set.insert(m)).count())
        }) {
            Ok(n) => out.int(n as i64),
            Err(e) => out.err(&e),
        }
    });
}

pub fn srem<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, members) = (args[1], &args[2..]);
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        match modify_existing(s, h, key, now, |v| {
            let set = as_set(v)?;
            Ok(members.iter().filter(|m| set.remove(m)).count())
        }) {
            Ok(n) => out.int(n.unwrap_or(0) as i64),
            Err(e) => out.err(&e),
        }
    });
}

pub fn smembers<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.set(0),
        Some(Value::Set(set)) => {
            out.set(set.len());
            for m in set.iter() {
                out.bulk(m);
            }
        }
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn sismember<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let member = args[2];
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.int(0),
        Some(Value::Set(set)) => out.int(i64::from(set.contains(member))),
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn smismember<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let members = &args[2..];
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => {
            out.array(members.len());
            for _ in members {
                out.int(0);
            }
        }
        Some(Value::Set(set)) => {
            out.array(members.len());
            for m in members {
                out.int(i64::from(set.contains(m)));
            }
        }
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn scard<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.int(0),
        Some(Value::Set(set)) => out.int(set.len() as i64),
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn spop<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let count = match args.get(2) {
        None => None,
        Some(c) => match parse_int(c) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return out.error("ERR value is out of range, must be positive"),
        },
    };
    if args.len() > 3 {
        return out.error(SYNTAX);
    }
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        let mut r = s.next_random();
        let result = modify_existing(s, h, key, now, |v| {
            let set = as_set(v)?;
            let n = count.unwrap_or(1).min(set.len());
            let mut popped = Vec::with_capacity(n);
            for _ in 0..n {
                r = r.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                match set.pop_random(r >> 11) {
                    Some(m) => popped.push(m),
                    None => break,
                }
            }
            Ok(popped)
        });
        match (result, count) {
            (Err(e), _) => out.err(&e),
            (Ok(None), None) => out.null(),
            (Ok(None), Some(_)) => out.set(0),
            (Ok(Some(items)), None) => match items.first() {
                Some(m) => out.bulk(m),
                None => out.null(),
            },
            (Ok(Some(items)), Some(_)) => {
                out.set(items.len());
                for m in &items {
                    out.bulk(m);
                }
            }
        }
    });
}

pub fn srandmember<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let count = match args.get(2) {
        None => None,
        Some(c) => match parse_int(c) {
            Some(n) => Some(n),
            None => return out.err(&KvError::NotInteger),
        },
    };
    if args.len() > 3 {
        return out.error(SYNTAX);
    }
    read_key(a, args[1], out, |slot, out| {
        let set = match slot.map(|s| &s.value) {
            None => return if count.is_some() { out.array(0) } else { out.null() },
            Some(Value::Set(set)) => set,
            Some(_) => return out.err(&KvError::WrongType),
        };
        match count {
            None => out.bulk(set.random(random_u64()).expect("non-empty")),
            Some(n) if n < 0 => {
                // Repeats allowed: each pick independent.
                let n = n.unsigned_abs() as usize;
                out.array(n);
                for _ in 0..n {
                    out.bulk(set.random(random_u64()).expect("non-empty"));
                }
            }
            Some(n) => {
                let n = (n as usize).min(set.len());
                if n == set.len() {
                    out.array(n);
                    for m in set.iter() {
                        out.bulk(m);
                    }
                    return;
                }
                // Distinct members: a partial shuffle of the members.
                let mut all: Vec<&[u8]> = set.iter().collect();
                for i in 0..n {
                    let j = i + (random_u64() as usize) % (all.len() - i);
                    all.swap(i, j);
                }
                out.array(n);
                for m in &all[..n] {
                    out.bulk(m);
                }
            }
        }
    });
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Inter,
    Union,
    Diff,
}

/// `SINTER`/`SUNION`/`SDIFF` and their `…STORE` forms.
///
/// `args[1..]` are the source keys either way: for the plain forms `args[0]`
/// is the command name, for the store forms the caller passes the arguments
/// from the destination on, so `args[0]` is the destination.
pub fn combine<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, op: Op, dest: Option<&[u8]>) {
    let sources = &args[1..];
    let now = a.now();
    let mut hashes: Vec<u64> = sources.iter().map(|k| a.hash(k)).collect();
    let hd = dest.map(|d| a.hash(d));
    if let Some(hd) = hd {
        hashes.push(hd);
    }
    // Reads each source in turn, accumulating the result. Works through
    // either shard view since it only needs one shard at a time.
    let compute = |get: &mut dyn FnMut(u64, &[u8]) -> Result<Option<Vec<Vec<u8>>>, KvError>,
                   member_of: &mut dyn FnMut(u64, &[u8], &[u8]) -> bool|
     -> Result<Vec<Vec<u8>>, KvError> {
        // Type-check every source first, as Redis does.
        let mut first: Option<Vec<Vec<u8>>> = None;
        let mut all: Vec<Option<Vec<Vec<u8>>>> = Vec::new();
        for (i, (k, h)) in sources.iter().zip(&hashes).enumerate() {
            let members = get(*h, k)?;
            if i == 0 {
                first = members.clone();
            }
            if op == Op::Union {
                all.push(members);
            }
        }
        Ok(match op {
            Op::Union => {
                let mut acc = SetVal::new();
                for m in all.iter().flatten().flatten() {
                    acc.insert(m);
                }
                acc.iter().map(|m| m.to_vec()).collect()
            }
            Op::Inter => {
                let mut result = first.unwrap_or_default();
                for (k, h) in sources.iter().zip(&hashes).skip(1) {
                    result.retain(|m| member_of(*h, k, m));
                }
                result
            }
            Op::Diff => {
                let mut result = first.unwrap_or_default();
                for (k, h) in sources.iter().zip(&hashes).skip(1) {
                    result.retain(|m| !member_of(*h, k, m));
                }
                result
            }
        })
    };
    match (dest, hd) {
        (Some(dest), Some(hd)) => {
            let source_hashes = hashes.clone();
            a.write_many(&source_hashes, |shards| {
                let shards = std::cell::RefCell::new(shards);
                let mut get = |h: u64, k: &[u8]| -> Result<Option<Vec<Vec<u8>>>, KvError> {
                    let mut s = shards.borrow_mut();
                    match s.get(h).live(h, k, now).map(|s| &s.value) {
                        None => Ok(None),
                        Some(Value::Set(set)) => Ok(Some(set.iter().map(|m| m.to_vec()).collect())),
                        Some(_) => Err(KvError::WrongType),
                    }
                };
                let mut member_of = |h: u64, k: &[u8], m: &[u8]| -> bool {
                    let mut s = shards.borrow_mut();
                    matches!(s.get(h).live(h, k, now).map(|s| &s.value), Some(Value::Set(set)) if set.contains(m))
                };
                match compute(&mut get, &mut member_of) {
                    Ok(members) => {
                        let mut s = shards.borrow_mut();
                        let d = s.get(hd);
                        d.remove(hd, dest);
                        let n = members.len();
                        if n > 0 {
                            let mut set = SetVal::with_capacity(n);
                            for m in &members {
                                set.insert(m);
                            }
                            let slot = Slot::with_value(dest, Value::Set(Box::new(set)), 0);
                            if let Err(e) = room(d, slot.cost()) {
                                return out.err(&e);
                            }
                            d.insert(hd, slot);
                        }
                        out.int(n as i64);
                    }
                    Err(e) => out.err(&e),
                }
            });
        }
        _ => {
            a.read_many(&hashes, |shards| {
                let mut get = |h: u64, k: &[u8]| -> Result<Option<Vec<Vec<u8>>>, KvError> {
                    match shards.get(h).live(h, k, now).map(|s| &s.value) {
                        None => Ok(None),
                        Some(Value::Set(set)) => Ok(Some(set.iter().map(|m| m.to_vec()).collect())),
                        Some(_) => Err(KvError::WrongType),
                    }
                };
                let mut member_of = |h: u64, k: &[u8], m: &[u8]| -> bool {
                    matches!(shards.get(h).live(h, k, now).map(|s| &s.value), Some(Value::Set(set)) if set.contains(m))
                };
                match compute(&mut get, &mut member_of) {
                    Ok(members) => {
                        out.set(members.len());
                        for m in &members {
                            out.bulk(m);
                        }
                    }
                    Err(e) => out.err(&e),
                }
            });
        }
    }
}

/// `SINTERCARD numkeys key … [LIMIT limit]`.
pub fn sintercard<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let n = match parse_int(args[1]) {
        Some(n) if n > 0 => n as usize,
        _ => return out.error("ERR numkeys should be greater than 0"),
    };
    if args.len() < 2 + n {
        return out.error("ERR Number of keys can't be greater than number of args");
    }
    let keys = &args[2..2 + n];
    let mut limit = usize::MAX;
    match &args[2 + n..] {
        [] => {}
        [l, v] if eq(l, b"LIMIT") => match parse_int(v) {
            Some(0) => {}
            Some(x) if x > 0 => limit = x as usize,
            _ => return out.error("ERR LIMIT can't be negative"),
        },
        _ => return out.error(SYNTAX),
    }
    let now = a.now();
    let hashes: Vec<u64> = keys.iter().map(|k| a.hash(k)).collect();
    a.read_many(&hashes, |shards| {
        let mut sets = Vec::with_capacity(keys.len());
        for (k, h) in keys.iter().zip(&hashes) {
            match shards.get(*h).live(*h, k, now).map(|s| &s.value) {
                None => return out.int(0),
                Some(Value::Set(set)) => sets.push(&**set),
                Some(_) => return out.err(&KvError::WrongType),
            }
        }
        sets.sort_by_key(|s| s.len());
        let count = sets[0]
            .iter()
            .filter(|m| sets[1..].iter().all(|s| s.contains(m)))
            .take(limit)
            .count();
        out.int(count as i64);
    });
}

pub fn smove<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (from, to, member) = (args[1], args[2], args[3]);
    let now = a.now();
    let (hf, ht) = (a.hash(from), a.hash(to));
    a.write_many(&[hf, ht], |shards| {
        for (h, k) in [(hf, from), (ht, to)] {
            if let Some(slot) = shards.get(h).live(h, k, now) {
                if !matches!(slot.value, Value::Set(_)) {
                    return out.err(&KvError::WrongType);
                }
            }
        }
        let removed = modify_existing(shards.get(hf), hf, from, now, |v| Ok(as_set(v)?.remove(member)));
        if !matches!(removed, Ok(Some(true))) {
            return out.int(0);
        }
        match modify(shards.get(ht), ht, to, now, new_set, |v| {
            as_set(v)?.insert(member);
            Ok(())
        }) {
            Ok(()) => out.int(1),
            Err(e) => out.err(&e),
        }
    });
}

pub fn sscan<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (pattern, _) = match super::hashes::scan_options(&args[3..], false) {
        Ok(o) => o,
        Err(e) => return out.error(e),
    };
    if std::str::from_utf8(args[2]).ok().and_then(|s| s.parse::<u64>().ok()).is_none() {
        return out.error("ERR invalid cursor");
    }
    read_key(a, args[1], out, |slot, out| {
        let set = match slot.map(|s| &s.value) {
            None => {
                out.array(2);
                out.bulk(b"0");
                return out.array(0);
            }
            Some(Value::Set(set)) => set,
            Some(_) => return out.err(&KvError::WrongType),
        };
        let hits: Vec<&[u8]> = set
            .iter()
            .filter(|m| pattern.is_none_or(|p| crate::core::kv_cache::glob::matches(p, m)))
            .collect();
        out.array(2);
        out.bulk(b"0");
        out.array(hits.len());
        for m in hits {
            out.bulk(m);
        }
    });
}
