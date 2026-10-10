//! Hash commands.

use super::{Hset, SYNTAX, eq, float_arg, int_arg, modify, modify_existing, parse_int, random_u64, read_key, room};
use crate::core::kv_cache::KvError;
use crate::core::kv_cache::reply::{Reply, double_string, write_i64};
use crate::core::kv_cache::store::{Access, Value};
use crate::core::kv_cache::values::{ELEMENT_OVERHEAD, HashVal};

fn new_hash() -> Value {
    Value::Hash(Box::new(HashVal::new()))
}

fn as_hash(v: &mut Value) -> Result<&mut HashVal, KvError> {
    match v {
        Value::Hash(h) => Ok(h),
        _ => Err(KvError::WrongType),
    }
}

pub fn hset<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, reply: Hset) {
    if args.len() % 2 != 0 {
        return out.error(&format!(
            "ERR wrong number of arguments for '{}' command",
            match reply {
                Hset::Count => "hset",
                Hset::Ok => "hmset",
            }
        ));
    }
    let key = args[1];
    let pairs = &args[2..];
    let need: usize = pairs.iter().map(|b| b.len()).sum::<usize>() + ELEMENT_OVERHEAD * pairs.len() / 2;
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, need) {
            return out.err(&e);
        }
        let result = modify(s, h, key, now, new_hash, |v| {
            let map = as_hash(v)?;
            Ok(pairs
                .chunks(2)
                .filter(|p| map.insert(p[0], p[1]))
                .count())
        });
        match (result, &reply) {
            (Ok(n), Hset::Count) => out.int(n as i64),
            (Ok(_), Hset::Ok) => out.ok(),
            (Err(e), _) => out.err(&e),
        }
    });
}

pub fn hsetnx<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, field, value) = (args[1], args[2], args[3]);
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, field.len() + value.len() + ELEMENT_OVERHEAD) {
            return out.err(&e);
        }
        let result = modify(s, h, key, now, new_hash, |v| {
            let map = as_hash(v)?;
            if map.get(field).is_some() {
                return Ok(0);
            }
            map.insert(field, value);
            Ok(1)
        });
        match result {
            Ok(n) => out.int(n),
            Err(e) => out.err(&e),
        }
    });
}

pub fn hget<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let field = args[2];
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.null(),
        Some(Value::Hash(map)) => match map.get(field) {
            Some(v) => out.bulk(v),
            None => out.null(),
        },
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn hmget<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let fields = &args[2..];
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => {
            out.array(fields.len());
            for _ in fields {
                out.null();
            }
        }
        Some(Value::Hash(map)) => {
            out.array(fields.len());
            for f in fields {
                match map.get(f) {
                    Some(v) => out.bulk(v),
                    None => out.null(),
                }
            }
        }
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn hgetall<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, fields: bool, values: bool) {
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => {
            if fields && values {
                out.map(0)
            } else {
                out.array(0)
            }
        }
        Some(Value::Hash(map)) => {
            if fields && values {
                out.map(map.len());
                for (f, v) in map.iter() {
                    out.bulk(f);
                    out.bulk(v);
                }
            } else {
                out.array(map.len());
                for (f, v) in map.iter() {
                    out.bulk(if fields { f } else { v });
                }
            }
        }
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn hdel<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, fields) = (args[1], &args[2..]);
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        let result = modify_existing(s, h, key, now, |v| {
            let map = as_hash(v)?;
            Ok(fields.iter().filter(|f| map.remove(f)).count())
        });
        match result {
            Ok(n) => out.int(n.unwrap_or(0) as i64),
            Err(e) => out.err(&e),
        }
    });
}

pub fn hexists<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let field = args[2];
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.int(0),
        Some(Value::Hash(map)) => out.int(i64::from(map.get(field).is_some())),
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn hlen<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.int(0),
        Some(Value::Hash(map)) => out.int(map.len() as i64),
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn hstrlen<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let field = args[2];
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.int(0),
        Some(Value::Hash(map)) => out.int(map.get(field).map_or(0, |v| v.len()) as i64),
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn hincrby<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, field) = (args[1], args[2]);
    let delta = match int_arg(args[3]) {
        Ok(d) => d,
        Err(e) => return out.err(&e),
    };
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, field.len() + 20 + ELEMENT_OVERHEAD) {
            return out.err(&e);
        }
        let result = modify(s, h, key, now, new_hash, |v| {
            let map = as_hash(v)?;
            let current = match map.get(field) {
                Some(b) => parse_int(b).ok_or(KvError::Other("hash value is not an integer".into()))?,
                None => 0,
            };
            let next = current.checked_add(delta).ok_or(KvError::Overflow)?;
            let mut text = Vec::with_capacity(20);
            write_i64(&mut text, next);
            map.insert(field, &text);
            Ok(next)
        });
        match result {
            Ok(n) => out.int(n),
            Err(e) => out.err(&e),
        }
    });
}

pub fn hincrbyfloat<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, field) = (args[1], args[2]);
    let delta = match float_arg(args[3]) {
        Ok(d) => d,
        Err(e) => return out.err(&e),
    };
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, field.len() + 24 + ELEMENT_OVERHEAD) {
            return out.err(&e);
        }
        let result = modify(s, h, key, now, new_hash, |v| {
            let map = as_hash(v)?;
            let current = match map.get(field) {
                Some(b) => super::parse_float(b).ok_or(KvError::Other("hash value is not a float".into()))?,
                None => 0.0,
            };
            let next = current + delta;
            if next.is_nan() || next.is_infinite() {
                return Err(KvError::Other("increment would produce NaN or Infinity".into()));
            }
            let text = double_string(next);
            map.insert(field, &text);
            Ok(text)
        });
        match result {
            Ok(text) => out.bulk(&text),
            Err(e) => out.err(&e),
        }
    });
}

/// `HRANDFIELD key [count [WITHVALUES]]`.
pub fn hrandfield<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let count = match args.get(2) {
        None => None,
        Some(c) => match int_arg(c) {
            Ok(n) => Some(n),
            Err(e) => return out.err(&e),
        },
    };
    let with_values = match args.get(3) {
        None => false,
        Some(o) if eq(o, b"WITHVALUES") && args.len() == 4 => true,
        Some(_) => return out.error(SYNTAX),
    };
    read_key(a, args[1], out, |slot, out| {
        let map = match slot.map(|s| &s.value) {
            None => {
                return if count.is_some() { out.array(0) } else { out.null() };
            }
            Some(Value::Hash(map)) => map,
            Some(_) => return out.err(&KvError::WrongType),
        };
        let entries: Vec<(&[u8], &[u8])> = map.iter().collect();
        let pick = |n: usize| -> Vec<usize> {
            // Positive: distinct fields; negative: may repeat.
            let mut idx: Vec<usize> = (0..entries.len()).collect();
            for i in (1..idx.len()).rev() {
                idx.swap(i, (random_u64() as usize) % (i + 1));
            }
            idx.truncate(n);
            idx
        };
        match count {
            None => out.bulk(entries[(random_u64() as usize) % entries.len()].0),
            Some(n) => {
                let chosen: Vec<usize> = if n >= 0 {
                    pick(n as usize)
                } else {
                    (0..n.unsigned_abs() as usize)
                        .map(|_| (random_u64() as usize) % entries.len())
                        .collect()
                };
                if with_values && out.resp3() {
                    out.array(chosen.len());
                    for i in chosen {
                        out.array(2);
                        out.bulk(entries[i].0);
                        out.bulk(entries[i].1);
                    }
                } else {
                    out.array(chosen.len() * if with_values { 2 } else { 1 });
                    for i in chosen {
                        out.bulk(entries[i].0);
                        if with_values {
                            out.bulk(entries[i].1);
                        }
                    }
                }
            }
        }
    });
}

/// `HSCAN key cursor [MATCH p] [COUNT n] [NOVALUES]`: the whole hash in one
/// page (cursor `0`), which is what Redis does for any hash small enough to be
/// compactly encoded and is always a correct answer.
pub fn hscan<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (pattern, novalues) = match scan_options(&args[3..], true) {
        Ok(o) => o,
        Err(e) => return out.error(e),
    };
    if std::str::from_utf8(args[2]).ok().and_then(|s| s.parse::<u64>().ok()).is_none() {
        return out.error("ERR invalid cursor");
    }
    read_key(a, args[1], out, |slot, out| {
        let map = match slot.map(|s| &s.value) {
            None => {
                out.array(2);
                out.bulk(b"0");
                return out.array(0);
            }
            Some(Value::Hash(map)) => map,
            Some(_) => return out.err(&KvError::WrongType),
        };
        let hits: Vec<(&[u8], &[u8])> = map
            .iter()
            .filter(|(f, _)| pattern.is_none_or(|p| crate::core::kv_cache::glob::matches(p, f)))
            .collect();
        out.array(2);
        out.bulk(b"0");
        out.array(hits.len() * if novalues { 1 } else { 2 });
        for (f, v) in hits {
            out.bulk(f);
            if !novalues {
                out.bulk(v);
            }
        }
    });
}

/// `MATCH`/`COUNT` (and `NOVALUES` for `HSCAN`) after a cursor.
pub fn scan_options<'a>(opts: &[&'a [u8]], allow_novalues: bool) -> Result<(Option<&'a [u8]>, bool), &'static str> {
    let mut pattern = None;
    let mut novalues = false;
    let mut i = 0;
    while i < opts.len() {
        if eq(opts[i], b"NOVALUES") && allow_novalues {
            novalues = true;
            i += 1;
            continue;
        }
        if i + 1 >= opts.len() {
            return Err(SYNTAX);
        }
        if eq(opts[i], b"MATCH") {
            pattern = Some(opts[i + 1]).filter(|p| *p != b"*");
        } else if eq(opts[i], b"COUNT") {
            match parse_int(opts[i + 1]) {
                Some(n) if n >= 1 => {}
                _ => return Err(SYNTAX),
            }
        } else {
            return Err(SYNTAX);
        }
        i += 2;
    }
    Ok((pattern, novalues))
}
