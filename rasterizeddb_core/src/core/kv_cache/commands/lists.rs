//! List commands, and the single attempt the blocking pops are made of.

use super::{Cmd, SYNTAX, Spec, eq, int_arg, modify, modify_existing, parse_int, range_of, read_key, room};
use crate::core::kv_cache::KvError;
use crate::core::kv_cache::reply::Reply;
use crate::core::kv_cache::store::{Access, Value};
use crate::core::kv_cache::values::{ELEMENT_OVERHEAD, ListVal};

fn new_list() -> Value {
    Value::List(Box::new(ListVal::new()))
}

fn as_list(v: &mut Value) -> Result<&mut ListVal, KvError> {
    match v {
        Value::List(l) => Ok(l),
        _ => Err(KvError::WrongType),
    }
}

pub fn push<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, left: bool, only_existing: bool) {
    let (key, values) = (args[1], &args[2..]);
    let need: usize = values.iter().map(|v| v.len() + ELEMENT_OVERHEAD).sum();
    let now = a.now();
    let h = a.hash(key);
    let pushed = a.write(h, |s| {
        if let Err(e) = room(s, need) {
            out.err(&e);
            return false;
        }
        let apply = |v: &mut Value| -> Result<usize, KvError> {
            let list = as_list(v)?;
            for value in values {
                if left {
                    list.push_front(value);
                } else {
                    list.push_back(value);
                }
            }
            Ok(list.len())
        };
        let result = if only_existing {
            modify_existing(s, h, key, now, apply).map(|n| n.unwrap_or(0))
        } else {
            modify(s, h, key, now, new_list, apply)
        };
        match result {
            Ok(n) => {
                out.int(n as i64);
                n > 0
            }
            Err(e) => {
                out.err(&e);
                false
            }
        }
    });
    if pushed {
        a.store().waiters.wake(key, values.len());
    }
}

pub fn pop<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, left: bool) {
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
        let result = modify_existing(s, h, key, now, |v| {
            let list = as_list(v)?;
            let n = count.unwrap_or(1);
            let mut popped = Vec::with_capacity(n.min(list.len()));
            for _ in 0..n {
                match if left { list.pop_front() } else { list.pop_back() } {
                    Some(item) => popped.push(item),
                    None => break,
                }
            }
            Ok(popped)
        });
        match (result, count) {
            (Err(e), _) => out.err(&e),
            (Ok(None), None) => out.null(),
            (Ok(None), Some(_)) => out.null_array(),
            (Ok(Some(items)), None) => match items.first() {
                Some(item) => out.bulk(item),
                None => out.null(),
            },
            (Ok(Some(items)), Some(_)) => {
                out.array(items.len());
                for item in &items {
                    out.bulk(item);
                }
            }
        }
    });
}

pub fn llen<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.int(0),
        Some(Value::List(l)) => out.int(l.len() as i64),
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn lrange<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (Ok(start), Ok(stop)) = (int_arg(args[2]), int_arg(args[3])) else {
        return out.err(&KvError::NotInteger);
    };
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.array(0),
        Some(Value::List(l)) => match range_of(start, stop, l.len()) {
            Some((s, e)) => {
                out.array(e - s + 1);
                for item in l.range(s, e + 1) {
                    out.bulk(item);
                }
            }
            None => out.array(0),
        },
        Some(_) => out.err(&KvError::WrongType),
    });
}

fn index_of(index: i64, len: usize) -> Option<usize> {
    let i = if index < 0 { len as i64 + index } else { index };
    (i >= 0 && (i as usize) < len).then_some(i as usize)
}

pub fn lindex<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let index = match int_arg(args[2]) {
        Ok(i) => i,
        Err(e) => return out.err(&e),
    };
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.null(),
        Some(Value::List(l)) => match index_of(index, l.len()).and_then(|i| l.get(i)) {
            Some(item) => out.bulk(item),
            None => out.null(),
        },
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn lset<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, value) = (args[1], args[3]);
    let index = match int_arg(args[2]) {
        Ok(i) => i,
        Err(e) => return out.err(&e),
    };
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, value.len()) {
            return out.err(&e);
        }
        let result = modify_existing(s, h, key, now, |v| {
            let list = as_list(v)?;
            let i = index_of(index, list.len()).ok_or(KvError::Other("index out of range".into()))?;
            list.set(i, value);
            Ok(())
        });
        match result {
            Ok(Some(())) => out.ok(),
            Ok(None) => out.error("ERR no such key"),
            Err(e) => out.err(&e),
        }
    });
}

pub fn lrem<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, value) = (args[1], args[3]);
    let count = match int_arg(args[2]) {
        Ok(c) => c,
        Err(e) => return out.err(&e),
    };
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        match modify_existing(s, h, key, now, |v| Ok(as_list(v)?.remove_matching(value, count))) {
            Ok(n) => out.int(n.unwrap_or(0) as i64),
            Err(e) => out.err(&e),
        }
    });
}

pub fn ltrim<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let (Ok(start), Ok(stop)) = (int_arg(args[2]), int_arg(args[3])) else {
        return out.err(&KvError::NotInteger);
    };
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        let result = modify_existing(s, h, key, now, |v| {
            let list = as_list(v)?;
            match range_of(start, stop, list.len()) {
                Some((s, e)) => list.retain_range(s, e + 1),
                None => list.retain_range(0, 0),
            }
            Ok(())
        });
        match result {
            Ok(_) => out.ok(),
            Err(e) => out.err(&e),
        }
    });
}

pub fn linsert<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, pivot, value) = (args[1], args[3], args[4]);
    let before = if eq(args[2], b"BEFORE") {
        true
    } else if eq(args[2], b"AFTER") {
        false
    } else {
        return out.error(SYNTAX);
    };
    let now = a.now();
    let h = a.hash(key);
    let inserted = a.write(h, |s| {
        if let Err(e) = room(s, value.len() + ELEMENT_OVERHEAD) {
            out.err(&e);
            return false;
        }
        let result = modify_existing(s, h, key, now, |v| {
            let list = as_list(v)?;
            Ok(match list.position(pivot) {
                Some(i) => {
                    list.insert(if before { i } else { i + 1 }, value);
                    list.len() as i64
                }
                None => -1,
            })
        });
        match result {
            Ok(Some(n)) => {
                out.int(n);
                n > 0
            }
            Ok(None) => {
                out.int(0);
                false
            }
            Err(e) => {
                out.err(&e);
                false
            }
        }
    });
    if inserted {
        a.store().waiters.wake(key, 1);
    }
}

/// `LPOS key element [RANK rank] [COUNT num] [MAXLEN len]`.
pub fn lpos<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let element = args[2];
    let (mut rank, mut count, mut maxlen): (i64, Option<usize>, usize) = (1, None, 0);
    let mut i = 3;
    while i < args.len() {
        if i + 1 >= args.len() {
            return out.error(SYNTAX);
        }
        let Some(n) = parse_int(args[i + 1]) else {
            return out.err(&KvError::NotInteger);
        };
        if eq(args[i], b"RANK") {
            if n == 0 || n == i64::MIN {
                return out.error(
                    "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                );
            }
            rank = n;
        } else if eq(args[i], b"COUNT") {
            if n < 0 {
                return out.error("ERR COUNT can't be negative");
            }
            count = Some(n as usize);
        } else if eq(args[i], b"MAXLEN") {
            if n < 0 {
                return out.error("ERR MAXLEN can't be negative");
            }
            maxlen = n as usize;
        } else {
            return out.error(SYNTAX);
        }
        i += 2;
    }
    read_key(a, args[1], out, |slot, out| {
        let list = match slot.map(|s| &s.value) {
            None => {
                return if count.is_some() { out.array(0) } else { out.null() };
            }
            Some(Value::List(l)) => l,
            Some(_) => return out.err(&KvError::WrongType),
        };
        let want = match count {
            Some(0) => usize::MAX,
            Some(n) => n,
            None => 1,
        };
        let limit = if maxlen == 0 { usize::MAX } else { maxlen };
        let mut skip = rank.unsigned_abs() as usize - 1;
        let mut found = Vec::new();
        let len = list.len();
        let mut visit = |i: usize, item: &[u8]| -> bool {
            if item == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    found.push(i as i64);
                    if found.len() >= want {
                        return false;
                    }
                }
            }
            true
        };
        if rank > 0 {
            for (n, item) in list.iter().enumerate().take(limit) {
                if !visit(n, item) {
                    break;
                }
            }
        } else {
            for (n, item) in list.iter().rev().enumerate().take(limit) {
                if !visit(len - 1 - n, item) {
                    break;
                }
            }
        }
        match count {
            None => match found.first() {
                Some(&p) => out.int(p),
                None => out.null(),
            },
            Some(_) => {
                out.array(found.len());
                for p in found {
                    out.int(p);
                }
            }
        }
    });
}

/// One try of a pop or move: `true` when it replied (with the element, or an
/// error), `false` when every source list was empty and nothing was written.
///
/// The whole of the non-blocking `LMOVE`/`RPOPLPUSH`, and what a blocking
/// command retries each time it is woken.
pub fn bpop_attempt<A: Access, O: Reply>(spec: &Spec, a: &mut A, args: &[&[u8]], out: &mut O) -> bool {
    match spec.cmd {
        Cmd::BLPop | Cmd::BRPop => {
            let left = spec.cmd == Cmd::BLPop;
            let now = a.now();
            for &key in &args[1..args.len() - 1] {
                let h = a.hash(key);
                let popped = a.write(h, |s| {
                    modify_existing(s, h, key, now, |v| {
                        let list = as_list(v)?;
                        let item = if left { list.pop_front() } else { list.pop_back() };
                        Ok((item, list.len()))
                    })
                });
                match popped {
                    Err(e) => {
                        out.err(&e);
                        return true;
                    }
                    Ok(Some((Some(item), remaining))) => {
                        out.array(2);
                        out.bulk(key);
                        out.bulk(&item);
                        if remaining > 0 {
                            // Pass the wake-up on: there is more for the
                            // next waiter.
                            a.store().waiters.wake(key, 1);
                        }
                        return true;
                    }
                    _ => {}
                }
            }
            false
        }
        Cmd::LMove | Cmd::BLMove | Cmd::RPopLPush | Cmd::BRPopLPush => {
            let (from_left, to_left) = match spec.cmd {
                Cmd::RPopLPush | Cmd::BRPopLPush => (false, true),
                _ => {
                    let side = |b: &[u8]| -> Option<bool> {
                        if eq(b, b"LEFT") {
                            Some(true)
                        } else if eq(b, b"RIGHT") {
                            Some(false)
                        } else {
                            None
                        }
                    };
                    match (side(args[3]), side(args[4])) {
                        (Some(f), Some(t)) => (f, t),
                        _ => {
                            out.error(SYNTAX);
                            return true;
                        }
                    }
                }
            };
            lmove(a, args[1], args[2], from_left, to_left, out)
        }
        _ => false,
    }
}

fn lmove<A: Access, O: Reply>(a: &mut A, from: &[u8], to: &[u8], from_left: bool, to_left: bool, out: &mut O) -> bool {
    let now = a.now();
    let (hf, ht) = (a.hash(from), a.hash(to));
    let (replied, moved, more) = a.write_many(&[hf, ht], |shards| {
        // The destination's type is checked before anything moves.
        match shards.get(ht).live(ht, to, now).map(|s| &s.value) {
            None | Some(Value::List(_)) => {}
            Some(_) => {
                out.err(&KvError::WrongType);
                return (true, false, false);
            }
        }
        let popped = modify_existing(shards.get(hf), hf, from, now, |v| {
            let list = as_list(v)?;
            Ok((if from_left { list.pop_front() } else { list.pop_back() }, list.len()))
        });
        let (item, remaining) = match popped {
            Err(e) => {
                out.err(&e);
                return (true, false, false);
            }
            Ok(Some((Some(item), remaining))) => (item, remaining),
            _ => return (false, false, false),
        };
        let dest = shards.get(ht);
        let pushed = modify(dest, ht, to, now, new_list, |v| {
            let list = as_list(v)?;
            if to_left {
                list.push_front(&item);
            } else {
                list.push_back(&item);
            }
            Ok(())
        });
        match pushed {
            Ok(()) => {
                out.bulk(&item);
                (true, true, remaining > 0)
            }
            Err(e) => {
                out.err(&e);
                (true, false, false)
            }
        }
    });
    if moved {
        a.store().waiters.wake(to, 1);
    }
    if more {
        a.store().waiters.wake(from, 1);
    }
    replied
}
