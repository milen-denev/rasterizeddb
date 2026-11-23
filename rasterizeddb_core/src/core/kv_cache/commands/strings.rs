//! String commands: `GET`, `SET` and every variant, counters, ranges, bits.

use super::{
    SYNTAX, bulk_int, eq, float_arg, int_arg, read_key, room, send_string,
};
use crate::core::kv_cache::KvError;
use crate::core::kv_cache::reply::{Reply, double_string, write_i64};
use crate::core::kv_cache::store::{Access, SLOT_OVERHEAD, Slot, Value};

/// The largest string Redis allows (`proto-max-bulk-len`).
pub const MAX_STRING: usize = 512 * 1024 * 1024;

pub fn get<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    read_key(a, args[1], out, |slot, out| match slot {
        Some(slot) => send_string(slot, out),
        None => out.null(),
    });
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Cond {
    Always,
    Nx,
    Xx,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ttl {
    /// No lifetime option: a plain `SET` clears any lifetime.
    Clear,
    Keep,
    Persist,
    At(u64),
}

/// Parses `EX s` / `PX ms` / `EXAT ts` / `PXAT ms-ts` into an absolute time.
pub(crate) fn expiry(unit: &[u8], value: &[u8], now: u64, cmd: &str) -> Result<u64, KvError> {
    let invalid = || KvError::Other(format!("invalid expire time in '{cmd}' command"));
    let n = int_arg(value)?;
    if n <= 0 {
        return Err(invalid());
    }
    let n = n as u64;
    let at = if eq(unit, b"EX") {
        n.checked_mul(1000).and_then(|ms| now.checked_add(ms))
    } else if eq(unit, b"PX") {
        now.checked_add(n)
    } else if eq(unit, b"EXAT") {
        n.checked_mul(1000)
    } else {
        Some(n)
    };
    match at {
        Some(at) if at <= i64::MAX as u64 => Ok(at),
        _ => Err(invalid()),
    }
}

fn is_ttl_option(o: &[u8]) -> bool {
    eq(o, b"EX") || eq(o, b"PX") || eq(o, b"EXAT") || eq(o, b"PXAT")
}

pub fn set<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, value) = (args[1], args[2]);
    let now = a.now();
    let mut cond = Cond::Always;
    let mut want_old = false;
    let mut ttl = Ttl::Clear;
    let mut i = 3;
    while i < args.len() {
        let o = args[i];
        if eq(o, b"NX") && cond == Cond::Always {
            cond = Cond::Nx;
        } else if eq(o, b"XX") && cond == Cond::Always {
            cond = Cond::Xx;
        } else if eq(o, b"GET") {
            want_old = true;
        } else if eq(o, b"KEEPTTL") && ttl == Ttl::Clear {
            ttl = Ttl::Keep;
        } else if is_ttl_option(o) && ttl == Ttl::Clear && i + 1 < args.len() {
            match expiry(o, args[i + 1], now, "set") {
                Ok(at) => ttl = Ttl::At(at),
                Err(e) => return out.err(&e),
            }
            i += 1;
        } else {
            return out.error(SYNTAX);
        }
        i += 1;
    }
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, SLOT_OVERHEAD + key.len() + value.len()) {
            return out.err(&e);
        }
        // The plain form, by far the commonest: overwrite in place (no new
        // allocation when the length is unchanged) or insert.
        if cond == Cond::Always && !want_old && ttl != Ttl::Keep {
            let at = match ttl {
                Ttl::At(at) => at,
                _ => 0,
            };
            let updated = s
                .update(h, key, now, |slot| {
                    slot.set_str(value);
                    slot.expires_at = at;
                })
                .is_some();
            if !updated {
                s.insert(h, Slot::string(key, value, at));
            }
            return out.ok();
        }
        let current = s.live_mut(h, key, now);
        let exists = current.is_some();
        let kept_ttl = current.as_ref().map_or(0, |c| c.expires_at);
        let old = match current {
            Some(slot) if want_old => match slot.string_value() {
                Some(v) => Some(v.into_owned()),
                None => return out.err(&KvError::WrongType),
            },
            _ => None,
        };
        let proceed = match cond {
            Cond::Always => true,
            Cond::Nx => !exists,
            Cond::Xx => exists,
        };
        if proceed {
            let at = match ttl {
                Ttl::At(at) => at,
                Ttl::Keep => kept_ttl,
                _ => 0,
            };
            if exists {
                s.update(h, key, now, |slot| {
                    slot.set_str(value);
                    slot.expires_at = at;
                });
            } else {
                s.insert(h, Slot::string(key, value, at));
            }
        }
        match (want_old, proceed) {
            (true, _) => match old {
                Some(v) => out.bulk(&v),
                None => out.null(),
            },
            (false, true) => out.ok(),
            (false, false) => out.null(),
        }
    });
}

pub fn setnx<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, value) = (args[1], args[2]);
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if s.live_mut(h, key, now).is_some() {
            return out.int(0);
        }
        if let Err(e) = room(s, SLOT_OVERHEAD + key.len() + value.len()) {
            return out.err(&e);
        }
        s.insert(h, Slot::string(key, value, 0));
        out.int(1);
    });
}

pub fn setex<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, unit_ms: u64) {
    let (key, value) = (args[1], args[3]);
    let now = a.now();
    let cmd = if unit_ms == 1 { "psetex" } else { "setex" };
    let at = match expiry(if unit_ms == 1 { b"PX" } else { b"EX" }, args[2], now, cmd) {
        Ok(at) => at,
        Err(e) => return out.err(&e),
    };
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, SLOT_OVERHEAD + key.len() + value.len()) {
            return out.err(&e);
        }
        if s
            .update(h, key, now, |slot| {
                slot.set_str(value);
                slot.expires_at = at;
            })
            .is_none()
        {
            s.insert(h, Slot::string(key, value, at));
        }
        out.ok();
    });
}

pub fn getset<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, value) = (args[1], args[2]);
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        let old = match s.live_mut(h, key, now) {
            Some(slot) => match slot.string_value() {
                Some(v) => Some(v.into_owned()),
                None => return out.err(&KvError::WrongType),
            },
            None => None,
        };
        if let Err(e) = room(s, SLOT_OVERHEAD + key.len() + value.len()) {
            return out.err(&e);
        }
        s.put(h, Slot::string(key, value, 0));
        match old {
            Some(v) => out.bulk(&v),
            None => out.null(),
        }
    });
}

pub fn getdel<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| match s.live_mut(h, key, now) {
        Some(slot) => {
            if !matches!(slot.value, Value::Str | Value::Int(_)) {
                return out.err(&KvError::WrongType);
            }
            let removed = s.remove(h, key).expect("present");
            send_string(&removed, out);
        }
        None => out.null(),
    });
}

pub fn getex<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let now = a.now();
    let mut ttl: Option<Ttl> = None;
    let mut i = 2;
    while i < args.len() {
        let o = args[i];
        if eq(o, b"PERSIST") && ttl.is_none() {
            ttl = Some(Ttl::Persist);
        } else if is_ttl_option(o) && ttl.is_none() && i + 1 < args.len() {
            match expiry(o, args[i + 1], now, "getex") {
                Ok(at) => ttl = Some(Ttl::At(at)),
                Err(e) => return out.err(&e),
            }
            i += 1;
        } else {
            return out.error(SYNTAX);
        }
        i += 1;
    }
    let h = a.hash(key);
    a.write(h, |s| {
        let Some(slot) = s.live_mut(h, key, now) else {
            return out.null();
        };
        if !matches!(slot.value, Value::Str | Value::Int(_)) {
            return out.err(&KvError::WrongType);
        }
        match ttl {
            None => send_string(slot, out),
            Some(t) => {
                let at = match t {
                    Ttl::At(at) => at,
                    _ => 0,
                };
                s.update(h, key, now, |slot| {
                    slot.expires_at = at;
                    send_string(slot, out);
                });
            }
        }
    });
}

pub fn mget<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let keys = &args[1..];
    let now = a.now();
    let hashes: Vec<u64> = keys.iter().map(|k| a.hash(k)).collect();
    if keys.len() == 1 {
        out.array(1);
        return get(a, args, out);
    }
    a.read_many(&hashes, |shards| {
        out.array(keys.len());
        for (key, &h) in keys.iter().zip(&hashes) {
            match shards.get(h).live(h, key, now) {
                Some(slot) => {
                    slot.touch();
                    match &slot.value {
                        Value::Str => out.bulk(slot.str_bytes()),
                        Value::Int(n) => bulk_int(out, *n),
                        // MGET answers nil for a key of another type.
                        _ => out.null(),
                    }
                }
                None => out.null(),
            }
        }
    });
}

pub fn mset<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, only_if_none: bool) {
    if args.len() % 2 == 0 {
        return out.error(&format!(
            "ERR wrong number of arguments for '{}' command",
            if only_if_none { "msetnx" } else { "mset" }
        ));
    }
    let now = a.now();
    let pairs: Vec<(&[u8], &[u8])> = args[1..].chunks(2).map(|c| (c[0], c[1])).collect();
    let hashes: Vec<u64> = pairs.iter().map(|(k, _)| a.hash(k)).collect();
    a.write_many(&hashes, |shards| {
        if only_if_none
            && pairs
                .iter()
                .zip(&hashes)
                .any(|((k, _), &h)| shards.get(h).live(h, k, now).is_some())
        {
            return out.int(0);
        }
        for ((k, v), &h) in pairs.iter().zip(&hashes) {
            let s = shards.get(h);
            if let Err(e) = room(s, SLOT_OVERHEAD + k.len() + v.len()) {
                // Redis checks memory once, before the command; a refusal
                // part way through would leave some keys set, so it is
                // only possible when the first pair is already refused.
                return out.err(&e);
            }
            if s
                .update(h, k, now, |slot| {
                    slot.set_str(v);
                    slot.expires_at = 0;
                })
                .is_none()
            {
                s.insert(h, Slot::string(k, v, 0));
            }
        }
        if only_if_none { out.int(1) } else { out.ok() }
    });
}

/// The integer a string slot holds, as `INCR` reads it.
fn integer_of(slot: &Slot) -> Result<i64, KvError> {
    match &slot.value {
        Value::Int(n) => Ok(*n),
        Value::Str => super::parse_int(slot.str_bytes()).ok_or(KvError::NotInteger),
        _ => Err(KvError::WrongType),
    }
}

pub fn incr_by<A: Access, O: Reply>(a: &mut A, key: &[u8], delta: Result<i64, KvError>, out: &mut O) {
    let delta = match delta {
        Ok(d) => d,
        Err(e) => return out.err(&e),
    };
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        let result = s.update(h, key, now, |slot| {
            let current = integer_of(slot)?;
            let next = current.checked_add(delta).ok_or(KvError::Overflow)?;
            slot.set_int(next);
            Ok(next)
        });
        match result {
            Some(Ok(n)) => out.int(n),
            Some(Err(e)) => out.err(&e),
            None => {
                if let Err(e) = room(s, SLOT_OVERHEAD + key.len()) {
                    return out.err(&e);
                }
                s.insert(h, Slot::with_value(key, Value::Int(delta), 0));
                out.int(delta);
            }
        }
    });
}

pub fn incr_by_float<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let delta = match float_arg(args[2]) {
        Ok(d) => d,
        Err(e) => return out.err(&e),
    };
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        let compute = |current: f64| -> Result<Vec<u8>, KvError> {
            let next = current + delta;
            if next.is_nan() || next.is_infinite() {
                return Err(KvError::Other("increment would produce NaN or Infinity".into()));
            }
            Ok(double_string(next))
        };
        let result = s.update(h, key, now, |slot| {
            let current = match &slot.value {
                Value::Int(n) => *n as f64,
                Value::Str => super::parse_float(slot.str_bytes()).ok_or(KvError::NotFloat)?,
                _ => return Err(KvError::WrongType),
            };
            let text = compute(current)?;
            slot.set_str(&text);
            Ok(text)
        });
        match result {
            Some(Ok(text)) => out.bulk(&text),
            Some(Err(e)) => out.err(&e),
            None => match compute(0.0) {
                Ok(text) => {
                    if let Err(e) = room(s, SLOT_OVERHEAD + key.len() + text.len()) {
                        return out.err(&e);
                    }
                    s.insert(h, Slot::string(key, &text, 0));
                    out.bulk(&text);
                }
                Err(e) => out.err(&e),
            },
        }
    });
}

pub fn append<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, suffix) = (args[1], args[2]);
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, suffix.len() + SLOT_OVERHEAD + key.len()) {
            return out.err(&e);
        }
        let result = s.update(h, key, now, |slot| {
            let mut bytes = slot.string_value().ok_or(KvError::WrongType)?.into_owned();
            if bytes.len() + suffix.len() > MAX_STRING {
                return Err(KvError::Other("string exceeds maximum allowed size (proto-max-bulk-len)".into()));
            }
            bytes.extend_from_slice(suffix);
            slot.set_str(&bytes);
            Ok(bytes.len())
        });
        match result {
            Some(Ok(n)) => out.int(n as i64),
            Some(Err(e)) => out.err(&e),
            None => {
                s.insert(h, Slot::string(key, suffix, 0));
                out.int(suffix.len() as i64);
            }
        }
    });
}

pub fn strlen<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    read_key(a, args[1], out, |slot, out| match slot.map(|s| (&s.value, s)) {
        None => out.int(0),
        Some((Value::Str, s)) => out.int(s.str_bytes().len() as i64),
        Some((Value::Int(n), _)) => {
            let mut v = Vec::new();
            write_i64(&mut v, *n);
            out.int(v.len() as i64)
        }
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn getrange<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (start, end) = match (int_arg(args[2]), int_arg(args[3])) {
        (Ok(s), Ok(e)) => (s, e),
        _ => return out.err(&KvError::NotInteger),
    };
    read_key(a, args[1], out, |slot, out| {
        let Some(slot) = slot else {
            return out.bulk(b"");
        };
        let Some(bytes) = slot.string_value() else {
            return out.err(&KvError::WrongType);
        };
        // Redis: a negative start/end counts from the end; both are clamped,
        // and an empty or inverted range is the empty string.
        let len = bytes.len() as i64;
        if len == 0 || (start < 0 && end < 0 && start > end) {
            return out.bulk(b"");
        }
        let s = if start < 0 { (len + start).max(0) } else { start };
        let e = if end < 0 { (len + end).max(0) } else { end.min(len - 1) };
        if s > e || s >= len {
            return out.bulk(b"");
        }
        out.bulk(&bytes[s as usize..=e as usize]);
    });
}

pub fn setrange<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let offset = match int_arg(args[2]) {
        Ok(n) if n >= 0 => n as usize,
        Ok(_) => return out.error("ERR offset is out of range"),
        Err(e) => return out.err(&e),
    };
    let value = args[3];
    if offset + value.len() > MAX_STRING {
        return out.error("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
    }
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, offset + value.len()) {
            return out.err(&e);
        }
        let write = |bytes: &mut Vec<u8>| {
            if value.is_empty() {
                return;
            }
            if bytes.len() < offset + value.len() {
                bytes.resize(offset + value.len(), 0);
            }
            bytes[offset..offset + value.len()].copy_from_slice(value);
        };
        let result = s.update(h, key, now, |slot| {
            let mut bytes = slot.string_value().ok_or(KvError::WrongType)?.into_owned();
            write(&mut bytes);
            slot.set_str(&bytes);
            Ok(bytes.len())
        });
        match result {
            Some(Ok(n)) => out.int(n as i64),
            Some(Err(e)) => out.err(&e),
            None => {
                if value.is_empty() {
                    return out.int(0);
                }
                let mut bytes = Vec::new();
                write(&mut bytes);
                s.insert(h, Slot::string(key, &bytes, 0));
                out.int(bytes.len() as i64);
            }
        }
    });
}

fn bit_offset(arg: &[u8]) -> Result<usize, KvError> {
    match super::parse_int(arg) {
        Some(n) if n >= 0 && (n as u64) < (MAX_STRING as u64) * 8 => Ok(n as usize),
        _ => Err(KvError::Other("bit offset is not an integer or out of range".into())),
    }
}

pub fn setbit<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let offset = match bit_offset(args[2]) {
        Ok(o) => o,
        Err(e) => return out.err(&e),
    };
    let on = match args[3] {
        b"1" => true,
        b"0" => false,
        _ => return out.error("ERR bit is not an integer or out of range"),
    };
    let (byte, mask) = (offset / 8, 0x80u8 >> (offset % 8));
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, byte + 1) {
            return out.err(&e);
        }
        let flip = |bytes: &mut Vec<u8>| -> bool {
            if bytes.len() <= byte {
                bytes.resize(byte + 1, 0);
            }
            let old = bytes[byte] & mask != 0;
            if on {
                bytes[byte] |= mask;
            } else {
                bytes[byte] &= !mask;
            }
            old
        };
        let result = s.update(h, key, now, |slot| {
            let mut bytes = slot.string_value().ok_or(KvError::WrongType)?.into_owned();
            let old = flip(&mut bytes);
            slot.set_str(&bytes);
            Ok(old)
        });
        match result {
            Some(Ok(old)) => out.int(i64::from(old)),
            Some(Err(e)) => out.err(&e),
            None => {
                let mut bytes = Vec::new();
                flip(&mut bytes);
                s.insert(h, Slot::string(key, &bytes, 0));
                out.int(0);
            }
        }
    });
}

pub fn getbit<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let offset = match bit_offset(args[2]) {
        Ok(o) => o,
        Err(e) => return out.err(&e),
    };
    read_key(a, args[1], out, |slot, out| {
        let Some(slot) = slot else { return out.int(0) };
        let Some(bytes) = slot.string_value() else {
            return out.err(&KvError::WrongType);
        };
        let bit = bytes
            .get(offset / 8)
            .is_some_and(|b| b & (0x80 >> (offset % 8)) != 0);
        out.int(i64::from(bit));
    });
}

pub fn bitcount<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let range = match args.len() {
        2 => None,
        4 | 5 => {
            let (Ok(start), Ok(end)) = (int_arg(args[2]), int_arg(args[3])) else {
                return out.err(&KvError::NotInteger);
            };
            let bits = match args.get(4) {
                None => false,
                Some(u) if eq(u, b"BYTE") => false,
                Some(u) if eq(u, b"BIT") => true,
                Some(_) => return out.error(SYNTAX),
            };
            Some((start, end, bits))
        }
        _ => return out.error(SYNTAX),
    };
    read_key(a, args[1], out, |slot, out| {
        let Some(slot) = slot else { return out.int(0) };
        let Some(bytes) = slot.string_value() else {
            return out.err(&KvError::WrongType);
        };
        let count = match range {
            None => bytes.iter().map(|b| b.count_ones() as i64).sum(),
            Some((start, end, false)) => match super::range_of(start, end, bytes.len()) {
                Some((s, e)) => bytes[s..=e].iter().map(|b| b.count_ones() as i64).sum(),
                None => 0,
            },
            Some((start, end, true)) => match super::range_of(start, end, bytes.len() * 8) {
                Some((s, e)) => (s..=e)
                    .filter(|&bit| bytes[bit / 8] & (0x80 >> (bit % 8)) != 0)
                    .count() as i64,
                None => 0,
            },
        };
        out.int(count);
    });
}
