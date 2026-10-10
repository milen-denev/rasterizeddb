//! Sorted-set commands.

use super::{SYNTAX, eq, float_arg, int_arg, modify, modify_existing, parse_float, parse_int, range_of, read_key, room};
use crate::core::kv_cache::KvError;
use crate::core::kv_cache::reply::Reply;
use crate::core::kv_cache::store::{Access, Slot, Value};
use crate::core::kv_cache::values::{ELEMENT_OVERHEAD, LexBound, ScoreBound, ZSet};

fn new_zset() -> Value {
    Value::ZSet(Box::new(ZSet::new()))
}

fn as_zset(v: &mut Value) -> Result<&mut ZSet, KvError> {
    match v {
        Value::ZSet(z) => Ok(z),
        _ => Err(KvError::WrongType),
    }
}

/// `(1.5`, `1.5`, `-inf`, `+inf`.
fn score_bound(b: &[u8]) -> Option<ScoreBound> {
    let (exclusive, text) = match b.first() {
        Some(b'(') => (true, &b[1..]),
        _ => (false, b),
    };
    parse_float(text).map(|value| ScoreBound { value, exclusive })
}

/// `-`, `+`, `[a`, `(a`.
fn lex_bound(b: &[u8]) -> Option<LexBound> {
    match b {
        b"-" => Some(LexBound::NegInf),
        b"+" => Some(LexBound::PosInf),
        [b'[', rest @ ..] => Some(LexBound::Incl(rest.to_vec())),
        [b'(', rest @ ..] => Some(LexBound::Excl(rest.to_vec())),
        _ => None,
    }
}

const MIN_MAX_NOT_FLOAT: &str = "ERR min or max is not a float";
const MIN_MAX_NOT_LEX: &str = "ERR min or max not valid string range item";

pub fn zadd<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) = (false, false, false, false, false, false);
    let mut i = 2;
    while i < args.len() {
        let o = args[i];
        if eq(o, b"NX") {
            nx = true;
        } else if eq(o, b"XX") {
            xx = true;
        } else if eq(o, b"GT") {
            gt = true;
        } else if eq(o, b"LT") {
            lt = true;
        } else if eq(o, b"CH") {
            ch = true;
        } else if eq(o, b"INCR") {
            incr = true;
        } else {
            break;
        }
        i += 1;
    }
    let rest = &args[i..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return out.error(SYNTAX);
    }
    if nx && xx {
        return out.error("ERR XX and NX options at the same time are not compatible");
    }
    if (gt && lt) || (gt && nx) || (lt && nx) {
        return out.error("ERR GT, LT, and/or NX options at the same time are not compatible");
    }
    if incr && rest.len() != 2 {
        return out.error("ERR INCR option supports a single increment-element pair");
    }
    let mut pairs = Vec::with_capacity(rest.len() / 2);
    for p in rest.chunks(2) {
        match parse_float(p[0]) {
            Some(score) => pairs.push((score, p[1])),
            None => return out.err(&KvError::NotFloat),
        }
    }
    let need: usize = pairs.iter().map(|(_, m)| 2 * (m.len() + ELEMENT_OVERHEAD)).sum();
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, need) {
            return out.err(&e);
        }
        if xx && s.live(h, key, now).is_none() {
            // XX on a missing key creates nothing.
            return if incr { out.null() } else { out.int(0) };
        }
        let result = modify(s, h, key, now, new_zset, |v| {
            let z = as_zset(v)?;
            let (mut added, mut changed) = (0i64, 0i64);
            let mut last: Option<f64> = None;
            for &(score, member) in &pairs {
                match z.score(member) {
                    Some(current) => {
                        if nx {
                            continue;
                        }
                        let next = if incr { current + score } else { score };
                        if next.is_nan() {
                            return Err(KvError::Other("resulting score is not a number (NaN)".into()));
                        }
                        if (gt && next <= current) || (lt && next >= current) {
                            continue;
                        }
                        if next != current {
                            z.insert(member, next);
                            changed += 1;
                        }
                        last = Some(next);
                    }
                    None => {
                        if xx {
                            continue;
                        }
                        z.insert(member, score);
                        added += 1;
                        last = Some(score);
                    }
                }
            }
            Ok((added, changed, last))
        });
        match result {
            Err(e) => out.err(&e),
            Ok((_, _, last)) if incr => match last {
                Some(score) => out.double(score),
                None => out.null(),
            },
            Ok((added, changed, _)) => out.int(if ch { added + changed } else { added }),
        }
    });
}

pub fn zincrby<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, member) = (args[1], args[3]);
    let delta = match float_arg(args[2]) {
        Ok(d) => d,
        Err(e) => return out.err(&e),
    };
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        if let Err(e) = room(s, 2 * (member.len() + ELEMENT_OVERHEAD)) {
            return out.err(&e);
        }
        match modify(s, h, key, now, new_zset, |v| {
            let z = as_zset(v)?;
            let next = z.score(member).unwrap_or(0.0) + delta;
            if next.is_nan() {
                return Err(KvError::Other("resulting score is not a number (NaN)".into()));
            }
            z.insert(member, next);
            Ok(next)
        }) {
            Ok(score) => out.double(score),
            Err(e) => out.err(&e),
        }
    });
}

pub fn zrem<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (key, members) = (args[1], &args[2..]);
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        match modify_existing(s, h, key, now, |v| {
            let z = as_zset(v)?;
            Ok(members.iter().filter(|m| z.remove(m)).count())
        }) {
            Ok(n) => out.int(n.unwrap_or(0) as i64),
            Err(e) => out.err(&e),
        }
    });
}

pub fn zscore<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let member = args[2];
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.null(),
        Some(Value::ZSet(z)) => match z.score(member) {
            Some(score) => out.double(score),
            None => out.null(),
        },
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn zmscore<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let members = &args[2..];
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => {
            out.array(members.len());
            for _ in members {
                out.null();
            }
        }
        Some(Value::ZSet(z)) => {
            out.array(members.len());
            for m in members {
                match z.score(m) {
                    Some(score) => out.double(score),
                    None => out.null(),
                }
            }
        }
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn zcard<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.int(0),
        Some(Value::ZSet(z)) => out.int(z.len() as i64),
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn zcount<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, lex: bool) {
    enum Bounds {
        Score(ScoreBound, ScoreBound),
        Lex(LexBound, LexBound),
    }
    let bounds = if lex {
        match (lex_bound(args[2]), lex_bound(args[3])) {
            (Some(lo), Some(hi)) => Bounds::Lex(lo, hi),
            _ => return out.error(MIN_MAX_NOT_LEX),
        }
    } else {
        match (score_bound(args[2]), score_bound(args[3])) {
            (Some(lo), Some(hi)) => Bounds::Score(lo, hi),
            _ => return out.error(MIN_MAX_NOT_FLOAT),
        }
    };
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.int(0),
        Some(Value::ZSet(z)) => {
            let (lo, hi) = match &bounds {
                Bounds::Score(lo, hi) => (z.first_rank_by_score(*lo), z.end_rank_by_score(*hi)),
                Bounds::Lex(lo, hi) => (z.first_rank_by_lex(lo), z.end_rank_by_lex(hi)),
            };
            out.int(hi.saturating_sub(lo) as i64)
        }
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn zrank<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, rev: bool) {
    let member = args[2];
    let with_score = match args.get(3) {
        None => false,
        Some(o) if eq(o, b"WITHSCORE") && args.len() == 4 => true,
        Some(_) => return out.error(SYNTAX),
    };
    read_key(a, args[1], out, |slot, out| {
        let z = match slot.map(|s| &s.value) {
            None => return if with_score { out.null_array() } else { out.null() },
            Some(Value::ZSet(z)) => z,
            Some(_) => return out.err(&KvError::WrongType),
        };
        match z.rank(member) {
            None => {
                if with_score {
                    out.null_array()
                } else {
                    out.null()
                }
            }
            Some(rank) => {
                let rank = if rev { z.len() - 1 - rank } else { rank } as i64;
                if with_score {
                    out.array(2);
                    out.int(rank);
                    out.double(z.score(member).expect("ranked"));
                } else {
                    out.int(rank);
                }
            }
        }
    });
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Form {
    Range,
    RevRange,
    ByScore,
    RevByScore,
    ByLex,
    RevByLex,
}

enum By {
    Rank(i64, i64),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound, LexBound),
}

struct RangeQuery {
    by: By,
    rev: bool,
    offset: usize,
    /// `None` is no limit.
    count: Option<usize>,
    with_scores: bool,
}

fn parse_range(args: &[&[u8]], form: Form) -> Result<RangeQuery, String> {
    let (mut by_score, mut by_lex, mut rev) = (
        matches!(form, Form::ByScore | Form::RevByScore),
        matches!(form, Form::ByLex | Form::RevByLex),
        matches!(form, Form::RevRange | Form::RevByScore | Form::RevByLex),
    );
    let (mut offset, mut count, mut with_scores, mut limited) = (0usize, None, false, false);
    let mut i = 4;
    while i < args.len() {
        let o = args[i];
        if eq(o, b"WITHSCORES") && !matches!(form, Form::ByLex | Form::RevByLex) {
            with_scores = true;
        } else if eq(o, b"LIMIT") && i + 2 < args.len() && form != Form::RevRange {
            let (Ok(off), Ok(cnt)) = (int_arg(args[i + 1]), int_arg(args[i + 2])) else {
                return Err("ERR value is not an integer or out of range".into());
            };
            if off < 0 {
                // Redis answers an empty result for a negative offset.
                offset = usize::MAX;
            } else {
                offset = off as usize;
            }
            count = if cnt < 0 { None } else { Some(cnt as usize) };
            limited = true;
            i += 2;
        } else if form == Form::Range && eq(o, b"BYSCORE") {
            by_score = true;
        } else if form == Form::Range && eq(o, b"BYLEX") {
            by_lex = true;
        } else if form == Form::Range && eq(o, b"REV") {
            rev = true;
        } else {
            return Err(SYNTAX.into());
        }
        i += 1;
    }
    if by_score && by_lex {
        return Err(SYNTAX.into());
    }
    if limited && !(by_score || by_lex) {
        return Err(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX".into(),
        );
    }
    if with_scores && by_lex {
        return Err("ERR syntax error, WITHSCORES not supported in combination with BYLEX".into());
    }
    // Reversed score and lex ranges name the bounds high first.
    let (lo_arg, hi_arg) = if rev && (by_score || by_lex) {
        (args[3], args[2])
    } else {
        (args[2], args[3])
    };
    let by = if by_score {
        match (score_bound(lo_arg), score_bound(hi_arg)) {
            (Some(lo), Some(hi)) => By::Score(lo, hi),
            _ => return Err(MIN_MAX_NOT_FLOAT.into()),
        }
    } else if by_lex {
        match (lex_bound(lo_arg), lex_bound(hi_arg)) {
            (Some(lo), Some(hi)) => By::Lex(lo, hi),
            _ => return Err(MIN_MAX_NOT_LEX.into()),
        }
    } else {
        match (int_arg(args[2]), int_arg(args[3])) {
            (Ok(s), Ok(e)) => By::Rank(s, e),
            _ => return Err("ERR value is not an integer or out of range".into()),
        }
    };
    Ok(RangeQuery {
        by,
        rev,
        offset,
        count,
        with_scores,
    })
}

/// The ascending ranks `[lo, hi)` a query selects, before LIMIT.
fn ranks(z: &ZSet, q: &RangeQuery) -> (usize, usize) {
    match &q.by {
        By::Rank(start, stop) => {
            if q.rev {
                // Ranks count from the top: map them to ascending ones.
                match range_of(*start, *stop, z.len()) {
                    Some((s, e)) => (z.len() - 1 - e, z.len() - s),
                    None => (0, 0),
                }
            } else {
                match range_of(*start, *stop, z.len()) {
                    Some((s, e)) => (s, e + 1),
                    None => (0, 0),
                }
            }
        }
        By::Score(lo, hi) => (z.first_rank_by_score(*lo), z.end_rank_by_score(*hi)),
        By::Lex(lo, hi) => (z.first_rank_by_lex(lo), z.end_rank_by_lex(hi)),
    }
}

/// The members a query selects, in reply order.
fn select<'z>(z: &'z ZSet, q: &RangeQuery) -> Vec<(f64, &'z [u8])> {
    let (lo, hi) = ranks(z, q);
    if lo >= hi || q.offset >= hi - lo {
        return Vec::new();
    }
    let available = hi - lo - q.offset;
    let n = q.count.map_or(available, |c| c.min(available));
    if q.rev {
        z.iter_rev_from(hi - 1 - q.offset).take(n).collect()
    } else {
        z.iter_from(lo + q.offset).take(n).collect()
    }
}

fn send_members<O: Reply>(out: &mut O, items: &[(f64, &[u8])], with_scores: bool) {
    if with_scores && out.resp3() {
        out.array(items.len());
        for (score, m) in items {
            out.array(2);
            out.bulk(m);
            out.double(*score);
        }
    } else if with_scores {
        out.array(items.len() * 2);
        for (score, m) in items {
            out.bulk(m);
            out.double(*score);
        }
    } else {
        out.array(items.len());
        for (_, m) in items {
            out.bulk(m);
        }
    }
}

pub fn zrange<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, form: Form) {
    let q = match parse_range(args, form) {
        Ok(q) => q,
        Err(e) => return out.error(&e),
    };
    read_key(a, args[1], out, |slot, out| match slot.map(|s| &s.value) {
        None => out.array(0),
        Some(Value::ZSet(z)) => {
            let items = select(z, &q);
            send_members(out, &items, q.with_scores);
        }
        Some(_) => out.err(&KvError::WrongType),
    });
}

pub fn zpop<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, max: bool) {
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
            let z = as_zset(v)?;
            let n = count.unwrap_or(1).min(z.len());
            let picked: Vec<(f64, Box<[u8]>)> = if max {
                z.iter_rev_from(z.len() - 1).take(n).map(|(s, m)| (s, m.into())).collect()
            } else {
                z.iter().take(n).map(|(s, m)| (s, m.into())).collect()
            };
            for (_, m) in &picked {
                z.remove(m);
            }
            Ok(picked)
        });
        let items = match result {
            Err(e) => return out.err(&e),
            Ok(items) => items.unwrap_or_default(),
        };
        let refs: Vec<(f64, &[u8])> = items.iter().map(|(s, m)| (*s, &**m)).collect();
        if count.is_none() {
            // Without a count: a flat member/score pair, or an empty array.
            out.array(refs.len() * 2);
            for (score, m) in &refs {
                out.bulk(m);
                out.double(*score);
            }
        } else {
            send_members(out, &refs, true);
        }
    });
}

#[derive(Clone, Copy)]
pub enum RemBy {
    Rank,
    Score,
    Lex,
}

pub fn zremrange<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, by: RemBy) {
    let key = args[1];
    let q = match by {
        RemBy::Rank => match (int_arg(args[2]), int_arg(args[3])) {
            (Ok(s), Ok(e)) => By::Rank(s, e),
            _ => return out.err(&KvError::NotInteger),
        },
        RemBy::Score => match (score_bound(args[2]), score_bound(args[3])) {
            (Some(lo), Some(hi)) => By::Score(lo, hi),
            _ => return out.error(MIN_MAX_NOT_FLOAT),
        },
        RemBy::Lex => match (lex_bound(args[2]), lex_bound(args[3])) {
            (Some(lo), Some(hi)) => By::Lex(lo, hi),
            _ => return out.error(MIN_MAX_NOT_LEX),
        },
    };
    let q = RangeQuery {
        by: q,
        rev: false,
        offset: 0,
        count: None,
        with_scores: false,
    };
    let now = a.now();
    let h = a.hash(key);
    a.write(h, |s| {
        match modify_existing(s, h, key, now, |v| {
            let z = as_zset(v)?;
            let (lo, hi) = ranks(z, &q);
            Ok(z.remove_rank_range(lo, hi))
        }) {
            Ok(n) => out.int(n.unwrap_or(0) as i64),
            Err(e) => out.err(&e),
        }
    });
}

pub fn zscan<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O) {
    let (pattern, _) = match super::hashes::scan_options(&args[3..], false) {
        Ok(o) => o,
        Err(e) => return out.error(e),
    };
    if std::str::from_utf8(args[2]).ok().and_then(|s| s.parse::<u64>().ok()).is_none() {
        return out.error("ERR invalid cursor");
    }
    read_key(a, args[1], out, |slot, out| {
        let z = match slot.map(|s| &s.value) {
            None => {
                out.array(2);
                out.bulk(b"0");
                return out.array(0);
            }
            Some(Value::ZSet(z)) => z,
            Some(_) => return out.err(&KvError::WrongType),
        };
        let hits: Vec<(f64, &[u8])> = z
            .iter()
            .filter(|(_, m)| pattern.is_none_or(|p| crate::core::kv_cache::glob::matches(p, m)))
            .collect();
        out.array(2);
        out.bulk(b"0");
        out.array(hits.len() * 2);
        for (score, m) in hits {
            out.bulk(m);
            out.bulk(&crate::core::kv_cache::reply::double_string(score));
        }
    });
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Inter,
    Diff,
}

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

fn aggregate(agg: Aggregate, a: f64, b: f64) -> f64 {
    let v = match agg {
        Aggregate::Sum => a + b,
        Aggregate::Min => a.min(b),
        Aggregate::Max => a.max(b),
    };
    // inf + -inf: Redis stores zero rather than NaN.
    if v.is_nan() { 0.0 } else { v }
}

/// `ZUNION[STORE]`, `ZINTER[STORE]`, `ZDIFF[STORE]`.
pub fn combine<A: Access, O: Reply>(a: &mut A, args: &[&[u8]], out: &mut O, op: SetOp, store: bool) {
    let (dest, numkeys_at) = if store { (Some(args[1]), 2) } else { (None, 1) };
    let n = match parse_int(args[numkeys_at]) {
        Some(n) if n > 0 => n as usize,
        _ => {
            return out.error(&format!(
                "ERR at least 1 input key is needed for '{}' command",
                match (op, store) {
                    (SetOp::Union, true) => "zunionstore",
                    (SetOp::Inter, true) => "zinterstore",
                    (SetOp::Diff, true) => "zdiffstore",
                    (SetOp::Union, false) => "zunion",
                    (SetOp::Inter, false) => "zinter",
                    (SetOp::Diff, false) => "zdiff",
                }
            ));
        }
    };
    let first = numkeys_at + 1;
    if args.len() < first + n {
        return out.error(SYNTAX);
    }
    let keys = &args[first..first + n];
    let mut weights = vec![1.0f64; n];
    let mut agg = Aggregate::Sum;
    let mut with_scores = false;
    let mut i = first + n;
    while i < args.len() {
        let o = args[i];
        if eq(o, b"WEIGHTS") && op != SetOp::Diff && i + n < args.len() {
            for (w, arg) in weights.iter_mut().zip(&args[i + 1..i + 1 + n]) {
                match parse_float(arg) {
                    Some(v) => *w = v,
                    None => return out.error("ERR weight value is not a float"),
                }
            }
            i += n;
        } else if eq(o, b"AGGREGATE") && op != SetOp::Diff && i + 1 < args.len() {
            agg = if eq(args[i + 1], b"SUM") {
                Aggregate::Sum
            } else if eq(args[i + 1], b"MIN") {
                Aggregate::Min
            } else if eq(args[i + 1], b"MAX") {
                Aggregate::Max
            } else {
                return out.error(SYNTAX);
            };
            i += 1;
        } else if eq(o, b"WITHSCORES") && !store {
            with_scores = true;
        } else {
            return out.error(SYNTAX);
        }
        i += 1;
    }
    let now = a.now();
    let hashes: Vec<u64> = keys.iter().map(|k| a.hash(k)).collect();
    // A source as (member, weighted score) pairs; a set counts as score 1.
    let read = |value: Option<&Value>, weight: f64| -> Result<Vec<(Vec<u8>, f64)>, KvError> {
        let weigh = |s: f64| {
            let v = s * weight;
            if v.is_nan() { 0.0 } else { v }
        };
        match value {
            None => Ok(Vec::new()),
            Some(Value::ZSet(z)) => Ok(z.iter().map(|(s, m)| (m.to_vec(), weigh(s))).collect()),
            Some(Value::Set(set)) => Ok(set.iter().map(|m| (m.to_vec(), weigh(1.0))).collect()),
            Some(_) => Err(KvError::WrongType),
        }
    };
    let compute = |sources: Vec<Vec<(Vec<u8>, f64)>>| -> ZSet {
        let mut acc: std::collections::HashMap<Vec<u8>, f64, ahash::RandomState> =
            std::collections::HashMap::with_hasher(crate::core::kv_cache::values::hasher());
        match op {
            SetOp::Union => {
                for src in sources {
                    for (m, s) in src {
                        acc.entry(m)
                            .and_modify(|cur| *cur = aggregate(agg, *cur, s))
                            .or_insert(s);
                    }
                }
            }
            SetOp::Inter => {
                let mut iter = sources.into_iter();
                if let Some(first) = iter.next() {
                    acc.extend(first);
                    for src in iter {
                        let mut next = std::collections::HashMap::with_hasher(crate::core::kv_cache::values::hasher());
                        for (m, s) in src {
                            if let Some(cur) = acc.get(&m) {
                                next.insert(m, aggregate(agg, *cur, s));
                            }
                        }
                        acc = next;
                    }
                }
            }
            SetOp::Diff => {
                let mut iter = sources.into_iter();
                if let Some(first) = iter.next() {
                    acc.extend(first);
                    for src in iter {
                        for (m, _) in src {
                            acc.remove(&m);
                        }
                    }
                }
            }
        }
        let mut z = ZSet::new();
        for (m, s) in acc {
            z.insert(&m, s);
        }
        z
    };
    if let Some(dest) = dest {
        let hd = a.hash(dest);
        let mut all = hashes.clone();
        all.push(hd);
        a.write_many(&all, |shards| {
            let mut sources = Vec::with_capacity(n);
            for ((k, h), w) in keys.iter().zip(&hashes).zip(&weights) {
                match read(shards.get(*h).live(*h, k, now).map(|s| &s.value), *w) {
                    Ok(src) => sources.push(src),
                    Err(e) => return out.err(&e),
                }
            }
            let z = compute(sources);
            let count = z.len();
            let d = shards.get(hd);
            d.remove(hd, dest);
            if count > 0 {
                let slot = Slot::with_value(dest, Value::ZSet(Box::new(z)), 0);
                if let Err(e) = room(d, slot.cost()) {
                    return out.err(&e);
                }
                d.insert(hd, slot);
            }
            out.int(count as i64);
        });
    } else {
        a.read_many(&hashes, |shards| {
            let mut sources = Vec::with_capacity(n);
            for ((k, h), w) in keys.iter().zip(&hashes).zip(&weights) {
                match read(shards.get(*h).live(*h, k, now).map(|s| &s.value), *w) {
                    Ok(src) => sources.push(src),
                    Err(e) => return out.err(&e),
                }
            }
            let z = compute(sources);
            let items: Vec<(f64, &[u8])> = z.iter().collect();
            send_members(out, &items, with_scores);
        });
    }
}
