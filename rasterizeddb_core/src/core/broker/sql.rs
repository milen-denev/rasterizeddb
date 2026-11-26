//! `BROKER …` statements on the SQL ports: the broker administered and used
//! from SQL, answered as rows.
//!
//! ```sql
//! BROKER CREATE TOPIC orders PARTITIONS 8 DURABILITY sync RETENTION '7d';
//! BROKER CREATE QUEUE jobs MAX DELIVERIES 5 DEAD LETTER jobs_dead;
//! BROKER CREATE EXCHANGE events TYPE topic;
//! BROKER BIND events TO orders KEY 'order.*';
//! BROKER PUBLISH orders 'payload' KEY 'customer-7' HEADER 'trace' '42';
//! BROKER SEND events 'order.created' 'payload';      -- through an exchange
//! BROKER READ orders PARTITION 0 FROM 100 LIMIT 10;   -- leaves no trace
//! BROKER POP jobs LIMIT 10;                           -- takes and acks
//! BROKER SHOW TOPICS | QUEUES | SUBSCRIPTIONS | EXCHANGES | BINDINGS;
//! BROKER SHOW PARTITIONS orders;
//! BROKER DROP TOPIC IF EXISTS orders;
//! ```
//!
//! Arguments are words or single-quoted strings (`''` for a quote), and any
//! may be hex bytes (`x'00ff'`). `BROKER BINARY READ …` / `BINARY POP` return
//! keys and values as `bytea`; otherwise they are text, and a value that is
//! not UTF-8 is refused. A statement's columns depend only on its text, so
//! the extended protocol can describe it before it runs.

use rasterizeddb_mq::batch::{BatchBuilder, Batches};
use rasterizeddb_mq::wire::{
    ANY_PARTITION, BY_KEY, Binding, Durability, ExchangeKind, QueueConfig, Start, SubConfig, SubMode, TopicConfig, XMatch,
};

use super::{Broker, BrokerError, BrokerHandle};
use crate::core::db_type::DbType;
use crate::core::introspect::{Cell, failure, row, rows};

/// Whether `sql` is a `BROKER …` statement.
pub fn is_broker_statement(sql: &str) -> bool {
    let t = sql.trim_start().as_bytes();
    t.len() > 7 && t[..6].eq_ignore_ascii_case(b"BROKER") && t[6].is_ascii_whitespace()
}

/// Whether `sql` is a `BROKER` statement that creates, changes or removes
/// something (what `--broker-ddl superuser` keeps to superusers).
pub fn is_broker_ddl(sql: &str) -> bool {
    if !is_broker_statement(sql) {
        return false;
    }
    let first = sql.trim_start()[6..].split_whitespace().next().unwrap_or("");
    ["CREATE", "DROP", "ALTER", "BIND", "UNBIND"].iter().any(|k| first.eq_ignore_ascii_case(k))
}

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Word(String),
    Str(Vec<u8>),
}

impl Tok {
    fn bytes(&self) -> &[u8] {
        match self {
            Tok::Word(w) => w.as_bytes(),
            Tok::Str(b) => b,
        }
    }
    fn text(&self) -> Result<String, String> {
        String::from_utf8(self.bytes().to_vec()).map_err(|_| "a name must be UTF-8 text".to_string())
    }
}

fn tokenize(sql: &str) -> Result<Vec<Tok>, String> {
    let t = sql.trim();
    let t = t.strip_suffix(';').unwrap_or(t).trim_end();
    let b = &t.as_bytes()[6..];
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && b[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let hex = (b[i] == b'x' || b[i] == b'X') && b.get(i + 1) == Some(&b'\'');
        if b[i] == b'\'' || hex {
            if hex {
                i += 1;
            }
            i += 1;
            let mut s = Vec::new();
            loop {
                match b.get(i) {
                    None => return Err("unterminated quoted string".into()),
                    Some(b'\'') if b.get(i + 1) == Some(&b'\'') => {
                        s.push(b'\'');
                        i += 2;
                    }
                    Some(b'\'') => {
                        i += 1;
                        break;
                    }
                    Some(&c) => {
                        s.push(c);
                        i += 1;
                    }
                }
            }
            if hex {
                let text = std::str::from_utf8(&s).map_err(|_| "bad hex string")?;
                if text.len() % 2 != 0 {
                    return Err("x'…' needs an even number of hex digits".into());
                }
                let mut bytes = Vec::with_capacity(text.len() / 2);
                for k in (0..text.len()).step_by(2) {
                    bytes.push(u8::from_str_radix(&text[k..k + 2], 16).map_err(|_| "bad hex digit in x'…'")?);
                }
                s = bytes;
            }
            out.push(Tok::Str(s));
            if i < b.len() && !b[i].is_ascii_whitespace() {
                return Err("a quoted string must be followed by a space".into());
            }
        } else {
            let start = i;
            while i < b.len() && !b[i].is_ascii_whitespace() {
                if b[i] == b'\'' {
                    return Err("a quote inside a word; quote the whole argument".into());
                }
                i += 1;
            }
            out.push(Tok::Word(String::from_utf8_lossy(&b[start..i]).into_owned()));
        }
    }
    Ok(out)
}

/// A cursor over the tokens, with keyword matching.
struct P {
    toks: Vec<Tok>,
    at: usize,
}

impl P {
    fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.toks.get(self.at), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw))
    }
    fn kw(&mut self, kw: &str) -> bool {
        if self.peek_kw(kw) {
            self.at += 1;
            true
        } else {
            false
        }
    }
    fn kws(&mut self, kws: &[&str]) -> bool {
        let save = self.at;
        for k in kws {
            if !self.kw(k) {
                self.at = save;
                return false;
            }
        }
        true
    }
    fn expect(&mut self, kw: &str) -> Result<(), String> {
        if self.kw(kw) { Ok(()) } else { Err(format!("expected {kw}")) }
    }
    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.at).cloned();
        if t.is_some() {
            self.at += 1;
        }
        t
    }
    fn arg(&mut self, what: &str) -> Result<Tok, String> {
        self.next().ok_or_else(|| format!("expected {what}"))
    }
    fn name(&mut self, what: &str) -> Result<String, String> {
        self.arg(what)?.text()
    }
    fn int(&mut self, what: &str) -> Result<i64, String> {
        let t = self.arg(what)?.text()?;
        t.parse().map_err(|_| format!("{what}: expected a number, got {t:?}"))
    }
    fn done(&self) -> bool {
        self.at >= self.toks.len()
    }
    fn end(&self) -> Result<(), String> {
        match self.toks.get(self.at) {
            None => Ok(()),
            Some(t) => Err(format!("unexpected {:?}", String::from_utf8_lossy(t.bytes()))),
        }
    }
}

/// `'7d'`, `'12h'`, `'30m'`, `'45s'`, `'500ms'`, a bare number of
/// milliseconds, or `FOREVER` / `-1`.
fn duration_ms(s: &str) -> Result<i64, String> {
    let s = s.trim().to_ascii_lowercase();
    if s == "forever" || s == "-1" || s == "infinite" {
        return Ok(-1);
    }
    let (num, mul) = if let Some(n) = s.strip_suffix("ms") {
        (n, 1)
    } else if let Some(n) = s.strip_suffix('d') {
        (n, 86_400_000)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3_600_000)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60_000)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1000)
    } else {
        (s.as_str(), 1)
    };
    num.trim()
        .parse::<i64>()
        .ok()
        .and_then(|n| n.checked_mul(mul))
        .filter(|n| *n >= 0)
        .ok_or_else(|| format!("{s:?} is not a duration (7d, 12h, 30m, 45s, 500ms)"))
}

/// `'512mb'`, `'4gb'`, a number of bytes, or `-1`.
fn bytes_of(s: &str) -> Result<i64, String> {
    let s = s.trim().to_ascii_lowercase();
    if s == "-1" || s == "unbounded" {
        return Ok(-1);
    }
    let (num, mul) = [("kb", 1i64 << 10), ("mb", 1 << 20), ("gb", 1 << 30), ("tb", 1 << 40), ("b", 1)]
        .iter()
        .find_map(|(suf, m)| s.strip_suffix(suf).map(|n| (n, *m)))
        .unwrap_or((s.as_str(), 1));
    num.trim()
        .parse::<i64>()
        .ok()
        .and_then(|n| n.checked_mul(mul))
        .filter(|n| *n >= 0)
        .ok_or_else(|| format!("{s:?} is not a size (512mb, 4gb, bytes)"))
}

/// What a statement is, enough to know its columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    Done,
    Created,
    Placed,
    Records { binary: bool },
    Topics,
    Queues,
    Subs,
    Exchanges,
    Bindings,
    Partitions,
}

fn shape(toks: &[Tok]) -> Shape {
    let mut p = P { toks: toks.to_vec(), at: 0 };
    let binary = p.kw("BINARY");
    if p.kw("READ") || p.kw("POP") {
        return Shape::Records { binary };
    }
    if p.kw("PUBLISH") || p.kw("SEND") {
        return Shape::Placed;
    }
    if p.kw("SHOW") {
        return if p.kw("TOPICS") {
            Shape::Topics
        } else if p.kw("QUEUES") {
            Shape::Queues
        } else if p.kw("SUBSCRIPTIONS") {
            Shape::Subs
        } else if p.kw("EXCHANGES") {
            Shape::Exchanges
        } else if p.kw("BINDINGS") {
            Shape::Bindings
        } else if p.kw("PARTITIONS") {
            Shape::Partitions
        } else {
            Shape::Done
        };
    }
    if p.kw("CREATE") && (p.peek_kw("TOPIC") || p.peek_kw("QUEUE")) {
        return Shape::Created;
    }
    Shape::Done
}

/// The columns a `BROKER` statement returns, for `Describe`.
pub fn result_fields(sql: &str) -> Vec<(String, DbType)> {
    let s = tokenize(sql).map(|t| shape(&t)).unwrap_or(Shape::Done);
    let t = |n: &str| (n.to_string(), DbType::STRING);
    let i = |n: &str| (n.to_string(), DbType::I64);
    let b = |n: &str| (n.to_string(), DbType::BOOL);
    match s {
        Shape::Done => vec![t("result")],
        Shape::Created => vec![t("name"), i("id"), i("partitions")],
        Shape::Placed => vec![t("topic"), i("partition"), i("offset")],
        Shape::Records { binary } => {
            let v = |n: &str| (n.to_string(), if binary { DbType::BYTEA } else { DbType::STRING });
            vec![i("partition"), i("offset"), i("time_us"), v("key"), v("value"), t("headers")]
        }
        Shape::Topics => vec![
            t("name"),
            i("id"),
            i("partitions"),
            t("durability"),
            b("queue"),
            i("messages"),
            i("bytes"),
            i("retention_ms"),
        ],
        Shape::Queues => vec![t("name"), i("messages"), i("ready"), i("unacked"), i("consumers")],
        Shape::Subs => vec![
            t("topic"),
            t("name"),
            t("mode"),
            i("members"),
            i("backlog"),
            i("inflight"),
            i("delivered"),
            i("acked"),
            i("dead"),
        ],
        Shape::Exchanges => vec![t("name"), t("type"), b("durable"), i("bindings")],
        Shape::Bindings => vec![t("exchange"), t("topic"), t("key")],
        Shape::Partitions => vec![i("partition"), i("start"), i("next"), i("durable"), i("bytes"), i("segments")],
    }
}

/// Runs a `BROKER` statement and encodes its rows (or its error) the way the
/// executor's other surfaces do.
pub async fn execute(sql: &str, handle: &BrokerHandle, superuser: bool) -> Vec<u8> {
    let r = (|| -> Result<Vec<Vec<Cell>>, String> {
        let toks = tokenize(sql)?;
        let broker = handle.get().map_err(|e| e.message)?;
        run(&broker, toks, superuser)
    })();
    match r {
        Ok(cells) => rows(cells.iter().map(|c| row(c)).collect()),
        Err(message) => failure(&message),
    }
}

fn err(e: BrokerError) -> String {
    e.message
}

fn done(msg: impl Into<String>) -> Vec<Vec<Cell>> {
    vec![vec![Cell::Text(msg.into())]]
}

fn ddl(superuser: bool) -> Result<(), String> {
    if super::ddl_policy() == super::DdlPolicy::Superuser && !superuser {
        return Err("creating and removing broker objects needs a superuser here".into());
    }
    Ok(())
}

fn run(broker: &Broker, toks: Vec<Tok>, superuser: bool) -> Result<Vec<Vec<Cell>>, String> {
    let mut p = P { toks, at: 0 };
    let binary = p.kw("BINARY");
    if binary && !(p.peek_kw("READ") || p.peek_kw("POP")) {
        return Err("BINARY goes before READ or POP".into());
    }
    if p.kw("CREATE") {
        ddl(superuser)?;
        if p.kw("TOPIC") {
            return create_topic(broker, &mut p);
        }
        if p.kw("QUEUE") {
            return create_queue(broker, &mut p);
        }
        if p.kw("EXCHANGE") {
            let name = p.name("an exchange name")?;
            let mut kind = ExchangeKind::Direct;
            let mut alternate = None;
            let mut durable = true;
            while !p.done() {
                if p.kw("TYPE") {
                    let k = p.name("an exchange type")?;
                    kind = ExchangeKind::parse(&k).ok_or_else(|| format!("unknown exchange type {k:?}"))?;
                } else if p.kw("ALTERNATE") {
                    alternate = Some(p.name("an exchange name")?);
                } else if p.kw("TRANSIENT") {
                    durable = false;
                } else {
                    p.end()?;
                }
            }
            broker.declare_exchange(&name, kind, alternate.as_deref(), durable).map_err(err)?;
            return Ok(done(format!("exchange {name} ({})", kind.name())));
        }
        if p.kw("SUBSCRIPTION") {
            let name = p.name("a subscription name")?;
            p.expect("ON")?;
            let topic = broker.require_topic(&p.name("a topic name")?).map_err(err)?;
            let mut config = SubConfig::default();
            while !p.done() {
                if p.kw("MODE") {
                    let m = p.name("a mode")?;
                    config.mode = SubMode::parse(&m).ok_or_else(|| format!("unknown mode {m:?}: ordered, shared or broadcast"))?;
                } else if p.kw("FROM") {
                    config.start = start(&mut p)?;
                } else if p.kws(&["ACK", "TIMEOUT"]) {
                    config.ack_timeout_ms = duration_ms(&p.name("a duration")?)?.clamp(1, u32::MAX as i64) as u32;
                } else if p.kws(&["MAX", "DELIVERIES"]) {
                    config.max_deliveries = p.int("a count")?.clamp(0, u32::MAX as i64) as u32;
                } else if p.kws(&["DEAD", "LETTER"]) {
                    config.dead_letter = p.name("a topic name")?;
                } else {
                    p.end()?;
                }
            }
            if config.mode == SubMode::Broadcast {
                return Err("a broadcast subscription is a connection's own; subscribe over the broker port".into());
            }
            broker.subscription(&topic, &name, config.clone()).map_err(err)?;
            return Ok(done(format!("subscription {name} on {} ({})", topic.name, config.mode.name())));
        }
        return Err("BROKER CREATE takes TOPIC, QUEUE, EXCHANGE or SUBSCRIPTION".into());
    }
    if p.kw("DROP") {
        ddl(superuser)?;
        let what = p.name("TOPIC, QUEUE, EXCHANGE or SUBSCRIPTION")?.to_ascii_uppercase();
        let if_exists = p.kws(&["IF", "EXISTS"]);
        let name = p.name("a name")?;
        let gone = match what.as_str() {
            "TOPIC" => broker.delete_topic(&name, if_exists).map_err(err)?,
            "QUEUE" => {
                if broker.topic(&name).is_some_and(|t| t.queue.is_none()) {
                    return Err(format!("\"{name}\" is a topic, not a queue"));
                }
                broker.delete_topic(&name, if_exists).map_err(err)?
            }
            "EXCHANGE" => broker.delete_exchange(&name, if_exists).map_err(err)?,
            "SUBSCRIPTION" => {
                p.expect("ON")?;
                let topic = broker.require_topic(&p.name("a topic name")?).map_err(err)?;
                broker.delete_subscription(&topic, &name, if_exists).map_err(err)?
            }
            other => return Err(format!("BROKER DROP {other}: expected TOPIC, QUEUE, EXCHANGE or SUBSCRIPTION")),
        };
        p.end()?;
        return Ok(done(if gone { format!("dropped {}", name) } else { format!("{name} did not exist") }));
    }
    if p.kw("ALTER") {
        ddl(superuser)?;
        p.expect("TOPIC")?;
        let name = p.name("a topic name")?;
        p.expect("SET")?;
        let mut changes: Vec<Box<dyn FnOnce(&mut TopicConfig)>> = Vec::new();
        while !p.done() {
            if p.kws(&["RETENTION", "BYTES"]) {
                let v = bytes_of(&p.name("a size")?)?;
                changes.push(Box::new(move |c| c.retention_bytes = v));
            } else if p.kw("RETENTION") {
                let v = duration_ms(&p.name("a duration")?)?;
                changes.push(Box::new(move |c| c.retention_ms = v));
            } else if p.kws(&["MAX", "BATCH"]) {
                let v = bytes_of(&p.name("a size")?)?.clamp(1, u32::MAX as i64) as u32;
                changes.push(Box::new(move |c| c.max_batch_bytes = v));
            } else if p.kws(&["SEGMENT", "BYTES"]) {
                let v = bytes_of(&p.name("a size")?)?.max(1) as u64;
                changes.push(Box::new(move |c| c.segment_bytes = v));
            } else {
                p.end()?;
            }
        }
        broker
            .alter_topic(&name, |c| {
                for f in changes {
                    f(c);
                }
            })
            .map_err(err)?;
        return Ok(done(format!("altered {name}")));
    }
    if p.peek_kw("BIND") || p.peek_kw("UNBIND") {
        let unbind = p.kw("UNBIND");
        if !unbind {
            p.kw("BIND");
        }
        ddl(superuser)?;
        let exchange = p.name("an exchange name")?;
        p.expect("TO").or_else(|_| p.expect("FROM"))?;
        let topic = p.name("a topic or queue name")?;
        let mut b = Binding { exchange, topic, key: String::new(), x_match: XMatch::All, args: Vec::new() };
        while !p.done() {
            if p.kw("KEY") {
                b.key = p.name("a binding key")?;
            } else if p.kw("MATCH") {
                b.x_match = if p.name("ALL or ANY")?.eq_ignore_ascii_case("any") { XMatch::Any } else { XMatch::All };
            } else if p.kw("HEADER") {
                let k = p.name("a header name")?;
                let v = p.arg("a header value")?.bytes().to_vec();
                b.args.push((k, v));
            } else {
                p.end()?;
            }
        }
        let changed = if unbind { broker.unbind(&b).map_err(err)? } else { broker.bind(b.clone()).map_err(err)? };
        return Ok(done(match (unbind, changed) {
            (false, true) => format!("bound {} to {}", b.exchange, b.topic),
            (false, false) => "already bound".to_string(),
            (true, true) => format!("unbound {} from {}", b.exchange, b.topic),
            (true, false) => "was not bound".to_string(),
        }));
    }
    if p.kw("PUBLISH") {
        let topic = broker.require_topic(&p.name("a topic or queue name")?).map_err(err)?;
        let (bytes, partition) = message(&mut p)?;
        let mut bytes = bytes;
        let (part, a) = broker
            .produce(&topic, partition.unwrap_or(BY_KEY), &mut bytes, false)
            .map_err(err)?;
        return Ok(vec![vec![Cell::Text(topic.name.clone()), Cell::Signed(part.index as i64), Cell::Signed(a.base_offset as i64)]]);
    }
    if p.kw("SEND") {
        let exchange = p.name("an exchange name")?;
        let key = p.name("a routing key")?;
        let (mut bytes, _) = message(&mut p)?;
        let placed = broker.publish(&exchange, &key, &mut bytes, false).map_err(err)?;
        return Ok(placed
            .into_iter()
            .map(|(part, a)| {
                let name = broker.topic_by_id(part.topic).map_or_else(String::new, |t| t.name.clone());
                vec![Cell::Text(name), Cell::Signed(part.index as i64), Cell::Signed(a.base_offset as i64)]
            })
            .collect());
    }
    if p.kw("READ") {
        let topic = broker.require_topic(&p.name("a topic name")?).map_err(err)?;
        let mut partition: Option<u32> = None;
        let mut from = Start::Earliest;
        let mut limit = 100i64;
        while !p.done() {
            if p.kw("PARTITION") {
                partition = Some(p.int("a partition")?.clamp(0, u32::MAX as i64) as u32);
            } else if p.kw("FROM") {
                from = start(&mut p)?;
            } else if p.kw("LIMIT") {
                limit = p.int("a limit")?.clamp(0, 1_000_000);
            } else {
                p.end()?;
            }
        }
        let parts: Vec<u32> = match partition {
            Some(x) => vec![x],
            None => (0..topic.partition_count()).collect(),
        };
        let mut out = Vec::new();
        for pi in parts {
            let part = topic.partition(pi).map_err(err)?;
            let mut at = super::sub::resolve_start(&topic, pi, from);
            while (out.len() as i64) < limit {
                let r = part.read(at, 1 << 20).map_err(err)?;
                if r.bytes.is_empty() {
                    break;
                }
                for b in Batches::trusted(&r.bytes).flatten() {
                    for rec in b.records() {
                        if rec.offset < at || out.len() as i64 >= limit {
                            continue;
                        }
                        out.push(record_row(
                            pi,
                            rec.offset,
                            rec.time_us,
                            rec.key,
                            rec.value,
                            rec.headers().collect::<Vec<_>>().as_slice(),
                            binary,
                        )?);
                    }
                    at = b.next_offset();
                }
            }
        }
        return Ok(out);
    }
    if p.kw("POP") {
        let name = p.name("a queue name")?;
        let mut limit = 1i64;
        let mut sub_name: Option<String> = None;
        while !p.done() {
            if p.kw("LIMIT") {
                limit = p.int("a limit")?.clamp(1, 100_000);
            } else if p.kw("SUBSCRIPTION") {
                sub_name = Some(p.name("a subscription")?);
            } else {
                p.end()?;
            }
        }
        let sub = match sub_name {
            Some(s) => {
                let t = broker.require_topic(&name).map_err(err)?;
                t.subscription(&s).ok_or_else(|| format!("subscription \"{s}\" does not exist on \"{name}\""))?
            }
            None => broker.queue(&name).map_err(err)?.1,
        };
        let taken = sub.take(limit as usize, true, 0).map_err(err)?;
        let mut out = Vec::with_capacity(taken.len());
        for t in &taken {
            let headers: Vec<(&[u8], &[u8])> = t.headers.iter().map(|(n, v)| (n.as_slice(), v.as_slice())).collect();
            out.push(record_row(t.partition, t.offset, t.time_us, t.key.as_deref(), &t.value, &headers, binary)?);
        }
        return Ok(out);
    }
    if p.kw("SHOW") {
        return show(broker, &mut p);
    }
    Err("unknown BROKER statement: CREATE, DROP, ALTER, BIND, UNBIND, PUBLISH, SEND, READ, POP or SHOW".into())
}

fn start(p: &mut P) -> Result<Start, String> {
    if p.kw("EARLIEST") || p.kw("BEGINNING") {
        return Ok(Start::Earliest);
    }
    if p.kw("LATEST") || p.kw("END") {
        return Ok(Start::Latest);
    }
    if p.kw("TIME") {
        return Ok(Start::Time(p.int("µs since the epoch")?));
    }
    let n = p.int("an offset, EARLIEST, LATEST or TIME µs")?;
    Ok(Start::Offset(n.max(0) as u64))
}

/// `'value' [KEY 'k'] [PARTITION n] [HEADER 'n' 'v']…`, as one stored batch.
fn message(p: &mut P) -> Result<(Vec<u8>, Option<u32>), String> {
    let value = p.arg("a message value")?.bytes().to_vec();
    let mut key: Option<Vec<u8>> = None;
    let mut partition = None;
    let mut headers: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while !p.done() {
        if p.kw("KEY") {
            key = Some(p.arg("a key")?.bytes().to_vec());
        } else if p.kw("PARTITION") {
            partition = Some(p.int("a partition")?.clamp(0, (ANY_PARTITION - 1) as i64) as u32);
        } else if p.kw("HEADER") {
            let n = p.arg("a header name")?.bytes().to_vec();
            let v = p.arg("a header value")?.bytes().to_vec();
            headers.push((n, v));
        } else {
            p.end()?;
        }
    }
    let mut b = BatchBuilder::with_capacity(value.len() + 128);
    let h: Vec<(&[u8], &[u8])> = headers.iter().map(|(n, v)| (n.as_slice(), v.as_slice())).collect();
    b.push(key.as_deref(), &value, &h).map_err(|e| e.to_string())?;
    Ok((b.finish(), partition))
}

fn record_row(
    partition: u32,
    offset: u64,
    time_us: i64,
    key: Option<&[u8]>,
    value: &[u8],
    headers: &[(&[u8], &[u8])],
    binary: bool,
) -> Result<Vec<Cell>, String> {
    let text = |b: &[u8]| -> Result<Cell, String> {
        if binary {
            return Ok(Cell::Bytes(b.to_vec()));
        }
        String::from_utf8(b.to_vec())
            .map(Cell::Text)
            .map_err(|_| "a message is not UTF-8 text; read it with BROKER BINARY READ (or POP)".to_string())
    };
    let mut h = String::new();
    for (i, (n, v)) in headers.iter().enumerate() {
        if i > 0 {
            h.push_str(", ");
        }
        h.push_str(&String::from_utf8_lossy(n));
        h.push('=');
        h.push_str(&String::from_utf8_lossy(v));
    }
    Ok(vec![
        Cell::Signed(partition as i64),
        Cell::Signed(offset as i64),
        Cell::Signed(time_us),
        match key {
            Some(k) => text(k)?,
            None => Cell::Null,
        },
        text(value)?,
        Cell::Text(h),
    ])
}

fn create_topic(broker: &Broker, p: &mut P) -> Result<Vec<Vec<Cell>>, String> {
    let if_not_exists = p.kws(&["IF", "NOT", "EXISTS"]);
    let name = p.name("a topic name")?;
    let mut c = TopicConfig::default();
    while !p.done() {
        if p.kw("PARTITIONS") {
            c.partitions = p.int("a partition count")?.clamp(0, u32::MAX as i64) as u32;
        } else if p.kw("DURABILITY") {
            let d = p.name("sync, async or memory")?;
            c.durability = Durability::parse(&d).ok_or_else(|| format!("unknown durability {d:?}: sync, async or memory"))?;
        } else if p.kws(&["RETENTION", "BYTES"]) {
            c.retention_bytes = bytes_of(&p.name("a size")?)?;
        } else if p.kw("RETENTION") {
            c.retention_ms = duration_ms(&p.name("a duration")?)?;
        } else if p.kws(&["MAX", "BATCH"]) {
            c.max_batch_bytes = bytes_of(&p.name("a size")?)?.clamp(1, u32::MAX as i64) as u32;
        } else if p.kws(&["SEGMENT", "BYTES"]) {
            c.segment_bytes = bytes_of(&p.name("a size")?)?.max(1) as u64;
        } else if p.kws(&["MEMORY", "BYTES"]) {
            c.memory_bytes = bytes_of(&p.name("a size")?)?.max(1) as u64;
        } else {
            p.end()?;
        }
    }
    let t = broker.create_topic(&name, c, if_not_exists).map_err(err)?;
    Ok(vec![vec![Cell::Text(t.name.clone()), Cell::Signed(t.id as i64), Cell::Signed(t.partition_count() as i64)]])
}

fn create_queue(broker: &Broker, p: &mut P) -> Result<Vec<Vec<Cell>>, String> {
    let if_not_exists = p.kws(&["IF", "NOT", "EXISTS"]);
    let name = p.name("a queue name")?;
    let mut q = QueueConfig::default();
    while !p.done() {
        if p.kw("DURABILITY") {
            let d = p.name("sync, async or memory")?;
            q.durability = Durability::parse(&d).ok_or_else(|| format!("unknown durability {d:?}"))?;
        } else if p.kws(&["ACK", "TIMEOUT"]) {
            q.ack_timeout_ms = duration_ms(&p.name("a duration")?)?.clamp(1, u32::MAX as i64) as u32;
        } else if p.kws(&["MAX", "DELIVERIES"]) {
            q.max_deliveries = p.int("a count")?.clamp(0, u32::MAX as i64) as u32;
        } else if p.kws(&["DEAD", "LETTER"]) {
            q.dead_letter = p.name("a topic or queue name")?;
        } else if p.kw("TTL") {
            q.message_ttl_ms = duration_ms(&p.name("a duration")?)?;
        } else if p.kws(&["MAX", "LENGTH"]) {
            q.max_length = p.int("a count")?;
        } else {
            p.end()?;
        }
    }
    let t = broker.declare_queue(&name, q, if_not_exists).map_err(err)?;
    Ok(vec![vec![Cell::Text(t.name.clone()), Cell::Signed(t.id as i64), Cell::Signed(1)]])
}

fn show(broker: &Broker, p: &mut P) -> Result<Vec<Vec<Cell>>, String> {
    let n = |v: u64| Cell::Signed(v.min(i64::MAX as u64) as i64);
    if p.kw("TOPICS") {
        p.end()?;
        return Ok(broker
            .topics()
            .iter()
            .map(|t| {
                let c = t.config();
                vec![
                    Cell::Text(t.name.clone()),
                    Cell::Signed(t.id as i64),
                    Cell::Signed(c.partitions as i64),
                    Cell::Text(c.durability.name().into()),
                    Cell::Flag(t.queue.is_some()),
                    n(t.depth()),
                    n(t.bytes()),
                    Cell::Signed(c.retention_ms),
                ]
            })
            .collect());
    }
    if p.kw("QUEUES") {
        p.end()?;
        let mut out = Vec::new();
        for t in broker.topics().iter().filter(|t| t.queue.is_some()) {
            let sub = t.subscription(&t.name);
            let (ready, unacked, consumers) = sub.as_ref().map_or((0, 0, 0), |s| {
                let inflight = s.inflight() as u64;
                (s.backlog().saturating_sub(inflight), inflight, s.member_count() as u64)
            });
            out.push(vec![Cell::Text(t.name.clone()), n(ready + unacked), n(ready), n(unacked), n(consumers)]);
        }
        return Ok(out);
    }
    if p.kw("SUBSCRIPTIONS") {
        let only = if p.kw("ON") { Some(p.name("a topic name")?) } else { None };
        p.end()?;
        let mut out = Vec::new();
        for t in broker.topics() {
            if only.as_deref().is_some_and(|o| o != t.name) {
                continue;
            }
            for s in t.subscriptions() {
                use std::sync::atomic::Ordering::Relaxed;
                out.push(vec![
                    Cell::Text(t.name.clone()),
                    Cell::Text(s.name.clone()),
                    Cell::Text(s.config.mode.name().into()),
                    n(s.member_count() as u64),
                    n(s.backlog()),
                    n(s.inflight() as u64),
                    n(s.delivered.load(Relaxed)),
                    n(s.acked.load(Relaxed)),
                    n(s.dead.load(Relaxed)),
                ]);
            }
        }
        return Ok(out);
    }
    if p.kw("EXCHANGES") {
        p.end()?;
        return Ok(broker
            .exchanges()
            .into_iter()
            .map(|(name, kind, durable, b)| {
                vec![Cell::Text(name), Cell::Text(kind.name().into()), Cell::Flag(durable), n(b.len() as u64)]
            })
            .collect());
    }
    if p.kw("BINDINGS") {
        p.end()?;
        let mut out = Vec::new();
        for (_, _, _, bindings) in broker.exchanges() {
            for b in bindings {
                out.push(vec![Cell::Text(b.exchange), Cell::Text(b.topic), Cell::Text(b.key)]);
            }
        }
        return Ok(out);
    }
    if p.kw("PARTITIONS") {
        let t = broker.require_topic(&p.name("a topic name")?).map_err(err)?;
        p.end()?;
        return Ok(t
            .partitions
            .iter()
            .map(|part| {
                vec![
                    Cell::Signed(part.index as i64),
                    n(part.log_start()),
                    n(part.next_offset()),
                    n(part.durable_offset()),
                    n(part.bytes.load(std::sync::atomic::Ordering::Relaxed)),
                    n(part.segment_count() as u64),
                ]
            })
            .collect());
    }
    Err("BROKER SHOW takes TOPICS, QUEUES, SUBSCRIPTIONS, EXCHANGES, BINDINGS or PARTITIONS <topic>".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_are_recognised_and_described() {
        assert!(is_broker_statement("broker show topics"));
        assert!(!is_broker_statement("BROKERS x"));
        assert_eq!(result_fields("BROKER SHOW TOPICS").len(), 8);
        assert_eq!(result_fields("BROKER BINARY READ t")[4].1, DbType::BYTEA);
        assert_eq!(result_fields("BROKER PUBLISH t 'x'")[2].0, "offset");
        assert_eq!(duration_ms("7d").unwrap(), 7 * 86_400_000);
        assert_eq!(bytes_of("4gb").unwrap(), 4 << 30);
    }
}
