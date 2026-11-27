//! The broker engine, in-process: storage, recovery, subscriptions, routing
//! and the SQL surface. The protocol adapters are tested against real
//! clients (`.claude/reviews/broker/`).

use std::sync::Arc;
use std::time::Duration;

use rasterizeddb_mq::batch::{BatchBuilder, Batches};
use rasterizeddb_mq::wire::{
    self, Binding, Code, Durability, ExchangeKind, QueueConfig, Range, Start, SubConfig, SubMode, TopicConfig, XMatch,
};

use super::*;

fn batch(values: &[&str]) -> Vec<u8> {
    let mut b = BatchBuilder::new();
    for v in values {
        b.push(None, v.as_bytes(), &[]).unwrap();
    }
    b.finish()
}

fn keyed(key: &str, value: &str) -> Vec<u8> {
    let mut b = BatchBuilder::new();
    b.push(Some(key.as_bytes()), value.as_bytes(), &[]).unwrap();
    b.finish()
}

fn values(p: &partition::Partition, from: u64) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    let mut at = from;
    loop {
        let r = p.read(at, 1 << 20).unwrap();
        if r.bytes.is_empty() {
            break;
        }
        for b in Batches::trusted(&r.bytes).flatten() {
            for rec in b.records().filter(|r| r.offset >= at) {
                out.push((rec.offset, String::from_utf8(rec.value.to_vec()).unwrap()));
            }
            at = b.next_offset();
        }
    }
    out
}

/// Waits (bounded) until `p` is durable through its next offset.
fn settle(p: &partition::Partition) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while p.durable_offset() < p.next_offset() {
        assert!(std::time::Instant::now() < deadline, "partition never became durable");
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn config(partitions: u32, durability: Durability) -> TopicConfig {
    TopicConfig { partitions, durability, ..TopicConfig::default() }
}

#[test]
fn produce_and_read_back() {
    let dir = tempfile::tempdir().unwrap();
    let b = Broker::open(Some(dir.path()), "t").unwrap();
    let t = b.create_topic("orders", config(2, Durability::Sync), false).unwrap();
    for i in 0..3 {
        let mut x = batch(&[&format!("a{i}"), &format!("b{i}")]);
        let (p, a) = b.produce(&t, 1, &mut x, true).unwrap();
        assert_eq!(p.index, 1);
        assert_eq!(a.base_offset, i * 2);
    }
    let p = t.partition(1).unwrap();
    settle(p);
    let got = values(p, 0);
    assert_eq!(got.len(), 6);
    assert_eq!(got[5], (5, "b2".to_string()));
    // From the middle of a batch: the batch comes whole, the caller skips.
    assert_eq!(values(p, 3)[0], (3, "b1".to_string()));
    assert!(t.partition(0).unwrap().read(0, 100).unwrap().bytes.is_empty());
    // A damaged batch is refused before anything is stored.
    let mut bad = batch(&["x"]);
    let n = bad.len();
    bad[n - 1] ^= 0xff;
    let e = b.produce(&t, 0, &mut bad, true).unwrap_err();
    assert_eq!(e.code, Code::BadBatch);
    assert_eq!(t.partition(0).unwrap().next_offset(), 0);
    b.shutdown();
}

#[test]
fn a_reopened_broker_has_everything_it_acknowledged() {
    let dir = tempfile::tempdir().unwrap();
    {
        let b = Broker::open(Some(dir.path()), "t").unwrap();
        let t = b.create_topic("events", config(1, Durability::Sync), false).unwrap();
        for i in 0..50 {
            let mut x = batch(&[&format!("e{i}")]);
            b.produce(&t, 0, &mut x, true).unwrap();
        }
        settle(t.partition(0).unwrap());
        b.declare_exchange("fan", ExchangeKind::Fanout, None, true).unwrap();
        b.bind(Binding { exchange: "fan".into(), topic: "events".into(), key: String::new(), x_match: XMatch::All, args: vec![] })
            .unwrap();
        b.shutdown();
    }
    let b = Broker::open(Some(dir.path()), "t").unwrap();
    let t = b.require_topic("events").unwrap();
    let p = t.partition(0).unwrap();
    assert_eq!(p.next_offset(), 50);
    assert_eq!(values(p, 48), vec![(48, "e48".into()), (49, "e49".into())]);
    // Appends continue where the log ended.
    let mut x = batch(&["after"]);
    assert_eq!(b.produce(&t, 0, &mut x, true).unwrap().1.base_offset, 50);
    // The exchange and its binding came back too.
    let mut y = batch(&["routed"]);
    assert_eq!(b.publish("fan", "", &mut y, true).unwrap().len(), 1);
    b.shutdown();
}

#[test]
fn a_torn_tail_is_cut_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let seg;
    {
        let b = Broker::open(Some(dir.path()), "t").unwrap();
        let t = b.create_topic("log", config(1, Durability::Sync), false).unwrap();
        for i in 0..5 {
            let mut x = batch(&[&format!("v{i}")]);
            b.produce(&t, 0, &mut x, true).unwrap();
        }
        let p = t.partition(0).unwrap();
        settle(p);
        seg = p.dir().unwrap().join(segment::seg_name(0));
        b.shutdown();
    }
    // A crash mid-append: half a batch after the last whole one.
    let half = &batch(&["never acknowledged"])[..40];
    let mut bytes = std::fs::read(&seg).unwrap();
    let whole = bytes.len();
    bytes.extend_from_slice(half);
    std::fs::write(&seg, &bytes).unwrap();
    let b = Broker::open(Some(dir.path()), "t").unwrap();
    let t = b.require_topic("log").unwrap();
    let p = t.partition(0).unwrap();
    assert_eq!(p.next_offset(), 5);
    assert_eq!(std::fs::metadata(&seg).unwrap().len() as usize, whole);
    let mut x = batch(&["v5"]);
    b.produce(&t, 0, &mut x, true).unwrap();
    settle(p);
    assert_eq!(values(p, 0).len(), 6);
    b.shutdown();
}

#[test]
fn an_idempotent_producer_is_stored_once() {
    let b = Broker::open(None, "t").unwrap();
    let t = b.create_topic("idem", config(1, Durability::Memory), false).unwrap();
    let mut bb = BatchBuilder::new();
    bb.set_producer(77, 0, 0);
    bb.push(None, b"one", &[]).unwrap();
    bb.push(None, b"two", &[]).unwrap();
    let first = bb.finish();
    let (_, a) = b.produce(&t, 0, &mut first.clone(), true).unwrap();
    let (_, again) = b.produce(&t, 0, &mut first.clone(), true).unwrap();
    assert!(again.duplicate);
    assert_eq!(again.base_offset, a.base_offset);
    assert_eq!(t.partition(0).unwrap().next_offset(), 2);
    // The next batch follows on (sequence 2); a gap is refused.
    bb.push(None, b"three", &[]).unwrap();
    let next = bb.finish();
    b.produce(&t, 0, &mut next.clone(), true).unwrap();
    let mut gap = BatchBuilder::new();
    gap.set_producer(77, 0, 10);
    gap.push(None, b"x", &[]).unwrap();
    assert_eq!(b.produce(&t, 0, &mut gap.finish(), true).unwrap_err().code, Code::OutOfOrder);
    // An older epoch is fenced.
    let mut old = BatchBuilder::new();
    old.set_producer(77, 0, 3);
    old.push(None, b"y", &[]).unwrap();
    let mut newer = BatchBuilder::new();
    newer.set_producer(77, 1, 0);
    newer.push(None, b"z", &[]).unwrap();
    b.produce(&t, 0, &mut newer.finish(), true).unwrap();
    assert_eq!(b.produce(&t, 0, &mut old.finish(), true).unwrap_err().code, Code::Fenced);
}

/// The records of every `DELIVER` frame waiting in `rx`, as `(offset, value)`,
/// waiting up to `wait` for at least `want`.
async fn drain(rx: &mut sub::OutboxRx, want: usize, wait: Duration) -> Vec<(u32, u64, String)> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + wait;
    while out.len() < want {
        let Ok(Some(f)) = tokio::time::timeout_at(deadline, rx.recv()).await else { break };
        let h = wire::parse_header(&f).unwrap().unwrap();
        if h.op != wire::op::DELIVER {
            continue;
        }
        let mut r = wire::Reader::new(&f[wire::FRAME_HEADER..]);
        let part = r.u32().unwrap();
        let ranges = wire::decode_ranges(&mut r, 1 << 20).unwrap();
        for b in Batches::trusted(r.rest()).flatten() {
            for rec in b.records() {
                if ranges.iter().any(|g| rec.offset >= g.offset && rec.offset < g.offset + g.count as u64) {
                    out.push((part, rec.offset, String::from_utf8(rec.value.to_vec()).unwrap()));
                }
            }
        }
    }
    out
}

fn one(p: u32, o: u64) -> Range {
    Range { partition: p, offset: o, count: 1 }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_shared_subscription_hands_each_record_to_one_member() {
    let b = Broker::open(None, "t").unwrap();
    let q = QueueConfig { durability: Durability::Memory, ..QueueConfig::default() };
    let t = b.declare_queue("jobs", q, false).unwrap();
    let (_, sub) = b.queue("jobs").unwrap();
    for i in 0..10 {
        let mut x = batch(&[&format!("j{i}")]);
        b.produce(&t, 0, &mut x, true).unwrap();
    }
    let rt = tokio::runtime::Handle::current();
    let (tx1, mut rx1) = sub::outbox();
    let (tx2, mut rx2) = sub::outbox();
    let (m1, _) = sub.join(1, 3, tx1, &rt).unwrap();
    let (_m2, _) = sub.join(2, 3, tx2, &rt).unwrap();
    let a = drain(&mut rx1, 3, Duration::from_secs(5)).await;
    let c = drain(&mut rx2, 3, Duration::from_secs(5)).await;
    assert_eq!(a.len() + c.len(), 6, "credit caps what is in flight: {a:?} {c:?}");
    let mut seen: Vec<u64> = a.iter().chain(&c).map(|x| x.1).collect();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 6, "no record twice");
    assert_eq!(sub.inflight(), 6);
    // Ack member 1's; nack one of member 2's; member 2 leaves.
    let ranges: Vec<Range> = a.iter().map(|x| one(0, x.1)).collect();
    assert_eq!(sub.ack(&ranges).unwrap(), a.len() as u64);
    sub.nack(&[one(0, c[0].1)], 0).unwrap();
    sub.leave(_m2);
    sub.credit(m1, 100);
    // Everything not acked reaches member 1 now: the nacked, the ones member
    // 2 held, and the four never sent.
    let rest = drain(&mut rx1, 7, Duration::from_secs(5)).await;
    let mut offs: Vec<u64> = rest.iter().map(|x| x.1).collect();
    offs.sort();
    offs.dedup();
    let mut want: Vec<u64> = (0..10).filter(|o| !a.iter().any(|x| x.1 == *o)).collect();
    want.sort();
    assert_eq!(offs, want);
    sub.ack(&offs.iter().map(|o| one(0, *o)).collect::<Vec<_>>()).unwrap();
    assert_eq!(sub.backlog(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_past_its_deliveries_goes_to_the_dead_letter_topic() {
    let b = Broker::open(None, "t").unwrap();
    let dlq = b.declare_queue("dead", QueueConfig { durability: Durability::Memory, ..Default::default() }, false).unwrap();
    let q = QueueConfig { durability: Durability::Memory, max_deliveries: 2, dead_letter: "dead".into(), ..Default::default() };
    let t = b.declare_queue("work", q, false).unwrap();
    let (_, sub) = b.queue("work").unwrap();
    let mut x = batch(&["poison"]);
    b.produce(&t, 0, &mut x, true).unwrap();
    let (tx, mut rx) = sub::outbox();
    let (_m, _) = sub.join(1, 10, tx, &tokio::runtime::Handle::current()).unwrap();
    for _ in 0..2 {
        let got = drain(&mut rx, 1, Duration::from_secs(5)).await;
        assert_eq!(got.len(), 1);
        sub.nack(&[one(0, got[0].1)], 0).unwrap();
    }
    // The third would pass max_deliveries: dead-lettered, not delivered.
    let none = drain(&mut rx, 1, Duration::from_millis(400)).await;
    assert!(none.is_empty(), "{none:?}");
    let p = dlq.partition(0).unwrap();
    let r = p.read(0, 1 << 16).unwrap();
    let rec = Batches::trusted(&r.bytes).flatten().next().expect("dead-lettered").records().next().unwrap();
    assert_eq!(rec.value, b"poison");
    assert_eq!(rec.header(b"x-death-reason"), Some(b"delivery_limit".as_slice()));
    assert_eq!(sub.backlog(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_records_wait_for_their_time() {
    let b = Broker::open(None, "t").unwrap();
    let t = b.declare_queue("later", QueueConfig { durability: Durability::Memory, ..Default::default() }, false).unwrap();
    let (_, sub) = b.queue("later").unwrap();
    let at = now_us() + 600_000;
    let mut bb = BatchBuilder::new();
    bb.push(None, b"soon", &[(wire::DELIVER_AT_HEADER, &wire::deliver_at(at))]).unwrap();
    bb.push(None, b"now", &[]).unwrap();
    b.produce(&t, 0, &mut bb.finish(), true).unwrap();
    let (tx, mut rx) = sub::outbox();
    let _ = sub.join(1, 10, tx, &tokio::runtime::Handle::current()).unwrap();
    let first = drain(&mut rx, 2, Duration::from_millis(300)).await;
    assert_eq!(first.iter().map(|x| x.2.as_str()).collect::<Vec<_>>(), ["now"]);
    let later = drain(&mut rx, 1, Duration::from_secs(3)).await;
    assert_eq!(later.iter().map(|x| x.2.as_str()).collect::<Vec<_>>(), ["soon"]);
    assert!(now_us() >= at);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_commits_survive_a_restart_and_shared_acks_too() {
    let dir = tempfile::tempdir().unwrap();
    let ordered = SubConfig { mode: SubMode::Ordered, start: Start::Earliest, ..Default::default() };
    {
        let b = Broker::open(Some(dir.path()), "t").unwrap();
        let t = b.create_topic("s", config(2, Durability::Sync), false).unwrap();
        for i in 0..6 {
            let mut x = batch(&[&format!("r{i}")]);
            b.produce(&t, (i % 2) as u32, &mut x, true).unwrap();
        }
        let o = b.subscription(&t, "readers", ordered.clone()).unwrap();
        o.commit(0, 2).unwrap();
        o.commit(1, 1).unwrap();
        let s = b.subscription(&t, "workers", SubConfig::default()).unwrap();
        let taken = s.take(4, false, 9).unwrap();
        assert_eq!(taken.len(), 4);
        // Two acked, two left in flight when the server stops.
        s.ack(&[one(taken[0].partition, taken[0].offset), one(taken[1].partition, taken[1].offset)]).unwrap();
        b.shutdown();
    }
    let b = Broker::open(Some(dir.path()), "t").unwrap();
    let t = b.require_topic("s").unwrap();
    let o = t.subscription("readers").unwrap();
    assert_eq!((o.committed(0), o.committed(1)), (Some(2), Some(1)));
    let s = t.subscription("workers").unwrap();
    assert_eq!(s.backlog(), 4, "the unacked ones are owed again after a restart");
    // A partition's members follow join order: two members split two partitions.
    let rt = tokio::runtime::Handle::current();
    let (tx, _rx) = sub::outbox();
    let (_, p1) = o.join(1, 10, tx.clone(), &rt).unwrap();
    assert_eq!(p1, vec![0, 1]);
    let (_, p2) = o.join(2, 10, tx, &rt).unwrap();
    assert_eq!(p2, vec![1]);
    b.shutdown();
}

#[test]
fn exchanges_route_by_kind() {
    let b = Broker::open(None, "t").unwrap();
    for n in ["a", "bq", "c", "h"] {
        b.create_topic(n, config(1, Durability::Memory), false).unwrap();
    }
    let bind = |x: &str, t: &str, key: &str, args: Vec<(String, Vec<u8>)>, m: XMatch| {
        b.bind(Binding { exchange: x.into(), topic: t.into(), key: key.into(), x_match: m, args }).unwrap()
    };
    b.declare_exchange("d", ExchangeKind::Direct, None, true).unwrap();
    b.declare_exchange("f", ExchangeKind::Fanout, None, true).unwrap();
    b.declare_exchange("t", ExchangeKind::Topic, None, true).unwrap();
    b.declare_exchange("hx", ExchangeKind::Headers, None, true).unwrap();
    bind("d", "a", "red", vec![], XMatch::All);
    bind("d", "bq", "red", vec![], XMatch::All);
    bind("f", "a", "", vec![], XMatch::All);
    bind("f", "c", "", vec![], XMatch::All);
    bind("t", "a", "order.*.eu", vec![], XMatch::All);
    bind("t", "c", "order.#", vec![], XMatch::All);
    bind("hx", "h", "", vec![("kind".into(), b"pdf".to_vec())], XMatch::All);
    let n = |x: &str, k: &str| b.publish(x, k, &mut batch(&["m"]), true).unwrap().len();
    assert_eq!(n("d", "red"), 2);
    assert_eq!(n("d", "blue"), 0);
    assert_eq!(n("f", "anything"), 2);
    assert_eq!(n("t", "order.new.eu"), 2);
    assert_eq!(n("t", "order"), 1);
    assert_eq!(n("", "bq"), 1, "the default exchange names a topic");
    let mut hb = BatchBuilder::new();
    hb.push(None, b"doc", &[(b"kind", b"pdf")]).unwrap();
    assert_eq!(b.publish("hx", "", &mut hb.finish(), true).unwrap().len(), 1);
    assert_eq!(n("hx", ""), 0);
    assert_eq!(b.publish("nope", "k", &mut batch(&["m"]), true).unwrap_err().code, Code::NoExchange);
    // A deleted topic leaves its bindings.
    b.delete_topic("a", false).unwrap();
    assert_eq!(n("d", "red"), 1);
}

#[test]
fn retention_removes_whole_old_segments() {
    let dir = tempfile::tempdir().unwrap();
    let b = Broker::open(Some(dir.path()), "t").unwrap();
    let c = TopicConfig { segment_bytes: 1 << 20, retention_bytes: 2 << 20, ..config(1, Durability::Async) };
    let t = b.create_topic("big", c, false).unwrap();
    let value = "x".repeat(60_000);
    for _ in 0..100 {
        let mut x = batch(&[&value]);
        b.produce(&t, 0, &mut x, true).unwrap();
    }
    let p = t.partition(0).unwrap();
    // Written (async: durable once written), so retention may remove segments.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while p.pending_bytes() > 0 || p.durable_offset() < p.next_offset() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(p.segment_count() > 3, "{} segments", p.segment_count());
    let removed = p.apply_retention(-1, 2 << 20);
    assert!(removed > 0);
    assert!(p.log_start() > 0);
    assert_eq!(p.read(0, 100).unwrap_err().code, Code::OffsetOutOfRange);
    assert_eq!(values(p, p.log_start()).len() as u64, p.next_offset() - p.log_start());
    b.shutdown();
}

#[test]
fn a_memory_topic_is_a_ring() {
    let b = Broker::open(None, "t").unwrap();
    let c = TopicConfig { memory_bytes: 1 << 20, ..config(1, Durability::Memory) };
    let t = b.create_topic("ring", c, false).unwrap();
    let value = "y".repeat(100_000);
    for _ in 0..40 {
        b.produce(&t, 0, &mut batch(&[&value]), true).unwrap();
    }
    let p = t.partition(0).unwrap();
    assert_eq!(p.next_offset(), 40);
    assert!(p.log_start() > 0, "the oldest went");
    assert_eq!(p.durable_offset(), 40, "memory is durable at once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_and_reject() {
    let b = Broker::open(None, "t").unwrap();
    b.declare_queue("dlq", QueueConfig { durability: Durability::Memory, ..Default::default() }, false).unwrap();
    let q = QueueConfig { durability: Durability::Memory, dead_letter: "dlq".into(), ..Default::default() };
    let t = b.declare_queue("q", q, false).unwrap();
    let (_, sub) = b.queue("q").unwrap();
    b.produce(&t, 0, &mut batch(&["a", "b", "c", "d"]), true).unwrap();
    let got = sub.take(1, false, 5).unwrap();
    sub.reject(&[one(0, got[0].offset)]);
    assert_eq!(b.require_topic("dlq").unwrap().partition(0).unwrap().next_offset(), 1);
    assert_eq!(sub.purge(), 3);
    assert_eq!(sub.backlog(), 0);
    b.produce(&t, 0, &mut batch(&["e"]), true).unwrap();
    assert_eq!(sub.take(5, true, 0).unwrap().len(), 1);
}

#[test]
fn keys_pick_their_partition() {
    let b = Broker::open(None, "t").unwrap();
    let t = b.create_topic("k", config(8, Durability::Memory), false).unwrap();
    let (p1, _) = b.produce(&t, wire::BY_KEY, &mut keyed("customer-7", "x"), true).unwrap();
    let (p2, _) = b.produce(&t, wire::BY_KEY, &mut keyed("customer-7", "y"), true).unwrap();
    assert_eq!(p1.index, p2.index);
    assert_eq!(p1.index, rasterizeddb_mq::hash::partition_for(b"customer-7", 8));
}

#[test]
fn declarations_check_what_exists() {
    let b = Broker::open(None, "t").unwrap();
    b.create_topic("x", config(2, Durability::Memory), false).unwrap();
    assert_eq!(b.create_topic("x", config(2, Durability::Memory), false).unwrap_err().code, Code::TopicExists);
    assert!(b.create_topic("x", config(2, Durability::Memory), true).is_ok());
    assert_eq!(b.create_topic("x", config(3, Durability::Memory), true).unwrap_err().code, Code::Mismatch);
    assert_eq!(b.create_topic("", config(1, Durability::Memory), false).unwrap_err().code, Code::Invalid);
    assert_eq!(b.create_topic("p", config(0, Durability::Memory), false).unwrap_err().code, Code::Invalid);
    assert!(b.alter_topic("x", |c| c.partitions = 4).is_err());
    assert_eq!(b.alter_topic("x", |c| c.retention_ms = 1000).unwrap().retention_ms, 1000);
    assert_eq!(b.delete_exchange("amq.direct", true).unwrap_err().code, Code::Permission);
}

#[tokio::test]
async fn broker_statements() {
    let h = BrokerHandle::new(None, "t");
    let run = |sql: &'static str| {
        let h = h.clone();
        async move { sql::execute(sql, &h, true).await }
    };
    let ok = |r: Vec<u8>| {
        assert_eq!(r[0], 2, "{}", String::from_utf8_lossy(&r[1..]));
        r
    };
    ok(run("BROKER CREATE TOPIC orders PARTITIONS 4 DURABILITY memory RETENTION '7d'").await);
    ok(run("BROKER CREATE QUEUE jobs DURABILITY memory MAX DELIVERIES 3").await);
    ok(run("BROKER PUBLISH orders 'hello' KEY 'c7' HEADER 'trace' '42'").await);
    ok(run("BROKER PUBLISH jobs 'job-1'").await);
    ok(run("BROKER READ orders LIMIT 10").await);
    ok(run("BROKER POP jobs LIMIT 5").await);
    ok(run("BROKER SHOW TOPICS").await);
    ok(run("BROKER SHOW QUEUES").await);
    ok(run("BROKER CREATE EXCHANGE ev TYPE topic").await);
    ok(run("BROKER BIND ev TO orders KEY 'order.*'").await);
    ok(run("BROKER SEND ev 'order.new' 'routed'").await);
    ok(run("BROKER SHOW BINDINGS").await);
    ok(run("BROKER SHOW PARTITIONS orders").await);
    ok(run("BROKER BINARY READ orders FROM EARLIEST").await);
    ok(run("BROKER DROP TOPIC IF EXISTS nothing").await);
    let bad = run("BROKER CREATE TOPIC orders").await;
    assert_eq!(bad[0], 3);
    let bad = run("BROKER FROB").await;
    assert_eq!(bad[0], 3);
    let broker = h.get().unwrap();
    assert_eq!(broker.require_topic("orders").unwrap().depth(), 2);
    assert_eq!(broker.queue("jobs").unwrap().1.backlog(), 0, "POP took it");
    assert!(sql::is_broker_ddl("broker drop topic x"));
    assert!(!sql::is_broker_ddl("broker read x"));
}

#[test]
fn the_wildcard_trie_is_shared_by_mqtt_filters() {
    let mut t = trie::Trie::new(trie::MQTT);
    t.insert(b"home/+/temp", 1u32);
    let mut hit = Vec::new();
    t.matches(b"home/kitchen/temp", |v| hit.push(*v));
    assert_eq!(hit, [1]);
    let _ = Arc::new(());
}

#[test]
fn memory_topics_live_beside_file_topics() {
    // A broker on disk still makes memory topics (AMQP transient queues,
    // MQTT clean sessions): no directory, nothing synced.
    let dir = tempfile::tempdir().unwrap();
    let b = Broker::open(Some(dir.path()), "t").unwrap();
    let t = b.create_topic("eph", config(2, Durability::Memory), false).unwrap();
    b.produce(&t, 0, &mut batch(&["x"]), true).unwrap();
    b.declare_queue("transient", QueueConfig { durability: Durability::Memory, ..Default::default() }, false).unwrap();
    assert!(b.queue("transient").is_ok());
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_producers_and_competing_consumers_lose_nothing() {
    const PRODUCERS: usize = 8;
    const EACH: usize = 3000;
    let dir = tempfile::tempdir().unwrap();
    let b = Broker::open(Some(dir.path()), "t").unwrap();
    let t = b.create_topic("busy", config(4, Durability::Sync), false).unwrap();
    let sub = b.subscription(&t, "workers", SubConfig::default()).unwrap();
    let rt = tokio::runtime::Handle::current();

    // Four members acking everything they get, recording what they saw.
    let seen = Arc::new(parking_lot::Mutex::new(std::collections::HashMap::<(u32, u64), usize>::new()));
    let mut consumers = Vec::new();
    for m in 0..4u32 {
        let (tx, mut rx) = sub::outbox();
        let (member, _) = sub.join(m + 1, 64, tx, &rt).unwrap();
        let (sub, seen) = (sub.clone(), seen.clone());
        consumers.push(tokio::spawn(async move {
            loop {
                let got = drain(&mut rx, 1, Duration::from_secs(3)).await;
                if got.is_empty() {
                    return;
                }
                let ranges: Vec<Range> = got.iter().map(|x| one(x.0, x.1)).collect();
                for x in &got {
                    *seen.lock().entry((x.0, x.1)).or_default() += 1;
                }
                sub.ack(&ranges).unwrap();
                sub.credit(member, got.len() as u32);
            }
        }));
    }
    // Producers on their own threads, all at once.
    let mut producers = Vec::new();
    for p in 0..PRODUCERS {
        let (b, t) = (b.clone(), t.clone());
        producers.push(std::thread::spawn(move || {
            for i in 0..EACH / 10 {
                let vals: Vec<String> = (0..10).map(|k| format!("{p}-{}", i * 10 + k)).collect();
                let refs: Vec<&str> = vals.iter().map(String::as_str).collect();
                b.produce(&t, wire::ANY_PARTITION, &mut batch(&refs), true).unwrap();
            }
        }));
    }
    for p in producers {
        p.join().unwrap();
    }
    for c in consumers {
        c.await.unwrap();
    }
    let seen = seen.lock();
    let total: u64 = t.partitions.iter().map(|p| p.next_offset()).sum();
    assert_eq!(total as usize, PRODUCERS * EACH);
    assert_eq!(seen.len(), PRODUCERS * EACH, "every record delivered");
    assert!(seen.values().all(|n| *n == 1), "none delivered twice");
    assert_eq!(sub.backlog(), 0);
    // Offsets are dense per partition: nothing skipped, nothing doubled.
    for p in &t.partitions {
        let got = values(p, 0);
        assert_eq!(got.len() as u64, p.next_offset());
        assert!(got.iter().enumerate().all(|(i, (o, _))| *o == i as u64));
    }
    b.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_amqp_style_member_keeps_its_inflight_until_settled() {
    let b = Broker::open(None, "t").unwrap();
    let t = b.declare_queue("q", QueueConfig { durability: Durability::Memory, ..Default::default() }, false).unwrap();
    let (_, sub) = b.queue("q").unwrap();
    b.produce(&t, 0, &mut batch(&["a", "b"]), true).unwrap();
    let (tx, mut rx) = sub::outbox();
    let (m, _) = sub.join(1, 10, tx, &tokio::runtime::Handle::current()).unwrap();
    let got = drain(&mut rx, 2, Duration::from_secs(3)).await;
    assert_eq!(got.len(), 2);
    sub.leave_keeping_inflight(m);
    assert_eq!(sub.inflight(), 2, "still the channel's to settle");
    sub.ack(&[one(0, got[0].1)]).unwrap();
    sub.nack(&[one(0, got[1].1)], 0).unwrap();
    assert_eq!(sub.inflight(), 0);
    assert_eq!(sub.take(5, true, 0).unwrap().len(), 1, "the nacked one is back");
}

#[test]
fn a_queue_redeclared_with_other_settings_is_refused() {
    let b = Broker::open(None, "t").unwrap();
    let q = QueueConfig { durability: Durability::Memory, ack_timeout_ms: 300_000, ..Default::default() };
    b.declare_queue("jobs", q.clone(), true).unwrap();
    assert!(b.declare_queue("jobs", q.clone(), true).is_ok(), "the same settings declare again");
    let other = QueueConfig { ack_timeout_ms: 5_000, ..q };
    let e = b.declare_queue("jobs", other, true).unwrap_err();
    assert_eq!(e.code, Code::Mismatch);
    assert!(e.message.contains("ack_timeout_ms 300000 vs 5000"), "{}", e.message);
}

/// Power loss, modelled by `core::power_loss`: cuts inside the broker's own
/// file and directory syncs, imaged with unsynced pages kept, dropped and
/// torn. Every image opens, and every record acked durable before its cut is
/// there with its value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acked_records_survive_power_loss() {
    use crate::core::power_loss::failpoint;
    use crate::core::power_loss::image::{CrashPoint, Recorder};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let base = tempfile::tempdir().unwrap();
    let rec_dir = base.path().join("rec");
    let bdir = rec_dir.join("broker");
    std::fs::create_dir_all(&rec_dir).unwrap();
    let b = Broker::open(Some(&bdir), "t").unwrap();
    let t = b.create_topic("pl", config(2, Durability::Sync), false).unwrap();
    b.produce(&t, 0, &mut batch(&["before"]), true).unwrap();
    settle(t.partition(0).unwrap());

    let rec = Recorder::start(&rec_dir);
    // (topic, partition, offset, value) of every record acked durable.
    let acked: Arc<parking_lot::Mutex<Vec<(String, u32, u64, String)>>> = Arc::new(parking_lot::Mutex::new(vec![(
        "pl".into(),
        0,
        0,
        "before".into(),
    )]));
    let count = Arc::new(AtomicUsize::new(1));
    let sink: Arc<parking_lot::Mutex<Vec<(usize, Result<CrashPoint, String>)>>> = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut guards = Vec::new();
    for (site, n) in [("sync:broker.file", 24u64), ("sync:broker.dir", 4)] {
        for nth in 1..=n {
            let (rec, count, sink) = (rec.clone(), count.clone(), sink.clone());
            let action: failpoint::Action = Arc::new(move |hit: &failpoint::Hit<'_>| {
                // What was acked before this sync was issued must survive it.
                let n = count.load(Ordering::SeqCst);
                let label = format!("{} #{} on {}", hit.name, hit.nth, hit.path.display());
                sink.lock().push((n, rec.cut_in_sync(label)));
                failpoint::Outcome::Continue
            });
            guards.push(failpoint::arm(site, &rec_dir, nth, action));
        }
    }
    let record = |topic: &str, p: &partition::Partition, off: u64, v: &str| {
        acked.lock().push((topic.to_string(), p.index, off, v.to_string()));
        count.store(acked.lock().len(), Ordering::SeqCst);
    };
    for i in 0..16u64 {
        let v = format!("v{i}");
        let (p, a) = b.produce(&t, (i % 2) as u32, &mut batch(&[&v]), true).unwrap();
        settle(&p);
        record("pl", &p, a.base_offset, &v);
        if i == 6 {
            // A topic made mid-way: its directories and the metadata log.
            let t2 = b.create_topic("late", config(1, Durability::Sync), false).unwrap();
            let (p2, a2) = b.produce(&t2, 0, &mut batch(&["late-0"]), true).unwrap();
            settle(&p2);
            record("late", &p2, a2.base_offset, "late-0");
        }
    }
    let end_count = count.load(Ordering::SeqCst);
    sink.lock().push((end_count, rec.cut_quiescent("after the last ack")));
    drop(guards);
    b.shutdown();
    drop(b);
    drop(rec);

    let points: Vec<(usize, CrashPoint)> =
        sink.lock().drain(..).filter_map(|(n, p)| p.map_err(|why| eprintln!("{why}")).ok().map(|p| (n, p))).collect();
    assert!(points.len() >= 6, "too few cuts survived: {}", points.len());
    let acked = acked.lock().clone();
    let img_dir = base.path().join("img");
    let mut problems = Vec::new();
    let mut images = 0;
    for (n, point) in &points {
        for seed in 0..5 {
            let _ = std::fs::remove_dir_all(&img_dir);
            point.write_image(seed, &img_dir);
            images += 1;
            let label = format!("{} seed {seed} ({n} acked)", point.label);
            let opened = Broker::open(Some(&img_dir.join("broker")), "img");
            let ib = match opened {
                Ok(ib) => ib,
                Err(e) => {
                    problems.push(format!("{label}: does not open: {e}"));
                    continue;
                }
            };
            for (topic, p, off, v) in &acked[..*n] {
                let Some(it) = ib.topic(topic) else {
                    problems.push(format!("{label}: topic {topic} is gone"));
                    break;
                };
                let got = values(it.partition(*p).unwrap(), *off).into_iter().find(|(o, _)| o == off);
                if got.as_ref().map(|(_, x)| x.as_str()) != Some(v.as_str()) {
                    problems.push(format!("{label}: {topic}/{p}@{off} should be {v:?}, is {got:?}"));
                }
            }
            ib.shutdown();
        }
    }
    assert!(problems.is_empty(), "{} of {images} images lost acked records:\n{}", problems.len(), problems.join("\n"));
    eprintln!("{} cuts, {images} images, every acked record intact", points.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redeliveries_carry_their_delivery_count_when_asked() {
    let b = Broker::open(None, "t").unwrap();
    let q = QueueConfig { durability: Durability::Memory, max_deliveries: 3, ..Default::default() };
    let t = b.declare_queue("counted", q, false).unwrap();
    let (_, sub) = b.queue("counted").unwrap();
    b.produce(&t, 0, &mut batch(&["m"]), true).unwrap();
    let (tx, mut rx) = sub::outbox();
    let _ = sub.join_with(1, 10, wire::SUB_DELIVERY_COUNTS, tx, &tokio::runtime::Handle::current()).unwrap();
    let mut seen = Vec::new();
    for _ in 0..3 {
        let f = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        let h = wire::parse_header(&f).unwrap().unwrap();
        let mut r = wire::Reader::new(&f[wire::FRAME_HEADER..]);
        r.u32().unwrap();
        let ranges = wire::decode_ranges(&mut r, 16).unwrap();
        let counts = if h.flags & wire::F_COUNTS != 0 { wire::decode_counts(&mut r, 16).unwrap() } else { Vec::new() };
        let n = counts.iter().find(|c| c.offset == ranges[0].offset).map_or(1, |c| c.deliveries);
        seen.push(n);
        sub.nack(&ranges, 0).unwrap();
    }
    assert_eq!(seen, [1, 2, 3], "first unlisted, then 2 and 3 (the last before dead-lettering)");
}
