//! Replication check: what one node's broker does, the other node has.
//!
//! ```text
//! broker_repl NODE1_ADDR NODE2_ADDR USER PASSWORD [records=20000]
//! ```
//!
//! On node one: a topic (3 partitions), a queue bound to an exchange, keyed
//! records, an ordered subscription's commits, and half the queue consumed
//! and acked. Then node two must have, within a minute: the topics, the same
//! bytes at the same offsets in every partition, and the same backlog in
//! each subscription.

use std::time::{Duration, Instant};

use rasterizeddb_client::broker::*;

fn arg(args: &[String], name: &str, default: u64) -> u64 {
    args.iter().find_map(|a| a.strip_prefix(&format!("{name}=")).and_then(|v| v.parse().ok())).unwrap_or(default)
}

async fn dump(b: &Broker, t: &TopicRef) -> Result<Vec<Vec<(u64, Vec<u8>)>>> {
    let mut parts = Vec::new();
    for p in 0..t.partitions {
        let mut at = 0;
        let mut recs = Vec::new();
        loop {
            let f = b.fetch(t, p, at, 4 << 20, Duration::ZERO).await?;
            if f.data.is_empty() {
                break;
            }
            for r in f.records() {
                recs.push((r.offset, r.value.to_vec()));
            }
            at = f.next_fetch();
        }
        parts.push(recs);
    }
    Ok(parts)
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let a: Vec<String> = std::env::args().collect();
    let n = arg(&a, "records", 20_000);
    let one = Broker::connect(&a[1], Options::password(&a[3], &a[4])).await?;
    let topic = one.create_topic("rep", &TopicConfig { partitions: 3, durability: Durability::Sync, ..Default::default() }, false).await?;
    let queue = one.declare_queue("rep-jobs", &QueueConfig { durability: Durability::Sync, ..Default::default() }, false).await?;
    one.declare_exchange("rep-x", ExchangeKind::Topic, true, None).await?;
    one.bind(&Binding { exchange: "rep-x".into(), topic: "rep-jobs".into(), key: "job.#".into(), x_match: XMatch::All, args: vec![] }).await?;
    let producer = one.producer(topic.clone(), ProducerOptions::default());
    let mut sends = Vec::new();
    for i in 0..n {
        sends.push(producer.send(Some(format!("k{}", i % 101).as_bytes()), format!("v{i}").as_bytes(), &[]));
    }
    for s in sends {
        s.await?;
    }
    for i in 0..200u32 {
        let mut b = BatchBuilder::new();
        b.push(None, format!("job-{i}").as_bytes(), &[])?;
        one.publish("rep-x", "job.new", &b.finish(), Ack::Durable, true).await?;
    }
    // An ordered reader commits partway; the queue is half consumed.
    let mut reader = one.subscribe(&topic, "readers", SubConfig { mode: SubMode::Ordered, ..Default::default() }, 1000).await?;
    for p in 0..3 {
        reader.commit(p, 100).await?;
    }
    let mut jobs = one.consume("rep-jobs", 100).await?;
    let mut acked = 0;
    while acked < 100 {
        let d = tokio::time::timeout(Duration::from_secs(10), jobs.next()).await?.ok_or("queue closed")??;
        let take: Vec<Range> = d.ranges.iter().copied().collect();
        acked += d.count();
        jobs.ack(&take)?;
    }
    drop(jobs);
    drop(reader);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let want = dump(&one, &topic).await?;
    let want_q = dump(&one, &queue).await?;
    let subs_one = one.list_subscriptions(&topic).await?;
    let q_one = one.list_subscriptions(&queue).await?;
    println!(
        "node one: {} records over 3 partitions, queue {} records; readers backlog {}, queue backlog {}",
        want.iter().map(Vec::len).sum::<usize>(),
        want_q[0].len(),
        subs_one.iter().find(|s| s.name == "readers").map_or(0, |s| s.backlog),
        q_one[0].backlog
    );

    let two = Broker::connect(&a[2], Options::password(&a[3], &a[4])).await?;
    let start = Instant::now();
    let mut last = String::new();
    loop {
        let ok = async {
            let t2 = two.topic("rep").await.ok()?;
            let q2 = two.topic("rep-jobs").await.ok()?;
            let got = dump(&two, &t2).await.ok()?;
            let got_q = dump(&two, &q2).await.ok()?;
            let subs = two.list_subscriptions(&t2).await.ok()?;
            let qs = two.list_subscriptions(&q2).await.ok()?;
            let r1 = subs_one.iter().find(|s| s.name == "readers")?.backlog;
            let r2 = subs.iter().find(|s| s.name == "readers")?.backlog;
            let same = got == want && got_q == want_q && r1 == r2 && qs.first()?.backlog == q_one[0].backlog;
            Some((same, format!("records {} vs {}, readers backlog {r2} vs {r1}, queue backlog {} vs {}",
                got.iter().map(Vec::len).sum::<usize>(), want.iter().map(Vec::len).sum::<usize>(), qs.first()?.backlog, q_one[0].backlog)))
        }
        .await;
        match ok {
            Some((true, s)) => {
                println!("node two caught up in {:.1}s: {s}", start.elapsed().as_secs_f64());
                break;
            }
            Some((false, s)) => last = s,
            None => last = "topics not there yet".into(),
        }
        if start.elapsed() > Duration::from_secs(60) {
            println!("FAIL node two did not converge: {last}");
            std::process::exit(1);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // The exchange routes on node two too.
    let mut b = BatchBuilder::new();
    b.push(None, b"on-two", &[])?;
    let routed = two.publish("rep-x", "job.again", &b.finish(), Ack::Durable, true).await?;
    println!("PASS replication ({routed} route on node two)");
    Ok(())
}
