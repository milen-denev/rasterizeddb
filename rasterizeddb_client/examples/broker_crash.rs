//! Crash check for the broker: every acknowledged record survives a SIGKILL.
//!
//! ```text
//! broker_crash ADDR USER PASSWORD produce ACKED_FILE START_SEQ   # until the server dies
//! broker_crash ADDR USER PASSWORD verify  ACKED_FILE
//! ```
//!
//! `produce` sends records `seq-<n>` from `START_SEQ` on to topic
//! `crash` (3 partitions, sync durability) through a batching producer with
//! durable acks, and appends `seq partition offset` to `ACKED_FILE` for each
//! ack, until the connection fails. `verify` reads every partition back and
//! checks that each acked record is at its offset with its value.

use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use rasterizeddb_client::broker::*;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let a: Vec<String> = std::env::args().collect();
    let broker = Broker::connect(&a[1], Options::password(&a[2], &a[3])).await?;
    let config = TopicConfig { partitions: 3, durability: Durability::Sync, ..Default::default() };
    let topic = broker.create_topic("crash", &config, true).await?;
    let file = &a[5];
    if a[4] == "produce" {
        let start: u64 = a[6].parse()?;
        let mut out = std::io::BufWriter::new(std::fs::OpenOptions::new().create(true).append(true).open(file)?);
        let producer = broker.producer(
            topic.clone(),
            ProducerOptions { batch_bytes: 32 * 1024, linger: Duration::from_millis(1), ack: Ack::Durable, idempotent: false },
        );
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(u64, u32, Result<u64>)>();
        let writer = tokio::spawn(async move {
            let mut acked = 0u64;
            let mut failed = 0u64;
            while let Some((seq, p, r)) = rx.recv().await {
                match r {
                    Ok(off) => {
                        writeln!(out, "{seq} {p} {off}").unwrap();
                        acked += 1;
                    }
                    Err(_) => failed += 1,
                }
            }
            out.flush().unwrap();
            (acked, failed)
        });
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(20_000));
        let mut seq = start;
        loop {
            let permit = sem.clone().acquire_owned().await?;
            let p = (seq % 3) as u32;
            let fut = producer.send_to(p, None, format!("seq-{seq}").as_bytes(), &[]);
            let tx = tx.clone();
            let s = seq;
            tokio::spawn(async move {
                let r = fut.await;
                let dead = r.is_err();
                let _ = tx.send((s, p, r));
                drop(permit);
                if dead {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            });
            seq += 1;
            if seq % 1000 == 0 {
                tokio::task::yield_now().await;
                if broker.ping().await.is_err() {
                    break;
                }
            }
        }
        // Let the in-flight sends fail or land.
        tokio::time::sleep(Duration::from_secs(2)).await;
        drop(tx);
        let (acked, failed) = writer.await?;
        println!("produced up to seq {seq}: {acked} acked, {failed} failed");
        return Ok(());
    }
    // verify
    let mut stored: HashMap<(u32, u64), Vec<u8>> = HashMap::new();
    for p in 0..3u32 {
        let mut at = 0u64;
        loop {
            let f = broker.fetch(&topic, p, at, 4 << 20, Duration::ZERO).await?;
            if f.data.is_empty() {
                break;
            }
            for r in f.records() {
                stored.insert((p, r.offset), r.value.to_vec());
            }
            at = f.next_fetch();
        }
    }
    let text = std::fs::read_to_string(file)?;
    let (mut ok, mut missing, mut wrong) = (0u64, 0u64, 0u64);
    for line in text.lines() {
        let mut it = line.split(' ');
        let (Some(seq), Some(p), Some(off)) = (it.next(), it.next(), it.next()) else { continue };
        let (p, off): (u32, u64) = (p.parse()?, off.parse()?);
        match stored.get(&(p, off)) {
            None => missing += 1,
            Some(v) if v == format!("seq-{seq}").as_bytes() => ok += 1,
            Some(_) => wrong += 1,
        }
    }
    println!("verify: {ok} acked records intact, {missing} missing, {wrong} wrong; {} stored in all", stored.len());
    if missing > 0 || wrong > 0 {
        std::process::exit(1);
    }
    Ok(())
}
