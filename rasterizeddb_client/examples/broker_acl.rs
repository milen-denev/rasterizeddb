//! One broker operation as one role, for permission checks: prints `OK …`
//! or `ERR <code> <message>` and exits 0 either way (1 only on bad usage).
//!
//! ```text
//! broker_acl ADDR USER PASSWORD create-topic NAME
//! broker_acl ADDR USER PASSWORD delete-topic NAME
//! broker_acl ADDR USER PASSWORD produce NAME
//! broker_acl ADDR USER PASSWORD fetch NAME            # partition 0 from 0
//! broker_acl ADDR USER PASSWORD subscribe NAME SUB    # ordered; counts what arrives in 2 s
//! broker_acl ADDR USER PASSWORD list                  # topic names, comma-separated
//! broker_acl ADDR USER PASSWORD declare-exchange NAME
//! broker_acl ADDR USER PASSWORD delete-exchange NAME
//! broker_acl ADDR USER PASSWORD publish EXCHANGE KEY
//! broker_acl ADDR USER PASSWORD bind EXCHANGE TOPIC
//! broker_acl ADDR USER PASSWORD revoke-wait NAME SUB  # subscribe, then credit every 200 ms until refused
//! ```

use std::time::{Duration, Instant};

use rasterizeddb_client::broker::*;

fn report<T: std::fmt::Debug>(r: Result<T>) {
    match r {
        Ok(v) => println!("OK {v:?}"),
        Err(e) => println!("ERR {:?} {}", e.code, e.message),
    }
}

fn batch() -> Vec<u8> {
    let mut b = BatchBuilder::new();
    b.push(Some(b"k"), b"acl-probe", &[]).expect("one record");
    b.finish()
}

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 5 {
        eprintln!("usage: broker_acl ADDR USER PASSWORD OP [ARGS]");
        std::process::exit(1);
    }
    let broker = match Broker::connect(&a[1], Options::password(&a[2], &a[3])).await {
        Ok(b) => b,
        Err(e) => {
            println!("ERR {:?} {}", e.code, e.message);
            return;
        }
    };
    let arg = |i: usize| a.get(i).cloned().unwrap_or_default();
    match a[4].as_str() {
        "create-topic" => report(broker.create_topic(&arg(5), &TopicConfig { partitions: 1, ..Default::default() }, false).await.map(|t| t.id)),
        "delete-topic" => report(broker.delete_topic(&arg(5), false).await),
        "produce" => {
            let r = async {
                let t = broker.topic(&arg(5)).await?;
                broker.produce(&t, 0, &batch(), Ack::Written).await.map(|p| p.base_offset)
            };
            report(r.await)
        }
        "fetch" => {
            let r = async {
                let t = broker.topic(&arg(5)).await?;
                broker.fetch(&t, 0, 0, 1 << 20, Duration::ZERO).await.map(|f| f.records().count())
            };
            report(r.await)
        }
        "subscribe" => {
            let r = async {
                let t = broker.topic(&arg(5)).await?;
                let mut s = broker.subscribe(&t, &arg(6), SubConfig::default(), 1000).await?;
                let mut n = 0u64;
                let end = Instant::now() + Duration::from_secs(2);
                while let Ok(Some(d)) = tokio::time::timeout_at(end.into(), s.next()).await {
                    n += d?.count();
                }
                Ok(n)
            };
            report(r.await)
        }
        "list" => report(
            broker
                .list_topics()
                .await
                .map(|v| v.into_iter().map(|t| t.name).filter(|n| !n.starts_with('$')).collect::<Vec<_>>().join(",")),
        ),
        "declare-exchange" => report(broker.declare_exchange(&arg(5), ExchangeKind::Fanout, true, None).await),
        "delete-exchange" => report(broker.delete_exchange(&arg(5), false).await),
        "publish" => report(broker.publish(&arg(5), &arg(6), &batch(), Ack::Written, false).await),
        "bind" => report(
            broker
                .bind(&Binding { exchange: arg(5), topic: arg(6), key: String::new(), x_match: XMatch::All, args: Vec::new() })
                .await,
        ),
        "revoke-wait" => {
            // Subscribed while allowed; told to stop at the first credit
            // after the grant goes. Prints how long that took.
            let r = async {
                let t = broker.topic(&arg(5)).await?;
                let s = broker.subscribe(&t, &arg(6), SubConfig::default(), 1).await?;
                println!("SUBSCRIBED");
                let start = Instant::now();
                loop {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    s.credit(1)?;
                    // A refused credit comes back as an error on the next
                    // request on the connection.
                    if let Err(e) = broker.list_subscriptions(&t).await {
                        return Ok::<_, BrokerError>(format!("refused after {:?}: {}", start.elapsed(), e.message));
                    }
                    if start.elapsed() > Duration::from_secs(30) {
                        return Ok("never refused".to_string());
                    }
                }
            };
            report(r.await)
        }
        other => {
            eprintln!("unknown op {other}");
            std::process::exit(1);
        }
    }
}
