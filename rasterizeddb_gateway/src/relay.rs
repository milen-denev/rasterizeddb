//! G0's floor: a pure byte relay to one address, with the same listeners
//! and threads as the gateway and no parsing at all. Its cost per statement
//! is the floor B-1 measures the gateway against.

use std::time::Duration;

use crate::config::Config;

pub fn run(cfg: &Config, upstream: String) -> Result<(), String> {
    let workers = cfg.worker_count();
    let mut threads = Vec::new();
    for id in 0..workers {
        let listen = cfg.listen.clone();
        let upstream = upstream.clone();
        threads.push(std::thread::Builder::new().name(format!("relay-{id}")).spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return log::error!("relay core {id}: {e}"),
            };
            let local = tokio::task::LocalSet::new();
            local.block_on(&rt, async move {
                let mut accepts = Vec::new();
                for addr in listen {
                    let upstream = upstream.clone();
                    accepts.push(tokio::task::spawn_local(async move {
                        let std_listener = match super::bind_reuse_port(addr) {
                            Ok(l) => l,
                            Err(e) => return log::error!("relay listen {addr}: {e}"),
                        };
                        let Ok(listener) = tokio::net::TcpListener::from_std(std_listener) else { return };
                        loop {
                            let Ok((mut client, _)) = listener.accept().await else {
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            };
                            let upstream = upstream.clone();
                            tokio::task::spawn_local(async move {
                                let _ = client.set_nodelay(true);
                                let Ok(mut server) = tokio::net::TcpStream::connect(&upstream).await else { return };
                                let _ = server.set_nodelay(true);
                                let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                            });
                        }
                    }));
                }
                for a in accepts {
                    let _ = a.await;
                }
            });
        }).map_err(|e| e.to_string())?);
    }
    for t in threads {
        let _ = t.join();
    }
    Ok(())
}
