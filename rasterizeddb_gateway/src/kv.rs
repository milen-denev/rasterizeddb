//! The Redis protocol through the gateway (sharding-plan.md B4): each
//! command goes to the shard its key hashes to, by the same routing function
//! as rows (`route_v1`, the key's bytes in the text family), so one key
//! lives on one shard's cache.
//!
//! `--kv-listen ADDR:PORT` and `--kv-shards ID=ADDR:PORT,…`, each shard's
//! `--kv-port`. Per client connection, the gateway keeps one connection per
//! shard, opened when first needed, and replays the client's `AUTH`/`HELLO`
//! on it.
//!
//! - A command whose keys all hash to one shard is relayed there as bytes,
//!   and its one reply relayed back.
//! - A multi-key command whose keys span shards is refused (`-CROSSSLOT`).
//! - Keyless commands that read or change a whole server (`KEYS`, `SCAN`,
//!   `FLUSHALL`, `INFO`, `CONFIG`, pub/sub, `MULTI`) are refused: they would
//!   answer for one shard only. `PING`, `ECHO` and `QUIT` are answered here.
//! - Moving a range between shards does not move cache entries: the store
//!   is a cache, and a moved key starts empty on its new shard.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use rasterizeddb_router::key::{KeyFamily, level_hash};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use crate::state::Shared;

/// The longest command the gateway relays (64 MiB).
const MAX_COMMAND: usize = 64 << 20;

/// Which arguments of a command are keys: `(first, step, last)`, `last`
/// negative counting from the end (Redis's own key specs, simplified).
fn key_spec(cmd: &str) -> Option<(usize, usize, isize)> {
    Some(match cmd {
        "GET" | "SET" | "SETNX" | "SETEX" | "PSETEX" | "GETSET" | "GETDEL" | "GETEX" | "INCR" | "DECR" | "INCRBY"
        | "DECRBY" | "INCRBYFLOAT" | "APPEND" | "STRLEN" | "GETRANGE" | "SETRANGE" | "EXPIRE" | "PEXPIRE"
        | "EXPIREAT" | "PEXPIREAT" | "TTL" | "PTTL" | "PERSIST" | "TYPE" | "HSET" | "HGET" | "HDEL" | "HGETALL"
        | "HMSET" | "HMGET" | "HINCRBY" | "HINCRBYFLOAT" | "HKEYS" | "HVALS" | "HLEN" | "HEXISTS" | "HSETNX"
        | "LPUSH" | "RPUSH" | "LPOP" | "RPOP" | "LLEN" | "LRANGE" | "LINDEX" | "LSET" | "LREM" | "LTRIM"
        | "SADD" | "SREM" | "SMEMBERS" | "SISMEMBER" | "SCARD" | "SPOP" | "SRANDMEMBER" | "ZADD" | "ZREM"
        | "ZRANGE" | "ZREVRANGE" | "ZSCORE" | "ZCARD" | "ZINCRBY" | "ZRANK" | "ZREVRANK" | "ZRANGEBYSCORE"
        | "ZCOUNT" | "ZREMRANGEBYSCORE" | "ZREMRANGEBYRANK" => (1, 1, 1),
        "DEL" | "UNLINK" | "EXISTS" | "MGET" | "TOUCH" => (1, 1, -1),
        "MSET" | "MSETNX" => (1, 2, -1),
        _ => return None,
    })
}

/// Answered at the gateway.
fn local(cmd: &str) -> bool {
    matches!(cmd, "PING" | "ECHO" | "QUIT")
}

/// Carries no key but must reach every shard connection: credentials.
fn session(cmd: &str) -> bool {
    matches!(cmd, "AUTH" | "HELLO" | "SELECT" | "CLIENT")
}

/// The shard a key's bytes hash to.
fn shard_of_key(shared: &Shared, key: &[u8]) -> Result<u32, String> {
    let h = level_hash(0, KeyFamily::Text, key)?;
    Ok(shared.map.get().shard_of_block(h >> 16))
}

/// Reads one RESP value's bytes from `r` into `out` (whole, nested).
fn read_value<'a, R: tokio::io::AsyncBufRead + Unpin + Send + 'a>(
    r: &'a mut R,
    out: &'a mut Vec<u8>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        use tokio::io::AsyncBufReadExt;
        let start = out.len();
        let n = r.read_until(b'\n', out).await?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let line = &out[start..];
        if line.len() < 3 || !line.ends_with(b"\r\n") {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "a malformed RESP line"));
        }
        let tag = line[0];
        let num = || -> std::io::Result<i64> {
            std::str::from_utf8(&line[1..line.len() - 2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "a malformed RESP length"))
        };
        match tag {
            b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' => Ok(()),
            b'$' | b'!' | b'=' => {
                let len = num()?;
                if len < 0 {
                    return Ok(());
                }
                let len = len as usize;
                if len > MAX_COMMAND {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "a RESP string past the limit"));
                }
                let at = out.len();
                out.resize(at + len + 2, 0);
                r.read_exact(&mut out[at..]).await?;
                Ok(())
            }
            b'*' | b'~' | b'>' | b'%' | b'|' => {
                let n = num()?;
                if n < 0 {
                    return Ok(());
                }
                let items = if tag == b'%' || tag == b'|' { 2 * n } else { n };
                for _ in 0..items {
                    read_value(r, out).await?;
                }
                Ok(())
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "an unknown RESP type")),
        }
    })
}

/// A command's arguments, from its RESP array bytes.
fn args_of(bytes: &[u8]) -> Option<Vec<&[u8]>> {
    let mut out = Vec::new();
    let mut i = bytes.iter().position(|&b| b == b'\n')? + 1;
    if bytes.first() != Some(&b'*') {
        return None;
    }
    while i < bytes.len() {
        if bytes[i] != b'$' {
            return None;
        }
        let nl = i + bytes[i..].iter().position(|&b| b == b'\n')?;
        let len: usize = std::str::from_utf8(&bytes[i + 1..nl - 1]).ok()?.parse().ok()?;
        let s = nl + 1;
        out.push(bytes.get(s..s + len)?);
        i = s + len + 2;
    }
    Some(out)
}

pub async fn serve(shared: Arc<Shared>, addr: SocketAddr, shards: BTreeMap<u32, String>) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            log::error!("kv: cannot listen on {addr}: {e}");
            return;
        }
    };
    log::info!("kv (Redis protocol) listening on {addr}");
    let shards = Arc::new(shards);
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            continue;
        };
        let (shared, shards) = (shared.clone(), shards.clone());
        tokio::spawn(async move {
            if let Err(e) = client(shared, shards, sock).await {
                log::debug!("kv client ended: {e}");
            }
        });
    }
}

struct Upstream {
    r: BufReader<tokio::net::tcp::OwnedReadHalf>,
    w: tokio::net::tcp::OwnedWriteHalf,
}

async fn client(shared: Arc<Shared>, shards: Arc<BTreeMap<u32, String>>, sock: TcpStream) -> std::io::Result<()> {
    let (rd, mut wr) = sock.into_split();
    let mut rd = BufReader::new(rd);
    let mut ups: BTreeMap<u32, Upstream> = BTreeMap::new();
    // Session commands replayed on each new upstream, in order.
    let mut replay: Vec<Vec<u8>> = Vec::new();
    loop {
        let mut cmd = Vec::new();
        match read_value(&mut rd, &mut cmd).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }
        let Some(args) = args_of(&cmd) else {
            wr.write_all(b"-ERR the gateway accepts commands as RESP arrays only\r\n").await?;
            continue;
        };
        let Some(name) = args.first().map(|a| String::from_utf8_lossy(a).to_ascii_uppercase()) else {
            continue;
        };
        if local(&name) {
            match name.as_str() {
                "PING" => match args.get(1) {
                    Some(m) => wr.write_all(&bulk(m)).await?,
                    None => wr.write_all(b"+PONG\r\n").await?,
                },
                "ECHO" => wr.write_all(&args.get(1).map(|m| bulk(m)).unwrap_or_else(|| b"-ERR wrong number of arguments\r\n".to_vec())).await?,
                _ => {
                    wr.write_all(b"+OK\r\n").await?;
                    return Ok(());
                }
            }
            continue;
        }
        if session(&name) {
            // Checked by the first shard it reaches; replayed on the others.
            let target = match ups.keys().next().copied().or_else(|| shards.keys().next().copied()) {
                Some(s) => s,
                None => {
                    wr.write_all(b"-ERR the gateway knows no kv shard (--kv-shards)\r\n").await?;
                    continue;
                }
            };
            let reply = match relay(&mut ups, &shards, &replay, target, &cmd).await {
                Ok(r) => r,
                Err(e) => format!("-ERR shard {target}: {e}\r\n").into_bytes(),
            };
            if !reply.starts_with(b"-") {
                replay.push(cmd.clone());
                // The connections already open get it too.
                for id in ups.clone_keys() {
                    if id != target {
                        let _ = relay(&mut ups, &shards, &[], id, &cmd).await;
                    }
                }
            }
            wr.write_all(&reply).await?;
            continue;
        }
        let Some((first, step, last)) = key_spec(&name) else {
            wr.write_all(format!("-ERR {name} is not available through the gateway: it has no key to route by\r\n").as_bytes()).await?;
            continue;
        };
        let end = if last < 0 { (args.len() as isize + last + 1).max(0) as usize } else { (last as usize + 1).min(args.len()) };
        let mut target: Option<u32> = None;
        let mut cross = false;
        let mut bad = None;
        let mut i = first;
        while i < end {
            match shard_of_key(&shared, args[i]) {
                Ok(s) if target.is_none_or(|t| t == s) => target = Some(s),
                Ok(_) => cross = true,
                Err(e) => bad = Some(e),
            }
            i += step;
        }
        if let Some(e) = bad {
            wr.write_all(format!("-ERR {e}\r\n").as_bytes()).await?;
            continue;
        }
        if cross {
            wr.write_all(b"-CROSSSLOT Keys in request don't hash to the same shard\r\n").await?;
            continue;
        }
        let Some(target) = target else {
            wr.write_all(b"-ERR wrong number of arguments\r\n").await?;
            continue;
        };
        let reply = match relay(&mut ups, &shards, &replay, target, &cmd).await {
            Ok(r) => r,
            Err(e) => {
                ups.remove(&target);
                format!("-ERR shard {target}: {e}\r\n").into_bytes()
            }
        };
        wr.write_all(&reply).await?;
    }
}

trait CloneKeys {
    fn clone_keys(&self) -> Vec<u32>;
}

impl CloneKeys for BTreeMap<u32, Upstream> {
    fn clone_keys(&self) -> Vec<u32> {
        self.keys().copied().collect()
    }
}

fn bulk(b: &[u8]) -> Vec<u8> {
    let mut out = format!("${}\r\n", b.len()).into_bytes();
    out.extend_from_slice(b);
    out.extend_from_slice(b"\r\n");
    out
}

/// Sends `cmd` to shard `id` (connecting, and replaying `replay`, first if
/// needed) and returns its one reply.
async fn relay(
    ups: &mut BTreeMap<u32, Upstream>,
    shards: &BTreeMap<u32, String>,
    replay: &[Vec<u8>],
    id: u32,
    cmd: &[u8],
) -> std::io::Result<Vec<u8>> {
    if !ups.contains_key(&id) {
        let addr = shards
            .get(&id)
            .ok_or_else(|| std::io::Error::other(format!("no --kv-shards address for shard {id}")))?;
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true)?;
        let (r, w) = sock.into_split();
        let mut up = Upstream { r: BufReader::new(r), w };
        for c in replay {
            up.w.write_all(c).await?;
            let mut ignored = Vec::new();
            read_value(&mut up.r, &mut ignored).await?;
        }
        ups.insert(id, up);
    }
    let up = ups.get_mut(&id).expect("inserted above");
    up.w.write_all(cmd).await?;
    let mut reply = Vec::new();
    read_value(&mut up.r, &mut reply).await?;
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_arguments_and_key_positions() {
        let cmd = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhe\r\nl\r\n";
        let args = args_of(cmd).unwrap();
        assert_eq!(args, vec![&b"SET"[..], b"k", b"he\r\nl"], "a value may hold CRLF");
        assert!(args_of(b"*1\r\n$9\r\nSHORT\r\n").is_none(), "a length past the end");
        assert!(args_of(b"PING\r\n").is_none());
        assert_eq!(key_spec("MSET"), Some((1, 2, -1)), "every other argument a key");
        assert_eq!(key_spec("DEL"), Some((1, 1, -1)));
        assert_eq!(key_spec("FLUSHALL"), None);
        assert!(local("PING") && session("AUTH") && !local("GET"));
    }
}
