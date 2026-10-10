//! Commands about the server rather than a key: `INFO`, `CONFIG`, `COMMAND`,
//! `FLUSHALL`, `MOVE`, `LASTSAVE`. They need every database, the settings or
//! the snapshot, so they take the handle rather than one store.

use std::fmt::Write as _;
use std::sync::atomic::Ordering;

use super::commands::{self, Spec, TABLE, eq, parse_int};
use super::reply::Reply;
use super::{
    DATABASES, EvictionPolicy, KvCacheHandle, REDIS_VERSION, eviction_policy, human_bytes, max_memory,
    parse_bytes, persist, save_interval_secs, set_eviction_policy, set_max_memory, set_persist,
    set_save_interval_secs,
};

/// Runs a server-level command other than `SAVE`/`BGSAVE`, which the caller
/// runs off the async worker.
pub fn run<O: Reply>(spec: &Spec, handle: &KvCacheHandle, db: usize, args: &[&[u8]], out: &mut O) {
    use commands::Cmd::*;
    match spec.cmd {
        Info => info(handle, args, out),
        Config => config(handle, args, out),
        Command => command(args, out),
        LastSave => out.int(handle.inner().last_save.load(Ordering::Relaxed) as i64),
        FlushAll => {
            match args.get(1) {
                None => {}
                Some(m) if eq(m, b"ASYNC") || eq(m, b"SYNC") => {}
                Some(_) => return out.error(commands::SYNTAX),
            }
            for d in 0..DATABASES {
                if let Some(store) = handle.existing(d) {
                    store.flush();
                }
            }
            out.ok()
        }
        SwapDb => out.error("ERR SWAPDB is not supported by this server"),
        Move => move_key(handle, db, args, out),
        _ => out.error(&format!("ERR '{}' cannot run here", spec.name)),
    }
}

fn move_key<O: Reply>(handle: &KvCacheHandle, db: usize, args: &[&[u8]], out: &mut O) {
    let key = args[1];
    let target = match parse_int(args[2]) {
        Some(n) if (0..DATABASES as i64).contains(&n) => n as usize,
        _ => return out.error("ERR DB index is out of range"),
    };
    if target == db {
        return out.error("ERR source and destination objects are the same");
    }
    let (src, dst) = (handle.db(db), handle.db(target));
    let (hs, hd) = (src.hash(key), dst.hash(key));
    // Two stores: lock the lower-numbered database's shard first, the order
    // every MOVE takes, so two crossing MOVEs cannot deadlock.
    let (mut first, mut second);
    let (s, d) = if db < target {
        first = src.cell(hs).lock.write();
        second = dst.cell(hd).lock.write();
        (&mut *first, &mut *second)
    } else {
        first = dst.cell(hd).lock.write();
        second = src.cell(hs).lock.write();
        (&mut *second, &mut *first)
    };
    let now = super::store::now_ms();
    if s.live_mut(hs, key, now).is_none() || d.live_mut(hd, key, now).is_some() {
        return out.int(0);
    }
    let slot = s.remove(hs, key).expect("live");
    d.insert(hd, slot);
    out.int(1);
}

fn info<O: Reply>(handle: &KvCacheHandle, args: &[&[u8]], out: &mut O) {
    let inner = handle.inner();
    let wanted: Vec<String> = args[1..]
        .iter()
        .map(|a| String::from_utf8_lossy(a).to_ascii_lowercase())
        .collect();
    let all = wanted.is_empty() || wanted.iter().any(|w| w == "all" || w == "everything" || w == "default");
    let want = |section: &str| all || wanted.iter().any(|w| w == section);

    let mut totals = super::store::StoreStats::default();
    let mut per_db = Vec::new();
    for d in 0..DATABASES {
        if let Some(store) = handle.existing(d) {
            let s = store.stats();
            totals.keys += s.keys;
            totals.expires += s.expires;
            totals.used += s.used;
            totals.hits += s.hits;
            totals.misses += s.misses;
            totals.evicted += s.evicted;
            totals.expired += s.expired;
            if s.keys > 0 {
                per_db.push((d, s));
            }
        }
    }

    let mut text = String::new();
    let uptime = inner.started.elapsed().as_secs();
    if want("server") {
        let _ = write!(
            text,
            "# Server\r\nredis_version:{REDIS_VERSION}\r\nserver_name:rasterizeddb\r\nrasterizeddb_version:{}\r\n\
             redis_mode:standalone\r\nos:{} {}\r\narch_bits:64\r\nmultiplexing_api:tokio\r\n\
             process_id:{}\r\ntcp_port:{}\r\nuptime_in_seconds:{uptime}\r\nuptime_in_days:{}\r\n\r\n",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::OS,
            std::env::consts::ARCH,
            std::process::id(),
            inner.port.load(Ordering::Relaxed),
            uptime / 86_400,
        );
    }
    if want("clients") {
        let _ = write!(
            text,
            "# Clients\r\nconnected_clients:{}\r\nblocked_clients:{}\r\npubsub_patterns:{}\r\n\r\n",
            inner.stats.connected.load(Ordering::Relaxed),
            inner.stats.blocked.load(Ordering::Relaxed),
            inner.pubsub.numpat(),
        );
    }
    if want("memory") {
        let max = max_memory();
        let _ = write!(
            text,
            "# Memory\r\nused_memory:{}\r\nused_memory_human:{}\r\nmaxmemory:{max}\r\nmaxmemory_human:{}\r\n\
             maxmemory_policy:{}\r\nmem_accounting:estimated per key; see MEMORY USAGE\r\n\r\n",
            totals.used,
            human_bytes(totals.used),
            human_bytes(max),
            eviction_policy().name(),
        );
    }
    if want("persistence") {
        let _ = write!(
            text,
            "# Persistence\r\nloading:0\r\nrdb_changes_since_last_save:{}\r\nrdb_bgsave_in_progress:{}\r\n\
             rdb_last_save_time:{}\r\nrdb_last_bgsave_status:{}\r\naof_enabled:0\r\nkv_persist:{}\r\n\
             kv_save_interval_secs:{}\r\nkv_snapshot:{}\r\n\r\n",
            handle.changes_since_save(),
            u8::from(inner.saving.load(Ordering::Relaxed)),
            inner.last_save.load(Ordering::Relaxed),
            if inner.last_save_ok.load(Ordering::Relaxed) { "ok" } else { "err" },
            u8::from(persist()),
            save_interval_secs(),
            handle
                .snapshot_path()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "(in-memory database; SAVE is refused)".into()),
        );
    }
    if want("stats") {
        let _ = write!(
            text,
            "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\n\
             rejected_connections:{}\r\nexpired_keys:{}\r\nevicted_keys:{}\r\nkeyspace_hits:{}\r\n\
             keyspace_misses:{}\r\npubsub_channels:{}\r\npubsub_patterns:{}\r\n\r\n",
            inner.stats.connections_total.load(Ordering::Relaxed),
            inner.stats.commands(),
            inner.stats.rejected.load(Ordering::Relaxed),
            totals.expired,
            totals.evicted,
            totals.hits,
            totals.misses,
            inner.pubsub.channels(None).len(),
            inner.pubsub.numpat(),
        );
    }
    if want("replication") {
        text.push_str("# Replication\r\nrole:master\r\nconnected_slaves:0\r\n\r\n");
    }
    if want("keyspace") {
        text.push_str("# Keyspace\r\n");
        for (d, s) in per_db {
            let _ = write!(text, "db{d}:keys={},expires={},avg_ttl=0\r\n", s.keys, s.expires);
        }
        text.push_str("\r\n");
    }
    out.verbatim(text.trim_end_matches("\r\n"));
}

/// The `CONFIG GET` parameters and their current values.
fn parameters() -> Vec<(&'static str, String)> {
    vec![
        ("maxmemory", max_memory().to_string()),
        ("maxmemory-policy", eviction_policy().name().to_string()),
        ("maxmemory-samples", "5".into()),
        (
            "save",
            if persist() {
                format!("{} 1", save_interval_secs())
            } else {
                String::new()
            },
        ),
        ("appendonly", "no".into()),
        ("databases", DATABASES.to_string()),
        ("timeout", "0".into()),
        ("tcp-keepalive", "300".into()),
        ("maxclients", "10000".into()),
        ("notify-keyspace-events", String::new()),
        ("proto-max-bulk-len", "536870912".into()),
        ("lazyfree-lazy-user-del", "no".into()),
        ("cluster-enabled", "no".into()),
    ]
}

fn config<O: Reply>(handle: &KvCacheHandle, args: &[&[u8]], out: &mut O) {
    let sub = args[1];
    if eq(sub, b"GET") {
        if args.len() < 3 {
            return out.error("ERR wrong number of arguments for 'config|get' command");
        }
        let params = parameters();
        let mut hits: Vec<(&str, String)> = Vec::new();
        for pattern in &args[2..] {
            let pattern = pattern.to_ascii_lowercase();
            for (name, value) in &params {
                if super::glob::matches(&pattern, name.as_bytes()) && !hits.iter().any(|(n, _)| n == name) {
                    hits.push((name, value.clone()));
                }
            }
        }
        out.map(hits.len());
        for (name, value) in hits {
            out.bulk(name.as_bytes());
            out.bulk(value.as_bytes());
        }
    } else if eq(sub, b"SET") {
        if args.len() < 4 || args.len() % 2 != 0 {
            return out.error("ERR wrong number of arguments for 'config|set' command");
        }
        for pair in args[2..].chunks(2) {
            let name = String::from_utf8_lossy(pair[0]).to_ascii_lowercase();
            let value = String::from_utf8_lossy(pair[1]).to_string();
            if let Err(e) = set_parameter(&name, &value) {
                return out.error(&format!(
                    "ERR CONFIG SET failed (possibly related to argument '{name}') - {e}"
                ));
            }
        }
        out.ok()
    } else if eq(sub, b"RESETSTAT") {
        handle.inner().stats.reset();
        for d in 0..DATABASES {
            if let Some(store) = handle.existing(d) {
                store.reset_stats();
            }
        }
        out.ok()
    } else if eq(sub, b"REWRITE") {
        out.error("ERR The server is running without a config file")
    } else {
        out.error(&format!(
            "ERR unknown subcommand '{}'. Try CONFIG HELP.",
            String::from_utf8_lossy(sub)
        ))
    }
}

/// Applies one setting, from `CONFIG SET` or `SET CONFIG kv.*`.
pub fn set_parameter(name: &str, value: &str) -> Result<(), String> {
    match name {
        "maxmemory" | "max_memory" => set_max_memory(parse_bytes(value)?),
        "maxmemory-policy" | "eviction" => set_eviction_policy(EvictionPolicy::parse(value)?),
        "save" => {
            let v = value.trim();
            if v.is_empty() {
                set_persist(false);
            } else {
                let secs: u64 = v
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| format!("`{value}` is not `<seconds> <changes>`"))?;
                set_save_interval_secs(secs)?;
                set_persist(true);
            }
        }
        "persist" => set_persist(match value.trim().to_ascii_lowercase().as_str() {
            "on" | "true" | "yes" | "1" => true,
            "off" | "false" | "no" | "0" => false,
            other => return Err(format!("`{other}` is not on or off")),
        }),
        "save_interval_secs" => set_save_interval_secs(
            value
                .trim()
                .parse()
                .map_err(|_| format!("`{value}` is not a number of seconds"))?,
        )?,
        "appendonly" if value.eq_ignore_ascii_case("no") => {}
        "appendonly" => return Err("append-only persistence is not supported; use save".into()),
        "maxmemory-samples" | "timeout" | "tcp-keepalive" | "notify-keyspace-events"
        | "lazyfree-lazy-user-del" | "maxclients" => {
            // Accepted and ignored: clients set them at start-up, and refusing
            // would stop a client that only wanted to be sure of a default.
        }
        _ => return Err("Unknown option or number of arguments for CONFIG SET".into()),
    }
    Ok(())
}

fn flag_names(spec: &Spec) -> Vec<&'static str> {
    let mut f = Vec::new();
    if spec.is(commands::WRITE) {
        f.push("write");
    }
    if spec.is(commands::READONLY) {
        f.push("readonly");
    }
    if spec.is(commands::DENYOOM) {
        f.push("denyoom");
    }
    if spec.is(commands::ADMIN) {
        f.push("admin");
    }
    if spec.is(commands::PUBSUB) {
        f.push("pubsub");
    }
    if spec.is(commands::BLOCKING) {
        f.push("blocking");
    }
    if spec.is(commands::FAST) {
        f.push("fast");
    }
    if spec.is(commands::NO_MULTI) {
        f.push("no_multi");
    }
    if spec.is(commands::NO_AUTH) {
        f.push("no_auth");
    }
    f
}

fn command_info<O: Reply>(spec: &Spec, out: &mut O) {
    out.array(10);
    out.bulk(spec.name.as_bytes());
    out.int(spec.arity as i64);
    let flags = flag_names(spec);
    out.set(flags.len());
    for f in flags {
        out.simple(f);
    }
    out.int(spec.first as i64);
    out.int(spec.last as i64);
    out.int(spec.step as i64);
    out.set(0);
    out.set(0);
    out.array(0);
    out.array(0);
}

fn command<O: Reply>(args: &[&[u8]], out: &mut O) {
    let Some(sub) = args.get(1) else {
        out.array(TABLE.len());
        for spec in TABLE {
            command_info(spec, out);
        }
        return;
    };
    if eq(sub, b"COUNT") {
        out.int(TABLE.len() as i64)
    } else if eq(sub, b"LIST") {
        out.array(TABLE.len());
        for spec in TABLE {
            out.bulk(spec.name.as_bytes());
        }
    } else if eq(sub, b"INFO") {
        let names = &args[2..];
        if names.is_empty() {
            out.array(TABLE.len());
            for spec in TABLE {
                command_info(spec, out);
            }
            return;
        }
        out.array(names.len());
        for name in names {
            match commands::lookup(name) {
                Some(spec) => command_info(spec, out),
                None => out.null_array(),
            }
        }
    } else if eq(sub, b"DOCS") {
        // No docs are shipped. An empty map is the documented answer for
        // names it has none for, and is what `redis-cli` handles.
        out.map(0)
    } else if eq(sub, b"GETKEYS") {
        let Some(name) = args.get(2) else {
            return out.error("ERR wrong number of arguments for 'command|getkeys' command");
        };
        let Some(spec) = commands::lookup(name) else {
            return out.error("ERR Invalid command specified");
        };
        let call = &args[2..];
        if !spec.arity_ok(call.len()) {
            return out.error("ERR Invalid number of arguments specified for command");
        }
        let keys = spec.keys(call);
        if keys.is_empty() {
            return out.error("ERR The command has no key arguments");
        }
        out.array(keys.len());
        for k in keys {
            out.bulk(k);
        }
    } else {
        out.error(&format!(
            "ERR unknown subcommand '{}'. Try COMMAND HELP.",
            String::from_utf8_lossy(sub)
        ))
    }
}
