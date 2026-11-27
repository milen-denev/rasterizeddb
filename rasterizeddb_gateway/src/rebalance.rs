//! Rebalancing the cluster from a client (the studio's Shards tab): the plan
//! the balancer would run, and a run of it in the background.
//!
//! The moves themselves are `rdb-shard rebalance --apply --online`'s: each
//! range copied at one snapshot, verified, and handed off one map epoch up
//! (module docs of `rdb-shard`). The gateway runs that tool as a child
//! process, one run at a time, keeps the last lines it printed and how it
//! ended, and picks the new map up from the shards as any epoch change.
//!
//! ```text
//! rebalance_tool = /opt/rdb-cluster/bin/rdb-shard
//! rebalance_roles = rasterized            # who may plan and run it
//! rebalance_user = rasterized             # the tool's login on each shard
//! rebalance_password_env = RDB_PW         # the environment variable holding its password
//! rebalance_native = 0=127.0.0.1:61201, 1=127.0.0.1:61202
//! rebalance_extra = --accept-any          # more rdb-shard flags
//! rebalance_tolerance = 0.10
//! rebalance_max_moves = 4
//! ```

use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::sync::Mutex;

use rasterizeddb_router::balance::{self, BalanceConfig, PartitionReport};
use rasterizeddb_router::shard_map::ShardMap;

use crate::config::Config;

/// Lines of the tool's output kept for `SHOW REBALANCE STATUS`.
const LOG_LINES: usize = 400;

struct Job {
    command: String,
    started: u64,
    finished: Option<u64>,
    exit: Option<i32>,
    log: VecDeque<String>,
}

static JOB: Mutex<Option<Job>> = Mutex::new(None);

fn now_secs() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn push_line(line: String) {
    if let Some(job) = JOB.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
        if job.log.len() == LOG_LINES {
            job.log.pop_front();
        }
        job.log.push_back(line);
    }
}

/// Where a run keeps its state files and the map it starts from.
pub fn state_dir(cfg: &Config) -> std::path::PathBuf {
    cfg.config_dir.clone().unwrap_or_else(std::env::temp_dir).join("rebalance")
}

/// The arguments `rdb-shard` runs with: plan from `current` (the map the
/// gateway holds now, which the map file may lag), apply online, and write
/// the next map to the gateway's map file.
pub fn tool_args(cfg: &Config, current: &str, max_moves: Option<usize>) -> Result<Vec<String>, String> {
    let map = cfg.map_file.clone().ok_or("rebalancing writes the next map to the gateway's map file: set map (a file), not map_from")?;
    let state_dir = state_dir(cfg).to_string_lossy().into_owned();
    let mut args = vec![
        "rebalance".to_string(),
        "--map".into(),
        current.to_string(),
        "--apply".into(),
        "--online".into(),
        "--out".into(),
        map,
        "--state-dir".into(),
        state_dir,
        "--tolerance".into(),
        cfg.rebalance_tolerance.to_string(),
        "--max-moves".into(),
        max_moves.unwrap_or(cfg.rebalance_max_moves).to_string(),
    ];
    if !cfg.rebalance_user.is_empty() {
        args.push("--user".into());
        args.push(cfg.rebalance_user.clone());
    }
    for n in &cfg.rebalance_native {
        args.push("--native".into());
        args.push(n.clone());
    }
    args.extend(cfg.rebalance_extra.iter().cloned());
    Ok(args)
}

/// Starts a run; refused while one runs, or when it is not configured.
/// The line returned says what was started.
pub fn start(cfg: &Config, current: &ShardMap, max_moves: Option<usize>) -> Result<String, String> {
    let tool = cfg.rebalance_tool.clone().ok_or("rebalancing is not configured on this gateway: set rebalance_tool (the rdb-shard binary)")?;
    let mut guard = JOB.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(job) = guard.as_ref()
        && job.finished.is_none()
    {
        return Err(format!("a rebalance started at {} is still running: SHOW REBALANCE STATUS", job.started));
    }
    let dir = state_dir(cfg);
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let start_map = dir.join(format!("start-epoch-{}.map", current.epoch()));
    std::fs::write(&start_map, current.to_text()).map_err(|e| format!("{}: {e}", start_map.display()))?;
    let args = tool_args(cfg, &start_map.to_string_lossy(), max_moves)?;
    let mut command = std::process::Command::new(&tool);
    command.args(&args).stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped());
    if !cfg.rebalance_user.is_empty() {
        let password = std::env::var(&cfg.rebalance_password_env)
            .map_err(|_| format!("rebalance_user needs its password in the gateway's environment variable {}", cfg.rebalance_password_env))?;
        command.env("RDB_PASSWORD", password);
    }
    let mut child = command.spawn().map_err(|e| format!("{tool}: {e}"))?;
    let shown = format!("{tool} {}", args.join(" "));
    *guard = Some(Job { command: shown.clone(), started: now_secs(), finished: None, exit: None, log: VecDeque::new() });
    drop(guard);
    log::warn!("rebalance started: {shown}");
    let readers: Vec<Box<dyn std::io::Read + Send>> = [
        child.stdout.take().map(|s| Box::new(s) as Box<dyn std::io::Read + Send>),
        child.stderr.take().map(|s| Box::new(s) as Box<dyn std::io::Read + Send>),
    ]
    .into_iter()
    .flatten()
    .collect();
    let mut threads = Vec::new();
    for r in readers {
        threads.push(std::thread::spawn(move || {
            for line in BufReader::new(r).lines().map_while(Result::ok) {
                push_line(line);
            }
        }));
    }
    std::thread::spawn(move || {
        let status = child.wait();
        for t in threads {
            let _ = t.join();
        }
        let exit = status.ok().and_then(|s| s.code()).unwrap_or(-1);
        if let Some(job) = JOB.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
            job.finished = Some(now_secs());
            job.exit = Some(exit);
        }
        log::warn!("rebalance finished with exit code {exit}");
    });
    Ok(format!("rebalance started; SHOW REBALANCE STATUS follows it ({shown})"))
}

/// `SHOW REBALANCE STATUS`: `(name, value)` rows, then the tool's output as
/// `log` rows, oldest first.
pub fn status_rows() -> Vec<(String, String)> {
    let guard = JOB.lock().unwrap_or_else(|e| e.into_inner());
    let Some(job) = guard.as_ref() else {
        return vec![("state".into(), "idle".into())];
    };
    let state = match job.exit {
        None => "running",
        Some(0) => "done",
        Some(_) => "failed",
    };
    let mut rows = vec![
        ("state".to_string(), state.to_string()),
        ("started_at".to_string(), job.started.to_string()),
        ("finished_at".to_string(), job.finished.map(|f| f.to_string()).unwrap_or_default()),
        ("exit_code".to_string(), job.exit.map(|e| e.to_string()).unwrap_or_default()),
        ("command".to_string(), job.command.clone()),
    ];
    rows.extend(job.log.iter().map(|l| ("log".to_string(), l.clone())));
    rows
}

/// `SHOW REBALANCE PLAN`'s columns.
pub const PLAN_COLUMNS: [&str; 8] = ["kind", "shard", "to_shard", "lo_block", "hi_block", "bytes", "load_before", "load_after"];

/// The plan from every shard's `SHOW SHARD LOAD` (`answers`): a `load` row
/// per shard (its share before and after), a `move` row per move, a `split`
/// row per range to split first, or one `balanced` row.
pub fn plan_rows(map: &ShardMap, cfg: &Config, answers: &[crate::fanout::ShardAnswer]) -> Result<Vec<Vec<Option<String>>>, String> {
    let mut reports = Vec::new();
    for (shard, columns, rows) in answers {
        let col = |n: &str| columns.iter().position(|c| c == n).ok_or_else(|| format!("shard {shard}: SHOW SHARD LOAD has no {n} column"));
        let (lo, hi, bytes, ops) = (col("lo_block")?, col("hi_block")?, col("total_bytes")?, col("statements")?);
        for r in rows {
            let text = |i: usize| r.get(i).cloned().flatten().unwrap_or_default();
            let block = |s: String| {
                let t = s.trim();
                match t.strip_prefix("0x") {
                    Some(h) => u64::from_str_radix(h, 16),
                    None => t.parse(),
                }
                .map_err(|_| format!("shard {shard}: {t:?} is not a block"))
            };
            reports.push(PartitionReport {
                shard: *shard,
                lo: block(text(lo))?,
                hi: block(text(hi))?,
                bytes: text(bytes).parse::<i64>().unwrap_or(0).max(0) as u64,
                ops: text(ops).parse::<i64>().unwrap_or(0).max(0) as u64,
            });
        }
    }
    let bcfg = BalanceConfig { tolerance: cfg.rebalance_tolerance, max_moves: cfg.rebalance_max_moves, ..BalanceConfig::default() };
    let plan = balance::plan(map, &balance::segments(map, &reports), &bcfg)?;
    let pct = |f: f64| format!("{:.2}", f * 100.0);
    let mut out = Vec::new();
    for ((id, before), (_, after)) in plan.before.iter().zip(&plan.after) {
        out.push(vec![Some("load".into()), Some(id.to_string()), None, None, None, None, Some(pct(*before)), Some(pct(*after))]);
    }
    if plan.balanced {
        out.push(vec![Some("balanced".into()), None, None, None, None, None, None, None]);
    }
    for mv in &plan.moves {
        out.push(vec![
            Some("move".into()),
            Some(mv.from.to_string()),
            Some(mv.to.to_string()),
            Some(format!("{:#x}", mv.lo)),
            Some(format!("{:#x}", mv.hi)),
            Some(mv.bytes.to_string()),
            None,
            None,
        ]);
    }
    for (lo, hi, at) in &plan.splits {
        out.push(vec![Some("split".into()), None, None, Some(format!("{lo:#x}")), Some(format!("{hi:#x}")), Some(format!("at {at:#x}")), None, None]);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_tool_runs_online_into_the_gateways_map_file() {
        let mut cfg = Config::default();
        assert!(tool_args(&cfg, "/now.map", None).is_err(), "no map file, nothing to write the next map to");
        cfg.set("map", "/etc/rdb/cluster.map").unwrap();
        cfg.set("rebalance_user", "rasterized").unwrap();
        cfg.set("rebalance_native", "0=127.0.0.1:1, 1=127.0.0.1:2").unwrap();
        cfg.set("rebalance_extra", "--accept-any").unwrap();
        cfg.set("rebalance_max_moves", "2").unwrap();
        let args = tool_args(&cfg, "/now.map", Some(1)).unwrap();
        let joined = args.join(" ");
        assert!(joined.starts_with("rebalance --map /now.map --apply --online --out /etc/rdb/cluster.map"), "{joined}");
        assert!(joined.contains("--max-moves 1"), "{joined}");
        assert!(joined.contains("--user rasterized"), "{joined}");
        assert!(joined.contains("--native 0=127.0.0.1:1 --native 1=127.0.0.1:2"), "{joined}");
        assert!(joined.ends_with("--accept-any"), "{joined}");
    }

    #[test]
    fn a_run_is_refused_without_a_tool() {
        let mut cfg = Config::default();
        cfg.set("map", "/m").unwrap();
        let shard = rasterizeddb_router::shard_map::ShardInfo { id: 0, addr: "127.0.0.1:1".into(), standbys: Vec::new() };
        let map = ShardMap::initial(7, vec![shard]).unwrap();
        assert!(start(&cfg, &map, None).unwrap_err().contains("rebalance_tool"));
    }
}
