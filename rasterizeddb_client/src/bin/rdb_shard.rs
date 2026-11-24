//! `rdb-shard`: the offline shard tool (sharding-plan A3, A6).
//!
//! ```text
//! rdb-shard init        --map MAP
//! rdb-shard show        --map MAP
//! rdb-shard move-tenant --map MAP --out NEW_MAP --state STATE
//!                       --key-type int|text|uuid --key VALUE --to SHARD
//!                       --table TABLE:COLUMN [--table …] --fenced
//! rdb-shard rollback    --map NEW_MAP --out MAP --state STATE
//! rdb-shard retire      --map NEW_MAP --state STATE [--retention-hours 24] [--force]
//! ```
//!
//! Every command also takes `--user U` (with the password in
//! `RDB_PASSWORD`), `--ca PEM`, `--pin SHA256` or `--accept-any` (the
//! default), and `--database NAME`.
//!
//! A move is offline and fenced by the application: the tool refuses to
//! run without `--fenced`, the operator's word that no writer touches the
//! tenant until the hand-off. It copies each table's rows at one snapshot,
//! verifies count and a digest of every row on both sides, publishes the map
//! with the tenant on its new shard (one epoch up), and writes a state file.
//! The source rows stay until `retire` (after the retention, 24 h by
//! default); until then `rollback` puts the tenant back.

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use rasterizeddb_client::orm::{ShardFamily, ShardKey};
use rasterizeddb_client::shard::{Identity, ShardId, ShardMap};
use rasterizeddb_client::{Client, Error, Executor, Isolation, Options, Result, ToSql, TransactionOptions, Value};

/// Rows per destination transaction.
const COPY_TX_ROWS: usize = 10_000;
/// Rows per INSERT statement.
const COPY_STATEMENT_ROWS: usize = 500;
/// Seed of a row digest ("RDBMOVE").
const ROW_SEED: u64 = 0x5244_424D_4F56_4500;

struct Args {
    command: String,
    flags: BTreeMap<String, Vec<String>>,
    switches: Vec<String>,
}

impl Args {
    fn parse() -> std::result::Result<Args, String> {
        let mut it = std::env::args().skip(1);
        let command = it.next().ok_or_else(usage)?;
        let mut flags: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut switches = Vec::new();
        while let Some(a) = it.next() {
            let Some(name) = a.strip_prefix("--") else {
                return Err(format!("unexpected argument {a:?}\n{}", usage()));
            };
            if matches!(name, "fenced" | "force" | "accept-any") {
                switches.push(name.to_string());
            } else {
                let v = it.next().ok_or_else(|| format!("--{name} needs a value"))?;
                flags.entry(name.to_string()).or_default().push(v);
            }
        }
        Ok(Args { command, flags, switches })
    }

    fn one(&self, name: &str) -> std::result::Result<&str, String> {
        self.opt(name).ok_or_else(|| format!("--{name} is required"))
    }

    fn opt(&self, name: &str) -> Option<&str> {
        self.flags.get(name).and_then(|v| v.last()).map(String::as_str)
    }

    fn has(&self, name: &str) -> bool {
        self.switches.iter().any(|s| s == name)
    }
}

fn usage() -> String {
    "usage: rdb-shard init|show|move-tenant|rollback|retire --map MAP … (see the source header)".to_string()
}

fn main() -> ExitCode {
    let args = match Args::parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("rdb-shard: no runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(&args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("rdb-shard: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &Args) -> std::result::Result<(), String> {
    let options = options(args)?;
    match args.command.as_str() {
        "init" => init(&read_map(args.one("map")?)?, &options).await,
        "show" => show(&read_map(args.one("map")?)?, &options).await,
        "move-tenant" => move_tenant(args, &options).await,
        "rollback" => rollback(args, &options).await,
        "retire" => retire(args, &options).await,
        other => Err(format!("unknown command {other:?}\n{}", usage())),
    }
}

fn options(args: &Args) -> std::result::Result<Options, String> {
    let mut o = Options::default();
    if let Some(user) = args.opt("user") {
        let password = std::env::var("RDB_PASSWORD").map_err(|_| "--user needs RDB_PASSWORD set".to_string())?;
        o.login = rasterizeddb_client::Login::password(user, password);
    }
    if let Some(db) = args.opt("database") {
        o.database = Some(db.to_string());
    }
    if let Some(ca) = args.opt("ca") {
        o.trust = rasterizeddb_client::load_ca(ca).map_err(|e| e.to_string())?;
    } else if let Some(pin) = args.opt("pin") {
        o.trust = rasterizeddb_client::pin(&[pin]).map_err(|e| e.to_string())?;
    }
    o.migrations = None;
    o.auto_migrate = rasterizeddb_client::orm::AutoMigrate::Off;
    Ok(o)
}

fn read_map(path: &str) -> std::result::Result<ShardMap, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    ShardMap::from_text(&text).map_err(|e| format!("{path}: {e}"))
}

fn write_map(path: &str, map: &ShardMap) -> std::result::Result<(), String> {
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, map.to_text()).map_err(|e| format!("{tmp}: {e}"))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("{path}: {e}"))
}

async fn connect(map: &ShardMap, id: ShardId, options: &Options) -> std::result::Result<Client, String> {
    let info = map.shard(id).ok_or_else(|| format!("shard {id} is not in the map"))?;
    Client::connect(&info.addr, options.clone())
        .await
        .map_err(|e| format!("shard {id} ({}): {e}", info.addr))
}

async fn init(map: &ShardMap, options: &Options) -> std::result::Result<(), String> {
    for info in map.shards() {
        let c = connect(map, info.id, options).await?;
        if let Some(old) = Identity::read(&c).await.map_err(|e| e.to_string())?
            && (old.cluster_id != map.cluster_id() || old.shard_id != info.id)
        {
            return Err(format!(
                "shard {} ({}) already says it is shard {} of cluster {:032x}: refusing to overwrite",
                info.id, info.addr, old.shard_id, old.cluster_id
            ));
        }
        Identity::of(map, info.id).write(&c).await.map_err(|e| format!("shard {}: {e}", info.id))?;
        println!("shard {} ({}): identity written at epoch {}", info.id, info.addr, map.epoch());
    }
    Ok(())
}

async fn show(map: &ShardMap, options: &Options) -> std::result::Result<(), String> {
    println!(
        "cluster {:032x}  epoch {}  digest {:016x}  {} shards, {} ranges",
        map.cluster_id(),
        map.epoch(),
        map.digest(),
        map.shards().len(),
        map.entries().len()
    );
    for info in map.shards() {
        let blocks: u64 = map.ranges_of(info.id).iter().map(|(lo, hi)| hi - lo).sum();
        let state = match connect(map, info.id, options).await {
            Err(e) => format!("unreachable: {e}"),
            Ok(c) => match Identity::read(&c).await {
                Ok(Some(i)) if i.cluster_id == map.cluster_id() && i.shard_id == info.id => {
                    format!("ok, identity at epoch {}", i.map_epoch)
                }
                Ok(Some(i)) => format!("WRONG identity: shard {} of {:032x}", i.shard_id, i.cluster_id),
                Ok(None) => "no identity row (run init)".to_string(),
                Err(e) => format!("identity unreadable: {e}"),
            },
        };
        println!(
            "shard {:>5}  {:<24} {:>6.2}% of blocks  {}",
            info.id,
            info.addr,
            blocks as f64 * 100.0 / rasterizeddb_client::shard::BLOCKS as f64,
            state
        );
    }
    Ok(())
}

/// The tenant a move names.
struct Tenant {
    family: ShardFamily,
    text: String,
}

impl Tenant {
    fn key(&self) -> std::result::Result<ShardKey<'static>, String> {
        let k = ShardKey::Text(self.text.clone().into());
        k.coerce(self.family)
            .map(ShardKey::into_owned)
            .ok_or_else(|| format!("{:?} is not a {:?} key", self.text, self.family))
    }

    fn block(&self) -> std::result::Result<u64, String> {
        rasterizeddb_client::shard::block_of_key(&self.key()?, self.family).map_err(|e| e.to_string())
    }

    /// The value as a statement parameter.
    fn with_param<T>(&self, f: impl FnOnce(&dyn ToSql) -> T) -> std::result::Result<T, String> {
        Ok(match self.key()? {
            ShardKey::Int(v) => match i64::try_from(v) {
                Ok(v) => f(&v),
                Err(_) => f(&v),
            },
            ShardKey::Uuid(b) => f(&rasterizeddb_client::Uuid(b)),
            ShardKey::Text(t) => f(&t.into_owned()),
        })
    }
}

fn family_of(s: &str) -> std::result::Result<ShardFamily, String> {
    match s {
        "int" | "integer" | "bigint" => Ok(ShardFamily::Integer),
        "text" | "varchar" => Ok(ShardFamily::Text),
        "uuid" => Ok(ShardFamily::Uuid),
        _ => Err(format!("--key-type {s:?}: write int, text or uuid")),
    }
}

/// The move's record, one `key value` per line.
struct MoveState {
    fields: BTreeMap<String, String>,
    tables: Vec<(String, String)>,
}

impl MoveState {
    fn read(path: &str) -> std::result::Result<MoveState, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
        let mut fields = BTreeMap::new();
        let mut tables = Vec::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let (k, v) = line.split_once(' ').ok_or_else(|| format!("{path}: bad line {line:?}"))?;
            if k == "table" {
                let (t, c) = v.split_once(':').ok_or_else(|| format!("{path}: bad table {v:?}"))?;
                tables.push((t.to_string(), c.to_string()));
            } else {
                fields.insert(k.to_string(), v.to_string());
            }
        }
        Ok(MoveState { fields, tables })
    }

    fn write(&self, path: &str) -> std::result::Result<(), String> {
        let mut out = String::new();
        for (k, v) in &self.fields {
            out.push_str(&format!("{k} {v}\n"));
        }
        for (t, c) in &self.tables {
            out.push_str(&format!("table {t}:{c}\n"));
        }
        let tmp = format!("{path}.tmp");
        std::fs::write(&tmp, out).map_err(|e| format!("{tmp}: {e}"))?;
        std::fs::rename(&tmp, path).map_err(|e| format!("{path}: {e}"))
    }

    fn get(&self, k: &str) -> std::result::Result<&str, String> {
        self.fields.get(k).map(String::as_str).ok_or_else(|| format!("the move state has no {k}"))
    }

    fn tenant(&self) -> std::result::Result<Tenant, String> {
        Ok(Tenant {
            family: family_of(self.get("key_type")?)?,
            text: self.get("key")?.to_string(),
        })
    }

    fn shard(&self, k: &str) -> std::result::Result<ShardId, String> {
        self.get(k)?.parse().map_err(|_| format!("the move state's {k} is not a shard id"))
    }
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn tables(args: &Args) -> std::result::Result<Vec<(String, String)>, String> {
    let list = args.flags.get("table").ok_or("--table TABLE:COLUMN is required (once per sharded table)")?;
    list.iter()
        .map(|t| {
            let (table, column) = t.split_once(':').ok_or_else(|| format!("--table {t:?}: write TABLE:COLUMN"))?;
            rasterizeddb_client::quote_ident(table).map_err(|e| e.to_string())?;
            rasterizeddb_client::quote_ident(column).map_err(|e| e.to_string())?;
            Ok((table.to_string(), column.to_string()))
        })
        .collect()
}

/// A table's columns, in order.
async fn columns(c: &Client, table: &str) -> Result<Vec<String>> {
    let names: Vec<(String,)> = c
        .fetch_all(
            "SELECT column_name FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position",
            &[&table],
        )
        .await?;
    if names.is_empty() {
        return Err(Error::Param(format!("table {table} has no columns (does it exist?)")));
    }
    Ok(names.into_iter().map(|(n,)| n).collect())
}

/// `SELECT cols FROM table WHERE column = key`.
fn select_sql(table: &str, cols: &[String], column: &str, tenant: &Tenant) -> std::result::Result<String, String> {
    let list = cols
        .iter()
        .map(|c| rasterizeddb_client::quote_ident(c))
        .collect::<Result<Vec<_>>>()
        .map_err(|e| e.to_string())?
        .join(", ");
    let head = format!(
        "SELECT {list} FROM {} WHERE {} = $1",
        rasterizeddb_client::quote_ident(table).map_err(|e| e.to_string())?,
        rasterizeddb_client::quote_ident(column).map_err(|e| e.to_string())?
    );
    tenant
        .with_param(|p| rasterizeddb_client::render_dyn(&head, &[p]))?
        .map_err(|e| e.to_string())
}

fn delete_sql(table: &str, column: &str, tenant: &Tenant) -> std::result::Result<String, String> {
    let head = format!(
        "DELETE FROM {} WHERE {} = $1",
        rasterizeddb_client::quote_ident(table).map_err(|e| e.to_string())?,
        rasterizeddb_client::quote_ident(column).map_err(|e| e.to_string())?
    );
    tenant
        .with_param(|p| rasterizeddb_client::render_dyn(&head, &[p]))?
        .map_err(|e| e.to_string())
}

/// `(rows, Σ xxh64(row) mod 2⁶⁴)` over rows decoded as values.
fn digest(rows: &rasterizeddb_client::Rows, width: usize) -> Result<(u64, u64)> {
    let mut sum = 0u64;
    for row in rows.iter() {
        let mut canon = String::new();
        for i in 0..width {
            let v: Value = row.get(i)?;
            canon.push_str(&format!("{v:?}\u{1f}"));
        }
        sum = sum.wrapping_add(rasterizeddb_router::xxh64::xxh64(canon.as_bytes(), ROW_SEED));
    }
    Ok((rows.len() as u64, sum))
}

async fn move_tenant(args: &Args, options: &Options) -> std::result::Result<(), String> {
    if !args.has("fenced") {
        return Err(
            "a move is offline: fence the tenant's writes in the application first, then say --fenced".to_string(),
        );
    }
    let map = read_map(args.one("map")?)?;
    let out = args.one("out")?;
    let state_path = args.one("state")?;
    if std::path::Path::new(state_path).exists() {
        return Err(format!("{state_path} exists: a move is already recorded there"));
    }
    let tenant = Tenant {
        family: family_of(args.one("key-type")?)?,
        text: args.one("key")?.to_string(),
    };
    let to: ShardId = args.one("to")?.parse().map_err(|_| "--to takes a shard id".to_string())?;
    if map.shard(to).is_none() {
        return Err(format!("shard {to} is not in the map"));
    }
    let tables = tables(args)?;
    let block = tenant.block()?;
    let from = map.shard_of_block(block);
    if from == to {
        return Err(format!("the tenant is already on shard {to}"));
    }
    let src = connect(&map, from, options).await?;
    let dst = connect(&map, to, options).await?;
    // Both shards must be who the map says, at this map.
    for (id, c) in [(from, &src), (to, &dst)] {
        match Identity::read(c).await.map_err(|e| e.to_string())? {
            Some(i) if i.cluster_id == map.cluster_id() && i.shard_id == id && i.map_epoch <= map.epoch() => {}
            other => return Err(format!("shard {id}'s identity does not match the map: {other:?}")),
        }
    }
    // Copy each table at one snapshot of the source.
    let snapshot = src
        .begin_with(TransactionOptions {
            isolation: Some(Isolation::RepeatableRead),
            read_only: true,
            ..Default::default()
        })
        .await
        .map_err(|e| e.to_string())?;
    let mut copied = Vec::new();
    for (table, column) in &tables {
        let cols = columns(&src, table).await.map_err(|e| format!("{table}: {e}"))?;
        let sql = select_sql(table, &cols, column, &tenant)?;
        let existing = dst.run(&sql).await.map_err(|e| format!("{table} on shard {to}: {e}"))?;
        if !existing.is_empty() {
            return Err(format!(
                "shard {to} already holds {} {table} rows of this tenant: clean it first",
                existing.len()
            ));
        }
        let rows = snapshot.run(&sql).await.map_err(|e| format!("{table} on shard {from}: {e}"))?;
        let want = digest(&rows, cols.len()).map_err(|e| e.to_string())?;
        let head = format!(
            "INSERT INTO {} ({}) VALUES ",
            rasterizeddb_client::quote_ident(table).map_err(|e| e.to_string())?,
            cols.iter()
                .map(|c| rasterizeddb_client::quote_ident(c))
                .collect::<Result<Vec<_>>>()
                .map_err(|e| e.to_string())?
                .join(", ")
        );
        let all: Vec<Vec<Value>> = rows
            .iter()
            .map(|r| (0..cols.len()).map(|i| r.get::<Value>(i)).collect::<Result<Vec<_>>>())
            .collect::<Result<_>>()
            .map_err(|e| e.to_string())?;
        for tx_rows in all.chunks(COPY_TX_ROWS) {
            let tx = dst.begin().await.map_err(|e| e.to_string())?;
            for stmt_rows in tx_rows.chunks(COPY_STATEMENT_ROWS) {
                let mut sql = head.clone();
                for (n, row) in stmt_rows.iter().enumerate() {
                    if n > 0 {
                        sql.push_str(", ");
                    }
                    let placeholders: Vec<String> = (1..=row.len()).map(|i| format!("${i}")).collect();
                    let params: Vec<&dyn ToSql> = row.iter().map(|v| v as &dyn ToSql).collect();
                    let tuple = rasterizeddb_client::render_dyn(&format!("({})", placeholders.join(", ")), &params)
                        .map_err(|e| e.to_string())?;
                    sql.push_str(&tuple);
                }
                tx.run(&sql).await.map_err(|e| format!("{table} into shard {to}: {e}"))?;
            }
            tx.commit().await.map_err(|e| e.to_string())?;
        }
        // Verify: the destination's rows, read back, against the snapshot's.
        let back = dst.run(&sql).await.map_err(|e| e.to_string())?;
        let got = digest(&back, cols.len()).map_err(|e| e.to_string())?;
        if got != want {
            return Err(format!(
                "{table}: the copy does not verify (source {} rows, digest {:016x}; destination {} rows, \
                 digest {:016x}). Nothing was handed off; delete the destination's rows of this tenant and retry",
                want.0, want.1, got.0, got.1
            ));
        }
        println!("{table}: {} rows copied and verified", want.0);
        copied.push((table.clone(), column.clone()));
    }
    snapshot.rollback().await.map_err(|e| e.to_string())?;
    // Hand off: the tenant's block on the destination, one epoch up.
    let next = map.reassign(block, block + 1, to)?;
    let state = MoveState {
        fields: BTreeMap::from([
            ("key".to_string(), tenant.text.clone()),
            ("key_type".to_string(), format!("{:?}", tenant.family).to_ascii_lowercase()),
            ("from".to_string(), from.to_string()),
            ("to".to_string(), to.to_string()),
            ("block".to_string(), format!("{block:#x}")),
            ("epoch".to_string(), next.epoch().to_string()),
            ("moved_at".to_string(), now_secs().to_string()),
            ("status".to_string(), "moved".to_string()),
        ]),
        tables: copied,
    };
    state.write(state_path)?;
    for (id, c) in [(from, &src), (to, &dst)] {
        Identity::of(&next, id).write(c).await.map_err(|e| format!("shard {id}: {e}"))?;
    }
    write_map(out, &next)?;
    println!(
        "moved to shard {to}; map epoch {} written to {out}. Reload every router, then lift the fence. \
         The source rows stay until `rdb-shard retire`.",
        next.epoch()
    );
    Ok(())
}

async fn rollback(args: &Args, options: &Options) -> std::result::Result<(), String> {
    let map = read_map(args.one("map")?)?;
    let out = args.one("out")?;
    let state_path = args.one("state")?;
    let mut state = MoveState::read(state_path)?;
    if state.get("status")? != "moved" {
        return Err(format!("the move is {}: only a moved, unretired tenant rolls back", state.get("status")?));
    }
    let tenant = state.tenant()?;
    let (from, to) = (state.shard("from")?, state.shard("to")?);
    let block = tenant.block()?;
    if map.shard_of_block(block) != to {
        return Err(format!("the map does not have the tenant on shard {to}: roll back from the map the move wrote"));
    }
    let back = map.reassign(block, block + 1, from)?;
    let src = connect(&map, from, options).await?;
    let dst = connect(&map, to, options).await?;
    for (id, c) in [(from, &src), (to, &dst)] {
        Identity::of(&back, id).write(c).await.map_err(|e| format!("shard {id}: {e}"))?;
    }
    write_map(out, &back)?;
    for (table, column) in &state.tables {
        let n = dst
            .run(&delete_sql(table, column, &tenant)?)
            .await
            .map_err(|e| format!("{table} on shard {to}: {e}"))?
            .rows_affected()
            .unwrap_or(0);
        println!("{table}: {n} copied rows removed from shard {to}");
    }
    state.fields.insert("status".to_string(), "rolled_back".to_string());
    state.write(state_path)?;
    println!("the tenant is back on shard {from}; map epoch {} written to {out}. Reload every router.", back.epoch());
    Ok(())
}

async fn retire(args: &Args, options: &Options) -> std::result::Result<(), String> {
    let map = read_map(args.one("map")?)?;
    let state_path = args.one("state")?;
    let mut state = MoveState::read(state_path)?;
    if state.get("status")? != "moved" {
        return Err(format!("the move is {}: nothing to retire", state.get("status")?));
    }
    let hours: u64 = args.opt("retention-hours").unwrap_or("24").parse().map_err(|_| "--retention-hours takes a number")?;
    let moved_at: u64 = state.get("moved_at")?.parse().map_err(|_| "the move state's moved_at is not a time")?;
    let age = now_secs().saturating_sub(moved_at);
    if age < hours * 3600 && !args.has("force") {
        return Err(format!(
            "the move is {}h old, inside its {hours}h retention: wait, or say --force",
            age / 3600
        ));
    }
    let tenant = state.tenant()?;
    let (from, to) = (state.shard("from")?, state.shard("to")?);
    if map.shard_of_block(tenant.block()?) != to {
        return Err(format!("the map does not have the tenant on shard {to}: refusing to delete its source rows"));
    }
    let src = connect(&map, from, options).await?;
    for (table, column) in &state.tables {
        let n = src
            .run(&delete_sql(table, column, &tenant)?)
            .await
            .map_err(|e| format!("{table} on shard {from}: {e}"))?
            .rows_affected()
            .unwrap_or(0);
        println!("{table}: {n} source rows removed from shard {from}");
    }
    state.fields.insert("status".to_string(), "retired".to_string());
    state.write(state_path)?;
    Ok(())
}
