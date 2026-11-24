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
//! rdb-shard rebalance   --map MAP [--tolerance 0.10] [--max-moves 4] [--max-bytes N]
//!                       [--pin LO:HI …] [--exclude SHARD …]
//!                       [--apply --fenced --out NEW_MAP --state-dir DIR
//!                        --table TABLE:COLUMN:TYPE [--table …]]
//! rdb-shard ddl         --map MAP --file DDL.sql --table TABLE [--table …]
//!                       [--home SHARD] [--state STATE]
//! ```
//!
//! `ddl` is cluster DDL (sharding-plan.md B1a.10): the file runs on the home
//! shard, then on every other shard in id order; then every named table's
//! `SHOW SCHEMA FINGERPRINT` must agree on every shard, and only then is
//! each shard's `shard.schema_epoch` raised past the highest, so gateways
//! drop their old templates. A failure or a disagreement stops, names the
//! shard, and records the shards already applied in STATE; the next run
//! with the same STATE resumes from there (DDL is not transactional).
//!
//! `rebalance` reads `SHOW SHARD LOAD` from every shard and prints the round
//! the auto-balancer plans (sharding-plan.md §4.6b). With `--apply` it runs
//! each move as an offline range move, one after another: the rows whose key
//! falls in the moved blocks (`rdb_route_block(TYPE, COLUMN)`) are copied at
//! one snapshot, verified, and handed off one map epoch up, with a state file
//! per move in DIR that `rollback` and `retire` take like a tenant move's.
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
            if matches!(name, "fenced" | "force" | "accept-any" | "apply") {
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
    "usage: rdb-shard init|show|move-tenant|rollback|retire|rebalance|ddl --map MAP … (see the source header)".to_string()
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
        "rebalance" => rebalance(args, &options).await,
        "ddl" => cluster_ddl(args, &options).await,
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

/// The rows a move carries: one tenant's, or every row whose key falls in a
/// range of level-0 blocks.
enum Selection {
    Tenant(Tenant),
    /// Blocks `[lo, hi)`; each table names its key column's type.
    Blocks { lo: u64, hi: u64 },
}

impl Selection {
    /// The `WHERE` naming the moved rows of a table keyed by `column` (of
    /// `key_type`, for a block range).
    fn filter(&self, column: &str, key_type: &str) -> std::result::Result<String, String> {
        let col = rasterizeddb_client::quote_ident(column).map_err(|e| e.to_string())?;
        match self {
            Selection::Tenant(t) => {
                let head = format!("{col} = $1");
                t.with_param(|p| rasterizeddb_client::render_dyn(&head, &[p]))?
                    .map_err(|e| e.to_string())
            }
            Selection::Blocks { lo, hi } => {
                family_of(key_type)?;
                Ok(format!(
                    "rdb_route_block('{key_type}', {col}) >= {lo} AND rdb_route_block('{key_type}', {col}) < {hi}"
                ))
            }
        }
    }
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

    /// What the move carried, and the blocks it handed off.
    fn selection(&self) -> std::result::Result<(Selection, u64, u64), String> {
        if self.fields.get("kind").map(String::as_str) == Some("range") {
            let lo = parse_block(self.get("lo")?)?;
            let hi = parse_block(self.get("hi")?)?;
            return Ok((Selection::Blocks { lo, hi }, lo, hi));
        }
        let t = self.tenant()?;
        let block = t.block()?;
        Ok((Selection::Tenant(t), block, block + 1))
    }

    /// A table's key type, for a range move (`table T:C:TYPE`).
    fn split_column(column: &str) -> (&str, &str) {
        column.split_once(':').unwrap_or((column, ""))
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

/// `SELECT cols FROM table WHERE <the selection>`.
fn select_sql(table: &str, cols: &[String], filter: &str) -> std::result::Result<String, String> {
    let list = cols
        .iter()
        .map(|c| rasterizeddb_client::quote_ident(c))
        .collect::<Result<Vec<_>>>()
        .map_err(|e| e.to_string())?
        .join(", ");
    Ok(format!(
        "SELECT {list} FROM {} WHERE {filter}",
        rasterizeddb_client::quote_ident(table).map_err(|e| e.to_string())?
    ))
}

fn delete_sql(table: &str, filter: &str) -> std::result::Result<String, String> {
    Ok(format!(
        "DELETE FROM {} WHERE {filter}",
        rasterizeddb_client::quote_ident(table).map_err(|e| e.to_string())?
    ))
}

/// Copies one table's selected rows from `snapshot` (on shard `from`) into
/// `dst` (shard `to`) and verifies them; the row count copied.
async fn copy_table(
    snapshot: &rasterizeddb_client::Transaction,
    src: &Client,
    dst: &Client,
    (from, to): (ShardId, ShardId),
    table: &str,
    filter: &str,
) -> std::result::Result<u64, String> {
    let cols = columns(src, table).await.map_err(|e| format!("{table}: {e}"))?;
    let sql = select_sql(table, &cols, filter)?;
    let existing = dst.run(&sql).await.map_err(|e| format!("{table} on shard {to}: {e}"))?;
    if !existing.is_empty() {
        return Err(format!(
            "shard {to} already holds {} {table} rows of what is being moved: clean it first",
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
             digest {:016x}). Nothing was handed off; delete the destination's copied rows and retry",
            want.0, want.1, got.0, got.1
        ));
    }
    Ok(want.0)
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
    let selection = Selection::Tenant(tenant);
    let mut copied = Vec::new();
    for (table, column) in &tables {
        let filter = selection.filter(column, "")?;
        let n = copy_table(&snapshot, &src, &dst, (from, to), table, &filter).await?;
        println!("{table}: {n} rows copied and verified");
        copied.push((table.clone(), column.clone()));
    }
    let Selection::Tenant(tenant) = selection else { unreachable!() };
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
    let (selection, lo, hi) = state.selection()?;
    let (from, to) = (state.shard("from")?, state.shard("to")?);
    if map.shard_of_block(lo) != to || map.shard_of_block(hi - 1) != to {
        return Err(format!("the map does not have the moved rows on shard {to}: roll back from the map the move wrote"));
    }
    let back = map.reassign(lo, hi, from)?;
    let src = connect(&map, from, options).await?;
    let dst = connect(&map, to, options).await?;
    for (id, c) in [(from, &src), (to, &dst)] {
        Identity::of(&back, id).write(c).await.map_err(|e| format!("shard {id}: {e}"))?;
    }
    write_map(out, &back)?;
    for (table, column) in &state.tables {
        let (column, key_type) = MoveState::split_column(column);
        let n = dst
            .run(&delete_sql(table, &selection.filter(column, key_type)?)?)
            .await
            .map_err(|e| format!("{table} on shard {to}: {e}"))?
            .rows_affected()
            .unwrap_or(0);
        println!("{table}: {n} copied rows removed from shard {to}");
    }
    state.fields.insert("status".to_string(), "rolled_back".to_string());
    state.write(state_path)?;
    println!("the moved rows are back on shard {from}; map epoch {} written to {out}. Reload every router.", back.epoch());
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
    let (selection, lo, hi) = state.selection()?;
    let (from, to) = (state.shard("from")?, state.shard("to")?);
    if map.shard_of_block(lo) != to || map.shard_of_block(hi - 1) != to {
        return Err(format!("the map does not have the moved rows on shard {to}: refusing to delete their source"));
    }
    let src = connect(&map, from, options).await?;
    for (table, column) in &state.tables {
        let (column, key_type) = MoveState::split_column(column);
        let n = src
            .run(&delete_sql(table, &selection.filter(column, key_type)?)?)
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

fn parse_block(s: &str) -> std::result::Result<u64, String> {
    match s.strip_prefix("0x") {
        Some(h) => u64::from_str_radix(h, 16),
        None => s.parse(),
    }
    .map_err(|_| format!("{s:?} is not a block number"))
}

/// `rebalance` (module docs).
async fn rebalance(args: &Args, options: &Options) -> std::result::Result<(), String> {
    use rasterizeddb_router::balance::{self, BalanceConfig, PartitionReport};
    let map = read_map(args.one("map")?)?;
    let mut cfg = BalanceConfig::default();
    if let Some(t) = args.opt("tolerance") {
        cfg.tolerance = t.parse().map_err(|_| "--tolerance takes a fraction, such as 0.10")?;
    }
    if let Some(m) = args.opt("max-moves") {
        cfg.max_moves = m.parse().map_err(|_| "--max-moves takes a number")?;
    }
    if let Some(b) = args.opt("max-bytes") {
        cfg.max_bytes = b.parse().map_err(|_| "--max-bytes takes a byte count")?;
    }
    for p in args.flags.get("pin").into_iter().flatten() {
        let (lo, hi) = p.split_once(':').ok_or_else(|| format!("--pin {p:?}: write LO:HI blocks"))?;
        cfg.pinned.push((parse_block(lo)?, parse_block(hi)?));
    }
    for x in args.flags.get("exclude").into_iter().flatten() {
        cfg.not_accepting.push(x.parse().map_err(|_| format!("--exclude {x:?}: a shard id"))?);
    }
    // Every shard's load.
    let mut reports = Vec::new();
    for info in map.shards() {
        let c = connect(&map, info.id, options).await?;
        let rows: Vec<(String, i64, String, String, i64, i64, i64)> =
            c.fetch_all("SHOW SHARD LOAD", &[]).await.map_err(|e| format!("shard {}: {e}", info.id))?;
        for (_, _, lo, hi, _, bytes, ops) in rows {
            reports.push(PartitionReport {
                shard: info.id,
                lo: parse_block(&lo)?,
                hi: parse_block(&hi)?,
                bytes: bytes.max(0) as u64,
                ops: ops.max(0) as u64,
            });
        }
    }
    let segments = balance::segments(&map, &reports);
    let plan = balance::plan(&map, &segments, &cfg)?;
    for ((id, before), (_, after)) in plan.before.iter().zip(&plan.after) {
        println!("shard {id:>5}: load {:>6.2}% -> {:>6.2}%", before * 100.0, after * 100.0);
    }
    if plan.balanced {
        println!("balanced: every shard is within {:.0}% of the mean", cfg.tolerance * 100.0);
        return Ok(());
    }
    for mv in &plan.moves {
        println!("move blocks [{:#x}, {:#x}) from shard {} to shard {} ({} bytes)", mv.lo, mv.hi, mv.from, mv.to, mv.bytes);
    }
    for (lo, hi, at) in &plan.splits {
        println!("split [{lo:#x}, {hi:#x}) at {at:#x} before a later round can move part of it");
    }
    if !args.has("apply") {
        println!("(plan only; --apply --fenced runs it)");
        return Ok(());
    }
    if !args.has("fenced") {
        return Err("--apply moves rows offline: fence writes to the moved ranges first, then say --fenced".to_string());
    }
    let out = args.one("out")?;
    let dir = args.one("state-dir")?;
    std::fs::create_dir_all(dir).map_err(|e| format!("{dir}: {e}"))?;
    let tables: Vec<(String, String, String)> = args
        .flags
        .get("table")
        .ok_or("--table TABLE:COLUMN:TYPE is required (once per sharded table)")?
        .iter()
        .map(|t| {
            let mut it = t.splitn(3, ':');
            match (it.next(), it.next(), it.next()) {
                (Some(a), Some(b), Some(c)) => {
                    family_of(c)?;
                    Ok((a.to_string(), b.to_string(), c.to_string()))
                }
                _ => Err(format!("--table {t:?}: write TABLE:COLUMN:TYPE")),
            }
        })
        .collect::<std::result::Result<_, String>>()?;
    let mut current = map.clone();
    for (n, mv) in plan.moves.iter().enumerate() {
        let src = connect(&current, mv.from, options).await?;
        let dst = connect(&current, mv.to, options).await?;
        let snapshot = src
            .begin_with(TransactionOptions {
                isolation: Some(Isolation::RepeatableRead),
                read_only: true,
                ..Default::default()
            })
            .await
            .map_err(|e| e.to_string())?;
        let selection = Selection::Blocks { lo: mv.lo, hi: mv.hi };
        let mut copied = Vec::new();
        for (table, column, key_type) in &tables {
            let filter = selection.filter(column, key_type)?;
            let rows = copy_table(&snapshot, &src, &dst, (mv.from, mv.to), table, &filter).await?;
            println!("move {n}: {table}: {rows} rows copied and verified");
            copied.push((table.clone(), format!("{column}:{key_type}")));
        }
        snapshot.rollback().await.map_err(|e| e.to_string())?;
        let next = current.reassign(mv.lo, mv.hi, mv.to)?;
        let state = MoveState {
            fields: BTreeMap::from([
                ("kind".to_string(), "range".to_string()),
                ("lo".to_string(), format!("{:#x}", mv.lo)),
                ("hi".to_string(), format!("{:#x}", mv.hi)),
                ("from".to_string(), mv.from.to_string()),
                ("to".to_string(), mv.to.to_string()),
                ("epoch".to_string(), next.epoch().to_string()),
                ("moved_at".to_string(), now_secs().to_string()),
                ("status".to_string(), "moved".to_string()),
            ]),
            tables: copied,
        };
        state.write(&format!("{dir}/move-{}-{n}.state", next.epoch()))?;
        for (id, c) in [(mv.from, &src), (mv.to, &dst)] {
            Identity::of(&next, id).write(c).await.map_err(|e| format!("shard {id}: {e}"))?;
        }
        write_map(out, &next)?;
        current = next;
    }
    println!(
        "{} moves applied; map epoch {} written to {out}. Reload every router, then lift the fence. \
         The source rows stay until `rdb-shard retire` of each state file in {dir}.",
        plan.moves.len(),
        current.epoch()
    );
    Ok(())
}

/// `ddl` (module docs).
async fn cluster_ddl(args: &Args, options: &Options) -> std::result::Result<(), String> {
    let map = read_map(args.one("map")?)?;
    let file = args.one("file")?;
    let sql = std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))?;
    let tables: Vec<String> = args.flags.get("table").cloned().ok_or("--table TABLE is required (each table the DDL changes)")?;
    let home: ShardId = match args.opt("home") {
        Some(h) => h.parse().map_err(|_| "--home takes a shard id".to_string())?,
        None => map.shards().first().map(|s| s.id).ok_or("the map has no shards")?,
    };
    if map.shard(home).is_none() {
        return Err(format!("home shard {home} is not in the map"));
    }
    let state_path = args.opt("state").map(str::to_string).unwrap_or_else(|| format!("{file}.state"));
    // Shards already applied by an earlier run of this DDL.
    let digest = rasterizeddb_router::xxh64::xxh64(sql.as_bytes(), 0x5244_4244_444C_0001);
    let mut applied: Vec<ShardId> = Vec::new();
    if let Ok(text) = std::fs::read_to_string(&state_path) {
        let mut lines = text.lines();
        if lines.next() != Some(&format!("ddl {digest:016x}")) {
            return Err(format!("{state_path} records another DDL file: remove it or pass another --state"));
        }
        applied = lines.filter_map(|l| l.strip_prefix("applied ")).filter_map(|v| v.parse().ok()).collect();
    }
    let record = |applied: &[ShardId]| -> std::result::Result<(), String> {
        let mut out = format!("ddl {digest:016x}\n");
        for id in applied {
            out.push_str(&format!("applied {id}\n"));
        }
        let tmp = format!("{state_path}.tmp");
        std::fs::write(&tmp, out).map_err(|e| format!("{tmp}: {e}"))?;
        std::fs::rename(&tmp, &state_path).map_err(|e| format!("{state_path}: {e}"))
    };
    let mut order: Vec<ShardId> = vec![home];
    order.extend(map.shards().iter().map(|s| s.id).filter(|&id| id != home));
    let mut clients = Vec::with_capacity(order.len());
    for &id in &order {
        clients.push((id, connect(&map, id, options).await?));
    }
    for (id, c) in &clients {
        if applied.contains(id) {
            println!("shard {id}: already applied");
            continue;
        }
        if let Err(e) = c.run(&sql).await {
            record(&applied)?;
            return Err(format!(
                "shard {id}: {e}. Shards {applied:?} have the DDL; fix the cause and run again with --state {state_path} to resume"
            ));
        }
        applied.push(*id);
        record(&applied)?;
        println!("shard {id}: applied");
    }
    // Every table's fingerprint must agree everywhere.
    let mut highest_epoch = 0i64;
    for table in &tables {
        let mut seen: Option<(ShardId, String)> = None;
        for (id, c) in &clients {
            let rows: Vec<(String, String, i64)> = c
                .fetch_all(&format!("SHOW SCHEMA FINGERPRINT {table}"), &[])
                .await
                .map_err(|e| format!("shard {id}, {table}: {e}"))?;
            let Some((_, fp, epoch)) = rows.into_iter().next() else {
                return Err(format!("shard {id} gave no fingerprint for {table}"));
            };
            highest_epoch = highest_epoch.max(epoch);
            match &seen {
                None => seen = Some((*id, fp)),
                Some((first, want)) if *want != fp => {
                    return Err(format!(
                        "PARTIAL: {table} has fingerprint {want} on shard {first} but {fp} on shard {id}. \
                         Gateways keep the old schema epoch; make the shards agree, then run again with --state {state_path}"
                    ));
                }
                Some(_) => {}
            }
        }
        println!("{table}: one fingerprint on every shard");
    }
    let next = highest_epoch + 1;
    for (id, c) in &clients {
        c.run(&format!("SET CONFIG shard.schema_epoch = {next}"))
            .await
            .map_err(|e| format!("shard {id}: raising the schema epoch: {e}"))?;
    }
    let _ = std::fs::remove_file(&state_path);
    println!("schema epoch {next} on every shard");
    Ok(())
}
