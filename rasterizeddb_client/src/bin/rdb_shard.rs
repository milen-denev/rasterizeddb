//! `rdb-shard`: the offline shard tool (sharding-plan A3, A6).
//!
//! ```text
//! rdb-shard init        --map MAP
//! rdb-shard show        --map MAP
//! rdb-shard move-tenant --map MAP --out NEW_MAP --state STATE
//!                       --key-type int|text|uuid --key VALUE --to SHARD
//!                       --table TABLE:COLUMN [--table …] --fenced|--online
//! rdb-shard rollback    --map NEW_MAP --out MAP --state STATE
//! rdb-shard retire      --map NEW_MAP --state STATE [--retention-hours 24] [--force]
//! rdb-shard rebalance   --map MAP [--tolerance 0.10] [--max-moves 4] [--max-bytes N]
//!                       [--pin LO:HI …] [--exclude SHARD …]
//!                       [--apply --fenced|--online --out NEW_MAP --state-dir DIR
//!                        [--table TABLE:COLUMN:TYPE …]] [--every SECS]
//!                       (no --table: every partitioned table, by its key)
//! rdb-shard split       --map MAP --shard HOT --new-id N --new-addr HOST:PORT [--at BLOCK]
//!                       --fenced|--online --out NEW_MAP --state-dir DIR --table TABLE:COLUMN:TYPE [--table …]
//! rdb-shard exec        --map MAP --sql SQL
//! rdb-shard backup      --map MAP --out MANIFEST [--snapshot] [--home SHARD]
//! rdb-shard restore     --manifest MANIFEST [--map MAP]
//! rdb-shard resolve     --map MAP [--home SHARD] [--grace SECS]
//! rdb-shard keygen      --key FILE
//! rdb-shard sign        --map MAP --key FILE
//! rdb-shard ddl         --map MAP --file DDL.sql --table TABLE [--table …]
//!                       [--home SHARD] [--state STATE] [--frontend ADDR …]
//! rdb-shard ddl         --file SCHEMA.sql --frontend ADDR [--frontend …] --frontend-only
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
//! `backup` takes a consistent cut of the whole cluster in every shard's own
//! S3 archive (each shard started with its `--s3-*` flags) and writes a
//! manifest naming each shard's restore point. With `--snapshot` each shard
//! first takes a full snapshot. Then each shard's archive is flushed
//! (`BACKUP WAL`) to drain the backlog, and, with commit decisions held on the
//! anchor shard (`SHARD HOLD COMMITS`) and every decided two-phase commit
//! finished everywhere, each shard's log is flushed again: that flush is the
//! shard's point. The hold lasts until every shard's clock has left the
//! second of its point, so no later flush can share it, and is then released.
//! No two-phase transaction is committed on one shard at its point and not on
//! another; one prepared and undecided is rolled back at the restore (`rdb-shard
//! resolve`). Single-shard transactions are not held: each shard's point is
//! exact for it, as a restore point is in other distributed databases, so two
//! separate transactions on two shards are not ordered by the cut.
//!
//! `restore` checks every shard's archive still holds its point and prints
//! what to run: every shard stopped; each restored into a new directory with
//! its usual `--s3-*` flags and `--restore-to <dir> --restore-at-time '<its
//! point>'`; each started on that directory with a new `--s3-prefix` (the
//! restored database is a new timeline, and its old prefix also holds what
//! came after the point, so the server refuses to archive there); then
//! `rdb-shard resolve`. It cannot stop the processes itself.
//!
//! Every command that writes a map signs it with `--key FILE` (the
//! authority's key, `keygen`), writing `MAP.sig` beside it; shards started
//! with `--authority-key` accept only signed maps.
//!
//! Every command also takes `--user U` (with the password in
//! `RDB_PASSWORD`), `--ca PEM`, `--pin SHA256` or `--accept-any` (the
//! default), and `--database NAME`. The tool speaks the native protocol to
//! each shard: at the map's address, or where `--native ID=HOST:PORT`
//! (repeatable; `split`: `--new-native HOST:PORT`) says, when the map's
//! addresses are the shards' rshard listeners.
//!
//! A move is offline. Between shards (`--role shard`) the engine fences it:
//! the source refuses writes to the moved blocks (`SHARD FENCE`), the
//! destination takes the span in (`SHARD TAKE`), and after the hand-off the
//! new map is installed on both and the source gives the span up (`SHARD
//! GIVE`), so no flag is needed and a stale router gets `RDB02`, never a
//! write in the wrong place. Between plain servers the tool refuses to run
//! without `--fenced`, the operator's word that no writer touches the moved
//! rows until the hand-off. It copies each table's rows at one snapshot,
//! verifies count and a digest of every row on both sides, publishes the map
//! with the tenant on its new shard (one epoch up), and writes a state file.
//! The source rows stay until `retire` (after the retention, 24 h by
//! default); until then `rollback` puts the tenant back.
//!
//! `--online` (shards only, B4.4) moves without stopping writers. The
//! destination takes the range; the rows are copied at a snapshot; unfenced
//! rounds bring the destination up to the source's rows until few change
//! between two; then the source is fenced for one last round and both
//! sides are compared before the map moves. Writes to the moving rows are
//! held only for that last round, which the tool reports in milliseconds.

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
            if matches!(name, "fenced" | "online" | "force" | "accept-any" | "apply" | "frontend-only" | "snapshot") {
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
    let mut native = BTreeMap::new();
    for spec in args.flags.get("native").into_iter().flatten() {
        let (id, addr) = spec.split_once('=').ok_or_else(|| format!("--native {spec:?}: write ID=HOST:PORT"))?;
        let id: ShardId = id.trim().parse().map_err(|_| format!("--native {spec:?}: {id:?} is not a shard id"))?;
        native.insert(id, addr.trim().to_string());
    }
    *NATIVE.lock().unwrap_or_else(|e| e.into_inner()) = native;
    match args.command.as_str() {
        "init" => init(&read_map(args.one("map")?)?, &options).await,
        "show" => show(&read_map(args.one("map")?)?, &options).await,
        "move-tenant" => move_tenant(args, &options).await,
        "rollback" => rollback(args, &options).await,
        "retire" => retire(args, &options).await,
        "rebalance" if args.opt("every").is_some() => rebalance_forever(args, &options).await,
        "rebalance" => rebalance(args, &options).await,
        "split" => split(args, &options).await,
        "ddl" => cluster_ddl(args, &options).await,
        "keygen" => keygen(args),
        "resolve" => resolve(args, &options).await,
        "backup" => cluster_backup(args, &options).await,
        "restore" => cluster_restore(args, &options).await,
        "exec" => exec_each(args, &options).await,
        "sign" => {
            let path = args.one("map")?;
            let map = read_map(path)?;
            if authority_key(args)?.is_none() {
                return Err("sign needs --key FILE".to_string());
            }
            write_signed(args, path, &map)?;
            println!("{path}.sig written for map epoch {}", map.epoch());
            Ok(())
        }
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

/// The authority's key pair from `--key FILE` (PKCS#8, hex), if given.
fn authority_key(args: &Args) -> std::result::Result<Option<aws_lc_rs::signature::Ed25519KeyPair>, String> {
    let Some(path) = args.opt("key") else {
        return Ok(None);
    };
    let hex = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let der = unhex(hex.trim()).ok_or_else(|| format!("{path}: not a hex PKCS#8 key"))?;
    aws_lc_rs::signature::Ed25519KeyPair::from_pkcs8(&der).map(Some).map_err(|_| format!("{path}: not an Ed25519 PKCS#8 key"))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok()).collect()
}

/// Writes `map` to `path`, and with `--key` its signature to `path.sig`;
/// returns the signature (hex) for `SHARD MAP INSTALL`.
fn write_signed(args: &Args, path: &str, map: &ShardMap) -> std::result::Result<Option<String>, String> {
    write_map(path, map)?;
    let Some(key) = authority_key(args)? else {
        return Ok(None);
    };
    let sig = hex(key.sign(&map.to_binary()).as_ref());
    let sig_path = format!("{path}.sig");
    let tmp = format!("{sig_path}.tmp");
    std::fs::write(&tmp, format!("{sig}\n")).map_err(|e| format!("{tmp}: {e}"))?;
    std::fs::rename(&tmp, &sig_path).map_err(|e| format!("{sig_path}: {e}"))?;
    Ok(Some(sig))
}

/// `keygen --key FILE`: a new authority key pair; the public half printed,
/// for every shard's `--authority-key`.
fn keygen(args: &Args) -> std::result::Result<(), String> {
    use aws_lc_rs::signature::KeyPair;
    let path = args.one("key")?;
    if std::path::Path::new(path).exists() {
        return Err(format!("{path} exists: refusing to replace an authority key"));
    }
    let rng = aws_lc_rs::rand::SystemRandom::new();
    let pkcs8 = aws_lc_rs::signature::Ed25519KeyPair::generate_pkcs8(&rng).map_err(|_| "key generation failed".to_string())?;
    let pair = aws_lc_rs::signature::Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).map_err(|_| "key generation failed".to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .map_err(|e| format!("{path}: {e}"))?;
        f.write_all(format!("{}\n", hex(pkcs8.as_ref())).as_bytes()).map_err(|e| format!("{path}: {e}"))?;
    }
    #[cfg(not(unix))]
    std::fs::write(path, format!("{}\n", hex(pkcs8.as_ref()))).map_err(|e| format!("{path}: {e}"))?;
    println!("authority key written to {path}; start every shard with --authority-key {}", hex(pair.public_key().as_ref()));
    Ok(())
}

/// `--native ID=HOST:PORT`: where shard ID's native endpoint is, when the
/// map's address is its rshard listener (the gateways' mTLS link), which
/// this tool does not speak.
static NATIVE: std::sync::Mutex<BTreeMap<ShardId, String>> = std::sync::Mutex::new(BTreeMap::new());

fn native_addr(id: ShardId, map_addr: &str) -> String {
    NATIVE.lock().unwrap_or_else(|e| e.into_inner()).get(&id).cloned().unwrap_or_else(|| map_addr.to_string())
}

async fn connect(map: &ShardMap, id: ShardId, options: &Options) -> std::result::Result<Client, String> {
    let info = map.shard(id).ok_or_else(|| format!("shard {id} is not in the map"))?;
    let addr = native_addr(id, &info.addr);
    Client::connect(&addr, options.clone()).await.map_err(|e| format!("shard {id} ({addr}): {e}"))
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
#[derive(Clone)]
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
    let engine = begin_move(args, &src, &dst, block, block + 1).await?;
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
    if args.has("online") {
        let filters: Vec<(String, String)> = copied
            .iter()
            .map(|(t, c)| Ok((t.clone(), Selection::Tenant(tenant.clone()).filter(c, "")?)))
            .collect::<std::result::Result<_, String>>()?;
        online_tail(&src, &dst, &filters, block, block + 1).await?;
    }
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
    let sig = write_signed(args, out, &next)?;
    if engine {
        finish_move(&src, &dst, &next, sig.as_deref(), block, block + 1).await?;
        install_on_the_rest(&next, sig.as_deref(), options, &[from, to]).await?;
        println!(
            "moved to shard {to}; map epoch {} installed on both shards and written to {out}. Reload every router \
             and gateway; the source's rows stay unread until it is restarted or retired.",
            next.epoch()
        );
    } else {
        println!(
            "moved to shard {to}; map epoch {} written to {out}. Reload every router, then lift the fence. \
             The source rows stay until `rdb-shard retire`.",
            next.epoch()
        );
    }
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
    if is_shard(&src).await {
        return Err(format!(
            "on shards the source gave its span up when the move finished: move the rows back instead \
             (rdb-shard move-tenant or rebalance onto shard {from})"
        ));
    }
    for (id, c) in [(from, &src), (to, &dst)] {
        Identity::of(&back, id).write(c).await.map_err(|e| format!("shard {id}: {e}"))?;
    }
    write_signed(args, out, &back)?;
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
    apply_moves(args, options, &map, &plan.moves).await
}

/// `split` (B6.2): a hot shard's range split onto a new shard. The new
/// node, already running as a shard, joins the map one epoch up; then the
/// upper half of the range (or from `--at`) moves there as any move does,
/// online with `--online`.
async fn split(args: &Args, options: &Options) -> std::result::Result<(), String> {
    let map = read_map(args.one("map")?)?;
    let hot: ShardId = args.one("shard")?.parse().map_err(|_| "--shard takes the hot shard's id".to_string())?;
    let new_id: ShardId = args.one("new-id")?.parse().map_err(|_| "--new-id takes a shard id".to_string())?;
    let new_addr = args.one("new-addr")?.to_string();
    if map.shard(new_id).is_some() {
        return Err(format!("shard {new_id} is in the map already"));
    }
    // The hot shard's largest range, split in half unless --at says where.
    let (lo, hi) = map
        .ranges_of(hot)
        .into_iter()
        .max_by_key(|(lo, hi)| hi - lo)
        .ok_or_else(|| format!("shard {hot} owns no range"))?;
    let at = match args.opt("at") {
        Some(a) => parse_block(a)?,
        None => lo + (hi - lo) / 2,
    };
    if at <= lo || at >= hi {
        return Err(format!("--at {at:#x} is not inside shard {hot}'s range [{lo:#x}, {hi:#x})"));
    }
    let grown = map.with_shard(rasterizeddb_router::shard_map::ShardInfo { id: new_id, addr: new_addr.clone(), standbys: Vec::new() })?;
    // `--new-native`: the new shard's native endpoint, when `--new-addr` (its
    // map address) is its rshard listener.
    let new_native = args.opt("new-native").map(str::to_string).unwrap_or_else(|| native_addr(new_id, &new_addr));
    // Every later connection to the new shard goes there too.
    NATIVE.lock().unwrap_or_else(|e| e.into_inner()).insert(new_id, new_native.clone());
    let dst = Client::connect(&new_native, options.clone()).await.map_err(|e| format!("the new shard {new_native}: {e}"))?;
    if !is_shard(&dst).await {
        return Err(format!("{new_addr} is not running as a shard (start it with --role shard)"));
    }
    Identity::of(&grown, new_id).write(&dst).await.map_err(|e| format!("shard {new_id}: {e}"))?;
    // Every shard learns of the new one before any range moves to it.
    let text = grown.to_text().replace('\'', "''");
    let sig = write_signed(args, &format!("{}.grown", args.one("out")?), &grown)?;
    let signed = sig.as_deref().map(|s| format!(" SIGNATURE '{s}'")).unwrap_or_default();
    for info in grown.shards() {
        let c = connect(&grown, info.id, options).await?;
        c.run(&format!("SHARD MAP INSTALL '{text}'{signed}")).await.map_err(|e| format!("shard {}: installing the grown map: {e}", info.id))?;
    }
    println!("shard {new_id} ({new_addr}) joined at map epoch {}; moving [{at:#x}, {hi:#x}) to it", grown.epoch());
    let mv = rasterizeddb_router::balance::Move { lo: at, hi, from: hot, to: new_id, bytes: 0 };
    apply_moves(args, options, &grown, &[mv]).await
}

/// Runs `moves` from `map` (rebalance --apply, split): each copied,
/// verified and handed off in turn, the map one epoch up after each.
async fn apply_moves(
    args: &Args,
    options: &Options,
    map: &ShardMap,
    moves: &[rasterizeddb_router::balance::Move],
) -> std::result::Result<(), String> {
    let out = args.one("out")?;
    let dir = args.one("state-dir")?;
    std::fs::create_dir_all(dir).map_err(|e| format!("{dir}: {e}"))?;
    let tables: Vec<(String, String, String)> = match args.flags.get("table") {
        // Without --table, every partitioned table a shard holds.
        None => {
            let first = map.shards().first().map(|s| s.id).ok_or("the map has no shards")?;
            let found = sharded_tables(&connect(map, first, options).await?).await.map_err(|e| format!("shard {first}: {e}"))?;
            if found.is_empty() {
                return Err("no partitioned table to move; name them with --table TABLE:COLUMN:TYPE".to_string());
            }
            for (t, c, k) in &found {
                println!("moving {t} by {c} ({k})");
            }
            found
        }
        Some(given) => given
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
        .collect::<std::result::Result<_, String>>()?,
    };
    let mut current = map.clone();
    for (n, mv) in moves.iter().enumerate() {
        let src = connect(&current, mv.from, options).await?;
        let dst = connect(&current, mv.to, options).await?;
        let engine = begin_move(args, &src, &dst, mv.lo, mv.hi).await?;
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
        if args.has("online") {
            let filters: Vec<(String, String)> = tables
                .iter()
                .map(|(t, c, k)| Ok((t.clone(), selection.filter(c, k)?)))
                .collect::<std::result::Result<_, String>>()?;
            online_tail(&src, &dst, &filters, mv.lo, mv.hi).await?;
        }
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
        let sig = write_signed(args, out, &next)?;
        if engine {
            finish_move(&src, &dst, &next, sig.as_deref(), mv.lo, mv.hi).await?;
            install_on_the_rest(&next, sig.as_deref(), options, &[mv.from, mv.to]).await?;
        }
        current = next;
    }
    println!(
        "{} moves applied; map epoch {} written to {out}. Reload every router, then lift the fence. \
         The source rows stay until `rdb-shard retire` of each state file in {dir}.",
        moves.len(),
        current.epoch()
    );
    Ok(())
}

/// Every partitioned table `c`'s database holds, with its key column and the
/// key's family (`int`, `text`, `uuid`): what a move copies when no `--table`
/// names them. A key of several columns is refused: name such tables.
async fn sharded_tables(c: &Client) -> std::result::Result<Vec<(String, String, String)>, String> {
    let names = c.run("SHOW TABLES").await.map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for r in names.iter() {
        let table: String = r.get(0).map_err(|e| e.to_string())?;
        // System catalogues: never partitioned, and not ours to read.
        if table.starts_with("__") || table == "rdb_roles" {
            continue;
        }
        let quoted = format!("\"{}\"", table.replace('"', "\"\""));
        let stats = match c.run(&format!("SHOW TABLE STATS {quoted}")).await {
            Ok(s) => s,
            Err(e) => {
                println!("{table}: not moved, its statistics cannot be read ({e}); name it with --table if it is sharded");
                continue;
            }
        };
        let key = stats.iter().find_map(|m| {
            let metric: String = m.get(0).ok()?;
            (metric == "partition_key").then(|| m.get::<String>(1).ok()).flatten()
        });
        let Some(key) = key else { continue };
        if key.contains(',') {
            return Err(format!("{table} is partitioned by {key}; name it with --table TABLE:COLUMN:TYPE"));
        }
        let columns = c.run(&format!("SHOW COLUMNS FROM {quoted}")).await.map_err(|e| format!("{table}: {e}"))?;
        let ty = columns
            .iter()
            .find_map(|col| {
                let name: String = col.get(1).ok()?;
                name.eq_ignore_ascii_case(key.trim()).then(|| col.get::<String>(2).ok()).flatten()
            })
            .ok_or_else(|| format!("{table}: no column {key}"))?
            .to_ascii_uppercase();
        let family = if ty.contains("UUID") {
            "uuid"
        } else if ty.contains("TEXT") || ty.contains("STRING") || ty.contains("CHAR") {
            "text"
        } else if ty.contains("INT") || ty.contains("SERIAL") || ((ty.starts_with('I') || ty.starts_with('U')) && ty.len() > 1 && ty[1..].chars().all(|ch| ch.is_ascii_digit())) {
            // INTEGER, BIGINT, SMALLINT, INT8, SERIAL, or the engine's I32, U64.
            "int"
        } else {
            return Err(format!("{table}: its key {key} is {ty}, which no move routes by; name it with --table"));
        };
        out.push((table, key.trim().to_string(), family.to_string()));
    }
    Ok(out)
}

/// `ddl` (module docs).
async fn cluster_ddl(args: &Args, options: &Options) -> std::result::Result<(), String> {
    let map = read_map(args.one("map")?)?;
    let file = args.one("file")?;
    let sql = std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))?;
    // Front-end nodes (B4.7) keep a copy of the schema, no rows: they take
    // the DDL once every shard has it, or alone with --frontend-only (a new
    // node seeded with the schema's DDL).
    let frontends: Vec<String> = args.flags.get("frontend").cloned().unwrap_or_default();
    if args.has("frontend-only") {
        if frontends.is_empty() {
            return Err("--frontend-only needs --frontend ADDR".to_string());
        }
        for addr in &frontends {
            let c = Client::connect(addr, options.clone()).await.map_err(|e| format!("front-end node {addr}: {e}"))?;
            c.run(&sql).await.map_err(|e| format!("front-end node {addr}: {e}"))?;
            println!("front-end node {addr}: applied");
        }
        return Ok(());
    }
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
    // B4.6, step 1: the DDL tried on every shard and rolled back, so one it
    // would fail on is applied on none. A statement a block cannot hold
    // (25001) is not tried there.
    if applied.is_empty() {
        for (id, c) in &clients {
            let tx = c.begin().await.map_err(|e| format!("shard {id}: {e}"))?;
            let tried = tx.run(&sql).await;
            let _ = tx.rollback().await;
            match tried {
                Ok(_) => {}
                Err(e) if e.to_string().contains("25001") || e.to_string().contains("inside a transaction block") => {
                    println!("shard {id}: the DDL cannot be tried in a block; applied without a dry run");
                }
                Err(e) => return Err(format!("shard {id} would refuse the DDL, so no shard was changed: {e}")),
            }
        }
    }
    // Step 2: the changed tables fenced everywhere until the epoch moves.
    let fence = tables.join(",").replace('\'', "''");
    for (id, c) in &clients {
        c.run(&format!("SET CONFIG shard.ddl_fence = '{fence}'")).await.map_err(|e| format!("shard {id}: fencing {fence}: {e}"))?;
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
    // The epoch has moved: the tables are open again.
    for (id, c) in &clients {
        c.run("SET CONFIG shard.ddl_fence = ''").await.map_err(|e| format!("shard {id}: lifting the fence: {e}"))?;
    }
    let _ = std::fs::remove_file(&state_path);
    println!("schema epoch {next} on every shard; the fence on {fence} is lifted");
    for addr in &frontends {
        let applied = match Client::connect(addr, options.clone()).await {
            Ok(c) => c.run(&sql).await.map(drop).map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        match applied {
            Ok(()) => println!("front-end node {addr}: applied"),
            Err(e) => eprintln!(
                "front-end node {addr}: {e}. The shards have the DDL; bring the node level with \
                 rdb-shard ddl --file {file} --frontend {addr} --frontend-only"
            ),
        }
    }
    Ok(())
}

/// Whether `c` runs as a shard (`--role shard`): its `SHOW SHARDS` has rows.
async fn is_shard(c: &Client) -> bool {
    matches!(c.run("SHOW SHARDS").await, Ok(rows) if !rows.is_empty())
}

/// Before a copy into `dst` of blocks `[lo, hi)` from `src`, on shards: the
/// source's writes there fenced, the destination's span taken in (B2).
/// Plain servers need the operator's `--fenced`.
async fn begin_move(args: &Args, src: &Client, dst: &Client, lo: u64, hi: u64) -> std::result::Result<bool, String> {
    let engine = is_shard(src).await && is_shard(dst).await;
    if args.has("online") {
        // B4.4: the destination takes the range at once and the source keeps
        // taking writes; it is fenced only for the last difference
        // (`online_tail`).
        if !engine {
            return Err("--online needs both servers to be shards (--role shard)".to_string());
        }
        dst.run(&format!("SHARD TAKE RANGE {lo:#x} {hi:#x}")).await.map_err(|e| format!("taking the range on the destination: {e}"))?;
        return Ok(true);
    }
    if engine {
        src.run(&format!("SHARD FENCE RANGE {lo:#x} {hi:#x}")).await.map_err(|e| format!("fencing the source: {e}"))?;
        if let Err(e) = dst.run(&format!("SHARD TAKE RANGE {lo:#x} {hi:#x}")).await {
            let _ = src.run(&format!("SHARD UNFENCE RANGE {lo:#x} {hi:#x}")).await;
            return Err(format!("taking the range on the destination: {e}"));
        }
    } else if !args.has("fenced") {
        return Err("these servers are not shards (--role shard): fence the moved rows' writes in the application \
                    first, then say --fenced"
            .to_string());
    }
    Ok(engine)
}

/// How many changed rows an unfenced round may still find before the
/// source is fenced for the last one.
const ONLINE_CONVERGED: u64 = 256;
/// At most this many unfenced rounds; a source changing faster than a round
/// copies is fenced anyway.
const ONLINE_ROUNDS: usize = 8;

/// An online move's tail (B4.4), after the snapshot copy: unfenced rounds
/// that bring the destination up to the source's moving rows until few
/// change between two, then the source fenced for the last round, and
/// both sides' rows compared. The fence holds only for that last round.
async fn online_tail(src: &Client, dst: &Client, tables: &[(String, String)], lo: u64, hi: u64) -> std::result::Result<(), String> {
    for round in 1..=ONLINE_ROUNDS {
        let mut changed = 0;
        for (table, filter) in tables {
            changed += sync_table(src, dst, table, filter).await?;
        }
        println!("online round {round}: {changed} rows brought up to date");
        if changed <= ONLINE_CONVERGED {
            break;
        }
    }
    let started = std::time::Instant::now();
    src.run(&format!("SHARD FENCE RANGE {lo:#x} {hi:#x}")).await.map_err(|e| format!("fencing the source: {e}"))?;
    let finish = async {
        let mut changed = 0;
        for (table, filter) in tables {
            changed += sync_table(src, dst, table, filter).await?;
            let cols = columns(src, table).await.map_err(|e| e.to_string())?;
            let sql = select_sql(table, &cols, filter)?;
            let a = digest(&src.run(&sql).await.map_err(|e| e.to_string())?, cols.len()).map_err(|e| e.to_string())?;
            let b = digest(&dst.run(&sql).await.map_err(|e| e.to_string())?, cols.len()).map_err(|e| e.to_string())?;
            if a != b {
                return Err(format!("{table}: the copy does not verify after the fenced round ({a:?} against {b:?})"));
            }
        }
        Ok::<u64, String>(changed)
    };
    match finish.await {
        Ok(changed) => {
            println!("fenced round: {changed} rows, writes held {} ms", started.elapsed().as_millis());
            Ok(())
        }
        Err(e) => {
            let _ = src.run(&format!("SHARD UNFENCE RANGE {lo:#x} {hi:#x}")).await;
            Err(e)
        }
    }
}

/// Makes the destination's rows of `table` under `filter` equal to the
/// source's, as a multiset of whole rows (a table needs no primary key):
/// surplus copies deleted by matching every column, missing ones inserted.
/// Returns how many rows changed.
async fn sync_table(src: &Client, dst: &Client, table: &str, filter: &str) -> std::result::Result<u64, String> {
    let cols = columns(src, table).await.map_err(|e| format!("{table}: {e}"))?;
    let sql = select_sql(table, &cols, filter)?;
    let read = |rows: rasterizeddb_client::Rows| -> std::result::Result<BTreeMap<String, (Vec<Value>, i64)>, String> {
        let mut out: BTreeMap<String, (Vec<Value>, i64)> = BTreeMap::new();
        for r in rows.iter() {
            let vals: Vec<Value> = (0..cols.len()).map(|i| r.get::<Value>(i)).collect::<Result<_>>().map_err(|e| e.to_string())?;
            let key: String = vals.iter().map(|v| format!("{v:?}\u{1f}")).collect();
            out.entry(key).or_insert_with(|| (vals, 0)).1 += 1;
        }
        Ok(out)
    };
    let want = read(src.run(&sql).await.map_err(|e| format!("{table} on the source: {e}"))?)?;
    let have = read(dst.run(&sql).await.map_err(|e| format!("{table} on the destination: {e}"))?)?;
    let quoted = rasterizeddb_client::quote_ident(table).map_err(|e| e.to_string())?;
    let mut changed = 0u64;
    let tx = dst.begin().await.map_err(|e| e.to_string())?;
    let keys: std::collections::BTreeSet<&String> = want.keys().chain(have.keys()).collect();
    for key in keys {
        let w = want.get(key).map_or(0, |(_, n)| *n);
        let h = have.get(key).map_or(0, |(_, n)| *n);
        if w == h {
            continue;
        }
        let vals = &want.get(key).or(have.get(key)).expect("in one of them").0;
        // Every copy of the row out, then as many in as the source has.
        if h > 0 {
            let mut pred = Vec::new();
            for (c, v) in cols.iter().zip(vals) {
                let qc = rasterizeddb_client::quote_ident(c).map_err(|e| e.to_string())?;
                if matches!(v, Value::Null) {
                    pred.push(format!("{qc} IS NULL"));
                } else {
                    pred.push(rasterizeddb_client::render_dyn(&format!("{qc} = $1"), &[v as &dyn ToSql]).map_err(|e| e.to_string())?);
                }
            }
            let mut del = format!("DELETE FROM {quoted} WHERE {}", pred.join(" AND "));
            if !filter.is_empty() {
                del.push_str(&format!(" AND ({filter})"));
            }
            tx.run(&del).await.map_err(|e| format!("{table} on the destination: {e}"))?;
        }
        if w > 0 {
            let col_list = cols.iter().map(|c| rasterizeddb_client::quote_ident(c)).collect::<Result<Vec<_>>>().map_err(|e| e.to_string())?.join(", ");
            let placeholders: Vec<String> = (1..=vals.len()).map(|i| format!("${i}")).collect();
            let params: Vec<&dyn ToSql> = vals.iter().map(|v| v as &dyn ToSql).collect();
            let tuple = rasterizeddb_client::render_dyn(&format!("({})", placeholders.join(", ")), &params).map_err(|e| e.to_string())?;
            for _ in 0..w {
                tx.run(&format!("INSERT INTO {quoted} ({col_list}) VALUES {tuple}")).await.map_err(|e| format!("{table} into the destination: {e}"))?;
            }
        }
        changed += (w - h).unsigned_abs().max(1) as u64;
    }
    tx.commit().await.map_err(|e| e.to_string())?;
    Ok(changed)
}

/// After a verified copy, on shards: the new map installed on both, the
/// source's span given up (its rows kept, unread), its fence lifted.
async fn finish_move(
    src: &Client,
    dst: &Client,
    next: &ShardMap,
    signature: Option<&str>,
    lo: u64,
    hi: u64,
) -> std::result::Result<(), String> {
    let text = next.to_text().replace('\'', "''");
    let signed = signature.map(|s| format!(" SIGNATURE '{s}'")).unwrap_or_default();
    for (who, c) in [("destination", dst), ("source", src)] {
        c.run(&format!("SHARD MAP INSTALL '{text}'{signed}")).await.map_err(|e| format!("installing the map on the {who}: {e}"))?;
    }
    src.run(&format!("SHARD GIVE RANGE {lo:#x} {hi:#x}")).await.map_err(|e| format!("giving up the source's span: {e}"))?;
    src.run(&format!("SHARD UNFENCE RANGE {lo:#x} {hi:#x}")).await.map_err(|e| format!("unfencing the source: {e}"))?;
    Ok(())
}

/// After a move, the new map on every shard but the two that took part
/// (which [`finish_move`] gave it): a shard on the old map routes the moved
/// range, and coordinates, as if nothing had moved, and a gateway whose home
/// it is learns nothing from it. One that cannot be reached is named, not
/// skipped: the move is done, and the map must still reach it.
async fn install_on_the_rest(next: &ShardMap, signature: Option<&str>, options: &Options, done: &[ShardId]) -> std::result::Result<(), String> {
    let text = next.to_text().replace('\'', "''");
    let signed = signature.map(|s| format!(" SIGNATURE '{s}'")).unwrap_or_default();
    let mut missed = Vec::new();
    for info in next.shards().iter().filter(|i| !done.contains(&i.id)) {
        let installed = match connect(next, info.id, options).await {
            Ok(c) => c.run(&format!("SHARD MAP INSTALL '{text}'{signed}")).await.map(drop).map_err(|e| e.to_string()),
            Err(e) => Err(e),
        };
        if let Err(e) = installed {
            missed.push(format!("shard {}: {e}", info.id));
        }
    }
    if missed.is_empty() {
        return Ok(());
    }
    Err(format!(
        "the move is done, but map epoch {} did not reach every shard ({}); install it there (rdb-shard show lists each shard's epoch)",
        next.epoch(),
        missed.join("; ")
    ))
}

/// `exec` (EXECUTE ON EACH SHARD, sharding-plan.md B2): the statement on
/// every shard as that shard's own transaction, in shard order. Each shard
/// answers for its own rows only (its guard refuses the rest), so an
/// `UPDATE`/`DELETE` without a key touches every row exactly once. Not
/// atomic across shards: a failure names the shards that already applied.
async fn exec_each(args: &Args, options: &Options) -> std::result::Result<(), String> {
    let map = read_map(args.one("map")?)?;
    let sql = args.one("sql")?;
    let mut applied: Vec<ShardId> = Vec::new();
    let mut total = 0u64;
    for info in map.shards() {
        let c = connect(&map, info.id, options).await?;
        match c.run(sql).await {
            Ok(r) => {
                let n = r.rows_affected().unwrap_or(r.len() as u64);
                total += n;
                applied.push(info.id);
                println!("shard {}: {n}", info.id);
            }
            Err(e) => {
                return Err(format!("shard {}: {e}. Already applied on shards {applied:?}", info.id));
            }
        }
    }
    println!("{total} on {} shards", applied.len());
    Ok(())
}

/// `rebalance --every SECS` (sharding-plan.md §4.6b, B6's automatic
/// balancing): a round every SECS seconds against the map the last round
/// wrote (`--out`), applied when `--apply` is given. Stops at the first
/// round that fails, so a failed move is looked at, not retried over.
async fn rebalance_forever(args: &Args, options: &Options) -> std::result::Result<(), String> {
    let secs: u64 = args.one("every")?.parse().map_err(|_| "--every takes seconds".to_string())?;
    let out = args.opt("out").map(str::to_string);
    let mut round = 0u64;
    loop {
        round += 1;
        println!("round {round}");
        // From the second round on, plan from the map the previous wrote.
        let mut a = Args { command: args.command.clone(), flags: args.flags.clone(), switches: args.switches.clone() };
        if round > 1
            && let Some(o) = &out
            && std::path::Path::new(o).exists()
        {
            a.flags.insert("map".to_string(), vec![o.clone()]);
        }
        rebalance(&a, options).await.map_err(|e| format!("round {round}: {e}"))?;
        tokio::time::sleep(std::time::Duration::from_secs(secs.max(1))).await;
    }
}

/// `resolve` (sharding-plan.md B5): every transaction a gateway left
/// prepared on a shard, finished by the anchor's decision: committed when it
/// says commit, rolled back when it says abort, and, with no decision and
/// older than `--grace` seconds (300), decided abort on the anchor first
/// (presumed abort), so a late gateway cannot commit it, then rolled back.
/// `SHOW BACKUPS`' rows.
type BackupRow = (String, String, String, i64, i64, i64, i64, String);

/// The newest log flush a shard's archive holds: (its time, RFC 3339, as the
/// shard's clock wrote it; its last LSN).
async fn newest_flush(c: &Client, id: ShardId) -> std::result::Result<Option<(String, i64)>, String> {
    let rows: Vec<BackupRow> = c.fetch_all("SHOW BACKUPS", &[]).await.map_err(|e| format!("shard {id}: SHOW BACKUPS: {e}"))?;
    Ok(rows.into_iter().find(|r| r.0 == "wal").map(|r| (r.2, r.4)))
}

async fn cluster_backup(args: &Args, options: &Options) -> std::result::Result<(), String> {
    use std::time::{Duration, Instant};
    let map_path = args.one("map")?;
    let map = read_map(map_path)?;
    let out = args.one("out")?;
    let home: ShardId = match args.opt("home") {
        Some(h) => h.parse().map_err(|_| "--home takes a shard id".to_string())?,
        None => map.shards().first().map(|s| s.id).ok_or("the map has no shards")?,
    };
    let mut shards = Vec::new();
    for info in map.shards() {
        shards.push((info.id, connect(&map, info.id, options).await?));
    }
    // A connection of its own: the hold is renewed while the others work.
    let anchor = connect(&map, home, options).await?;
    if args.has("snapshot") {
        for (id, c) in &shards {
            c.run("BACKUP DATABASE").await.map_err(|e| format!("shard {id}: BACKUP DATABASE: {e}"))?;
            println!("shard {id}: snapshot taken");
        }
    }
    // The backlog first, without holding anything, so the flushes under the
    // hold are short.
    for (id, c) in &shards {
        c.run("BACKUP WAL").await.map_err(|e| format!("shard {id}: BACKUP WAL: {e}"))?;
    }
    const HOLD_MS: u64 = 5_000;
    anchor.run(&format!("SHARD HOLD COMMITS {HOLD_MS}")).await.map_err(|e| format!("anchor shard {home}: {e}"))?;
    let mut held_at = Instant::now();
    let cut = async {
        // Renewed well before it lapses; a lapse means a decision may have
        // landed between two shards' points, and the cut is refused.
        let renew = async |held_at: &mut Instant| -> std::result::Result<(), String> {
            if held_at.elapsed() >= Duration::from_millis(HOLD_MS) {
                return Err("the commit hold lapsed while the shards were flushed; run the backup again".to_string());
            }
            if held_at.elapsed() >= Duration::from_millis(1_500) {
                anchor.run(&format!("SHARD HOLD COMMITS {HOLD_MS}")).await.map_err(|e| format!("anchor shard {home}: {e}"))?;
                *held_at = Instant::now();
            }
            Ok(())
        };
        // Every commit already decided, finished on every shard.
        let deadline = Instant::now() + Duration::from_secs(4);
        loop {
            let mut in_flight = Vec::new();
            for (id, c) in &shards {
                let prepared: Vec<(String, i64)> =
                    c.fetch_all("SHOW PREPARED TRANSACTIONS", &[]).await.map_err(|e| format!("shard {id}: {e}"))?;
                for (gid, _) in prepared {
                    let quoted = gid.replace('\'', "''");
                    let decided: Vec<(String,)> = anchor
                        .fetch_all(&format!("SHOW SHARD DECISION '{quoted}'"), &[])
                        .await
                        .map_err(|e| format!("anchor shard {home}: {e}"))?;
                    if decided.first().is_some_and(|(d,)| d == "commit") {
                        in_flight.push(format!("{gid} on shard {id}"));
                    }
                }
            }
            if in_flight.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(format!("decided commits did not finish on every shard: {}; run the backup again", in_flight.join(", ")));
            }
            renew(&mut held_at).await?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let mut points = Vec::new();
        for (id, c) in &shards {
            renew(&mut held_at).await?;
            c.run("BACKUP WAL").await.map_err(|e| format!("shard {id}: BACKUP WAL: {e}"))?;
            let Some(point) = newest_flush(c, *id).await? else {
                return Err(format!("shard {id}'s archive holds no log flush: is it started with --s3-bucket?"));
            };
            points.push((*id, point));
        }
        renew(&mut held_at).await?;
        // Out of the second of every point before letting decisions go: a
        // restore to a time takes every flush at or before it.
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        renew(&mut held_at).await?;
        Ok::<_, String>(points)
    }
    .await;
    let _ = anchor.run("SHARD RELEASE COMMITS").await;
    let points = cut?;
    let map_copy = format!("{out}.map");
    std::fs::write(&map_copy, map.to_text()).map_err(|e| format!("{map_copy}: {e}"))?;
    let created = points.iter().map(|(_, (at, _))| at.as_str()).max().unwrap_or("");
    let mut text = format!(
        "# rdb-shard cluster backup: restore with `rdb-shard restore --manifest {out}`\nformat = rdb-cluster-backup 1\ncluster = {:032x}\nmap_epoch = {}\nmap = {map_copy}\nanchor = {home}\ncreated = {created}\n",
        map.cluster_id(),
        map.epoch()
    );
    for (id, (at, lsn)) in &points {
        text.push_str(&format!("shard {id} = {at} lsn {lsn}\n"));
        println!("shard {id}: point {at} (lsn {lsn})");
    }
    let tmp = format!("{out}.tmp");
    std::fs::write(&tmp, &text).map_err(|e| format!("{tmp}: {e}"))?;
    std::fs::rename(&tmp, out).map_err(|e| format!("{out}: {e}"))?;
    println!("cluster backup written to {out}");
    Ok(())
}

/// A manifest `backup` wrote: the map's path, and each shard's point.
fn read_manifest(path: &str) -> std::result::Result<(String, Vec<(ShardId, String)>), String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let mut map = None;
    let mut points = Vec::new();
    let mut format_ok = false;
    for line in text.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')) {
        let (k, v) = line.split_once('=').map(|(k, v)| (k.trim(), v.trim())).ok_or_else(|| format!("{path}: {line:?} is not key = value"))?;
        match k {
            "format" => format_ok = v == "rdb-cluster-backup 1",
            "map" => map = Some(v.to_string()),
            _ if k.starts_with("shard ") => {
                let id: ShardId = k[6..].trim().parse().map_err(|_| format!("{path}: {k:?} is not a shard id"))?;
                let at = v.split_whitespace().next().ok_or_else(|| format!("{path}: shard {id} has no point"))?;
                points.push((id, at.to_string()));
            }
            _ => {}
        }
    }
    if !format_ok {
        return Err(format!("{path} is not an rdb-shard cluster backup manifest"));
    }
    Ok((map.ok_or_else(|| format!("{path} names no map"))?, points))
}

async fn cluster_restore(args: &Args, options: &Options) -> std::result::Result<(), String> {
    let manifest = args.one("manifest")?;
    let (map_path, points) = read_manifest(manifest)?;
    let map_path = args.opt("map").map(str::to_string).unwrap_or(map_path);
    let map = read_map(&map_path)?;
    // Each shard's archive must still hold its point; checked while they run.
    let mut missing = Vec::new();
    for (id, at) in &points {
        let c = connect(&map, *id, options).await?;
        let rows: Vec<BackupRow> = c.fetch_all("SHOW BACKUPS", &[]).await.map_err(|e| format!("shard {id}: SHOW BACKUPS: {e}"))?;
        let flushed = rows.iter().any(|r| r.0 == "wal" && r.2 == *at);
        let snapshot_before = rows.iter().any(|r| r.0 == "snapshot" && r.2.as_str() <= at.as_str());
        if !flushed || !snapshot_before {
            missing.push(format!(
                "shard {id}: {}",
                if !flushed { format!("no log flush at {at} (pruned, or another prefix?)") } else { format!("no snapshot at or before {at}") }
            ));
        }
    }
    if !missing.is_empty() {
        return Err(format!("the archives cannot restore this backup:\n  {}", missing.join("\n  ")));
    }
    println!("Every shard's archive holds its point. To restore the cluster to it:");
    println!("  1. Stop every gateway, then every shard.");
    println!("  2. Restore each shard into a new directory, with its usual --s3-* flags (its current prefix) and:");
    for (id, at) in &points {
        println!("       shard {id}: --restore-to <new dir> --restore-at-time '{at}'");
    }
    println!("  3. Start each shard on its new directory (--location) with a NEW --s3-prefix: the restore is a new");
    println!("     timeline, and the old prefix also holds what came after the point.");
    println!("  4. Roll back what was prepared and undecided at the cut: rdb-shard resolve --map {map_path} --grace 0");
    println!("  5. Start the gateways.");
    Ok(())
}

async fn resolve(args: &Args, options: &Options) -> std::result::Result<(), String> {
    let map = read_map(args.one("map")?)?;
    let home: ShardId = match args.opt("home") {
        Some(h) => h.parse().map_err(|_| "--home takes a shard id".to_string())?,
        None => map.shards().first().map(|s| s.id).ok_or("the map has no shards")?,
    };
    let grace: u64 = args.opt("grace").unwrap_or("300").parse().map_err(|_| "--grace takes seconds".to_string())?;
    let anchor = connect(&map, home, options).await?;
    let now_ns = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    for info in map.shards() {
        let c = connect(&map, info.id, options).await?;
        let prepared: Vec<(String, i64)> =
            c.fetch_all("SHOW PREPARED TRANSACTIONS", &[]).await.map_err(|e| format!("shard {}: {e}", info.id))?;
        for (gid, _) in prepared {
            if !gid.starts_with("rdb2pc:") {
                println!("shard {}: {gid} is not a gateway's; left alone", info.id);
                continue;
            }
            let quoted = gid.replace('\'', "''");
            let decided: Vec<(String,)> = anchor
                .fetch_all(&format!("SHOW SHARD DECISION '{quoted}'"), &[])
                .await
                .map_err(|e| format!("anchor shard {home}: {e}"))?;
            let commit = match decided.first().map(|(d,)| d.as_str()) {
                Some("commit") => true,
                Some(_) => false,
                None => {
                    // rdb2pc:<gateway>:<nanos hex>:<n>
                    let born = gid.split(':').nth(2).and_then(|h| u128::from_str_radix(h, 16).ok()).unwrap_or(0);
                    let age = now_ns.saturating_sub(born) / 1_000_000_000;
                    if age < grace as u128 {
                        println!("shard {}: {gid} is undecided and {age}s old; left for its gateway", info.id);
                        continue;
                    }
                    // Refused when a commit was decided first: the re-read
                    // below then says so.
                    let _ = anchor.run(&format!("SHARD DECIDE '{quoted}' ABORT")).await;
                    // Re-read: a gateway may have decided commit first.
                    let again: Vec<(String,)> = anchor
                        .fetch_all(&format!("SHOW SHARD DECISION '{quoted}'"), &[])
                        .await
                        .map_err(|e| format!("anchor shard {home}: {e}"))?;
                    again.first().is_some_and(|(d,)| d == "commit")
                }
            };
            let sql = if commit { format!("COMMIT PREPARED '{quoted}'") } else { format!("ROLLBACK PREPARED '{quoted}'") };
            c.run(&sql).await.map_err(|e| format!("shard {}: {gid}: {e}", info.id))?;
            println!("shard {}: {gid} {}", info.id, if commit { "committed" } else { "rolled back" });
        }
    }
    Ok(())
}
