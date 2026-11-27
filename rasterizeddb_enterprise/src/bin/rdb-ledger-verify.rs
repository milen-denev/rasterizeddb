//! The offline ledger verifier (docs/ledger-and-vault-plan.md §3.4): checks
//! a database's chain and anchors, and with `--data` every row of every
//! ledger table, without a running server, as an auditor would.
//!
//! ```text
//! rdb-ledger-verify --store <data directory as the server knew it>
//!                   [--data <a copy of it, or the directory with the server stopped>]
//!                   [--default-db <name>] [--ledger-dir <dir>] [--anchor <sink>]…
//! ```
//!
//! - `--store` names the chain: the data directory's path as the server ran
//!   with it, since the chain is kept under that name
//!   (`<ledger-dir>/<store>/chain.log`, or `<store>/rdb_ledger/chain.log`).
//! - `--data` opens a data directory and reads every ledger table's rows. Give
//!   a copy (a restored backup, a snapshot) or the directory of a stopped
//!   server: opening takes the directory's lock, so a running server's is
//!   refused, and opening may replay its write-ahead log.
//!   Each database found under it is checked against the chain of the same
//!   database under `--store`.
//!
//! Exits 0 when everything checked is intact, 1 when something is not, 2 on
//! a usage error.

use rasterizeddb_core::core::ledger_hooks::LedgerRequest;
use rasterizeddb_enterprise::ledger::{anchor, store};

fn usage() -> ! {
    eprintln!(
        "rdb-ledger-verify --store <data dir as served> [--data <copy>] [--default-db <name>] \
         [--ledger-dir <dir>] [--anchor <sink>]…"
    );
    std::process::exit(2);
}

fn main() {
    let mut args = std::env::args().skip(1);
    let (mut store_key, mut data, mut ledger_dir, mut sinks) = (None, None, None, Vec::new());
    let mut default_db = "rasterizeddb".to_string();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--store" => store_key = args.next(),
            "--data" => data = args.next(),
            "--default-db" => default_db = args.next().unwrap_or_else(|| usage()),
            "--ledger-dir" => ledger_dir = args.next(),
            "--anchor" => sinks.extend(args.next()),
            "-h" | "--help" => usage(),
            other => {
                eprintln!("unknown argument {other}");
                usage();
            }
        }
    }
    let Some(store_key) = store_key else { usage() };
    if let Some(dir) = ledger_dir {
        store::set_ledger_dir(dir.into());
    }
    for s in sinks {
        anchor::add_global_sink(s);
    }

    let mut bad = chain_and_anchors(&store_key);
    if let Some(data) = data {
        bad |= rows(&store_key, &data, &default_db);
    }
    std::process::exit(if bad { 1 } else { 0 });
}

/// The chain's links and the anchors: whether anything is wrong.
fn chain_and_anchors(store_key: &str) -> bool {
    let path = store::chain_dir(store_key).join("chain.log");
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("{}: {e}", path.display());
            std::process::exit(2);
        }
    };
    let read = store::read(&bytes);
    let mut bad = false;
    match &read.broken {
        Some((seq, why)) => {
            bad = true;
            println!("TAMPERED  chain {}: {why} (first bad entry {seq})", path.display());
        }
        None => println!(
            "OK        chain {}: {} entries, head {}{}",
            path.display(),
            read.entries.len(),
            read.entries.last().map_or_else(|| "(empty)".to_string(), |e| store::hex(&e.hash)),
            if read.torn_tail { " (a torn last append, ignored)" } else { "" }
        ),
    }
    anchor::load_acks(store_key);
    for line in anchor::verify_lines(store_key, &read) {
        bad |= line.starts_with("TAMPERED") || line.starts_with("UNKNOWN");
        println!("{line}");
    }
    bad
}

/// Every ledger table's rows under `data`, against the chain of the same
/// database under `store_key`: whether anything is wrong.
fn rows(store_key: &str, data: &str, default_db: &str) -> bool {
    if let Err(e) = rasterizeddb_enterprise::install_always() {
        eprintln!("ledger hooks: {e}");
        std::process::exit(2);
    }
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap_or_else(|e| {
        eprintln!("runtime: {e}");
        std::process::exit(2);
    });
    runtime.block_on(async move {
        let _ = rasterizeddb_core::MAX_PERMITS_THREADS.get_or_init(|| 2);
        let _ = rasterizeddb_core::BATCH_SIZE.get_or_init(|| 1024);
        let catalog = match rasterizeddb_core::core::database::catalog::DatabaseCatalog::open(Some(data), default_db).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("{data}: {e}");
                std::process::exit(2);
            }
        };
        let data_root = data.trim_end_matches('/');
        let store_root = store_key.trim_end_matches('/');
        let mut bad = false;
        for name in catalog.names() {
            let Some(db) = catalog.get(&name) else { continue };
            // The chain of this database as the server named it: the copy's
            // path with the copy's root swapped for the served one.
            let here = db.ledger_store();
            let served = match here.strip_prefix(data_root) {
                Some(rest) => format!("{store_root}{rest}"),
                None => here.clone(),
            };
            let mut held = Vec::new();
            for t in db.relation_names() {
                let Some(rasterizeddb_core::core::database::relation::Relation::Plain(table)) = db.relation(&t) else {
                    continue;
                };
                if !table.schema.special.is_special() {
                    continue;
                }
                match table.ledger_rows().await {
                    Ok(rows) => held.push((table, rows)),
                    Err(e) => {
                        bad = true;
                        println!("UNKNOWN   {name}.{t}: rows could not be read: {e}");
                    }
                }
            }
            if held.is_empty() {
                continue;
            }
            let request = LedgerRequest {
                sql: "VERIFY LEDGER",
                tables: held.iter().map(|(t, rows)| (&t.schema, rows.clone())).collect(),
                versions: Vec::new(),
                role: "rdb-ledger-verify",
                superuser: true,
                tx: 0,
            };
            match rasterizeddb_core::core::ledger_hooks::statement(&served, request) {
                Ok(reply) => {
                    for l in reply.lines.iter().filter(|l| !l.contains(" chain:") && !l.contains("anchor")) {
                        bad |= l.starts_with("TAMPERED") || l.starts_with("UNKNOWN");
                        println!("{l}   [{name}]");
                    }
                }
                Err(e) => {
                    bad = true;
                    println!("UNKNOWN   {name}: {e}");
                }
            }
        }
        bad
    })
}
