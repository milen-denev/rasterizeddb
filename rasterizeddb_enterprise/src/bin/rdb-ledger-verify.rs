//! The offline ledger verifier (docs/ledger-and-vault-plan.md §3.4): checks
//! a database's chain and its anchors without a server, as an auditor would
//! on a copy of the files.
//!
//! ```text
//! rdb-ledger-verify --store <database directory> [--ledger-dir <dir>] [--anchor dir:<path> | s3://…]…
//! ```
//!
//! `--store` is the database's directory as the server knows it: it names the
//! chain (`<ledger-dir>/<store>/chain.log`, or `<store>/rdb_ledger/chain.log`
//! without a ledger directory) and the anchors' directory under each sink.
//! Exits 0 when everything checked is intact, 1 when something is not, 2 on
//! a usage error.

use rasterizeddb_enterprise::ledger::{anchor, store};

fn main() {
    let mut args = std::env::args().skip(1);
    let (mut store_key, mut ledger_dir, mut sinks) = (None, None, Vec::new());
    while let Some(a) = args.next() {
        match a.as_str() {
            "--store" => store_key = args.next(),
            "--ledger-dir" => ledger_dir = args.next(),
            "--anchor" => sinks.extend(args.next()),
            "-h" | "--help" => {
                println!("rdb-ledger-verify --store <database directory> [--ledger-dir <dir>] [--anchor <sink>]…");
                return;
            }
            other => {
                eprintln!("unknown argument {other}");
                std::process::exit(2);
            }
        }
    }
    let Some(store_key) = store_key else {
        eprintln!("--store is required");
        std::process::exit(2);
    };
    if let Some(dir) = ledger_dir {
        store::set_ledger_dir(dir.into());
    }
    for s in sinks {
        anchor::add_global_sink(s);
    }
    let path = store::chain_dir(&store_key).join("chain.log");
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
    anchor::load_acks(&store_key);
    for line in anchor::verify_lines(&store_key, &read) {
        bad |= line.starts_with("TAMPERED") || line.starts_with("UNKNOWN");
        println!("{line}");
    }
    std::process::exit(if bad { 1 } else { 0 });
}
