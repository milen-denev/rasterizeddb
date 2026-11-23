// How many columns `Row` should hold inline.
//
//   cargo build --release -p rasterizeddb_core --example row_inline_capacity_bench
//   target/release/examples/row_inline_capacity_bench [--rows 500000] [--rounds 5] [--tune-alloc on]
//
// `Row.columns` is a `SmallVec<[Column; N]>`. Changing N touches every file
// that spells the type out, so this measures the choice on a stand-in first:
// the same `Column`, the same pool allocations and the same shapes of work the
// engine does with a materialised result —
//
//   scan:  build each row column by column, `mem::take` it into a growing
//          per-worker `Vec`, concatenate the workers' vectors, serialise.
//   group: the same materialisation, then fold by one column into a hash map.
//
// for inline capacities 8, 12, 16 and 32 against rows of 4, 8, 12, 20 and 40
// columns. Each (rows, columns) cell interleaves the capacities round by round
// so drift on this box hits all of them alike, and reports the minimum.

use std::time::{Duration, Instant};

use rasterizeddb_core::core::db_type::DbType;
use rasterizeddb_core::core::row::row::Column;
use rasterizeddb_core::memory_pool::MEMORY_POOL;
use smallvec::SmallVec;

// `smallvec` 1.x implements `Array` per length rather than over a const
// generic, so the stand-in is generic over the array type.
struct RowN<A: smallvec::Array<Item = Column>> {
    #[allow(dead_code)]
    position: u64,
    columns: SmallVec<A>,
    #[allow(dead_code)]
    length: u32,
}

impl<A: smallvec::Array<Item = Column>> Default for RowN<A> {
    fn default() -> Self {
        RowN {
            position: 0,
            columns: SmallVec::new(),
            length: 0,
        }
    }
}

const WORKERS: usize = 8;
const BATCH: usize = 4096;

fn column_bytes(row: usize, col: usize) -> ([u8; 16], usize) {
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&((row * 31 + col) as u64).to_le_bytes());
    b[8..].copy_from_slice(&((row % 97) as u64).to_le_bytes());
    // Mix of 8- and 16-byte values, like integer and text columns.
    (b, if col % 2 == 0 { 8 } else { 16 })
}

fn materialise<A: smallvec::Array<Item = Column>>(rows: usize, cols: usize) -> Vec<RowN<A>> {
    let per_worker = rows / WORKERS;
    let mut parts: Vec<Vec<RowN<A>>> = Vec::with_capacity(WORKERS);
    for w in 0..WORKERS {
        let mut acc: Vec<RowN<A>> = Vec::new();
        let mut buffer = RowN::<A>::default();
        for i in 0..per_worker {
            if i % BATCH == 0 {
                acc.reserve(BATCH.min(per_worker - i));
            }
            let r = w * per_worker + i;
            for c in 0..cols {
                let (bytes, len) = column_bytes(r, c);
                let mut data = MEMORY_POOL.acquire(len);
                data.into_slice_mut().copy_from_slice(&bytes[..len]);
                buffer.columns.push(Column {
                    schema_id: c as u32,
                    data,
                    column_type: DbType::I64,
                });
            }
            acc.push(std::mem::take(&mut buffer));
        }
        parts.push(acc);
    }
    // `drain_rows`: one vector out of the workers'.
    let total: usize = parts.iter().map(Vec::len).sum();
    let mut all = Vec::with_capacity(total);
    for p in parts {
        all.extend(p);
    }
    all
}

fn serialise<A: smallvec::Array<Item = Column>>(rows: &[RowN<A>]) -> usize {
    let mut out = Vec::with_capacity(rows.len() * 64);
    for row in rows {
        let payload: usize = row.columns.iter().map(|c| 17 + c.data.len()).sum();
        out.extend_from_slice(&(payload as u32).to_le_bytes());
        for c in &row.columns {
            let d = c.data.into_slice();
            out.extend_from_slice(&(d.len() as u64).to_le_bytes());
            out.extend_from_slice(&c.schema_id.to_le_bytes());
            out.extend_from_slice(d);
            out.push(c.column_type.to_byte());
        }
    }
    out.len()
}

fn group<A: smallvec::Array<Item = Column>>(rows: &[RowN<A>]) -> usize {
    let mut groups: ahash::HashMap<Vec<u8>, u64> = ahash::HashMap::default();
    for row in rows {
        let key = row.columns.get(1).or(row.columns.first()).map(|c| c.data.into_slice());
        if let Some(key) = key {
            *groups.entry(key[8..].to_vec()).or_default() += 1;
        }
    }
    groups.len()
}

fn run<A: smallvec::Array<Item = Column>>(rows: usize, cols: usize, work: &str) -> Duration {
    let t = Instant::now();
    let all = materialise::<A>(rows, cols);
    let n = match work {
        "scan" => serialise(&all),
        _ => group(&all),
    };
    drop(all);
    std::hint::black_box(n);
    t.elapsed()
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut rows = 500_000usize;
    let mut rounds = 5usize;
    let mut i = 0;
    while i < argv.len() {
        match argv[i].as_str() {
            "--rows" => {
                i += 1;
                rows = argv[i].parse().unwrap()
            }
            "--rounds" => {
                i += 1;
                rounds = argv[i].parse().unwrap()
            }
            "--tune-alloc" => {
                i += 1;
                if argv[i] == "on" {
                    rasterizeddb_core::tune_allocator();
                }
            }
            other => panic!("unknown argument {other}"),
        }
        i += 1;
    }
    println!(
        "size_of Row stand-in: N=8 {} B, N=12 {} B, N=16 {} B, N=32 {} B",
        std::mem::size_of::<RowN<[Column; 8]>>(),
        std::mem::size_of::<RowN<[Column; 12]>>(),
        std::mem::size_of::<RowN<[Column; 16]>>(),
        std::mem::size_of::<RowN<[Column; 32]>>()
    );
    println!("{rows} rows, {rounds} rounds, min wall clock per (materialise + work + drop)");
    for work in ["scan", "group"] {
        for cols in [4usize, 8, 12, 20, 40] {
            let mut best = [Duration::MAX; 4];
            for _ in 0..rounds {
                let t = [
                    run::<[Column; 8]>(rows, cols, work),
                    run::<[Column; 12]>(rows, cols, work),
                    run::<[Column; 16]>(rows, cols, work),
                    run::<[Column; 32]>(rows, cols, work),
                ];
                for (b, t) in best.iter_mut().zip(t) {
                    *b = (*b).min(t);
                }
            }
            println!(
                "{work:<5} cols={cols:>2}  N=8 {:>8.1?}  N=12 {:>8.1?}  N=16 {:>8.1?}  N=32 {:>8.1?}",
                best[0], best[1], best[2], best[3]
            );
        }
    }
}
