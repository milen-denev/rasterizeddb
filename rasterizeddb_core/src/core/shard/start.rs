//! Starting a node as a shard (sharding-plan.md B1a.1, B1a.3, B0.8).
//!
//! `--role shard --cluster-map FILE --shard-id N [--cluster-id HEX]`:
//! 1. the WAL must be on (a shard's ownership and its moves are logged);
//! 2. the map file (text or binary form, `rasterizeddb_router::ShardMap`)
//!    must name this cluster and this shard;
//! 3. its epoch must be at least the highest this data directory has seen,
//!    and at an equal epoch its digest must match: two maps under one epoch
//!    mean someone edited one without raising it, and serving either could
//!    route a row two ways;
//! 4. the directory is marked `shard_v1` (`incompat_features`), so a build
//!    without `sharding` refuses it instead of serving every range;
//! 5. the shard's ranges are installed as its [`Ownership`].

use std::path::{Path, PathBuf};

use rasterizeddb_router::ShardMap;

use super::ownership::{self, Ownership};

/// What a start was given.
#[derive(Clone, Debug)]
pub struct ShardConfig {
    /// The data directory: where the epoch file and the marker live.
    pub data_dir: PathBuf,
    pub map_file: PathBuf,
    pub shard_id: u32,
    /// When given, the map must name this cluster.
    pub cluster_id: Option<u128>,
}

/// A started shard.
#[derive(Clone, Debug)]
pub struct ShardStarted {
    pub map: ShardMap,
    pub shard_id: u32,
}

/// The file recording the highest map epoch, and its digest, this directory
/// has served under.
pub const EPOCH_FILE: &str = "rdb_shard_epoch";

/// Reads a map in either form, by its first bytes.
pub fn read_map(path: &Path) -> Result<ShardMap, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("cannot read the cluster map {}: {e}", path.display()))?;
    if bytes.starts_with(&rasterizeddb_router::shard_map::BINARY_MAGIC) {
        ShardMap::from_binary(&bytes)
    } else {
        let text = String::from_utf8(bytes).map_err(|_| format!("the cluster map {} is not UTF-8 text", path.display()))?;
        ShardMap::from_text(&text)
    }
    .map_err(|e| format!("the cluster map {}: {e}", path.display()))
}

/// The highest `(epoch, digest)` recorded in `dir`, if any.
pub fn recorded_epoch(dir: &Path) -> Result<Option<(u64, u64)>, String> {
    let path = dir.join(EPOCH_FILE);
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
    };
    let mut words = text.split_whitespace();
    let epoch = words.next().and_then(|w| w.parse::<u64>().ok());
    let digest = words.next().and_then(|w| u64::from_str_radix(w, 16).ok());
    match (epoch, digest) {
        (Some(e), Some(d)) => Ok(Some((e, d))),
        _ => Err(format!("{} is malformed; it holds `<epoch> <digest hex>`", path.display())),
    }
}

/// Records `(epoch, digest)` durably: written beside, synced, renamed over,
/// and the directory synced.
pub fn record_epoch(dir: &Path, epoch: u64, digest: u64) -> Result<(), String> {
    use std::io::Write;
    let path = dir.join(EPOCH_FILE);
    let tmp = dir.join(format!("{EPOCH_FILE}.tmp"));
    let io = |e: std::io::Error| format!("cannot record the map epoch in {}: {e}", path.display());
    let mut f = std::fs::File::create(&tmp).map_err(io)?;
    f.write_all(format!("{epoch} {digest:016x}\n").as_bytes()).map_err(io)?;
    f.sync_all().map_err(io)?;
    drop(f);
    std::fs::rename(&tmp, &path).map_err(io)?;
    crate::core::sync_path::sync_dir(dir).map_err(io)?;
    Ok(())
}

/// Checks `map` against what `dir` has served: never a lower epoch, never a
/// different map under the same one.
pub fn check_epoch(dir: &Path, map: &ShardMap) -> Result<(), String> {
    if let Some((epoch, digest)) = recorded_epoch(dir)? {
        if map.epoch() < epoch {
            return Err(format!(
                "57P03: the cluster map is at epoch {}, but this shard has served epoch {epoch}; reload a current map",
                map.epoch()
            ));
        }
        if map.epoch() == epoch && map.digest() != digest {
            return Err(format!(
                "57P03: the cluster map at epoch {epoch} has digest {:016x}, but this shard served epoch {epoch} with digest {digest:016x}; a changed map must raise its epoch",
                map.digest()
            ));
        }
    }
    Ok(())
}

/// Starts this node as a shard (module docs).
pub fn start(config: &ShardConfig) -> Result<ShardStarted, String> {
    if !crate::wal_enabled() {
        return Err("--role shard needs the write-ahead log, which is turned off".to_string());
    }
    let map = read_map(&config.map_file)?;
    if let Some(cluster) = config.cluster_id
        && cluster != map.cluster_id()
    {
        return Err(format!(
            "the cluster map is for cluster {:032x}, not {cluster:032x}",
            map.cluster_id()
        ));
    }
    if map.shard(config.shard_id).is_none() {
        return Err(format!("the cluster map has no shard {}", config.shard_id));
    }
    std::fs::create_dir_all(&config.data_dir)
        .map_err(|e| format!("cannot create {}: {e}", config.data_dir.display()))?;
    check_epoch(&config.data_dir, &map)?;
    crate::core::database::incompat::require(&config.data_dir, crate::core::database::incompat::SHARD_V1)
        .map_err(|e| format!("cannot mark {} as a shard: {e}", config.data_dir.display()))?;
    record_epoch(&config.data_dir, map.epoch(), map.digest())?;
    ownership::install(Ownership {
        shard_id: config.shard_id,
        map_epoch: map.epoch(),
        owned: map.ranges_of(config.shard_id),
        incoming: Vec::new(),
    });
    super::map_state::install(map.clone(), config.shard_id);
    Ok(ShardStarted { map, shard_id: config.shard_id })
}
