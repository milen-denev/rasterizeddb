//! The range map: which partition owns each routing point (§1.1, §1.2).
//!
//! The code lives in `rasterizeddb_router`, so the client, the gateway and
//! the engine share one copy of route_v1's map.

pub use rasterizeddb_router::map::*;
