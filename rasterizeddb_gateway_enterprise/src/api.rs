//! The enterprise gateway's paths on the configuration API
//! (`rasterizeddb_gateway::api`): per-user routes kept in the local store,
//! customers provisioned onto their shard, and the client statistics.
//!
//! | method | path | does |
//! |---|---|---|
//! | GET | `/v1/routes` | every route |
//! | PUT | `/v1/routes/{database}/{role}` | `{"shard": N}` or `{"refuse": true}`: stored, live at the role's next login |
//! | DELETE | `/v1/routes/{database}/{role}` | removes it |
//! | POST | `/v1/customers` | `{"database", "role", "password", "shard"}`: the database and its login role made on the shard, and the route |
//! | GET | `/v1/clients` | `SHOW CLIENT STATS` as JSON (feature `client_stats`) |
//!
//! Routes from a `--user-routes` file are read-only here (409).

use std::sync::Arc;

use rasterizeddb_gateway::api::{ApiContext, ApiExtension, BoxFuture, Reply, sql_text};
use rasterizeddb_gateway::hooks::Pin;
use serde_json::{Value, json};

use crate::Routes;

pub struct EnterpriseApi {
    pub routes: Arc<Routes>,
    /// The role that runs provisioning statements on a shard (a superuser
    /// of its operator database).
    pub shard_role: String,
    #[cfg(feature = "client_stats")]
    pub stats: Option<Arc<crate::stats::ClientStats>>,
    /// The client statistics' history interval and retention, kept in the
    /// configuration store once it opens.
    #[cfg(feature = "client_stats_embedded")]
    pub history: Option<(std::time::Duration, std::time::Duration)>,
}

const ROUTES_TABLE: &str =
    "CREATE TABLE IF NOT EXISTS user_routes (route TEXT PRIMARY KEY, database TEXT NOT NULL, role TEXT NOT NULL, target TEXT NOT NULL)";

fn route_key(database: &str, role: &str) -> String {
    format!("{database}\u{1f}{role}")
}

fn pin_json(p: &Pin) -> Value {
    match p {
        Pin::Shard(s) => json!({ "shard": s }),
        Pin::Refuse(_) => json!({ "refuse": true }),
        Pin::None => Value::Null,
    }
}

/// A name a customer's database or role may have: letters, digits and `_`,
/// starting with a letter, at most 63 bytes. Anything else would need
/// quoting rules on every protocol; refusing it is simpler and safe.
/// A route the gateway would not act on: `user_routing` is off, so every
/// login goes to the home shard whatever is stored.
fn routing_off(ctx: &ApiContext) -> Option<Reply> {
    (ctx.shared.cfg.user_routing == rasterizeddb_gateway::config::UserRoutingMode::Off).then(|| {
        (
            409,
            json!({ "error": "user_routing is off in this gateway's configuration, so a route would not be acted on: set user_routing = prefer or require (PUT /v1/settings/user_routing) and restart the gateway" }),
        )
    })
}

fn plain_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 63 && s.as_bytes()[0].is_ascii_alphabetic() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

impl EnterpriseApi {
    async fn put_route(&self, ctx: &ApiContext, database: &str, role: &str, pin: Pin) -> Result<(), String> {
        let target = match &pin {
            Pin::Shard(s) => s.to_string(),
            _ => "refuse".to_string(),
        };
        let sql = format!(
            "INSERT INTO user_routes (route, database, role, target) VALUES ({}, {}, {}, {}) ON CONFLICT (route) DO UPDATE SET target = EXCLUDED.target",
            sql_text(&route_key(database, role)),
            sql_text(database),
            sql_text(role),
            sql_text(&target)
        );
        ctx.store.run(&sql).await.map_err(|e| e.to_string())?;
        let pin = match pin {
            Pin::Refuse(_) => Pin::Refuse(format!("role \"{role}\" may not log in to \"{database}\" through this gateway")),
            p => p,
        };
        self.routes.set(database, role, Some(pin));
        Ok(())
    }

    fn pin_of(ctx: &ApiContext, body: &Value) -> Result<Pin, String> {
        if body.get("refuse").and_then(Value::as_bool) == Some(true) {
            return Ok(Pin::Refuse(String::new()));
        }
        let shard = body.get("shard").and_then(Value::as_u64).ok_or("the body is {\"shard\": N} or {\"refuse\": true}")?;
        let shard = u32::try_from(shard).map_err(|_| "a shard id is a u32".to_string())?;
        if ctx.shared.map.get().shard(shard).is_none() {
            return Err(format!("shard {shard} is not in the map"));
        }
        Ok(Pin::Shard(shard))
    }
}

impl ApiExtension for EnterpriseApi {
    fn schema(&self) -> Vec<String> {
        vec![ROUTES_TABLE.to_string()]
    }

    #[cfg(feature = "client_stats_embedded")]
    fn store_ready(&self, store: Arc<rasterizeddb_client::Client>, rt: tokio::runtime::Handle) {
        let (Some(stats), Some((flush, retention))) = (&self.stats, self.history) else { return };
        let h = crate::stats::History::start_on(store, &rt, flush, retention, Arc::downgrade(stats));
        if stats.set_history(h) {
            log::info!("client statistics history kept in the configuration store, an interval every {} s", flush.as_secs());
        }
    }

    fn load<'a>(&'a self, store: &'a rasterizeddb_client::Client) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if self.routes.from_file() {
                return Ok(());
            }
            let rows = store.run("SELECT database, role, target FROM user_routes").await.map_err(|e| e.to_string())?;
            for r in rows.iter() {
                let (db, role, target) = (
                    r.get::<String>(0).map_err(|e| e.to_string())?,
                    r.get::<String>(1).map_err(|e| e.to_string())?,
                    r.get::<String>(2).map_err(|e| e.to_string())?,
                );
                let pin = match target.parse::<u32>() {
                    Ok(s) => Pin::Shard(s),
                    Err(_) => Pin::Refuse(format!("role \"{role}\" may not log in to \"{db}\" through this gateway")),
                };
                self.routes.set(&db, &role, Some(pin));
            }
            log::info!("{} user route(s) loaded from the configuration store", rows.len());
            Ok(())
        })
    }

    fn handle<'a>(&'a self, ctx: &'a ApiContext, method: &'a str, path: &'a str, _query: &'a str, body: &'a Value) -> Option<BoxFuture<'a, Reply>> {
        let segs: Vec<String> = path.split('/').filter(|s| !s.is_empty()).map(str::to_string).collect();
        let seg: Vec<&str> = segs.iter().map(String::as_str).collect();
        let fut: BoxFuture<'a, Reply> = match (method, seg.as_slice()) {
            ("GET", ["v1", "routes"]) => {
                let list: Vec<Value> = self
                    .routes
                    .entries()
                    .iter()
                    .map(|((db, role), pin)| {
                        let mut v = json!({ "database": db, "role": role });
                        if let (Value::Object(m), Value::Object(p)) = (&mut v, pin_json(pin)) {
                            m.extend(p);
                        }
                        v
                    })
                    .collect();
                Box::pin(async move { (200, json!({ "routes": list, "from_file": self.routes.from_file() })) })
            }
            ("PUT", ["v1", "routes", db, role]) => {
                let (db, role) = (db.to_string(), role.to_string());
                Box::pin(async move {
                    if self.routes.from_file() {
                        return (409, json!({ "error": "routes come from the --user-routes file; edit it there" }));
                    }
                    if let Some(r) = routing_off(ctx) {
                        return r;
                    }
                    let pin = match Self::pin_of(ctx, body) {
                        Ok(p) => p,
                        Err(e) => return (400, json!({ "error": e })),
                    };
                    match self.put_route(ctx, &db, &role, pin.clone()).await {
                        Ok(()) => (200, json!({ "database": db, "role": role, "route": pin_json(&pin) })),
                        Err(e) => (500, json!({ "error": e })),
                    }
                })
            }
            ("DELETE", ["v1", "routes", db, role]) => {
                let (db, role) = (db.to_string(), role.to_string());
                Box::pin(async move {
                    if self.routes.from_file() {
                        return (409, json!({ "error": "routes come from the --user-routes file; edit it there" }));
                    }
                    let sql = format!("DELETE FROM user_routes WHERE route = {}", sql_text(&route_key(&db, &role)));
                    match ctx.store.run(&sql).await {
                        Ok(r) if r.rows_affected().unwrap_or(0) > 0 => {
                            self.routes.set(&db, &role, None);
                            (200, json!({ "database": db, "role": role, "removed": true }))
                        }
                        Ok(_) => (404, json!({ "error": format!("no route for {db}/{role}") })),
                        Err(e) => (500, json!({ "error": e.to_string() })),
                    }
                })
            }
            ("POST", ["v1", "customers"]) => Box::pin(async move {
                if self.routes.from_file() {
                    return (409, json!({ "error": "routes come from the --user-routes file; a customer's route cannot be stored" }));
                }
                if let Some(r) = routing_off(ctx) {
                    return r;
                }
                let text = |k: &str| body.get(k).and_then(Value::as_str).map(str::to_string);
                let (Some(db), Some(role), Some(password)) = (text("database"), text("role"), text("password")) else {
                    return (400, json!({ "error": "the body is {\"database\", \"role\", \"password\", \"shard\"}" }));
                };
                if !plain_name(&db) || !plain_name(&role) {
                    return (400, json!({ "error": "a database or role name is letters, digits and _, starting with a letter, at most 63 bytes" }));
                }
                if password.len() < 12 {
                    return (400, json!({ "error": "the password must be at least 12 characters" }));
                }
                let pin = match Self::pin_of(ctx, body) {
                    Ok(Pin::Shard(s)) => s,
                    Ok(_) => return (400, json!({ "error": "a customer is placed on a shard: {\"shard\": N}" })),
                    Err(e) => return (400, json!({ "error": e })),
                };
                // The database, then its login role inside it, then what the
                // role may do there.
                let made = rasterizeddb_gateway::api::shard_statements(&ctx.shared, pin, "postgres", &self.shard_role, &[format!("CREATE DATABASE {db}")]).await;
                if let Err(e) = made {
                    return (409, json!({ "error": e }));
                }
                let statements = vec![
                    format!("CREATE ROLE {role} LOGIN PASSWORD {}", sql_text(&password)),
                    format!("GRANT ALL ON DATABASE {db} TO {role}"),
                    format!("GRANT CREATE ON SCHEMA public TO {role}"),
                ];
                if let Err(e) = rasterizeddb_gateway::api::shard_statements(&ctx.shared, pin, &db, &self.shard_role, &statements).await {
                    return (500, json!({ "error": format!("the database {db} was made on shard {pin}, its role was not: {e}") }));
                }
                // The shard admits the role's pinned sessions from now on: a
                // server administration statement, so in the operator
                // database, as CREATE DATABASE.
                let host = format!("SHARD HOST ROLE {} {}", sql_text(&db), sql_text(&role));
                if let Err(e) = rasterizeddb_gateway::api::shard_statements(&ctx.shared, pin, "postgres", &self.shard_role, &[host]).await {
                    return (500, json!({ "error": format!("{db} and {role} were made on shard {pin}, but the shard does not host the role yet (needs the enterprise server with pinned_roles_file): {e}") }));
                }
                match self.put_route(ctx, &db, &role, Pin::Shard(pin)).await {
                    Ok(()) => (201, json!({ "database": db, "role": role, "shard": pin })),
                    Err(e) => (500, json!({ "error": format!("{db} and {role} were made on shard {pin}, the route was not stored: {e}") })),
                }
            }),
            #[cfg(feature = "client_stats")]
            ("GET", ["v1", "clients"]) => Box::pin(async move {
                let Some(stats) = &self.stats else {
                    return (404, json!({ "error": "client statistics are off (--client-stats off)" }));
                };
                let list: Vec<Value> = stats
                    .snapshot()
                    .iter()
                    .map(|(k, a)| {
                        json!({
                            "database": k.database, "role": k.role, "client": k.client, "application": k.application,
                            "sessions_open": a.open, "sessions": a.sessions, "statements": a.statements, "errors": a.errors,
                            "p50_us": a.quantile_us(0.5), "p95_us": a.quantile_us(0.95), "p99_us": a.quantile_us(0.99), "max_us": a.latency_max_us,
                            "client_bytes_in": a.client_in, "client_bytes_out": a.client_out, "shard_calls": a.shard_calls,
                            "shard_bytes_out": a.shard_out_plain, "shard_bytes_out_wire": a.shard_out_wire,
                            "shard_bytes_in": a.shard_in_plain, "shard_bytes_in_wire": a.shard_in_wire, "last_seen": a.last_seen,
                        })
                    })
                    .collect();
                (200, json!({ "clients": list }))
            }),
            _ => return None,
        };
        Some(fut)
    }
}
