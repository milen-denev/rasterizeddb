//! Flags for the message broker (docs/broker.md).

use rasterizeddb_core::core::broker::net::{NativeAuth, NativeConfig};
use rasterizeddb_core::core::broker::{self, DdlPolicy};
use rasterizeddb_core::core::listen::SocketSpec;

#[derive(clap::Args, Debug, Clone)]
pub struct BrokerArgs {
    /// Port for the broker's native protocol (rdbmq/1; 7171 is the usual
    /// choice). Closed unless given; `BROKER …` statements on the SQL ports
    /// work either way.
    #[arg(long = "broker-port", value_name = "PORT")]
    pub broker_port: Option<u16>,

    /// Serve the native protocol on this unix socket, beside `--broker-port`
    /// or instead of it. Its permissions and group are `--socket-mode` and
    /// `--socket-group`.
    #[arg(long = "broker-socket", value_name = "PATH")]
    pub broker_socket: Option<std::path::PathBuf>,

    /// Address the broker's ports listen on.
    #[arg(long = "broker-addr", default_value = "127.0.0.1")]
    pub broker_addr: String,

    /// How broker clients authenticate: `password` checks them against the
    /// database's roles (the default); `trust` asks nothing and is refused
    /// on anything but loopback.
    #[arg(long = "broker-auth", default_value = "password")]
    pub broker_auth: String,

    /// TLS on `--broker-port`, with the PostgreSQL port's certificate and key
    /// (`--pg-cert`/`--pg-key`, or a self-signed pair made at start).
    #[arg(long = "broker-tls", default_value = "off", value_name = "on|off")]
    pub broker_tls: String,

    /// I/O threads for the broker's ports; one per CPU by default.
    #[arg(long = "broker-threads", value_name = "N")]
    pub broker_threads: Option<usize>,

    /// Milliseconds between syncs of `async` topics.
    #[arg(long = "broker-flush-ms", value_name = "MS")]
    pub broker_flush_ms: Option<u64>,

    /// Who may create, alter and drop topics, queues, exchanges and
    /// bindings: `any` role that logged in (the default, as RabbitMQ's
    /// default permissions) or `superuser`.
    #[arg(long = "broker-ddl", default_value = "any", value_name = "any|superuser")]
    pub broker_ddl: String,
}

impl BrokerArgs {
    /// Applies the process-wide settings.
    pub fn apply(&self) -> Result<(), String> {
        if let Some(ms) = self.broker_flush_ms {
            broker::set_flush_ms(ms);
        }
        let ddl = DdlPolicy::parse(&self.broker_ddl)
            .ok_or_else(|| format!("--broker-ddl {:?}: expected any or superuser", self.broker_ddl))?;
        broker::set_ddl_policy(ddl);
        Ok(())
    }

    fn tls(&self, cert: Option<&str>, key: Option<&str>) -> Result<Option<tokio_rustls::TlsAcceptor>, String> {
        match self.broker_tls.trim().to_ascii_lowercase().as_str() {
            "on" | "true" | "yes" | "require" => rasterizeddb_core::pgwire::tls_acceptor(cert, key)
                .map(Some)
                .map_err(|e| format!("--broker-tls on: {e}")),
            "off" | "false" | "no" | "disable" => Ok(None),
            other => Err(format!("--broker-tls {other:?}: expected on or off")),
        }
    }

    /// The native listener's settings, or `None` when neither a port nor a
    /// socket was given.
    pub fn native(
        &self,
        socket_mode: u32,
        socket_group: Option<u32>,
        cert: Option<&str>,
        key: Option<&str>,
    ) -> Result<Option<NativeConfig>, String> {
        if self.broker_port.is_none() && self.broker_socket.is_none() {
            return Ok(None);
        }
        let tls = self.tls(cert, key)?;
        if tls.is_some() && self.broker_port.is_none() {
            return Err("--broker-tls on needs --broker-port: the unix socket is served without TLS".into());
        }
        let auth = match self.broker_auth.trim().to_ascii_lowercase().as_str() {
            "password" | "roles" | "scram-sha-256" => NativeAuth::Roles,
            "trust" => NativeAuth::Trust,
            other => return Err(format!("--broker-auth {other:?}: expected password or trust")),
        };
        Ok(Some(NativeConfig {
            addr: self.broker_addr.clone(),
            port: self.broker_port,
            socket: self.broker_socket.clone().map(|path| SocketSpec { path, mode: socket_mode, group: socket_group }),
            auth,
            threads: self.broker_threads,
            tls,
        }))
    }
}
