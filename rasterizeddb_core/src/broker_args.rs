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

/// Flags for the Kafka protocol.
#[cfg(feature = "broker_kafka")]
#[derive(clap::Args, Debug, Clone)]
pub struct KafkaArgs {
    /// Port for the Kafka protocol (9092 is Kafka's own). Closed unless given.
    #[arg(long = "kafka-port", value_name = "PORT")]
    pub kafka_port: Option<u16>,

    /// Address the Kafka port listens on; `--broker-addr` when not given.
    #[arg(long = "kafka-addr", value_name = "ADDR")]
    pub kafka_addr: Option<String>,

    /// The host Kafka clients are told to connect to (Metadata,
    /// FindCoordinator). The listen address when not given, and required
    /// when that is a wildcard such as 0.0.0.0.
    #[arg(long = "kafka-advertised-host", value_name = "HOST")]
    pub kafka_advertised_host: Option<String>,

    /// The port clients are told, when a proxy or NAT puts them on another.
    #[arg(long = "kafka-advertised-port", value_name = "PORT")]
    pub kafka_advertised_port: Option<u16>,

    /// `sasl` (SASL PLAIN against the database's roles, the default) or
    /// `trust` (loopback only).
    #[arg(long = "kafka-auth", default_value = "sasl")]
    pub kafka_auth: String,

    /// TLS on the Kafka port (`SSL` / `SASL_SSL` to a client), with the
    /// PostgreSQL port's certificate.
    #[arg(long = "kafka-tls", default_value = "off", value_name = "on|off")]
    pub kafka_tls: String,

    /// Create a topic the first time a client names it, as Kafka's
    /// `auto.create.topics.enable`.
    #[arg(long = "kafka-auto-create", default_value_t = true, action = clap::ArgAction::Set)]
    pub kafka_auto_create: bool,

    /// Partitions of a topic created that way.
    #[arg(long = "kafka-default-partitions", default_value_t = 1)]
    pub kafka_default_partitions: u32,
}

#[cfg(feature = "broker_kafka")]
impl KafkaArgs {
    pub fn config(
        &self,
        broker: &BrokerArgs,
        cert: Option<&str>,
        key: Option<&str>,
    ) -> Result<Option<rasterizeddb_core::core::broker::kafka::KafkaConfig>, String> {
        use rasterizeddb_core::core::broker::kafka::{KafkaAuth, KafkaConfig};
        let Some(port) = self.kafka_port else { return Ok(None) };
        let addr = self.kafka_addr.clone().unwrap_or_else(|| broker.broker_addr.clone());
        let advertised = match &self.kafka_advertised_host {
            Some(h) => h.clone(),
            None if addr == "0.0.0.0" || addr == "::" => {
                return Err(format!(
                    "--kafka-port on {addr} needs --kafka-advertised-host: clients connect to the host the server names"
                ));
            }
            None => addr.clone(),
        };
        let auth = match self.kafka_auth.trim().to_ascii_lowercase().as_str() {
            "sasl" | "password" | "plain" => KafkaAuth::Sasl,
            "trust" | "none" => KafkaAuth::Trust,
            other => return Err(format!("--kafka-auth {other:?}: expected sasl or trust")),
        };
        let tls = match self.kafka_tls.trim().to_ascii_lowercase().as_str() {
            "on" | "true" | "yes" | "require" => Some(
                rasterizeddb_core::pgwire::tls_acceptor(cert, key).map_err(|e| format!("--kafka-tls on: {e}"))?,
            ),
            "off" | "false" | "no" | "disable" => None,
            other => return Err(format!("--kafka-tls {other:?}: expected on or off")),
        };
        Ok(Some(KafkaConfig {
            addr,
            port: Some(port),
            socket: None,
            advertised_host: advertised,
            advertised_port: self.kafka_advertised_port,
            auth,
            threads: broker.broker_threads,
            tls,
            auto_create: self.kafka_auto_create,
            default_partitions: self.kafka_default_partitions.max(1),
        }))
    }
}

/// Flags for AMQP 0-9-1.
#[cfg(feature = "broker_amqp")]
#[derive(clap::Args, Debug, Clone)]
pub struct AmqpArgs {
    /// Port for AMQP 0-9-1 (5672 is RabbitMQ's). Closed unless given.
    #[arg(long = "amqp-port", value_name = "PORT")]
    pub amqp_port: Option<u16>,

    /// Address the AMQP port listens on; `--broker-addr` when not given.
    #[arg(long = "amqp-addr", value_name = "ADDR")]
    pub amqp_addr: Option<String>,

    /// `password` (PLAIN/AMQPLAIN against the database's roles, the default)
    /// or `trust` (loopback only).
    #[arg(long = "amqp-auth", default_value = "password")]
    pub amqp_auth: String,

    /// TLS on the AMQP port (amqps://), with the PostgreSQL port's certificate.
    #[arg(long = "amqp-tls", default_value = "off", value_name = "on|off")]
    pub amqp_tls: String,

    /// The heartbeat the server proposes, in seconds (0 turns them off).
    #[arg(long = "amqp-heartbeat", default_value_t = 60)]
    pub amqp_heartbeat: u16,

    /// How long a delivered message may stay unacked before another consumer
    /// gets it, as RabbitMQ's `consumer_timeout`; a queue's
    /// `x-consumer-timeout` overrides it.
    #[arg(long = "amqp-consumer-timeout-ms", default_value_t = 1_800_000)]
    pub amqp_consumer_timeout_ms: u32,
}

#[cfg(feature = "broker_amqp")]
impl AmqpArgs {
    pub fn config(
        &self,
        broker: &BrokerArgs,
        cert: Option<&str>,
        key: Option<&str>,
    ) -> Result<Option<rasterizeddb_core::core::broker::amqp::AmqpConfig>, String> {
        use rasterizeddb_core::core::broker::amqp::{AmqpAuth, AmqpConfig};
        let Some(port) = self.amqp_port else { return Ok(None) };
        let auth = match self.amqp_auth.trim().to_ascii_lowercase().as_str() {
            "password" | "roles" | "plain" => AmqpAuth::Roles,
            "trust" | "none" => AmqpAuth::Trust,
            other => return Err(format!("--amqp-auth {other:?}: expected password or trust")),
        };
        let tls = match self.amqp_tls.trim().to_ascii_lowercase().as_str() {
            "on" | "true" | "yes" | "require" => Some(
                rasterizeddb_core::pgwire::tls_acceptor(cert, key).map_err(|e| format!("--amqp-tls on: {e}"))?,
            ),
            "off" | "false" | "no" | "disable" => None,
            other => return Err(format!("--amqp-tls {other:?}: expected on or off")),
        };
        Ok(Some(AmqpConfig {
            addr: self.amqp_addr.clone().unwrap_or_else(|| broker.broker_addr.clone()),
            port: Some(port),
            socket: None,
            auth,
            threads: broker.broker_threads,
            tls,
            frame_max: 131_072,
            heartbeat: self.amqp_heartbeat,
            consumer_timeout_ms: self.amqp_consumer_timeout_ms.max(1),
        }))
    }
}

/// Flags for MQTT.
#[cfg(feature = "broker_mqtt")]
#[derive(clap::Args, Debug, Clone)]
pub struct MqttArgs {
    /// Port for MQTT 3.1.1 and 5.0 (1883 is the usual; 8883 with TLS).
    /// Closed unless given.
    #[arg(long = "mqtt-port", value_name = "PORT")]
    pub mqtt_port: Option<u16>,

    /// Address the MQTT port listens on; `--broker-addr` when not given.
    #[arg(long = "mqtt-addr", value_name = "ADDR")]
    pub mqtt_addr: Option<String>,

    /// `password` (CONNECT's user name and password against the database's
    /// roles, the default) or `trust` (loopback only).
    #[arg(long = "mqtt-auth", default_value = "password")]
    pub mqtt_auth: String,

    /// TLS on the MQTT port, with the PostgreSQL port's certificate.
    #[arg(long = "mqtt-tls", default_value = "off", value_name = "on|off")]
    pub mqtt_tls: String,

    /// How persistent sessions' messages are synced: `async` (the default,
    /// synced every --broker-flush-ms) or `sync` (QoS 1/2 acknowledged once
    /// synced).
    #[arg(long = "mqtt-durability", default_value = "async", value_name = "sync|async")]
    pub mqtt_durability: String,

    /// The largest packet accepted, in bytes.
    #[arg(long = "mqtt-max-packet", default_value_t = 16 << 20)]
    pub mqtt_max_packet: usize,
}

#[cfg(feature = "broker_mqtt")]
impl MqttArgs {
    pub fn config(
        &self,
        broker: &BrokerArgs,
        cert: Option<&str>,
        key: Option<&str>,
    ) -> Result<Option<rasterizeddb_core::core::broker::mqtt::MqttConfig>, String> {
        use rasterizeddb_core::core::broker::mqtt::{MqttAuth, MqttConfig};
        use rasterizeddb_mq::wire::Durability;
        let Some(port) = self.mqtt_port else { return Ok(None) };
        let auth = match self.mqtt_auth.trim().to_ascii_lowercase().as_str() {
            "password" | "roles" => MqttAuth::Roles,
            "trust" | "none" => MqttAuth::Trust,
            other => return Err(format!("--mqtt-auth {other:?}: expected password or trust")),
        };
        let durability = match Durability::parse(&self.mqtt_durability) {
            Some(d @ (Durability::Sync | Durability::Async)) => d,
            _ => return Err(format!("--mqtt-durability {:?}: expected sync or async", self.mqtt_durability)),
        };
        let tls = match self.mqtt_tls.trim().to_ascii_lowercase().as_str() {
            "on" | "true" | "yes" | "require" => Some(
                rasterizeddb_core::pgwire::tls_acceptor(cert, key).map_err(|e| format!("--mqtt-tls on: {e}"))?,
            ),
            "off" | "false" | "no" | "disable" => None,
            other => return Err(format!("--mqtt-tls {other:?}: expected on or off")),
        };
        Ok(Some(MqttConfig {
            addr: self.mqtt_addr.clone().unwrap_or_else(|| broker.broker_addr.clone()),
            port: Some(port),
            socket: None,
            auth,
            threads: broker.broker_threads,
            tls,
            durability,
            max_packet: self.mqtt_max_packet.clamp(1024, 256 << 20),
        }))
    }
}
