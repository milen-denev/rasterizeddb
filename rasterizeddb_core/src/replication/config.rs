//! What a node's replication behaviour is made of, and how it is spelled.
//!
//! Every knob here is reachable three ways — a CLI flag at start-up, a
//! `REPLICATION SET` statement at runtime, and this struct in an embedding
//! program — so the parsing lives here rather than in any one of the three.

use std::fmt;
use std::time::Duration;

/// A node's identity within a cluster. Must be unique: it is half of every
/// transaction's global identifier, and two nodes sharing one would have their
/// transactions silently deduplicated against each other.
pub type NodeId = u64;

/// The compression algorithms a payload can be carried under.
///
/// The wire encoding is the discriminant, so adding one is additive: a peer
/// that does not know a codec simply never advertises it, and the sender picks
/// from the intersection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Codec {
    /// Send the bytes as they are.
    None = 0,
    /// Zstandard. Compiled in with the `replication_compression` feature.
    Zstd = 1,
}

impl Codec {
    #[inline]
    pub fn from_wire(byte: u8) -> Option<Codec> {
        match byte {
            0 => Some(Codec::None),
            1 => Some(Codec::Zstd),
            _ => None,
        }
    }

    #[inline]
    pub fn as_wire(self) -> u8 {
        self as u8
    }

    /// The bit this codec occupies in a handshake's support mask.
    #[inline]
    pub fn mask_bit(self) -> u8 {
        1 << (self as u8)
    }

    /// Whether this build can actually encode and decode it. A node advertises
    /// only what it can honour, so a build without the compression feature
    /// interoperates with one that has it — uncompressed.
    #[inline]
    pub fn is_available(self) -> bool {
        match self {
            Codec::None => true,
            Codec::Zstd => cfg!(feature = "replication_compression"),
        }
    }

    /// Every codec this build supports, best ratio first. The handshake picks
    /// the first entry both ends have.
    pub fn preference_order() -> &'static [Codec] {
        &[Codec::Zstd, Codec::None]
    }

    /// The mask advertised in a `Hello`.
    pub fn local_mask() -> u8 {
        let mut mask = 0;
        for codec in [Codec::None, Codec::Zstd] {
            if codec.is_available() {
                mask |= codec.mask_bit();
            }
        }
        mask
    }

    pub fn parse(text: &str) -> Option<Codec> {
        match text.trim().to_ascii_lowercase().as_str() {
            "none" | "off" | "plain" | "uncompressed" => Some(Codec::None),
            "zstd" | "zstandard" => Some(Codec::Zstd),
            _ => None,
        }
    }
}

impl fmt::Display for Codec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Codec::None => f.write_str("none"),
            Codec::Zstd => f.write_str("zstd"),
        }
    }
}

/// When to compress, as opposed to which algorithm to use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionMode {
    /// Never. Costs nothing and sends the most bytes — right on a loopback or
    /// a dedicated 25 GbE link where the CPU is scarcer than the bandwidth.
    Off,
    /// Always, above the size floor.
    Always,
    /// Compress while it is paying for itself. See [`Compression::min_ratio`].
    Auto,
}

impl CompressionMode {
    pub fn parse(text: &str) -> Option<CompressionMode> {
        match text.trim().to_ascii_lowercase().as_str() {
            "off" | "none" | "never" | "false" | "0" => Some(CompressionMode::Off),
            "on" | "always" | "true" | "1" => Some(CompressionMode::Always),
            "auto" | "adaptive" => Some(CompressionMode::Auto),
            _ => None,
        }
    }
}

impl fmt::Display for CompressionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionMode::Off => f.write_str("off"),
            CompressionMode::Always => f.write_str("always"),
            CompressionMode::Auto => f.write_str("auto"),
        }
    }
}

/// How outbound batches are compressed.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Compression {
    pub mode: CompressionMode,
    pub codec: Codec,
    /// Zstd level. 1 is the fast end and is the default: replication batches are
    /// repetitive SQL, which compresses several-fold even there, and a higher
    /// level buys little ratio for a lot of latency on the commit path.
    pub level: i32,
    /// Payloads below this are sent as they are. Compressing 200 bytes costs a
    /// function call and a header and saves nothing that a TLS record cares
    /// about.
    pub min_bytes: usize,
    /// Under [`CompressionMode::Auto`], the ratio below which compression is
    /// judged not to be paying for itself and is skipped for a while.
    ///
    /// Expressed as compressed/uncompressed, so *lower is better* and 0.9 means
    /// "keep compressing while it saves at least 10%".
    pub min_ratio: f64,
}

impl Default for Compression {
    fn default() -> Self {
        Compression {
            mode: CompressionMode::Auto,
            codec: Codec::Zstd,
            level: 1,
            min_bytes: 512,
            min_ratio: 0.90,
        }
    }
}

impl Compression {
    /// Parses the `COMPRESSION` option's value: `none`, `zstd`, `zstd:9`,
    /// `auto`, `auto:zstd:3`.
    pub fn parse(text: &str) -> Option<Compression> {
        let mut compression = Compression::default();
        let text = text.trim();

        let mut parts = text.split(':');
        let first = parts.next()?.trim();

        if let Some(mode) = CompressionMode::parse(first) {
            compression.mode = mode;
            if mode == CompressionMode::Off {
                compression.codec = Codec::None;
                return Some(compression);
            }
            // `auto` and `always` may name a codec after the colon.
            if let Some(codec_text) = parts.next() {
                compression.codec = Codec::parse(codec_text)?;
            }
        } else {
            let codec = Codec::parse(first)?;
            compression.codec = codec;
            compression.mode = if codec == Codec::None {
                CompressionMode::Off
            } else {
                CompressionMode::Always
            };
        }

        if let Some(level) = parts.next() {
            compression.level = level.trim().parse().ok()?;
        }

        Some(compression)
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.mode {
            CompressionMode::Off => f.write_str("off"),
            _ => write!(f, "{}:{}:{}", self.mode, self.codec, self.level),
        }
    }
}

/// How long a local commit waits for its peers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    /// Return as soon as the record is queued. The fastest, and the reason the
    /// commit path costs one channel send: the durability of the local commit
    /// is unchanged, only its propagation is deferred.
    Async,
    /// Wait for this many peers to acknowledge. `1` is the usual choice: it
    /// survives losing the node that took the write.
    Quorum(usize),
    /// Wait for every connected peer.
    All,
}

impl SyncMode {
    pub fn parse(text: &str) -> Option<SyncMode> {
        let lowered = text.trim().to_ascii_lowercase();
        match lowered.as_str() {
            "async" | "asynchronous" | "none" => Some(SyncMode::Async),
            "sync" | "synchronous" => Some(SyncMode::Quorum(1)),
            "all" | "every" => Some(SyncMode::All),
            _ => {
                let digits = lowered
                    .strip_prefix("quorum")?
                    .trim()
                    .trim_start_matches(':')
                    .trim();
                digits.parse().ok().map(SyncMode::Quorum)
            }
        }
    }

    /// How many acknowledgements a commit waits for, given how many peers are
    /// configured.
    #[inline]
    pub fn required_acks(self, peer_count: usize) -> usize {
        match self {
            SyncMode::Async => 0,
            SyncMode::Quorum(n) => n.min(peer_count),
            SyncMode::All => peer_count,
        }
    }
}

impl fmt::Display for SyncMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SyncMode::Async => f.write_str("async"),
            SyncMode::Quorum(n) => write!(f, "quorum:{}", n),
            SyncMode::All => f.write_str("all"),
        }
    }
}

/// What to do when a replicated statement will not apply.
///
/// In a multi-primary cluster this is not an exotic case: two nodes accepting
/// writes to the same key at the same time is the thing the topology is *for*,
/// and one of the two arrivals is going to hit a uniqueness violation or update
/// a row that no longer looks the way its origin expected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Abort the whole remote transaction and count it. Nothing partial lands,
    /// and the divergence is visible in `SHOW REPLICATION STATUS` rather than
    /// being papered over.
    Abort,
    /// Skip the statement that failed and apply the rest. Keeps a batch moving
    /// at the cost of applying a transaction in part.
    Skip,
    /// Let the later commit win, compared by hybrid-logical timestamp.
    ///
    /// The comparison is per table, not per row: this engine stores no
    /// per-row commit stamp, so the finest granularity available is "when did
    /// this node last commit anything to this table". A remote transaction
    /// older than the local node's last write to every table it touches is
    /// dropped as superseded; otherwise it applies and its failures are
    /// skipped.
    LastWriterWins,
}

impl ConflictPolicy {
    pub fn parse(text: &str) -> Option<ConflictPolicy> {
        match text
            .trim()
            .to_ascii_lowercase()
            .replace(['-', '_'], " ")
            .as_str()
        {
            "abort" | "error" | "reject" => Some(ConflictPolicy::Abort),
            "skip" | "ignore" => Some(ConflictPolicy::Skip),
            "lww" | "last writer wins" | "last write wins" | "latest" => {
                Some(ConflictPolicy::LastWriterWins)
            }
            _ => None,
        }
    }
}

impl fmt::Display for ConflictPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConflictPolicy::Abort => f.write_str("abort"),
            ConflictPolicy::Skip => f.write_str("skip"),
            ConflictPolicy::LastWriterWins => f.write_str("last-writer-wins"),
        }
    }
}

/// How a node establishes that a peer is really a member of its cluster.
///
/// TLS gives a private channel for free; it gives *identity* only when someone
/// configures it. Without that, an attacker in the network path presents any
/// certificate, is believed, and reads and rewrites everything — including the
/// shared token, which travels inside the tunnel and is therefore no defence
/// against an attacker who is already inside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    /// Encrypted but unauthenticated, and impersonable by anyone who can
    /// intercept the connection. Loopback and already-trusted links only.
    Insecure,
    /// Each node holds the SHA-256 fingerprints of the certificates it will
    /// accept, in both directions. No certificate authority to run: generate a
    /// self-signed certificate per node and distribute the fingerprints.
    Pinned,
    /// Mutual TLS against one certificate authority for the cluster, which
    /// signs a certificate per node. Worth the setup when nodes come and go
    /// often enough that redistributing fingerprints is a chore.
    Ca,
}

impl AuthMode {
    pub fn parse(text: &str) -> Option<AuthMode> {
        match text
            .trim()
            .to_ascii_lowercase()
            .replace(['-', '_'], "")
            .as_str()
        {
            "insecure" | "none" | "off" => Some(AuthMode::Insecure),
            "pinned" | "pin" | "fingerprint" => Some(AuthMode::Pinned),
            "ca" | "mtls" | "mutualtls" | "pki" => Some(AuthMode::Ca),
            _ => None,
        }
    }

    /// Whether this actually establishes who the peer is.
    #[inline]
    pub fn authenticates(self) -> bool {
        self != AuthMode::Insecure
    }
}

impl fmt::Display for AuthMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            AuthMode::Insecure => "insecure",
            AuthMode::Pinned => "pinned",
            AuthMode::Ca => "ca",
        })
    }
}

/// The material an [`AuthMode`] needs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsMaterial {
    /// This node's own certificate chain, presented to peers in both
    /// directions — as a server answering them and as a client calling them.
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// The cluster authority, under [`AuthMode::Ca`].
    pub ca_path: Option<String>,
    /// The certificates this node will accept, under [`AuthMode::Pinned`],
    /// as hex SHA-256. Listing every member including this node is fine and is
    /// the easiest thing to distribute.
    pub peer_fingerprints: Vec<String>,
}

impl TlsMaterial {
    /// Checks that what `mode` needs is actually present.
    pub fn validate(&self, mode: AuthMode) -> Result<(), String> {
        match mode {
            AuthMode::Insecure => Ok(()),
            AuthMode::Pinned => {
                if self.cert_path.is_none() || self.key_path.is_none() {
                    return Err(
                        "pinned authentication needs this node's own certificate: pass \
                         --replication-cert and --replication-key (or generate a pair with \
                         --replication-generate-identity)"
                            .to_string(),
                    );
                }
                if self.peer_fingerprints.is_empty() {
                    return Err("pinned authentication needs the peers' fingerprints: pass \
                         --replication-peer-fingerprint once per node"
                        .to_string());
                }
                for value in &self.peer_fingerprints {
                    rastcp::tls::parse_fingerprint(value)
                        .map_err(|e| format!("bad peer fingerprint {:?}: {}", value, e))?;
                }
                Ok(())
            }
            AuthMode::Ca => {
                if self.cert_path.is_none() || self.key_path.is_none() {
                    return Err("mutual TLS needs this node's own certificate: pass \
                         --replication-cert and --replication-key"
                        .to_string());
                }
                if self.ca_path.is_none() {
                    return Err(
                        "mutual TLS needs the cluster authority: pass --replication-ca".to_string(),
                    );
                }
                Ok(())
            }
        }
    }

    /// The parsed fingerprints, for the pinning verifiers.
    pub fn fingerprints(&self) -> Result<Vec<rastcp::tls::Fingerprint>, String> {
        self.peer_fingerprints
            .iter()
            .map(|value| {
                rastcp::tls::parse_fingerprint(value)
                    .map_err(|e| format!("bad peer fingerprint {:?}: {}", value, e))
            })
            .collect()
    }

    /// How an outbound link should decide whether to believe a peer.
    pub fn trust(&self, mode: AuthMode) -> Result<rastcp::tls::Trust, String> {
        Ok(match mode {
            AuthMode::Insecure => rastcp::tls::Trust::AcceptAny,
            AuthMode::Pinned => rastcp::tls::Trust::Pinned(self.fingerprints()?),
            AuthMode::Ca => {
                let path = self
                    .ca_path
                    .as_deref()
                    .ok_or("no certificate authority configured")?;
                rastcp::tls::Trust::RootCa(
                    rastcp::tls::load_certificates(path).map_err(|e| e.to_string())?,
                )
            }
        })
    }

    /// What an inbound listener should require of its callers.
    pub fn client_auth(&self, mode: AuthMode) -> Result<rastcp::tls::ClientAuth, String> {
        Ok(match mode {
            AuthMode::Insecure => rastcp::tls::ClientAuth::None,
            AuthMode::Pinned => rastcp::tls::ClientAuth::RequirePinned(self.fingerprints()?),
            AuthMode::Ca => {
                let path = self
                    .ca_path
                    .as_deref()
                    .ok_or("no certificate authority configured")?;
                rastcp::tls::ClientAuth::RequireCa(
                    rastcp::tls::load_certificates(path).map_err(|e| e.to_string())?,
                )
            }
        })
    }

    /// This node's own certificate and key, when it has one configured.
    pub fn identity(
        &self,
    ) -> Result<
        Option<(
            Vec<rustls::pki_types::CertificateDer<'static>>,
            rustls::pki_types::PrivateKeyDer<'static>,
        )>,
        String,
    > {
        let (Some(cert), Some(key)) = (self.cert_path.as_deref(), self.key_path.as_deref()) else {
            return Ok(None);
        };
        let certs = rastcp::tls::load_certificates(cert).map_err(|e| e.to_string())?;
        let key = rastcp::tls::load_private_key(key).map_err(|e| e.to_string())?;
        Ok(Some((certs, key)))
    }
}

/// One peer this node ships transactions to.
#[derive(Clone, Debug, PartialEq)]
pub struct PeerConfig {
    /// Operator-facing name. Also the key for `REPLICATION DROP PEER`.
    pub name: String,
    /// `host:port` of the peer's replication listener.
    pub address: String,
    /// A paused peer keeps its queue and its position but sends nothing.
    pub paused: bool,
    /// Overrides the node-level compression for this link, for a cluster whose
    /// members are not all across the same kind of network.
    pub compression: Option<Compression>,
}

impl PeerConfig {
    pub fn new(name: impl Into<String>, address: impl Into<String>) -> Self {
        PeerConfig {
            name: name.into(),
            address: address.into(),
            paused: false,
            compression: None,
        }
    }

    /// Parses a `--peer` flag: `name@host:port`, or bare `host:port` in which
    /// case the address doubles as the name.
    pub fn parse_flag(text: &str) -> Result<PeerConfig, String> {
        let text = text.trim();
        if text.is_empty() {
            return Err("a peer needs an address".to_string());
        }

        // Split on the *first* '@' so a name may not contain one but an IPv6
        // address in brackets is unaffected.
        let (name, address) = match text.split_once('@') {
            Some((name, address)) => (name.trim(), address.trim()),
            None => (text, text),
        };

        if address.is_empty() {
            return Err(format!("peer '{}' has no address", name));
        }
        if !address.contains(':') {
            return Err(format!(
                "peer address '{}' needs a port, as host:port",
                address
            ));
        }

        Ok(PeerConfig {
            name: name.to_string(),
            address: address.to_string(),
            paused: false,
            compression: None,
        })
    }
}

/// The default port for the replication listener: one above the SQL port, so a
/// deployment that opens `61170` knows where the other one is.
pub const DEFAULT_REPLICATION_PORT: u16 = crate::SERVER_PORT + 1;

/// Everything a node needs to replicate.
#[derive(Clone, Debug)]
pub struct ReplicationConfig {
    /// This node's identity. Zero means unset, and replication refuses to
    /// start without it — an unnamed node cannot be deduplicated against.
    pub node_id: NodeId,
    /// Nodes only accept frames from a peer naming the same cluster. It is a
    /// misconfiguration guard, not a security boundary; that is what `token`
    /// and TLS are for.
    pub cluster: String,
    /// A shared secret every member presents in its handshake. Empty disables
    /// the check.
    pub token: String,
    pub listen_addr: String,
    pub listen_port: u16,
    /// How peers prove they belong to this cluster.
    pub auth: AuthMode,
    /// The certificates and fingerprints [`AuthMode`] needs.
    pub tls: TlsMaterial,

    pub compression: Compression,
    pub sync_mode: SyncMode,
    /// How long a synchronous commit waits before giving up on its peers and
    /// returning anyway. The local commit already happened; this bounds how
    /// long a slow peer can hold a client's write open.
    pub ack_timeout: Duration,
    pub conflict: ConflictPolicy,

    /// Records a single outbound batch may carry.
    pub batch_max_records: usize,
    /// Bytes a single outbound batch may carry, before compression.
    pub batch_max_bytes: usize,
    /// How long a sender waits for more records before sending a partial batch.
    /// Zero sends immediately, which minimises latency at the cost of a frame
    /// per transaction.
    pub batch_linger: Duration,
    /// Records a peer's send queue holds before a commit has to wait for it.
    pub queue_capacity: usize,

    /// Forward transactions that arrived from one peer on to the others.
    ///
    /// Off by default, because the intended topology is a full mesh where every
    /// node already hears every commit first-hand, and relaying in a mesh sends
    /// each transaction N times over. Turn it on for a chain or a hub.
    pub relay: bool,

    /// Bytes of locally-originated history kept for peers that fall behind.
    /// A peer that asks for something older is told to resynchronise instead.
    pub log_retention_bytes: u64,

    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    /// How long a failed peer link waits before reconnecting, doubling up to
    /// [`ReplicationConfig::reconnect_max`].
    pub reconnect_base: Duration,
    pub reconnect_max: Duration,

    pub peers: Vec<PeerConfig>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        ReplicationConfig {
            node_id: 0,
            cluster: "rasterizeddb".to_string(),
            token: String::new(),
            listen_addr: "127.0.0.1".to_string(),
            listen_port: DEFAULT_REPLICATION_PORT,
            // Insecure by default only because the default listen address is
            // loopback. `validate` refuses the combination of this mode and an
            // address anything else can reach.
            auth: AuthMode::Insecure,
            tls: TlsMaterial::default(),
            compression: Compression::default(),
            sync_mode: SyncMode::Async,
            ack_timeout: Duration::from_secs(5),
            conflict: ConflictPolicy::Abort,
            batch_max_records: 256,
            batch_max_bytes: 1024 * 1024,
            batch_linger: Duration::from_micros(500),
            queue_capacity: 8192,
            relay: false,
            log_retention_bytes: 256 * 1024 * 1024,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            reconnect_base: Duration::from_millis(200),
            reconnect_max: Duration::from_secs(30),
            peers: Vec::new(),
        }
    }
}

impl ReplicationConfig {
    /// A hash of the cluster name, so the handshake compares one `u64` instead
    /// of carrying and comparing a string on every connection.
    pub fn cluster_id(&self) -> u64 {
        cluster_id_of(&self.cluster)
    }

    /// Rejects a configuration that cannot work, before anything binds a port.
    pub fn validate(&self) -> Result<(), String> {
        if self.node_id == 0 {
            return Err(
                "replication needs a node id: pass --node-id <N>, unique across the cluster"
                    .to_string(),
            );
        }
        if self.listen_port == 0 {
            return Err("replication needs a listen port".to_string());
        }
        if self.batch_max_records == 0 {
            return Err("batch-max-records must be at least 1".to_string());
        }
        if self.queue_capacity == 0 {
            return Err("queue-capacity must be at least 1".to_string());
        }

        self.tls.validate(self.auth)?;

        // The one combination that looks configured and is not: an
        // unauthenticated listener on an address other machines can reach. It
        // is encrypted, so it looks safe, and anyone in the path can
        // impersonate a peer and write to every node in the cluster.
        if !self.auth.authenticates() && !is_loopback(&self.listen_addr) {
            return Err(format!(
                "replication is listening on {} with authentication off. Anyone who can \
                 reach or intercept that address can impersonate a peer and write to this \
                 cluster. Set --replication-auth pinned (or ca), or bind loopback.",
                self.listen_addr
            ));
        }

        let mut names: Vec<&str> = self.peers.iter().map(|p| p.name.as_str()).collect();
        names.sort_unstable();
        if let Some(duplicate) = names.windows(2).find(|w| w[0] == w[1]) {
            return Err(format!("peer '{}' is configured twice", duplicate[0]));
        }

        Ok(())
    }

    /// The compression to use for one peer, which may override the node's.
    #[inline]
    pub fn compression_for(&self, peer: &PeerConfig) -> Compression {
        peer.compression.unwrap_or(self.compression)
    }
}

/// Whether an address is one only this machine can reach.
fn is_loopback(addr: &str) -> bool {
    match addr.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        // A hostname cannot be judged without resolving it, and resolving
        // during validation would make start-up depend on DNS. Treated as
        // reachable, which is the answer that fails safe.
        Err(_) => addr.eq_ignore_ascii_case("localhost"),
    }
}

/// The 64-bit form of a cluster name. FNV-1a: tiny, dependency-free, and only
/// ever compared for equality.
pub fn cluster_id_of(name: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for byte in name.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bare_codec_name_implies_whether_to_compress() {
        assert_eq!(
            Compression::parse("none").unwrap().mode,
            CompressionMode::Off
        );
        assert_eq!(
            Compression::parse("zstd").unwrap().mode,
            CompressionMode::Always
        );
        assert_eq!(Compression::parse("zstd").unwrap().codec, Codec::Zstd);
    }

    #[test]
    fn a_level_may_follow_the_codec() {
        let parsed = Compression::parse("zstd:9").unwrap();
        assert_eq!(parsed.codec, Codec::Zstd);
        assert_eq!(parsed.level, 9);
    }

    #[test]
    fn auto_may_name_a_codec_and_a_level() {
        let parsed = Compression::parse("auto:zstd:3").unwrap();
        assert_eq!(parsed.mode, CompressionMode::Auto);
        assert_eq!(parsed.codec, Codec::Zstd);
        assert_eq!(parsed.level, 3);
    }

    #[test]
    fn nonsense_compression_is_rejected_rather_than_defaulted() {
        assert!(Compression::parse("snappy").is_none());
        assert!(Compression::parse("zstd:banana").is_none());
    }

    #[test]
    fn a_peer_flag_may_name_the_peer_or_just_address_it() {
        let named = PeerConfig::parse_flag("berlin@10.0.0.2:61171").unwrap();
        assert_eq!(named.name, "berlin");
        assert_eq!(named.address, "10.0.0.2:61171");

        let bare = PeerConfig::parse_flag("10.0.0.3:61171").unwrap();
        assert_eq!(bare.name, "10.0.0.3:61171");
        assert_eq!(bare.address, "10.0.0.3:61171");
    }

    #[test]
    fn a_peer_without_a_port_is_rejected() {
        assert!(PeerConfig::parse_flag("berlin@10.0.0.2").is_err());
        assert!(PeerConfig::parse_flag("").is_err());
    }

    #[test]
    fn a_node_without_an_id_cannot_replicate() {
        let config = ReplicationConfig::default();
        assert!(config.validate().is_err(), "node id 0 is not a node id");
    }

    #[test]
    fn two_peers_under_one_name_are_rejected() {
        let mut config = ReplicationConfig {
            node_id: 1,
            ..Default::default()
        };
        config.peers.push(PeerConfig::new("a", "1.1.1.1:1"));
        config.peers.push(PeerConfig::new("a", "2.2.2.2:2"));

        assert!(config.validate().is_err());
    }

    #[test]
    fn sync_modes_round_trip_through_their_text() {
        for text in ["async", "sync", "all", "quorum:3"] {
            let mode = SyncMode::parse(text).unwrap();
            assert_eq!(SyncMode::parse(&mode.to_string()).unwrap(), mode);
        }
    }

    #[test]
    fn quorum_never_waits_for_more_peers_than_exist() {
        assert_eq!(SyncMode::Quorum(5).required_acks(2), 2);
        assert_eq!(SyncMode::All.required_acks(3), 3);
        assert_eq!(SyncMode::Async.required_acks(3), 0);
    }

    #[test]
    fn the_local_codec_mask_always_offers_uncompressed() {
        assert_ne!(Codec::local_mask() & Codec::None.mask_bit(), 0);
    }
}
