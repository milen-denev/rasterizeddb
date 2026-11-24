//! `rshard`: the protocol between the gateway and a shard (sharding-plan.md
//! §4.9 G5, B1a.6, B1a.7, B1b).
//!
//! # Transport
//!
//! rshard rides rastcp/2's multiplexed request/response
//! (`rastcp::mux::MuxClient::send`): every gateway→shard message is one
//! rastcp request, and every shard→gateway message is that request's one
//! response. Many virtual sessions share one mux connection; nothing here
//! needs ordering between requests, because a virtual session never has two
//! requests in flight (the gateway waits for each reply, which is also what
//! bounds memory: a large result comes back in chunks the gateway pulls with
//! [`Request::Continue`], so a client that stops reading stops its own
//! session and nothing else).
//!
//! The gateway authenticates to the shard with mTLS (rastcp's
//! `ClientIdentity`); a shard admits rshard requests only on a connection
//! whose certificate it trusts as a gateway. The role a virtual session runs
//! as is asserted by the gateway in [`Request::OpenSession`] after it has
//! verified the client's SCRAM proof itself (G9); the shard re-checks grants.
//!
//! # Framing
//!
//! A request: `magic "RS" ‖ version:u8 ‖ kind:u8 ‖ body`.
//! A response: `version:u8 ‖ kind:u8 ‖ body`.
//!
//! Integers are little-endian. `str` is `len:u32 ‖ UTF-8`, `bytes` is
//! `len:u32 ‖ bytes`, `opt(T)` is a flag byte (0/1) then `T` when 1, a list
//! is `n:u32 ‖ n × item`. A decoder refuses unknown kinds, a short body and
//! trailing bytes. [`VERSION`] is 1; a shard answers a request of a version
//! it does not speak with [`Response::Refused`] (`08P01`, before effect).
//!
//! | kind | request | body |
//! |---|---|---|
//! | 0x01 | Hello | cluster_id u128, gateway str, map_epoch u64, map_digest u64 |
//! | 0x02 | MapFetch | none |
//! | 0x03 | AuthFetch | database str, role str |
//! | 0x04 | OpenSession | session u64, database str, role str, params list(str, str), gateway str, pinned u8 |
//! | 0x05 | Pg | session u64, map_epoch u64, bytes (pgwire frontend messages, as the client sent them) |
//! | 0x06 | Continue | session u64 |
//! | 0x07 | CloseSession | session u64 |
//! | 0x08 | Classify | database str, schema_epoch u64, source u8 (0 simple shape, 1 Parse text), text str |
//! | 0x09 | ExecPart | session u64, map_epoch u64, merge [`Merge`], bytes (pgwire frontend messages) |
//! | 0x0A | Cancel | backend_key u64 |
//!
//! | kind | response | body |
//! |---|---|---|
//! | 0x81 | HelloOk | cluster_id u128, shard_id u32, map_epoch u64, map_digest u64, schema_epoch u64 |
//! | 0x82 | MapBytes | signature opt(bytes), map bytes (`ShardMap::to_binary`) |
//! | 0x83 | AuthKeys | role_epoch u64, found u8, can_login u8, iterations u32, salt bytes, stored_key [32], server_key [32] |
//! | 0x84 | SessionOpened | session u64, backend_key u64, map_epoch u64, bytes (ParameterStatus… the client is to see, no ReadyForQuery) |
//! | 0x85 | PgReply | session u64, map_epoch u64, more u8, status u8 (ReadyForQuery's byte, 0 while `more`), bytes (pgwire backend messages) |
//! | 0x86 | Closed | session u64 |
//! | 0x87 | Template | schema_epoch u64, [`RouteTemplate`] |
//! | 0x88 | PartReply | session u64, map_epoch u64, more u8, status u8, head bytes, rows list(sort_key bytes, row bytes), tail bytes |
//! | 0x89 | Done | none |
//! | 0xFF | Refused | sqlstate [5], before_effect u8, map_epoch u64, message str |
//!
//! # Semantics
//!
//! - **Hello** is the first request on every mux connection. The shard answers
//!   with its identity; a gateway whose `cluster_id` differs, or whose map
//!   names another shard at that address, closes the connection. When the
//!   shard's `map_epoch` is higher the gateway fetches the map (MapFetch)
//!   before routing more writes (R7). Epochs never go down.
//! - **MapFetch** returns the map as the shard holds it. `signature` is empty
//!   until phase 2 (B2) signs maps.
//! - **AuthFetch** is sent to the home shard only, for `role` in
//!   `database`: what SCRAM-SHA-256 needs to verify a client's proof. Never a
//!   password. `role_epoch` rises with every change to the role's
//!   credentials or attributes, so a cache entry is replaced, never trusted
//!   past it.
//! - **OpenSession** creates a virtual session: a full pgwire session on the
//!   shard, already authenticated as `role` in `database`, with the client's
//!   startup `params`. `session` is chosen by the gateway, unique per mux
//!   connection. `pinned` is 1 for a per-user-shard session (G11): the shard
//!   checks its own placement list names the role and refuses
//!   (`Refused{Moved}`) otherwise. `backend_key` is what a CancelRequest
//!   for this session sends back as [`Request::Cancel`].
//! - **Pg** carries one batch of frontend messages: a `Query`, or an extended
//!   batch up to and including its `Sync` (or `Flush`). The shard runs them
//!   in the session and answers with the backend messages they produced. A
//!   reply larger than the shard's chunk size comes back as several
//!   PgReply with `more = 1`, each fetched by Continue; the last has
//!   `more = 0` and the ReadyForQuery status (`I`, `T` or `E`), and its
//!   bytes end with that ReadyForQuery. A refusal before anything ran is
//!   `Refused` with `before_effect = 1`: safe to retry elsewhere.
//! - **CloseSession** ends a virtual session; any open transaction rolls
//!   back. A session whose mux connection is lost is closed by the shard
//!   too (a lease on the connection, R6).
//! - **Classify** asks how a statement shape routes. `source = 0`: the text
//!   is a simple query's shape from [`crate::literals::shape`], where `$k`
//!   stands for literal slot k (1-based). `source = 1`: the text of a Parse,
//!   where `$k` is the client's parameter k. The shard parses it with its
//!   own parser, views and `route_where` and answers a [`RouteTemplate`]
//!   under its current schema epoch; the gateway caches the answer keyed by
//!   `(database, source, text)` and drops entries of older schema epochs.
//! - **ExecPart** runs one shard's part of a fan-out (B1b, B1c), in the
//!   session's context, with the rows annotated for the merge: for
//!   [`Merge::TopN`] every row carries a sort key the shard computed whose
//!   bytewise order is the statement's `ORDER BY` order (NULLS FIRST/LAST and
//!   DESC folded in), so the gateway merges without decoding a value. `head`
//!   is the RowDescription (and anything before the first row), `tail` the
//!   CommandComplete and ReadyForQuery.
//! - **Cancel** cancels whatever the session with that key is running.
//! - **Refused** is the one error reply: a SQLSTATE, whether nothing took
//!   effect (`before_effect`), and the shard's map epoch. The gateway retries
//!   only `before_effect` refusals (R2); `RDB02` (range not owned here),
//!   `RDB03` (moved, refresh the map) and `57P03` (fenced, retry after a
//!   pause) are the routing ones.

use crate::key::KeyFamily;

/// The protocol version this crate speaks.
pub const VERSION: u8 = 1;

/// A request's first two bytes.
pub const MAGIC: [u8; 2] = *b"RS";

/// SQLSTATE: the statement reaches ranges this shard does not own.
pub const NOT_OWNED: [u8; 5] = *b"RDB02";
/// SQLSTATE: the range has moved; refresh the map and retry.
pub const MOVED: [u8; 5] = *b"RDB03";
/// SQLSTATE: fenced for a move or failover; retry after a pause.
pub const FENCED: [u8; 5] = *b"57P03";
/// SQLSTATE: a protocol violation.
pub const PROTOCOL: [u8; 5] = *b"08P01";

/// The longest string or byte field a decoder accepts (1 GiB): a corrupt
/// length fails instead of allocating.
pub const MAX_FIELD: usize = 1 << 30;

/// How a statement shape routes (B1a.6).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteTemplate {
    /// The level-0 key is the client's parameter `index` (0-based), of
    /// `family`.
    Param { index: u16, family: KeyFamily },
    /// The level-0 key is literal slot `index` (0-based) of the shape.
    Slot { index: u16, family: KeyFamily },
    /// The key is fixed in the shape's text (a signed literal, a constant
    /// expression): every statement of this shape goes to `block`'s shard.
    Fixed { block: u64 },
    /// Touches no sharded table: global tables, sequences, catalogue reads,
    /// `SET`, DDL that the home shard runs.
    Home,
    /// Reaches every shard; how to combine the parts.
    FanOut { merge: Merge, writes: bool },
    /// A sharded table with no key the shard could see: refused or fanned
    /// out by the gateway's `unrouted` setting.
    NeedsKey,
}

/// How a fan-out's parts are combined (B1b tier 1, B1c).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Merge {
    /// Rows concatenated in shard order; the row count summed.
    Concat,
    /// One row of one `int8` column per part (`count(*)`): summed.
    CountSum,
    /// Any part's row answers (`EXISTS`, `LIMIT 1` without `ORDER BY`).
    Any,
    /// Each part returns its first `limit + offset` rows in sort-key order;
    /// the gateway merges, skips `offset` and cuts at `limit`.
    TopN { limit: u64, offset: u64 },
    /// Tier 2: the parts' partial aggregates are combined on a coordinator
    /// shard (B1c); the gateway only forwards.
    Coordinator,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Hello { cluster_id: u128, gateway: String, map_epoch: u64, map_digest: u64 },
    MapFetch,
    AuthFetch { database: String, role: String },
    OpenSession {
        session: u64,
        database: String,
        role: String,
        params: Vec<(String, String)>,
        gateway: String,
        pinned: bool,
    },
    Pg { session: u64, map_epoch: u64, bytes: Vec<u8> },
    Continue { session: u64 },
    CloseSession { session: u64 },
    Classify { database: String, schema_epoch: u64, source: Source, text: String },
    ExecPart { session: u64, map_epoch: u64, merge: Merge, bytes: Vec<u8> },
    Cancel { backend_key: u64 },
}

/// Where a classified text came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// A simple query's shape, `$k` standing for literal slot k.
    Shape = 0,
    /// A Parse's text, `$k` the client's parameter k.
    Parse = 1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    HelloOk { cluster_id: u128, shard_id: u32, map_epoch: u64, map_digest: u64, schema_epoch: u64 },
    MapBytes { signature: Option<Vec<u8>>, map: Vec<u8> },
    AuthKeys(AuthKeys),
    SessionOpened { session: u64, backend_key: u64, map_epoch: u64, bytes: Vec<u8> },
    PgReply { session: u64, map_epoch: u64, more: bool, status: u8, bytes: Vec<u8> },
    Closed { session: u64 },
    Template { schema_epoch: u64, template: RouteTemplate },
    PartReply(PartReply),
    Done,
    Refused(Refusal),
}

/// What SCRAM-SHA-256 needs to verify a role's proof (G9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthKeys {
    pub role_epoch: u64,
    /// False for an unknown role: the gateway still runs the exchange with a
    /// fake salt so the answer takes the same path (no user enumeration).
    pub found: bool,
    pub can_login: bool,
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

/// One shard's part of a fan-out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartReply {
    pub session: u64,
    pub map_epoch: u64,
    pub more: bool,
    pub status: u8,
    pub head: Vec<u8>,
    /// `(sort_key, DataRow message bytes)`; the sort key is empty unless the
    /// merge is [`Merge::TopN`].
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    pub tail: Vec<u8>,
}

/// A shard's refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refusal {
    pub sqlstate: [u8; 5],
    /// Nothing took effect: the gateway may retry the request elsewhere.
    pub before_effect: bool,
    pub map_epoch: u64,
    pub message: String,
}

impl Refusal {
    pub fn sqlstate_str(&self) -> &str {
        std::str::from_utf8(&self.sqlstate).unwrap_or("XX000")
    }
}

// ---------------------------------------------------------------- codec

/// An append-only encoder.
#[derive(Default)]
pub struct Writer {
    pub buf: Vec<u8>,
}

impl Writer {
    pub fn with_capacity(n: usize) -> Writer {
        Writer { buf: Vec::with_capacity(n) }
    }
    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }
    pub fn u16(&mut self, v: u16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn u128(&mut self, v: u128) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.u32(v.len() as u32);
        self.buf.extend_from_slice(v);
        self
    }
    pub fn str(&mut self, v: &str) -> &mut Self {
        self.bytes(v.as_bytes())
    }
    pub fn raw(&mut self, v: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(v);
        self
    }
}

/// A bounds-checked decoder.
pub struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    pub fn new(b: &'a [u8]) -> Reader<'a> {
        Reader { b, at: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.at.checked_add(n).filter(|&e| e <= self.b.len()).ok_or("rshard message is truncated")?;
        let s = &self.b[self.at..end];
        self.at = end;
        Ok(s)
    }
    pub fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    pub fn bool(&mut self) -> Result<bool, String> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(format!("rshard flag byte {b} is not 0 or 1")),
        }
    }
    pub fn bytes(&mut self) -> Result<&'a [u8], String> {
        let n = self.u32()? as usize;
        if n > MAX_FIELD {
            return Err(format!("rshard field of {n} bytes is too long"));
        }
        self.take(n)
    }
    pub fn str(&mut self) -> Result<String, String> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| "rshard string is not UTF-8".to_string())
    }
    pub fn array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        Ok(self.take(N)?.try_into().unwrap())
    }
    fn count(&mut self, item_min: usize) -> Result<usize, String> {
        let n = self.u32()? as usize;
        // Every item takes at least `item_min` bytes: a count the rest of the
        // message cannot hold is corrupt, and must not size an allocation.
        if n.saturating_mul(item_min.max(1)) > self.b.len() - self.at {
            return Err(format!("rshard list of {n} items does not fit its message"));
        }
        Ok(n)
    }
    pub fn end(&self) -> Result<(), String> {
        if self.at == self.b.len() {
            Ok(())
        } else {
            Err(format!("rshard message has {} trailing bytes", self.b.len() - self.at))
        }
    }
    pub fn rest(&self) -> &'a [u8] {
        &self.b[self.at..]
    }
}

fn family_byte(f: KeyFamily) -> u8 {
    f.tag()
}

fn family_from(b: u8) -> Result<KeyFamily, String> {
    KeyFamily::from_tag(b).ok_or_else(|| format!("rshard key family {b} is unknown"))
}

impl Merge {
    fn encode(&self, w: &mut Writer) {
        match self {
            Merge::Concat => {
                w.u8(0);
            }
            Merge::CountSum => {
                w.u8(1);
            }
            Merge::Any => {
                w.u8(2);
            }
            Merge::TopN { limit, offset } => {
                w.u8(3).u64(*limit).u64(*offset);
            }
            Merge::Coordinator => {
                w.u8(4);
            }
        }
    }
    fn decode(r: &mut Reader) -> Result<Merge, String> {
        Ok(match r.u8()? {
            0 => Merge::Concat,
            1 => Merge::CountSum,
            2 => Merge::Any,
            3 => Merge::TopN { limit: r.u64()?, offset: r.u64()? },
            4 => Merge::Coordinator,
            b => return Err(format!("rshard merge kind {b} is unknown")),
        })
    }
}

impl RouteTemplate {
    /// `tag:u8` then: Param/Slot `index:u16 family:u8`; Fixed `block:u64`;
    /// FanOut [`Merge`] `writes:u8`; Home and NeedsKey nothing.
    pub fn encode(&self, w: &mut Writer) {
        match self {
            RouteTemplate::Param { index, family } => {
                w.u8(0).u16(*index).u8(family_byte(*family));
            }
            RouteTemplate::Slot { index, family } => {
                w.u8(1).u16(*index).u8(family_byte(*family));
            }
            RouteTemplate::Fixed { block } => {
                w.u8(2).u64(*block);
            }
            RouteTemplate::Home => {
                w.u8(3);
            }
            RouteTemplate::FanOut { merge, writes } => {
                w.u8(4);
                merge.encode(w);
                w.u8(*writes as u8);
            }
            RouteTemplate::NeedsKey => {
                w.u8(5);
            }
        }
    }
    pub fn decode(r: &mut Reader) -> Result<RouteTemplate, String> {
        Ok(match r.u8()? {
            0 => RouteTemplate::Param { index: r.u16()?, family: family_from(r.u8()?)? },
            1 => RouteTemplate::Slot { index: r.u16()?, family: family_from(r.u8()?)? },
            2 => RouteTemplate::Fixed { block: r.u64()? },
            3 => RouteTemplate::Home,
            4 => RouteTemplate::FanOut { merge: Merge::decode(r)?, writes: r.bool()? },
            5 => RouteTemplate::NeedsKey,
            b => return Err(format!("rshard route template {b} is unknown")),
        })
    }
}

impl Request {
    pub fn kind(&self) -> u8 {
        match self {
            Request::Hello { .. } => 0x01,
            Request::MapFetch => 0x02,
            Request::AuthFetch { .. } => 0x03,
            Request::OpenSession { .. } => 0x04,
            Request::Pg { .. } => 0x05,
            Request::Continue { .. } => 0x06,
            Request::CloseSession { .. } => 0x07,
            Request::Classify { .. } => 0x08,
            Request::ExecPart { .. } => 0x09,
            Request::Cancel { .. } => 0x0A,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(64);
        w.raw(&MAGIC).u8(VERSION).u8(self.kind());
        match self {
            Request::Hello { cluster_id, gateway, map_epoch, map_digest } => {
                w.u128(*cluster_id).str(gateway).u64(*map_epoch).u64(*map_digest);
            }
            Request::MapFetch => {}
            Request::AuthFetch { database, role } => {
                w.str(database).str(role);
            }
            Request::OpenSession { session, database, role, params, gateway, pinned } => {
                w.u64(*session).str(database).str(role).u32(params.len() as u32);
                for (k, v) in params {
                    w.str(k).str(v);
                }
                w.str(gateway).u8(*pinned as u8);
            }
            Request::Pg { session, map_epoch, bytes } => {
                w.u64(*session).u64(*map_epoch).bytes(bytes);
            }
            Request::Continue { session } | Request::CloseSession { session } => {
                w.u64(*session);
            }
            Request::Classify { database, schema_epoch, source, text } => {
                w.str(database).u64(*schema_epoch).u8(*source as u8).str(text);
            }
            Request::ExecPart { session, map_epoch, merge, bytes } => {
                w.u64(*session).u64(*map_epoch);
                merge.encode(&mut w);
                w.bytes(bytes);
            }
            Request::Cancel { backend_key } => {
                w.u64(*backend_key);
            }
        }
        w.buf
    }

    /// Encodes a [`Request::Pg`] without building one: the hot path's
    /// encoder, which copies the client's bytes once.
    pub fn encode_pg(out: &mut Vec<u8>, session: u64, map_epoch: u64, bytes: &[u8]) {
        out.clear();
        out.reserve(26 + bytes.len());
        out.extend_from_slice(&MAGIC);
        out.push(VERSION);
        out.push(0x05);
        out.extend_from_slice(&session.to_le_bytes());
        out.extend_from_slice(&map_epoch.to_le_bytes());
        out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(bytes);
    }

    pub fn decode(bytes: &[u8]) -> Result<Request, String> {
        let mut r = Reader::new(bytes);
        if r.array::<2>()? != MAGIC {
            return Err("not an rshard request".to_string());
        }
        let v = r.u8()?;
        if v != VERSION {
            return Err(format!("rshard version {v} is not spoken here (this side speaks {VERSION})"));
        }
        let req = match r.u8()? {
            0x01 => Request::Hello { cluster_id: r.u128()?, gateway: r.str()?, map_epoch: r.u64()?, map_digest: r.u64()? },
            0x02 => Request::MapFetch,
            0x03 => Request::AuthFetch { database: r.str()?, role: r.str()? },
            0x04 => {
                let session = r.u64()?;
                let database = r.str()?;
                let role = r.str()?;
                let n = r.count(8)?;
                let mut params = Vec::with_capacity(n);
                for _ in 0..n {
                    params.push((r.str()?, r.str()?));
                }
                Request::OpenSession { session, database, role, params, gateway: r.str()?, pinned: r.bool()? }
            }
            0x05 => Request::Pg { session: r.u64()?, map_epoch: r.u64()?, bytes: r.bytes()?.to_vec() },
            0x06 => Request::Continue { session: r.u64()? },
            0x07 => Request::CloseSession { session: r.u64()? },
            0x08 => {
                let database = r.str()?;
                let schema_epoch = r.u64()?;
                let source = match r.u8()? {
                    0 => Source::Shape,
                    1 => Source::Parse,
                    b => return Err(format!("rshard classify source {b} is unknown")),
                };
                Request::Classify { database, schema_epoch, source, text: r.str()? }
            }
            0x09 => Request::ExecPart {
                session: r.u64()?,
                map_epoch: r.u64()?,
                merge: Merge::decode(&mut r)?,
                bytes: r.bytes()?.to_vec(),
            },
            0x0A => Request::Cancel { backend_key: r.u64()? },
            k => return Err(format!("rshard request kind {k:#04x} is unknown")),
        };
        r.end()?;
        Ok(req)
    }
}

impl Response {
    pub fn kind(&self) -> u8 {
        match self {
            Response::HelloOk { .. } => 0x81,
            Response::MapBytes { .. } => 0x82,
            Response::AuthKeys(_) => 0x83,
            Response::SessionOpened { .. } => 0x84,
            Response::PgReply { .. } => 0x85,
            Response::Closed { .. } => 0x86,
            Response::Template { .. } => 0x87,
            Response::PartReply(_) => 0x88,
            Response::Done => 0x89,
            Response::Refused(_) => 0xFF,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(64);
        w.u8(VERSION).u8(self.kind());
        match self {
            Response::HelloOk { cluster_id, shard_id, map_epoch, map_digest, schema_epoch } => {
                w.u128(*cluster_id).u32(*shard_id).u64(*map_epoch).u64(*map_digest).u64(*schema_epoch);
            }
            Response::MapBytes { signature, map } => {
                match signature {
                    Some(s) => {
                        w.u8(1).bytes(s);
                    }
                    None => {
                        w.u8(0);
                    }
                }
                w.bytes(map);
            }
            Response::AuthKeys(k) => {
                w.u64(k.role_epoch)
                    .u8(k.found as u8)
                    .u8(k.can_login as u8)
                    .u32(k.iterations)
                    .bytes(&k.salt)
                    .raw(&k.stored_key)
                    .raw(&k.server_key);
            }
            Response::SessionOpened { session, backend_key, map_epoch, bytes } => {
                w.u64(*session).u64(*backend_key).u64(*map_epoch).bytes(bytes);
            }
            Response::PgReply { session, map_epoch, more, status, bytes } => {
                w.u64(*session).u64(*map_epoch).u8(*more as u8).u8(*status).bytes(bytes);
            }
            Response::Closed { session } => {
                w.u64(*session);
            }
            Response::Template { schema_epoch, template } => {
                w.u64(*schema_epoch);
                template.encode(&mut w);
            }
            Response::PartReply(p) => {
                w.u64(p.session).u64(p.map_epoch).u8(p.more as u8).u8(p.status).bytes(&p.head);
                w.u32(p.rows.len() as u32);
                for (k, row) in &p.rows {
                    w.bytes(k).bytes(row);
                }
                w.bytes(&p.tail);
            }
            Response::Done => {}
            Response::Refused(r) => {
                w.raw(&r.sqlstate).u8(r.before_effect as u8).u64(r.map_epoch).str(&r.message);
            }
        }
        w.buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Response, String> {
        let mut r = Reader::new(bytes);
        let v = r.u8()?;
        if v != VERSION {
            return Err(format!("rshard version {v} is not spoken here (this side speaks {VERSION})"));
        }
        let resp = match r.u8()? {
            0x81 => Response::HelloOk {
                cluster_id: r.u128()?,
                shard_id: r.u32()?,
                map_epoch: r.u64()?,
                map_digest: r.u64()?,
                schema_epoch: r.u64()?,
            },
            0x82 => {
                let signature = if r.bool()? { Some(r.bytes()?.to_vec()) } else { None };
                Response::MapBytes { signature, map: r.bytes()?.to_vec() }
            }
            0x83 => Response::AuthKeys(AuthKeys {
                role_epoch: r.u64()?,
                found: r.bool()?,
                can_login: r.bool()?,
                iterations: r.u32()?,
                salt: r.bytes()?.to_vec(),
                stored_key: r.array()?,
                server_key: r.array()?,
            }),
            0x84 => Response::SessionOpened {
                session: r.u64()?,
                backend_key: r.u64()?,
                map_epoch: r.u64()?,
                bytes: r.bytes()?.to_vec(),
            },
            0x85 => Response::PgReply {
                session: r.u64()?,
                map_epoch: r.u64()?,
                more: r.bool()?,
                status: r.u8()?,
                bytes: r.bytes()?.to_vec(),
            },
            0x86 => Response::Closed { session: r.u64()? },
            0x87 => Response::Template { schema_epoch: r.u64()?, template: RouteTemplate::decode(&mut r)? },
            0x88 => {
                let session = r.u64()?;
                let map_epoch = r.u64()?;
                let more = r.bool()?;
                let status = r.u8()?;
                let head = r.bytes()?.to_vec();
                let n = r.count(8)?;
                let mut rows = Vec::with_capacity(n);
                for _ in 0..n {
                    rows.push((r.bytes()?.to_vec(), r.bytes()?.to_vec()));
                }
                Response::PartReply(PartReply { session, map_epoch, more, status, head, rows, tail: r.bytes()?.to_vec() })
            }
            0x89 => Response::Done,
            0xFF => Response::Refused(Refusal {
                sqlstate: r.array()?,
                before_effect: r.bool()?,
                map_epoch: r.u64()?,
                message: r.str()?,
            }),
            k => return Err(format!("rshard response kind {k:#04x} is unknown")),
        };
        r.end()?;
        Ok(resp)
    }

    /// The map epoch a reply carries, for R7's check; `None` for replies
    /// that do not carry one.
    pub fn map_epoch(&self) -> Option<u64> {
        match self {
            Response::HelloOk { map_epoch, .. }
            | Response::SessionOpened { map_epoch, .. }
            | Response::PgReply { map_epoch, .. } => Some(*map_epoch),
            Response::PartReply(p) => Some(p.map_epoch),
            Response::Refused(r) => Some(r.map_epoch),
            _ => None,
        }
    }
}

/// The native protocol's route option (A4b, B1a.6 step 1): a client puts
/// `route=<value>` among a `\x01SIMPLE:` request's options, and the gateway
/// routes by it without reading the statement. `<value>` is `i<decimal>`
/// for an integer key, `u<32 hex>` for a UUID, and `t<hex of the UTF-8>`
/// for text (hex, so a comma or semicolon in the key cannot end the
/// option).
pub fn parse_native_route(value: &str) -> Option<crate::key::KeyValue<'static>> {
    let (tag, rest) = value.split_at_checked(1)?;
    match tag {
        "i" => rest.parse::<i128>().ok().map(crate::key::KeyValue::Int),
        "u" => {
            if rest.len() != 32 {
                return None;
            }
            let mut b = [0u8; 16];
            for (i, out) in b.iter_mut().enumerate() {
                *out = u8::from_str_radix(rest.get(2 * i..2 * i + 2)?, 16).ok()?;
            }
            Some(crate::key::KeyValue::Uuid(b))
        }
        // Text keys borrow their bytes: `parse_native_text_route`.
        _ => None,
    }
}

/// A text route option's key bytes (`t<hex>`), decoded; `None` when the
/// option is not a well-formed text key.
pub fn parse_native_text_route(value: &str) -> Option<Vec<u8>> {
    let rest = value.strip_prefix('t')?;
    if rest.len() % 2 != 0 {
        return None;
    }
    (0..rest.len() / 2).map(|i| u8::from_str_radix(rest.get(2 * i..2 * i + 2)?, 16).ok()).collect()
}
