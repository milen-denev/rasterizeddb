//! Who may use which topic and exchange: the database's own privileges
//! (`rql::privileges`), so a role is granted a topic as it is a table.
//!
//! ```sql
//! GRANT PRODUCE, CONSUME ON TOPIC orders TO app;
//! GRANT CONSUME ON QUEUE jobs TO worker;       -- a queue is a topic
//! GRANT PUBLISH, BIND ON EXCHANGE events TO app;
//! REVOKE ALL ON TOPIC orders FROM PUBLIC;
//! ALTER TOPIC orders OWNER TO billing;
//! ```
//!
//! A topic or exchange is owned by the role that created it, kept as the
//! `OWNER` row of its ACL key (`@topic:<name>`, `@exchange:<name>`); its
//! owner may do everything with it, and only its owner (or a superuser) may
//! alter or delete it, or grant on it. Everyone else needs a grant:
//!
//! | privilege | on | lets a role |
//! |---|---|---|
//! | `PRODUCE` | topic | write records to it, directly or through the default exchange |
//! | `CONSUME` | topic | read it: fetch, subscribe, pop, purge a queue, list offsets, commit, manage its subscriptions |
//! | `PUBLISH` | exchange | publish through it (to wherever it is bound) |
//! | `BIND` | exchange | bind a topic to it, which also needs `PRODUCE` on the topic |
//!
//! Binding is the one act that needs two grants: a binding writes the
//! exchange's messages into the topic, so it needs `PRODUCE` on the topic as
//! a direct write would, and it reads what others publish through the
//! exchange, so it needs `BIND` on that. The built-in exchanges (`amq.*`)
//! are the database owner's, with `PUBLISH` and `BIND` granted to PUBLIC, as
//! RabbitMQ lets any user use them; a REVOKE takes that away.
//!
//! Every check fails closed: an object with no owner row is a superuser's
//! alone (an owner row lost to a crash between the create and its write
//! leaves the topic to superusers, never to everyone), and an ACL that
//! cannot be read refuses. Superusers, and every session of a front end
//! running without authentication, are not asked.
//!
//! The rows are read once per connection and kept, with each decision made
//! from them, until the catalogue's generation moves
//! ([`privileges::settled_generation`]): a check on the produce path is a
//! hash lookup, and a `GRANT` or `REVOKE` is seen by every open connection
//! at its next request.

use std::collections::HashMap;
use std::sync::Arc as StdArc;

use rasterizeddb_mq::wire::Code;

use super::BrokerError;
use crate::core::database::Database;
use crate::core::rql::privileges::{self, AclRow};

/// What a broker request needs of an object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Need {
    /// Writing to a topic.
    Produce,
    /// Reading a topic.
    Consume,
    /// Publishing through an exchange.
    Publish,
    /// Binding a topic to an exchange.
    Bind,
    /// Altering, purging or deleting: the owner's.
    Own,
}

impl Need {
    fn bit(self) -> u8 {
        match self {
            Need::Produce | Need::Publish => 1,
            Need::Consume | Need::Bind => 2,
            Need::Own => 4,
        }
    }

    fn word(self) -> &'static str {
        match self {
            Need::Produce => "PRODUCE",
            Need::Consume => "CONSUME",
            Need::Publish => "PUBLISH",
            Need::Bind => "BIND",
            Need::Own => "OWNER",
        }
    }
}

/// A broker object's kind, as its ACL key and its messages name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Topic,
    Exchange,
}

impl Kind {
    pub fn key(self, name: &str) -> String {
        match self {
            Kind::Topic => privileges::topic_key(name),
            Kind::Exchange => privileges::exchange_key(name),
        }
    }

    fn word(self) -> &'static str {
        match self {
            Kind::Topic => "topic",
            Kind::Exchange => "exchange",
        }
    }

    /// The privileges this kind takes, in bit order (1, 2).
    pub fn privileges(self) -> &'static [&'static str] {
        match self {
            Kind::Topic => privileges::TOPIC_PRIVILEGES,
            Kind::Exchange => privileges::EXCHANGE_PRIVILEGES,
        }
    }
}

/// A connection's standing: who it is, and what it may do, cached.
pub struct Access {
    user: String,
    superuser: bool,
    /// The generation `rows` were loaded at; `None` before the first load.
    epoch: Option<u64>,
    rows: StdArc<Vec<AclRow>>,
    /// The roles `user` acts as, from `rows`.
    acting: Vec<String>,
    /// Decisions made from `rows`: the privilege bits held on each object.
    topics: HashMap<String, u8>,
    exchanges: HashMap<String, u8>,
}

impl std::fmt::Debug for Access {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Access").field("user", &self.user).field("superuser", &self.superuser).finish()
    }
}

impl Access {
    /// A session that is asked nothing: a superuser's, or one of a front
    /// end that runs without authentication.
    pub fn unrestricted(user: &str) -> Access {
        let mut a = Access::new(user, false);
        a.superuser = true;
        a
    }

    /// `user`'s standing; a superuser passes every check.
    pub fn new(user: &str, superuser: bool) -> Access {
        Access {
            user: user.to_string(),
            superuser,
            epoch: None,
            rows: StdArc::new(Vec::new()),
            acting: Vec::new(),
            topics: HashMap::new(),
            exchanges: HashMap::new(),
        }
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn is_superuser(&self) -> bool {
        self.superuser
    }

    /// The catalogue rows decisions are made from (empty for a session
    /// that was never given them: an unrestricted one of the engine's).
    pub fn rows(&self) -> &[AclRow] {
        &self.rows
    }

    /// Brings the rows up to date, when the catalogue changed since they
    /// were read. Called before each request; a load runs on `main` (the
    /// server's runtime), as the role lookup does, since it runs SQL.
    pub async fn refresh(&mut self, database: &rclite::Arc<Database>, main: &tokio::runtime::Handle) -> Result<(), BrokerError> {
        if self.superuser {
            return Ok(());
        }
        // Taken before the load: a write that lands during it moves the
        // generation past this, and the next request loads again.
        let now = privileges::settled_generation(database);
        if self.epoch == Some(now) {
            return Ok(());
        }
        let db = database.clone();
        let rows = main
            .spawn(async move { privileges::load_shared(&db, None).await })
            .await
            .map_err(|e| denied_unread(&e.to_string()))?
            .map_err(|e| denied_unread(&e))?;
        self.set_rows(rows, Some(now));
        Ok(())
    }

    /// Takes `rows` as what the catalogue holds (tests, and a caller that
    /// loaded them already).
    pub fn set_rows(&mut self, rows: StdArc<Vec<AclRow>>, epoch: Option<u64>) {
        self.acting = privileges::roles_of(&rows, &self.user);
        self.rows = rows;
        self.topics.clear();
        self.exchanges.clear();
        self.epoch = epoch;
    }

    fn bits(&mut self, kind: Kind, name: &str) -> u8 {
        let memo = match kind {
            Kind::Topic => &mut self.topics,
            Kind::Exchange => &mut self.exchanges,
        };
        if let Some(b) = memo.get(name) {
            return *b;
        }
        let b = held(&self.rows, &self.acting, kind, &kind.key(name));
        memo.insert(name.to_string(), b);
        b
    }

    /// Whether this session may do `need` with `name`.
    pub fn may(&mut self, kind: Kind, name: &str, need: Need) -> bool {
        self.superuser || self.bits(kind, name) & need.bit() != 0
    }

    /// [`Self::may`] as a refusal.
    pub fn check(&mut self, kind: Kind, name: &str, need: Need) -> Result<(), BrokerError> {
        if self.may(kind, name, need) {
            return Ok(());
        }
        Err(refusal(kind, name, need))
    }

    pub fn topic(&mut self, name: &str, need: Need) -> Result<(), BrokerError> {
        self.check(Kind::Topic, name, need)
    }

    pub fn exchange(&mut self, name: &str, need: Need) -> Result<(), BrokerError> {
        self.check(Kind::Exchange, name, need)
    }

    /// A publish through `exchange` with `key`: `PUBLISH` on a named
    /// exchange; through the default exchange (`""`), which writes to the
    /// topic the key names, `PRODUCE` on that topic.
    pub fn publish(&mut self, exchange: &str, key: &str) -> Result<(), BrokerError> {
        if exchange.is_empty() { self.topic(key, Need::Produce) } else { self.exchange(exchange, Need::Publish) }
    }

    /// Binding `topic` to `exchange` (and unbinding it): `BIND` on the
    /// exchange and `PRODUCE` on the topic, or owning the topic.
    pub fn bind(&mut self, exchange: &str, topic: &str) -> Result<(), BrokerError> {
        self.exchange(exchange, Need::Bind)?;
        self.topic(topic, Need::Produce)
    }

    /// Whether `name` has an owner row: an object someone owns, as against
    /// one only superusers reach (or a front end's own, which has none).
    pub fn has_owner(&self, kind: Kind, name: &str) -> bool {
        privileges::owner_of(&self.rows, &kind.key(name)).is_some()
    }

    /// Whether the session may see `name` at all (a listing a client filters
    /// by, as Kafka's metadata does): any privilege on it.
    pub fn sees(&mut self, kind: Kind, name: &str) -> bool {
        self.superuser || self.bits(kind, name) != 0
    }
}

/// The privilege bits `acting` holds on the object kept under `key`: every
/// one for its owner, nothing on an object with no owner, else the grants
/// to PUBLIC and to any role it acts as.
fn held(rows: &[AclRow], acting: &[String], kind: Kind, key: &str) -> u8 {
    let Some(owner) = privileges::owner_of(rows, key) else {
        return 0;
    };
    if acting.iter().any(|r| r == owner) {
        return 1 | 2 | 4;
    }
    let names = kind.privileges();
    let mut b = 0;
    for r in rows.iter().filter(|r| r.relation == key) {
        if r.grantee != "public" && !acting.iter().any(|a| *a == r.grantee) {
            continue;
        }
        if let Some(i) = names.iter().position(|p| *p == r.privilege) {
            b |= 1 << i;
        }
    }
    b
}

fn refusal(kind: Kind, name: &str, need: Need) -> BrokerError {
    let message = match need {
        Need::Own => format!("must be owner of {} {name}", kind.word()),
        _ => format!("permission denied for {} {name}: it needs {}", kind.word(), need.word()),
    };
    BrokerError::new(Code::Permission, message)
}

fn denied_unread(e: &str) -> BrokerError {
    BrokerError::new(Code::Permission, format!("permission denied: the privilege catalogue could not be read ({e})"))
}

// ---------------------------------------------------------------------------
// Owners
// ---------------------------------------------------------------------------

/// After `owner` created the object: it owns it. Rows left by an earlier
/// object of that name (one deleted before its rows were) are cleared
/// first, so they confer nothing on the new one. Nothing is recorded for a
/// session with no role (a front end running without authentication):
/// that object is left to superusers. Runs on `main`; a failure is logged,
/// and leaves the object unowned, which fails closed.
pub async fn created(database: &rclite::Arc<Database>, main: &tokio::runtime::Handle, kind: Kind, name: &str, owner: &str) {
    if owner.is_empty() {
        return;
    }
    let (db, name, owner) = (database.clone(), name.to_string(), owner.to_string());
    let _ = main.spawn(async move { record_created(&db, kind, &name, &owner).await }).await;
}

/// [`created`], run where it is called (a SQL session, on the server's
/// runtime already).
pub async fn record_created(database: &rclite::Arc<Database>, kind: Kind, name: &str, owner: &str) {
    if owner.is_empty() {
        return;
    }
    let key = kind.key(name);
    let done = async {
        if database.has_table(privileges::ACL_TABLE) {
            privileges::delete_where(database, &format!("relation = {}", crate::auth::quote(&key)), None).await?;
        }
        let row = AclRow { relation: key.clone(), grantee: owner.to_string(), privilege: "OWNER".into(), grantor: owner.to_string(), grantable: false };
        privileges::put(database, &row, None).await
    }
    .await;
    if let Err(e) = done {
        log::warn!("broker: the owner of {} {name} could not be recorded: {e}", kind.word());
    }
}

/// After the object was deleted: its owner and grants go with it.
pub async fn removed(database: &rclite::Arc<Database>, main: &tokio::runtime::Handle, kind: Kind, name: &str) {
    let (db, name) = (database.clone(), name.to_string());
    let _ = main.spawn(async move { record_removed(&db, kind, &name).await }).await;
}

/// [`removed`], not waited for: a connection's cleanup as it closes (an
/// auto-delete queue whose last consumer was this connection's).
pub fn removed_detached(database: &rclite::Arc<Database>, main: &tokio::runtime::Handle, kind: Kind, name: &str) {
    let (db, name) = (database.clone(), name.to_string());
    drop(main.spawn(async move { record_removed(&db, kind, &name).await }));
}

/// [`removed`], run where it is called.
pub async fn record_removed(database: &rclite::Arc<Database>, kind: Kind, name: &str) {
    if !database.has_table(privileges::ACL_TABLE) {
        return;
    }
    let key = kind.key(name);
    if let Err(e) = privileges::delete_where(database, &format!("relation = {}", crate::auth::quote(&key)), None).await {
        log::warn!("broker: the grants on {} {name} could not be removed: {e}", kind.word());
    }
}

/// Gives each built-in exchange that has no owner yet the database owner,
/// with `PUBLISH` and `BIND` granted to PUBLIC. Run where the database's
/// relations are given theirs (`auth::prepare_owners`); the rows are keyed
/// by object, grantee and privilege, so two nodes that each run it write
/// the same rows.
pub async fn prepare_owners(database: &rclite::Arc<Database>, owner: &str) -> Result<(), String> {
    let rows = privileges::load_shared(database, None).await?;
    for (name, _) in super::engine::BUILTIN_EXCHANGES {
        let key = privileges::exchange_key(name);
        if privileges::owner_of(&rows, &key).is_some() {
            continue;
        }
        let mk = |grantee: &str, privilege: &str| AclRow {
            relation: key.clone(),
            grantee: grantee.to_string(),
            privilege: privilege.to_string(),
            grantor: owner.to_string(),
            grantable: false,
        };
        privileges::put(database, &mk(owner, "OWNER"), None).await?;
        for p in privileges::EXCHANGE_PRIVILEGES {
            privileges::put(database, &mk("public", p), None).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(relation: &str, grantee: &str, privilege: &str) -> AclRow {
        AclRow { relation: relation.into(), grantee: grantee.into(), privilege: privilege.into(), grantor: "boss".into(), grantable: false }
    }

    fn access(user: &str, rows: Vec<AclRow>) -> Access {
        let mut a = Access::new(user, false);
        a.set_rows(StdArc::new(rows), Some(1));
        a
    }

    #[test]
    fn owners_grants_and_public() {
        let rows = vec![
            row("@topic:orders", "billing", "OWNER"),
            row("@topic:orders", "app", "PRODUCE"),
            row("@topic:orders", "public", "CONSUME"),
            row("@exchange:events", "billing", "OWNER"),
            row("@exchange:events", "app", "PUBLISH"),
        ];
        let mut billing = access("billing", rows.clone());
        for need in [Need::Produce, Need::Consume, Need::Own] {
            assert!(billing.may(Kind::Topic, "orders", need));
        }
        let mut app = access("app", rows.clone());
        assert!(app.may(Kind::Topic, "orders", Need::Produce));
        assert!(app.may(Kind::Topic, "orders", Need::Consume), "PUBLIC's grant");
        assert!(!app.may(Kind::Topic, "orders", Need::Own));
        assert!(app.publish("events", "k").is_ok());
        assert_eq!(app.bind("events", "orders").unwrap_err().code, Code::Permission, "BIND was not granted");
        let mut other = access("other", rows);
        assert!(!other.may(Kind::Topic, "orders", Need::Produce));
        assert!(other.may(Kind::Topic, "orders", Need::Consume));
        assert!(other.publish("events", "k").is_err());
        // The default exchange writes to the topic its key names.
        assert!(other.publish("", "orders").is_err());
        assert!(app.publish("", "orders").is_ok());
    }

    #[test]
    fn an_unowned_object_is_a_superusers() {
        // A grant on an object with no owner row confers nothing: what a
        // crash between a create and its owner's write leaves.
        let rows = vec![row("@topic:t", "public", "PRODUCE"), row("@topic:t", "app", "CONSUME")];
        let mut app = access("app", rows);
        assert!(!app.may(Kind::Topic, "t", Need::Produce));
        assert!(!app.may(Kind::Topic, "t", Need::Consume));
        assert!(!app.sees(Kind::Topic, "t"));
        assert!(!app.may(Kind::Topic, "missing", Need::Consume));
        let mut su = Access::new("root", true);
        assert!(su.may(Kind::Topic, "t", Need::Own));
    }

    #[test]
    fn membership_carries_grants_and_ownership() {
        let rows = vec![
            row("@topic:t", "team", "OWNER"),
            row("@role:team", "alice", "MEMBER"),
            row("@topic:u", "boss", "OWNER"),
            row("@topic:u", "readers", "CONSUME"),
            row("@role:readers", "bob", "MEMBER"),
        ];
        let mut alice = access("alice", rows.clone());
        assert!(alice.may(Kind::Topic, "t", Need::Own));
        let mut bob = access("bob", rows);
        assert!(bob.may(Kind::Topic, "u", Need::Consume));
        assert!(!bob.may(Kind::Topic, "u", Need::Produce));
        assert!(!bob.may(Kind::Topic, "t", Need::Consume));
    }

    #[test]
    fn a_topic_grant_is_not_an_exchanges() {
        // Same name, other kind: a grant on topic `x` says nothing of
        // exchange `x`.
        let rows = vec![row("@topic:x", "boss", "OWNER"), row("@topic:x", "app", "PRODUCE"), row("@exchange:x", "boss", "OWNER")];
        let mut app = access("app", rows);
        assert!(app.may(Kind::Topic, "x", Need::Produce));
        assert!(!app.may(Kind::Exchange, "x", Need::Publish));
    }

    #[test]
    fn new_rows_drop_the_decisions() {
        let mut app = access("app", vec![row("@topic:t", "boss", "OWNER")]);
        assert!(!app.may(Kind::Topic, "t", Need::Produce));
        app.set_rows(StdArc::new(vec![row("@topic:t", "boss", "OWNER"), row("@topic:t", "app", "PRODUCE")]), Some(2));
        assert!(app.may(Kind::Topic, "t", Need::Produce), "the memo of the old rows was dropped");
    }
}
