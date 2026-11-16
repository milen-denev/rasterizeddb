//! The server's Redis-compatible key/value cache (`KV …` statements), typed.
//!
//! Three levels, each on the one before:
//!
//! * [`Kv`]: commands on byte strings. Every argument is sent as hex
//!   (`x'…'`) and values come back as bytes (`KV BINARY`), so any byte string
//!   is a key or a value.
//! * [`KvMap<K, V>`]: a typed map under a key prefix (`sessions:<key>`).
//!   Keys and values are any type with a text form: [`ToSql`] +
//!   [`FromSql`] types (`String`, integers, floats, `bool`, `Vec<u8>`,
//!   [`Uuid`](crate::Uuid), [`Timestamp`](crate::Timestamp),
//!   [`Numeric`](crate::Numeric), `#[derive(SqlEnum)]` enums, arrays), or
//!   [`KvJson<T>`] for a serde type.
//! * [`KvRepo<M>`]: a `#[derive(Model)]` struct stored as a hash, one field
//!   per column, under `<table>:<primary key>`; the same struct the SQL ORM
//!   uses.
//!
//! ```no_run
//! # use rasterizeddb_client::prelude::*;
//! # #[derive(Model, Clone)] struct Session { #[rdb(primary_key)] token: Uuid, user_id: i64, seen: Timestamp }
//! # async fn demo(client: &Client, token: Uuid, s: Session) -> rasterizeddb_client::Result<()> {
//! use std::time::Duration;
//!
//! // A typed map.
//! let hits = client.kv().map::<String, i64>("hits");
//! hits.incr_by(&"/index".to_string(), 1).await?;
//!
//! // A model as a hash, with a TTL.
//! let sessions = client.kv().repo::<Session>();
//! sessions.save_with_ttl(&s, Duration::from_secs(3600)).await?;
//! let again: Option<Session> = sessions.get(&[&token]).await?;
//! # let _ = again; Ok(()) }
//! ```
//!
//! Needs a server with binary KV statements (commit c0a27fe7 or later). No
//! command here is atomic with another: the native protocol has no
//! `MULTI`. Where a call takes two commands it says so.

use std::marker::PhantomData;
use std::time::Duration;

use crate::client::Client;
use crate::from_sql::FromSqlOwned;
use crate::orm::Model;
use crate::params::Param;
use crate::row::{Rows, tag};
use crate::to_sql::ToSql;
use crate::{Error, Result};

/// A value's text form, as stored in the cache: what [`FromSql`] reads back
/// from a text cell. `None` for a NULL.
///
/// [`FromSql`]: crate::FromSql
pub fn text_form(param: &Param<'_>) -> Result<Option<Vec<u8>>> {
    let mut out = Vec::new();
    Ok(push_text_form(&mut out, param)?.then_some(out))
}

/// [`text_form`], appended to `out`; `false` for a NULL (nothing written).
pub fn push_text_form(out: &mut Vec<u8>, param: &Param<'_>) -> Result<bool> {
    use std::io::Write as _;
    let float = |out: &mut Vec<u8>, v: f64, debug: String| {
        if v.is_nan() {
            out.extend_from_slice(b"NaN");
        } else if v.is_infinite() {
            out.extend_from_slice(if v < 0.0 { b"-Infinity" } else { b"Infinity" });
        } else {
            out.extend_from_slice(debug.as_bytes());
        }
    };
    match *param {
        Param::Null => return Ok(false),
        Param::Bool(b) => out.push(if b { b't' } else { b'f' }),
        Param::I16(v) => write!(out, "{v}").expect("vec"),
        Param::I32(v) => write!(out, "{v}").expect("vec"),
        Param::I64(v) => write!(out, "{v}").expect("vec"),
        Param::U64(v) => write!(out, "{v}").expect("vec"),
        Param::I128(v) => write!(out, "{v}").expect("vec"),
        Param::F32(v) => float(out, v.into(), format!("{v:?}")),
        Param::F64(v) => float(out, v, format!("{v:?}")),
        Param::Numeric(n) => {
            if !crate::types::is_decimal(n) {
                return Err(Error::Param(format!("{n:?} is not a number")));
            }
            out.extend_from_slice(n.as_bytes());
        }
        Param::Text(s) | Param::Json(s) => out.extend_from_slice(s.as_bytes()),
        Param::Bytes(b) => out.extend_from_slice(b),
        Param::Uuid(v) => write!(out, "{v}").expect("vec"),
        Param::Timestamp(v) => write!(out, "{v}").expect("vec"),
        Param::Date(v) => write!(out, "{v}").expect("vec"),
        Param::Time(v) => write!(out, "{v}").expect("vec"),
        Param::Interval(v) => write!(out, "{v}").expect("vec"),
        Param::Array(a) => {
            let mut body = String::new();
            crate::params::push_array_body(&mut body, &a)?;
            out.extend_from_slice(body.as_bytes());
        }
    }
    Ok(true)
}

fn hex_arg(sql: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    sql.reserve(bytes.len() * 2 + 4);
    sql.push_str(" x'");
    for b in bytes {
        sql.push(HEX[(b >> 4) as usize] as char);
        sql.push(HEX[(b & 0x0f) as usize] as char);
    }
    sql.push('\'');
}

fn millis(ttl: Duration) -> String {
    ttl.as_millis().clamp(1, i64::MAX as u128).to_string()
}

/// Escapes `*`, `?`, `[`, `]` and `\` for a `SCAN … MATCH` pattern.
fn glob_escape(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 2);
    for &b in bytes {
        if matches!(b, b'*' | b'?' | b'[' | b']' | b'\\') {
            out.push(b'\\');
        }
        out.push(b);
    }
    out
}

/// The key/value cache's commands, on byte strings. From [`Client::kv`].
#[derive(Clone, Copy)]
pub struct Kv<'c> {
    client: &'c Client,
}

impl<'c> Kv<'c> {
    pub(crate) fn new(client: &'c Client) -> Self {
        Kv { client }
    }

    /// Any command, every argument after its name sent as bytes; value
    /// columns come back as `bytea` (`KV BINARY`).
    pub async fn command(&self, command: &str, args: &[&[u8]]) -> Result<Rows> {
        if command.is_empty() || !command.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            return Err(Error::Param(format!("{command:?} is not a KV command name")));
        }
        let mut sql = format!("KV BINARY {command}");
        for arg in args {
            hex_arg(&mut sql, arg);
        }
        self.client.run(&sql).await
    }

    async fn int(&self, command: &str, args: &[&[u8]]) -> Result<i64> {
        let rows = self.command(command, args).await?;
        rows.get(0)
            .ok_or_else(|| Error::Protocol(format!("KV {command} answered no row")))?
            .get(0)
    }

    async fn bytes_column(&self, command: &str, args: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        let rows = self.command(command, args).await?;
        rows.iter().map(|r| r.get::<Option<Vec<u8>>>(0)).collect()
    }

    // ------------------------------------------------------------ strings

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.bytes_column("GET", &[key]).await?.into_iter().next().flatten())
    }

    /// `SET key value [PX ttl]`.
    pub async fn set(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<()> {
        match ttl {
            None => self.command("SET", &[key, value]).await.map(drop),
            Some(ttl) => {
                let ms = millis(ttl);
                self.command("SET", &[key, value, b"PX", ms.as_bytes()]).await.map(drop)
            }
        }
    }

    /// `SET key value NX [PX ttl]`: whether the key was absent and is now set.
    pub async fn set_if_absent(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<bool> {
        let ms = ttl.map(millis);
        let mut args: Vec<&[u8]> = vec![key, value, b"NX"];
        if let Some(ms) = &ms {
            args.extend([b"PX".as_slice(), ms.as_bytes()]);
        }
        Ok(!self.command("SET", &args).await?.is_empty())
    }

    /// `MGET`: each key's value, `None` for a missing one, in order.
    pub async fn get_many(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.bytes_column("MGET", keys).await
    }

    /// `MSET`: every pair at once.
    pub async fn set_many(&self, pairs: &[(&[u8], &[u8])]) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let args: Vec<&[u8]> = pairs.iter().flat_map(|(k, v)| [*k, *v]).collect();
        self.command("MSET", &args).await.map(drop)
    }

    /// The keys removed.
    pub async fn del(&self, keys: &[&[u8]]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        self.int("DEL", keys).await
    }

    pub async fn exists(&self, key: &[u8]) -> Result<bool> {
        Ok(self.int("EXISTS", &[key]).await? > 0)
    }

    pub async fn incr_by(&self, key: &[u8], by: i64) -> Result<i64> {
        let by = by.to_string();
        self.int("INCRBY", &[key, by.as_bytes()]).await
    }

    /// Whether the key existed to be given one.
    pub async fn expire(&self, key: &[u8], ttl: Duration) -> Result<bool> {
        let ms = millis(ttl);
        Ok(self.int("PEXPIRE", &[key, ms.as_bytes()]).await? == 1)
    }

    /// Whether the key had a TTL to remove.
    pub async fn persist(&self, key: &[u8]) -> Result<bool> {
        Ok(self.int("PERSIST", &[key]).await? == 1)
    }

    /// The time left: `None` for a key with no TTL or no key.
    pub async fn ttl(&self, key: &[u8]) -> Result<Option<Duration>> {
        let ms = self.int("PTTL", &[key]).await?;
        Ok((ms >= 0).then(|| Duration::from_millis(ms as u64)))
    }

    /// Every key matching a glob `pattern` (`*`, `?`, `[…]`), by `SCAN`:
    /// complete, but not a snapshot (keys written during the scan may or may
    /// not appear).
    pub async fn keys(&self, pattern: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut cursor = "0".to_string();
        loop {
            let rows = self
                .command("SCAN", &[cursor.as_bytes(), b"MATCH", pattern, b"COUNT", b"500"])
                .await?;
            let mut next = None;
            for row in rows.iter() {
                next = Some(row.get::<String>(0)?);
                if let Some(key) = row.get::<Option<Vec<u8>>>(1)? {
                    out.push(key);
                }
            }
            cursor = next.unwrap_or_else(|| "0".to_string());
            if cursor == "0" {
                return Ok(out);
            }
        }
    }

    // ------------------------------------------------------------- hashes

    /// `HSET`: the fields that were new.
    pub async fn hset(&self, key: &[u8], fields: &[(&[u8], &[u8])]) -> Result<i64> {
        if fields.is_empty() {
            return Ok(0);
        }
        let mut args: Vec<&[u8]> = vec![key];
        args.extend(fields.iter().flat_map(|(f, v)| [*f, *v]));
        self.int("HSET", &args).await
    }

    pub async fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.bytes_column("HGET", &[key, field]).await?.into_iter().next().flatten())
    }

    /// Every field and value; empty for a missing key.
    pub async fn hgetall(&self, key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows = self.command("HGETALL", &[key]).await?;
        rows.iter().map(|r| Ok((r.get(0)?, r.get(1)?))).collect()
    }

    /// The fields removed.
    pub async fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> Result<i64> {
        if fields.is_empty() {
            return Ok(0);
        }
        let mut args: Vec<&[u8]> = vec![key];
        args.extend_from_slice(fields);
        self.int("HDEL", &args).await
    }

    pub async fn hincr_by(&self, key: &[u8], field: &[u8], by: i64) -> Result<i64> {
        let by = by.to_string();
        self.int("HINCRBY", &[key, field, by.as_bytes()]).await
    }

    // -------------------------------------------------------------- lists

    /// `RPUSH`: the list's length after.
    pub async fn rpush(&self, key: &[u8], values: &[&[u8]]) -> Result<i64> {
        let mut args: Vec<&[u8]> = vec![key];
        args.extend_from_slice(values);
        self.int("RPUSH", &args).await
    }

    /// `LPUSH`: the list's length after.
    pub async fn lpush(&self, key: &[u8], values: &[&[u8]]) -> Result<i64> {
        let mut args: Vec<&[u8]> = vec![key];
        args.extend_from_slice(values);
        self.int("LPUSH", &args).await
    }

    pub async fn lpop(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.bytes_column("LPOP", &[key]).await?.into_iter().next().flatten())
    }

    pub async fn rpop(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.bytes_column("RPOP", &[key]).await?.into_iter().next().flatten())
    }

    /// `LRANGE key start stop` (negative counts from the end; `0, -1` is all).
    pub async fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        let (start, stop) = (start.to_string(), stop.to_string());
        let values = self.bytes_column("LRANGE", &[key, start.as_bytes(), stop.as_bytes()]).await?;
        Ok(values.into_iter().flatten().collect())
    }

    pub async fn llen(&self, key: &[u8]) -> Result<i64> {
        self.int("LLEN", &[key]).await
    }

    // --------------------------------------------------------------- sets

    /// The members that were new.
    pub async fn sadd(&self, key: &[u8], members: &[&[u8]]) -> Result<i64> {
        let mut args: Vec<&[u8]> = vec![key];
        args.extend_from_slice(members);
        self.int("SADD", &args).await
    }

    pub async fn srem(&self, key: &[u8], members: &[&[u8]]) -> Result<i64> {
        let mut args: Vec<&[u8]> = vec![key];
        args.extend_from_slice(members);
        self.int("SREM", &args).await
    }

    pub async fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool> {
        Ok(self.int("SISMEMBER", &[key, member]).await? == 1)
    }

    pub async fn smembers(&self, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(self.bytes_column("SMEMBERS", &[key]).await?.into_iter().flatten().collect())
    }

    pub async fn scard(&self, key: &[u8]) -> Result<i64> {
        self.int("SCARD", &[key]).await
    }

    // -------------------------------------------------------- sorted sets

    /// `ZADD`: the members that were new.
    pub async fn zadd(&self, key: &[u8], members: &[(f64, &[u8])]) -> Result<i64> {
        let scores: Vec<String> = members.iter().map(|(s, _)| format!("{s:?}")).collect();
        let mut args: Vec<&[u8]> = vec![key];
        for ((_, member), score) in members.iter().zip(&scores) {
            args.push(score.as_bytes());
            args.push(member);
        }
        self.int("ZADD", &args).await
    }

    pub async fn zscore(&self, key: &[u8], member: &[u8]) -> Result<Option<f64>> {
        let rows = self.command("ZSCORE", &[key, member]).await?;
        rows.get(0).map(|r| r.get::<Option<f64>>(0)).transpose().map(Option::flatten)
    }

    /// `ZRANGE key start stop WITHSCORES` (by rank, `REV` for highest first).
    pub async fn zrange(&self, key: &[u8], start: i64, stop: i64, rev: bool) -> Result<Vec<(Vec<u8>, f64)>> {
        let (start, stop) = (start.to_string(), stop.to_string());
        let mut args: Vec<&[u8]> = vec![key, start.as_bytes(), stop.as_bytes()];
        if rev {
            args.push(b"REV");
        }
        args.push(b"WITHSCORES");
        let rows = self.command("ZRANGE", &args).await?;
        rows.iter().map(|r| Ok((r.get(0)?, r.get(1)?))).collect()
    }

    pub async fn zrem(&self, key: &[u8], members: &[&[u8]]) -> Result<i64> {
        let mut args: Vec<&[u8]> = vec![key];
        args.extend_from_slice(members);
        self.int("ZREM", &args).await
    }

    pub async fn zincr_by(&self, key: &[u8], member: &[u8], by: f64) -> Result<f64> {
        let by = format!("{by:?}");
        let rows = self.command("ZINCRBY", &[key, by.as_bytes(), member]).await?;
        rows.get(0)
            .ok_or_else(|| Error::Protocol("KV ZINCRBY answered no row".to_string()))?
            .get(0)
    }

    // -------------------------------------------------------------- typed

    /// The value at `key`, as `T`: `client.kv().get_as::<i32>("visits")`.
    pub async fn get_as<T: KvValue>(&self, key: &(impl KvKey + ?Sized)) -> Result<Option<T>> {
        match self.get(&key.key_bytes()?).await? {
            None => Ok(None),
            Some(bytes) => T::decode(&bytes).map(Some),
        }
    }

    /// Stores `value` at `key`, with an optional TTL.
    pub async fn set_as<T: KvValue>(
        &self,
        key: &(impl KvKey + ?Sized),
        value: &T,
        ttl: Option<Duration>,
    ) -> Result<()> {
        self.set(&key.key_bytes()?, &value.encode()?, ttl).await
    }

    /// A query over the whole keyspace, keys read as `K` and values as `V`:
    /// `client.kv().query::<String, i32>().filter_keys(|k| k == "Value 1")`.
    pub fn query<K: KvKey + KvValue, V: KvValue>(&self) -> KvQuery<'c, K, V> {
        self.map::<K, V>("").query()
    }

    /// A typed map under `prefix:` (see [`KvMap`]); an empty prefix is the
    /// whole keyspace.
    pub fn map<K: KvKey, V: KvValue>(&self, prefix: &str) -> KvMap<'c, K, V> {
        KvMap { kv: *self, prefix: prefix.to_string(), _types: PhantomData }
    }

    /// Model `M`'s rows as hashes under `<table>:` (see [`KvRepo`]).
    pub fn repo<M: Model>(&self) -> KvRepo<'c, M> {
        KvRepo { kv: *self, prefix: M::TABLE.to_string(), _model: PhantomData }
    }
}

impl Client {
    /// The server's key/value cache: see [`Kv`].
    pub fn kv(&self) -> Kv<'_> {
        Kv::new(self)
    }
}

// --------------------------------------------------------------------- codecs

/// A key of a [`KvMap`]: its text form. Implemented for every [`ToSql`]
/// type.
pub trait KvKey {
    fn key_bytes(&self) -> Result<Vec<u8>>;
}

impl<T: ToSql + ?Sized> KvKey for T {
    fn key_bytes(&self) -> Result<Vec<u8>> {
        text_form(&self.to_param())?
            .ok_or_else(|| Error::Param("a NULL cannot be a key".to_string()))
    }
}

/// A value of a [`KvMap`]: bytes to store and read back. Implemented for
/// every type that is both [`ToSql`] and [`FromSql`](crate::FromSql), by its
/// text form; [`KvJson`] for a serde type.
pub trait KvValue: Sized {
    fn encode(&self) -> Result<Vec<u8>>;
    fn decode(bytes: &[u8]) -> Result<Self>;
}

impl<T: ToSql + FromSqlOwned> KvValue for T {
    fn encode(&self) -> Result<Vec<u8>> {
        text_form(&self.to_param())?.ok_or_else(|| {
            Error::Param("a NULL is not stored as a value: delete the key instead".to_string())
        })
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        T::from_sql(tag::STRING, bytes).map_err(|message| Error::Column { index: 0, message })
    }
}

/// A serde type as a [`KvMap`] value, stored as its JSON.
#[cfg(feature = "serde_json")]
#[derive(Debug, Clone, PartialEq, Default)]
pub struct KvJson<T>(pub T);

#[cfg(feature = "serde_json")]
impl<T: serde::Serialize + serde::de::DeserializeOwned> KvValue for KvJson<T> {
    fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.0).map_err(|e| Error::Param(format!("a value did not serialize: {e}")))
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes)
            .map(KvJson)
            .map_err(|e| Error::Column { index: 0, message: format!("a value did not deserialize: {e}") })
    }
}

/// Values of type `V` under keys `prefix:<key>`. From [`Kv::map`].
pub struct KvMap<'c, K: ?Sized, V> {
    kv: Kv<'c>,
    prefix: String,
    _types: PhantomData<fn(&K) -> V>,
}

impl<K: KvKey + ?Sized, V: KvValue> KvMap<'_, K, V> {
    /// The full key of `key`.
    pub fn full_key(&self, key: &K) -> Result<Vec<u8>> {
        let mut out = self.key_prefix();
        out.extend(key.key_bytes()?);
        Ok(out)
    }

    /// `prefix:`, or nothing for the whole keyspace.
    fn key_prefix(&self) -> Vec<u8> {
        if self.prefix.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.prefix.len() + 17);
        out.extend_from_slice(self.prefix.as_bytes());
        out.push(b':');
        out
    }

    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        match self.kv.get(&self.full_key(key)?).await? {
            None => Ok(None),
            Some(bytes) => V::decode(&bytes).map(Some),
        }
    }

    pub async fn set(&self, key: &K, value: &V) -> Result<()> {
        self.kv.set(&self.full_key(key)?, &value.encode()?, None).await
    }

    pub async fn set_with_ttl(&self, key: &K, value: &V, ttl: Duration) -> Result<()> {
        self.kv.set(&self.full_key(key)?, &value.encode()?, Some(ttl)).await
    }

    /// Sets it only when absent: whether it was set. With a TTL, a lease or
    /// a lock: the first caller wins until it expires.
    pub async fn set_if_absent(&self, key: &K, value: &V, ttl: Option<Duration>) -> Result<bool> {
        self.kv.set_if_absent(&self.full_key(key)?, &value.encode()?, ttl).await
    }

    /// Whether there was one to remove.
    pub async fn remove(&self, key: &K) -> Result<bool> {
        Ok(self.kv.del(&[&self.full_key(key)?]).await? > 0)
    }

    pub async fn contains(&self, key: &K) -> Result<bool> {
        self.kv.exists(&self.full_key(key)?).await
    }

    pub async fn expire(&self, key: &K, ttl: Duration) -> Result<bool> {
        self.kv.expire(&self.full_key(key)?, ttl).await
    }

    pub async fn ttl(&self, key: &K) -> Result<Option<Duration>> {
        self.kv.ttl(&self.full_key(key)?).await
    }

    /// Each key's value (`None` for a missing one), in one `MGET`.
    pub async fn get_many(&self, keys: &[&K]) -> Result<Vec<Option<V>>> {
        let full: Vec<Vec<u8>> = keys.iter().map(|k| self.full_key(k)).collect::<Result<_>>()?;
        let refs: Vec<&[u8]> = full.iter().map(Vec::as_slice).collect();
        self.kv
            .get_many(&refs)
            .await?
            .into_iter()
            .map(|v| v.map(|b| V::decode(&b)).transpose())
            .collect()
    }

    /// Adds `by` to a counter, from 0 when absent: its new value. The value
    /// type must read back an integer.
    pub async fn incr_by(&self, key: &K, by: i64) -> Result<i64> {
        self.kv.incr_by(&self.full_key(key)?, by).await
    }

    /// Every key's suffix after `prefix:`, as bytes.
    pub async fn raw_keys(&self) -> Result<Vec<Vec<u8>>> {
        let prefix = self.key_prefix();
        let mut pattern = glob_escape(&prefix);
        pattern.push(b'*');
        let cut = prefix.len();
        Ok(self.kv.keys(&pattern).await?.into_iter().map(|k| k[cut.min(k.len())..].to_vec()).collect())
    }

    /// Removes every key under the prefix: the number removed. A `SCAN`, then
    /// `DEL`s in batches: keys written meanwhile may survive. Refused for the
    /// whole keyspace (an empty prefix): use `KV FLUSHDB` for that.
    pub async fn clear(&self) -> Result<i64> {
        if self.prefix.is_empty() {
            return Err(Error::Unsupported(
                "clear() on the whole keyspace; use KV FLUSHDB if that is meant".to_string(),
            ));
        }
        let mut pattern = glob_escape(&self.key_prefix());
        pattern.push(b'*');
        let keys = self.kv.keys(&pattern).await?;
        let mut removed = 0;
        for chunk in keys.chunks(500) {
            let refs: Vec<&[u8]> = chunk.iter().map(Vec::as_slice).collect();
            removed += self.kv.del(&refs).await?;
        }
        Ok(removed)
    }
}

impl<'c, K: KvKey + KvValue, V: KvValue> KvMap<'c, K, V> {
    /// A query over this map's entries: narrowed on the server by key
    /// ([`prefix`](KvQuery::prefix), [`pattern`](KvQuery::pattern)), then
    /// here by typed closures ([`filter_keys`](KvQuery::filter_keys),
    /// [`filter`](KvQuery::filter)).
    pub fn query(&self) -> KvQuery<'c, K, V> {
        KvQuery {
            map: KvMap { kv: self.kv, prefix: self.prefix.clone(), _types: PhantomData },
            suffix: Vec::new(),
            key_filter: None,
            filter: None,
            limit: None,
            lenient: false,
        }
    }
}

type KeyFilter<K> = Box<dyn Fn(&K) -> bool + Send + Sync>;
type EntryFilter<K, V> = Box<dyn Fn(&K, &V) -> bool + Send + Sync>;

/// A query over a [`KvMap`]'s entries. From [`KvMap::query`] or
/// [`Kv::query`].
///
/// The server narrows by key pattern (`SCAN … MATCH`); the closures run
/// here, `filter_keys` before values are fetched (so a key it rejects costs
/// no read), `filter` after. A cache has no index on values: a value filter
/// reads every value the key filters let through.
pub struct KvQuery<'c, K, V> {
    map: KvMap<'c, K, V>,
    /// The glob for the key's suffix, after the map's prefix.
    suffix: Vec<u8>,
    key_filter: Option<KeyFilter<K>>,
    filter: Option<EntryFilter<K, V>>,
    limit: Option<usize>,
    lenient: bool,
}

impl<K: KvKey + KvValue, V: KvValue> KvQuery<'_, K, V> {
    /// Only keys (after the map's prefix) starting with `prefix`; matched on
    /// the server.
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.suffix = glob_escape(prefix.as_bytes());
        self.suffix.push(b'*');
        self
    }

    /// Only keys (after the map's prefix) matching a glob: `*` any run, `?`
    /// one byte, `[abc]` one of. Matched on the server.
    pub fn pattern(mut self, glob: &str) -> Self {
        self.suffix = glob.as_bytes().to_vec();
        self
    }

    /// Only keys for which `keep` holds, before their values are read.
    pub fn filter_keys(mut self, keep: impl Fn(&K) -> bool + Send + Sync + 'static) -> Self {
        self.key_filter = Some(Box::new(keep));
        self
    }

    /// Only entries for which `keep` holds.
    pub fn filter(mut self, keep: impl Fn(&K, &V) -> bool + Send + Sync + 'static) -> Self {
        self.filter = Some(Box::new(keep));
        self
    }

    /// At most `n` entries (scan order, which is no order).
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skip a key or value that does not read as `K` or `V`, instead of
    /// failing: for a keyspace holding other kinds of entries too.
    pub fn lenient(mut self) -> Self {
        self.lenient = true;
        self
    }

    fn pattern_bytes(&self) -> Vec<u8> {
        let mut pattern = glob_escape(&self.map.key_prefix());
        if self.suffix.is_empty() {
            pattern.push(b'*');
        } else {
            pattern.extend_from_slice(&self.suffix);
        }
        pattern
    }

    /// The matching keys, typed, filtered by `filter_keys` only.
    async fn matching_keys(&self) -> Result<Vec<(Vec<u8>, K)>> {
        let cut = self.map.key_prefix().len();
        let mut out = Vec::new();
        for full in self.map.kv.keys(&self.pattern_bytes()).await? {
            let key = match K::decode(&full[cut.min(full.len())..]) {
                Ok(key) => key,
                Err(_) if self.lenient => continue,
                Err(e) => {
                    return Err(Error::Unsupported(format!(
                        "key {:?} does not read as the query's key type ({e}); lenient() skips such keys",
                        String::from_utf8_lossy(&full)
                    )));
                }
            };
            if self.key_filter.as_ref().is_none_or(|keep| keep(&key)) {
                out.push((full, key));
            }
        }
        Ok(out)
    }

    /// The matching entries.
    pub async fn all(&self) -> Result<Vec<(K, V)>> {
        self.entries(self.limit).await
    }

    async fn entries(&self, limit: Option<usize>) -> Result<Vec<(K, V)>> {
        let mut out = Vec::new();
        let keys = self.matching_keys().await?;
        for chunk in keys.chunks(500) {
            let refs: Vec<&[u8]> = chunk.iter().map(|(full, _)| full.as_slice()).collect();
            let values = self.map.kv.get_many(&refs).await?;
            for ((full, key), value) in chunk.iter().zip(values) {
                // Deleted since the scan, or not a string value (a hash, a
                // list): not an entry of this map.
                let Some(bytes) = value else { continue };
                let value = match V::decode(&bytes) {
                    Ok(v) => v,
                    Err(_) if self.lenient => continue,
                    Err(e) => {
                        return Err(Error::Unsupported(format!(
                            "the value at {:?} does not read as the query's value type ({e});                              lenient() skips such entries",
                            String::from_utf8_lossy(full)
                        )));
                    }
                };
                if self.filter.as_ref().is_none_or(|keep| keep(key, &value)) {
                    out.push((K::decode(&full[self.map.key_prefix().len()..])?, value));
                    if limit.is_some_and(|n| out.len() >= n) {
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    }

    /// The matching keys. Without a value [`filter`](Self::filter), no value
    /// is read.
    pub async fn keys(&self) -> Result<Vec<K>> {
        if self.filter.is_some() {
            return Ok(self.all().await?.into_iter().map(|(k, _)| k).collect());
        }
        let mut keys: Vec<K> = self.matching_keys().await?.into_iter().map(|(_, k)| k).collect();
        if let Some(n) = self.limit {
            keys.truncate(n);
        }
        Ok(keys)
    }

    pub async fn values(&self) -> Result<Vec<V>> {
        Ok(self.all().await?.into_iter().map(|(_, v)| v).collect())
    }

    /// The first matching entry, in scan order.
    pub async fn first(&self) -> Result<Option<(K, V)>> {
        Ok(self.entries(Some(1)).await?.into_iter().next())
    }

    pub async fn count(&self) -> Result<usize> {
        if self.filter.is_some() {
            Ok(self.all().await?.len())
        } else {
            Ok(self.keys().await?.len())
        }
    }

    /// Removes the matching entries: how many were removed.
    pub async fn delete(&self) -> Result<i64> {
        if self.map.prefix.is_empty()
            && self.suffix.is_empty()
            && self.key_filter.is_none()
            && self.filter.is_none()
        {
            return Err(Error::Unsupported(
                "delete() with no prefix, pattern or filter would empty the keyspace".to_string(),
            ));
        }
        let fulls: Vec<Vec<u8>> = if self.filter.is_some() {
            self.all()
                .await?
                .iter()
                .map(|(k, _)| self.map.full_key(k))
                .collect::<Result<_>>()?
        } else {
            let mut keys: Vec<Vec<u8>> = self.matching_keys().await?.into_iter().map(|(f, _)| f).collect();
            if let Some(n) = self.limit {
                keys.truncate(n);
            }
            keys
        };
        let mut removed = 0;
        for chunk in fulls.chunks(500) {
            let refs: Vec<&[u8]> = chunk.iter().map(Vec::as_slice).collect();
            removed += self.map.kv.del(&refs).await?;
        }
        Ok(removed)
    }
}

// ----------------------------------------------------------------- KvRepo

/// Model `M`'s rows as hashes: one field per column (a NULL column has no
/// field), under `<table>:<primary key>`, the key's columns' text forms
/// joined by `:` (a `:` or `\` inside one escaped by a `\`). From
/// [`Kv::repo`].
///
/// The struct is the same one the SQL ORM maps, so a model can live in a
/// table, in the cache, or both (a read-through cache in front of a table).
/// Serial columns are stored as given: assign the key before saving.
pub struct KvRepo<'c, M> {
    kv: Kv<'c>,
    prefix: String,
    _model: PhantomData<fn() -> M>,
}

impl<M: Model> KvRepo<'_, M> {
    /// Another prefix than the table's name.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    fn key_columns() -> Result<Vec<usize>> {
        let key: Vec<usize> = M::COLUMNS
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect();
        if key.is_empty() {
            return Err(Error::Unsupported(format!(
                "{} has no #[rdb(primary_key)] field to key its hash by",
                M::TABLE
            )));
        }
        Ok(key)
    }

    /// `prefix:part:part…`, with `:` and `\` inside a part backslash-escaped,
    /// so that `("a:b", "c")` and `("a", "b:c")` are different keys.
    fn key_of(&self, parts: &[Vec<u8>]) -> Vec<u8> {
        let mut out = self.prefix.as_bytes().to_vec();
        for part in parts {
            out.push(b':');
            for &b in part {
                if b == b':' || b == b'\\' {
                    out.push(b'\\');
                }
                out.push(b);
            }
        }
        out
    }

    fn key_for_model(&self, m: &M) -> Result<Vec<u8>> {
        let parts = Self::key_columns()?
            .into_iter()
            .map(|i| {
                let mut out = Vec::new();
                if !m.write_text(i, &mut out)? {
                    return Err(Error::Param(format!("key column {} is NULL", M::COLUMNS[i].name)));
                }
                Ok(out)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self.key_of(&parts))
    }

    /// The hash key of the row whose primary key is `key` (in key column order).
    pub fn key_for(&self, key: &[&dyn ToSql]) -> Result<Vec<u8>> {
        let columns = Self::key_columns()?;
        if columns.len() != key.len() {
            return Err(Error::Param(format!(
                "{}'s primary key has {} column(s), {} value(s) were given",
                M::TABLE,
                columns.len(),
                key.len()
            )));
        }
        let parts = key.iter().map(|k| k.key_bytes()).collect::<Result<Vec<_>>>()?;
        Ok(self.key_of(&parts))
    }

    /// Stores the row: its non-NULL columns with `HSET`, then removes the
    /// fields of NULL ones with `HDEL` (two commands, not atomic).
    pub async fn save(&self, m: &M) -> Result<()> {
        let key = self.key_for_model(m)?;
        let mut values = Vec::with_capacity(M::COLUMNS.len());
        let mut nulls: Vec<&[u8]> = Vec::new();
        for (i, column) in M::COLUMNS.iter().enumerate() {
            let mut out = Vec::new();
            if m.write_text(i, &mut out)? {
                values.push((column.name.as_bytes(), out));
            } else {
                nulls.push(column.name.as_bytes());
            }
        }
        let fields: Vec<(&[u8], &[u8])> = values.iter().map(|(f, v)| (*f, v.as_slice())).collect();
        self.kv.hset(&key, &fields).await?;
        self.kv.hdel(&key, &nulls).await?;
        Ok(())
    }

    /// [`save`](Self::save), then a TTL on the whole hash.
    pub async fn save_with_ttl(&self, m: &M, ttl: Duration) -> Result<()> {
        self.save(m).await?;
        self.kv.expire(&self.key_for_model(m)?, ttl).await.map(drop)
    }

    /// The row whose primary key is `key`, if stored. A stored hash missing a
    /// non-NULL column is an error (it was written by another version of the
    /// struct, or by hand).
    pub async fn get(&self, key: &[&dyn ToSql]) -> Result<Option<M>> {
        self.load(&self.key_for(key)?).await
    }

    async fn load(&self, key: &[u8]) -> Result<Option<M>> {
        let fields = self.kv.hgetall(key).await?;
        if fields.is_empty() {
            return Ok(None);
        }
        // A row as the server would send it, the columns in the model's
        // order, so the derived FromRow reads it.
        let mut payload = Vec::new();
        for column in M::COLUMNS {
            let value = fields.iter().find(|(f, _)| f == column.name.as_bytes());
            let (cell_tag, bytes): (u8, &[u8]) = match value {
                Some((_, v)) => (tag::STRING, v),
                None => (tag::NULL, &[]),
            };
            payload.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            payload.extend_from_slice(&0u64.to_le_bytes());
            payload.extend_from_slice(bytes);
            payload.push(cell_tag);
        }
        let mut data = u32::try_from(payload.len())
            .map_err(|_| Error::Protocol("a stored hash is too large to read".to_string()))?
            .to_le_bytes()
            .to_vec();
        data.extend(payload);
        let rows = Rows::decode(data)?;
        let row = rows.get(0).expect("one row");
        M::from_row(&row).map(Some).map_err(|e| match e {
            Error::Column { index, message } => Error::Column {
                index,
                message: format!(
                    "{}.{} in the cache: {message}",
                    M::TABLE,
                    M::COLUMNS.get(index).map_or("?", |c| c.name)
                ),
            },
            other => other,
        })
    }

    /// Whether there was one to remove.
    pub async fn delete(&self, m: &M) -> Result<bool> {
        Ok(self.kv.del(&[&self.key_for_model(m)?]).await? > 0)
    }

    pub async fn delete_key(&self, key: &[&dyn ToSql]) -> Result<bool> {
        Ok(self.kv.del(&[&self.key_for(key)?]).await? > 0)
    }

    pub async fn exists(&self, key: &[&dyn ToSql]) -> Result<bool> {
        self.kv.exists(&self.key_for(key)?).await
    }

    pub async fn expire(&self, m: &M, ttl: Duration) -> Result<bool> {
        self.kv.expire(&self.key_for_model(m)?, ttl).await
    }

    /// Every stored row of this model, by `SCAN` (not a snapshot).
    pub async fn all(&self) -> Result<Vec<M>> {
        self.scan(None, None).await
    }

    /// The stored rows for which `keep` holds: `repo.filter(|s| s.user_id == 7)`.
    /// Every stored row is read to test it: a cache has no index.
    pub async fn filter(&self, keep: impl Fn(&M) -> bool) -> Result<Vec<M>> {
        self.scan(Some(&keep), None).await
    }

    /// The first stored row (in scan order) for which `keep` holds.
    pub async fn find(&self, keep: impl Fn(&M) -> bool) -> Result<Option<M>> {
        Ok(self.scan(Some(&keep), Some(1)).await?.into_iter().next())
    }

    async fn scan(&self, keep: Option<&dyn Fn(&M) -> bool>, limit: Option<usize>) -> Result<Vec<M>> {
        let mut pattern = glob_escape(self.prefix.as_bytes());
        pattern.extend_from_slice(b":*");
        let mut out = Vec::new();
        for key in self.kv.keys(&pattern).await? {
            if let Some(m) = self.load(&key).await?
                && keep.is_none_or(|keep| keep(&m))
            {
                out.push(m);
                if limit.is_some_and(|n| out.len() >= n) {
                    break;
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::ArrayRef;
    use crate::{Interval, Numeric, Timestamp, Uuid};

    fn form(p: Param<'_>) -> String {
        String::from_utf8(text_form(&p).unwrap().unwrap()).unwrap()
    }

    #[test]
    fn text_forms_read_back() {
        assert_eq!(form(Param::I64(-5)), "-5");
        assert_eq!(form(Param::Bool(false)), "f");
        assert_eq!(form(Param::F64(f64::NEG_INFINITY)), "-Infinity");
        assert_eq!(form(Param::F64(0.1)), "0.1");
        assert_eq!(form(Param::Text("it's")), "it's");
        assert_eq!(text_form(&Param::Null).unwrap(), None);
        let ints = [1i64, -2];
        assert_eq!(form(Param::Array(ArrayRef::new(&ints))), "{1,-2}");

        fn round<T: KvValue + PartialEq + std::fmt::Debug>(v: T) {
            assert_eq!(T::decode(&v.encode().unwrap()).unwrap(), v);
        }
        round(-5i64);
        round(u64::MAX);
        round(true);
        round(0.1f64);
        round(String::from("x y"));
        round(vec![0u8, 255]);
        round(Uuid::new_v4().unwrap());
        round(Timestamp::from_micros(-1_234_567));
        round(Interval::new(1, -2, 3));
        round(Numeric::new("-0.001").unwrap());
        round(vec![Some(1i64), None]);
        round(vec!["a,b".to_string(), "\"".to_string()]);
        assert!(f64::decode(&f64::NAN.encode().unwrap()).unwrap().is_nan());
    }

    #[cfg(feature = "derive")]
    #[derive(crate::Model)]
    struct Pair {
        #[rdb(primary_key)]
        a: String,
        #[rdb(primary_key)]
        b: String,
    }

    #[cfg(feature = "derive")]
    #[test]
    fn composite_keys_cannot_collide() {
        let client_free = |a: &str, b: &str| {
            let repo = KvRepo::<Pair> { kv: keys_only_kv(), prefix: "p".into(), _model: PhantomData };
            repo.key_for_model(&Pair { a: a.into(), b: b.into() }).unwrap()
        };
        assert_ne!(client_free("a:b", "c"), client_free("a", "b:c"));
        assert_eq!(client_free("x\\", ":"), b"p:x\\\\:\\:".to_vec());
    }

    /// A `Kv` for building keys only: it is never asked to send anything.
    #[cfg(feature = "derive")]
    fn keys_only_kv() -> Kv<'static> {
        static CLIENT: std::sync::OnceLock<Client> = std::sync::OnceLock::new();
        Kv::new(CLIENT.get_or_init(Client::detached))
    }

    #[test]
    fn keys() {
        assert_eq!(42i64.key_bytes().unwrap(), b"42");
        assert_eq!("a:b".key_bytes().unwrap(), b"a:b");
        assert!(Option::<i64>::None.key_bytes().is_err());
        assert_eq!(glob_escape(b"a*b?[c]\\"), b"a\\*b\\?\\[c\\]\\\\");
    }
}
