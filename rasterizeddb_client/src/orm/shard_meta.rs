//! What a model says about its shard: plain data, built without the
//! `sharding` feature, so derived models compile in every build. The
//! hashing that turns a [`ShardKey`] into a shard lives in
//! [`crate::shard`] alone (feature `sharding`).

use std::borrow::Cow;

use crate::params::Param;
use crate::types::Uuid;

/// The routing family of a shard key column, mirroring route_v1's: every
/// integer width hashes alike, a UUID as its 16 octets, text as its UTF-8
/// bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardFamily {
    /// `i8…i128`, `u8…u64`.
    Integer,
    /// A UUID.
    Uuid,
    /// Text.
    Text,
}

/// Where a model's rows live in a sharded deployment
/// ([`Model::SHARD`](super::Model::SHARD)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sharding {
    /// On the home shard only (`#[rdb(shard = "global")]`, or a model with
    /// neither `shard_by` nor `partition_by`): reference data, global
    /// settings. Every shard has the table, and only the home shard has rows.
    Global,
    /// Spread over the shards by `column`'s value (`#[rdb(shard_by =
    /// "…")]`, or the leading `partition_by` column).
    By {
        /// The shard key column.
        column: &'static str,
        /// Its routing family.
        family: ShardFamily,
    },
    /// `#[rdb(shard = "unsharded")]`: a router refuses the model, for an
    /// application that wants a forgotten model to be an error.
    Unsharded,
    /// Partitioned, so it would be sharded by its leading partition column,
    /// but it cannot be: the text says why (a `serial` column, a unique key
    /// without the shard column, a column type that does not route). A
    /// router refuses it; declare `shard_by` (and fix the reason) or
    /// `shard = "global"`.
    Unroutable(&'static str),
}

impl Sharding {
    /// The shard key column, for a sharded model.
    pub fn column(&self) -> Option<&'static str> {
        match self {
            Sharding::By { column, .. } => Some(column),
            _ => None,
        }
    }
}

/// A shard key value: what a row or a query routes by. Its family is the
/// value's; a router hashes it under the column's family, converting text
/// to an integer or a UUID (and back) as the server would compare them.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ShardKey<'a> {
    /// An integer of any width.
    Int(i128),
    /// A UUID's 16 octets.
    Uuid([u8; 16]),
    /// Text.
    Text(Cow<'a, str>),
}

impl ShardKey<'_> {
    /// The value's own family.
    pub fn family(&self) -> ShardFamily {
        match self {
            ShardKey::Int(_) => ShardFamily::Integer,
            ShardKey::Uuid(_) => ShardFamily::Uuid,
            ShardKey::Text(_) => ShardFamily::Text,
        }
    }

    /// The key with its text owned.
    pub fn into_owned(self) -> ShardKey<'static> {
        match self {
            ShardKey::Int(v) => ShardKey::Int(v),
            ShardKey::Uuid(b) => ShardKey::Uuid(b),
            ShardKey::Text(t) => ShardKey::Text(Cow::Owned(t.into_owned())),
        }
    }

    /// The same value in a column of `family`, as the server converts a
    /// literal compared with that column: `None` when it has no such form
    /// (an integer for a UUID column, text that is not a number).
    pub fn coerce(&self, family: ShardFamily) -> Option<ShardKey<'_>> {
        Some(match (self, family) {
            (ShardKey::Int(v), ShardFamily::Integer) => ShardKey::Int(*v),
            (ShardKey::Uuid(b), ShardFamily::Uuid) => ShardKey::Uuid(*b),
            (ShardKey::Text(t), ShardFamily::Text) => ShardKey::Text(Cow::Borrowed(t)),
            (ShardKey::Text(t), ShardFamily::Integer) => ShardKey::Int(t.trim().parse().ok()?),
            (ShardKey::Text(t), ShardFamily::Uuid) => ShardKey::Uuid(parse_uuid(t.trim())?),
            (ShardKey::Int(v), ShardFamily::Text) => ShardKey::Text(Cow::Owned(v.to_string())),
            (ShardKey::Uuid(b), ShardFamily::Text) => ShardKey::Text(Cow::Owned(uuid_text(b))),
            (ShardKey::Int(_), ShardFamily::Uuid) | (ShardKey::Uuid(_), ShardFamily::Integer) => {
                return None;
            }
        })
    }

    /// The key a literal parameter carries, if it is a routable value: an
    /// integer, text or a UUID.
    pub(crate) fn from_param(p: &Param<'_>) -> Option<ShardKey<'static>> {
        Some(match p {
            Param::I16(v) => ShardKey::Int(*v as i128),
            Param::I32(v) => ShardKey::Int(*v as i128),
            Param::I64(v) => ShardKey::Int(*v as i128),
            Param::U64(v) => ShardKey::Int(*v as i128),
            Param::I128(v) => ShardKey::Int(*v),
            Param::Text(t) => ShardKey::Text(Cow::Owned((*t).to_string())),
            Param::Uuid(u) => ShardKey::Uuid(u.0),
            _ => return None,
        })
    }
}

/// `8-4-4-4-12` hex digits, any case, as PostgreSQL's `uuid_in` reads the
/// common form.
fn parse_uuid(t: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = t.bytes().filter(|b| *b != b'-').collect();
    if hex.len() != 32 || t.len() != 36 && t.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks(2).enumerate() {
        let s = std::str::from_utf8(pair).ok()?;
        out[i] = u8::from_str_radix(s, 16).ok()?;
    }
    Some(out)
}

fn uuid_text(b: &[u8; 16]) -> String {
    let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
}

macro_rules! int_keys {
    ($($t:ty),*) => {$(
        impl From<$t> for ShardKey<'_> {
            fn from(v: $t) -> Self {
                ShardKey::Int(v as i128)
            }
        }
        impl ShardKeyType for $t {
            const FAMILY: ShardFamily = ShardFamily::Integer;
            fn shard_key(&self) -> ShardKey<'_> {
                ShardKey::Int(*self as i128)
            }
        }
    )*};
}
int_keys!(i8, i16, i32, i64, i128, u8, u16, u32, u64);

impl<'a> From<&'a str> for ShardKey<'a> {
    fn from(v: &'a str) -> Self {
        ShardKey::Text(Cow::Borrowed(v))
    }
}

impl From<String> for ShardKey<'_> {
    fn from(v: String) -> Self {
        ShardKey::Text(Cow::Owned(v))
    }
}

impl<'a> From<&'a String> for ShardKey<'a> {
    fn from(v: &'a String) -> Self {
        ShardKey::Text(Cow::Borrowed(v))
    }
}

impl From<Uuid> for ShardKey<'_> {
    fn from(v: Uuid) -> Self {
        ShardKey::Uuid(v.0)
    }
}

#[cfg(feature = "uuid")]
impl From<uuid::Uuid> for ShardKey<'_> {
    fn from(v: uuid::Uuid) -> Self {
        ShardKey::Uuid(*v.as_bytes())
    }
}

/// A Rust type a shard key field may have. What the derive requires of a
/// `shard_by` field: an integer, `String`, or a UUID. `Option`, floats,
/// decimals, dates, JSON and enums do not route (a NULL key has no shard,
/// and the others have no exact equality the router could hash).
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be a shard key",
    label = "the field `#[rdb(shard_by)]` names has this type",
    note = "a shard key is an integer (i8…i128, u8…u64), a String or a Uuid, never an Option"
)]
pub trait ShardKeyType {
    /// The column's routing family.
    const FAMILY: ShardFamily;
    /// The field's value as a key.
    fn shard_key(&self) -> ShardKey<'_>;
}

impl ShardKeyType for String {
    const FAMILY: ShardFamily = ShardFamily::Text;
    fn shard_key(&self) -> ShardKey<'_> {
        ShardKey::Text(Cow::Borrowed(self))
    }
}

impl ShardKeyType for Uuid {
    const FAMILY: ShardFamily = ShardFamily::Uuid;
    fn shard_key(&self) -> ShardKey<'_> {
        ShardKey::Uuid(self.0)
    }
}

#[cfg(feature = "uuid")]
impl ShardKeyType for uuid::Uuid {
    const FAMILY: ShardFamily = ShardFamily::Uuid;
    fn shard_key(&self) -> ShardKey<'_> {
        ShardKey::Uuid(*self.as_bytes())
    }
}

/// Where a [`Select`](super::Select) goes when it runs on a router.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum ShardTarget {
    /// By the filter's route hint, else refused.
    #[default]
    Hint,
    /// To the shard of this key ([`Select::on_shard`](super::Select::on_shard)).
    Key(ShardKey<'static>),
    /// To every shard, merged ([`Select::all_shards`](super::Select::all_shards)).
    All,
}

/// The routing a predicate carries beside its SQL text: which shard key
/// values it pins, column by column, and which other models' tables its
/// subqueries read.
#[derive(Debug, Clone, Default)]
pub(crate) struct RouteHint {
    /// `(column, values)`: the column equals one of the values. Several
    /// entries all hold (they came from an `AND`).
    pub(crate) pins: Vec<(&'static str, Vec<ShardKey<'static>>)>,
    /// The sharding of each model a subquery reads (`where_has`): a router
    /// sends the statement to one shard only if each is there too.
    pub(crate) reads: Vec<Sharding>,
}

impl RouteHint {
    /// `column` equals one of `values`; no pin when a value does not route.
    pub(crate) fn pin(column: &'static str, values: &[Param<'_>]) -> RouteHint {
        let keys: Option<Vec<ShardKey<'static>>> = values.iter().map(ShardKey::from_param).collect();
        RouteHint {
            pins: keys.map(|k| vec![(column, k)]).unwrap_or_default(),
            reads: Vec::new(),
        }
    }

    /// Both hold: every pin of either.
    pub(crate) fn and(mut self, other: RouteHint) -> RouteHint {
        self.pins.extend(other.pins);
        self.reads.extend(other.reads);
        self
    }

    /// Either holds, or the negation: no value is pinned any more, but the
    /// subqueries still read what they read.
    pub(crate) fn unpinned(mut self) -> RouteHint {
        self.pins.clear();
        self
    }
}

/// Where a related model's rows load from, next to a parent's.
#[cfg(feature = "sharding")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Placement {
    /// On the shard the parent row came from: the child shares its shard
    /// column (or both are global).
    Local,
    /// On the home shard: the child is global.
    Home,
}

/// Whether a child model's rows can be found next to a parent's, and where.
#[cfg(feature = "sharding")]
pub(crate) fn placement(parent: Sharding, child: Sharding) -> Result<Placement, String> {
    match (parent, child) {
        (Sharding::Global, Sharding::Global) => Ok(Placement::Local),
        (Sharding::By { .. }, Sharding::Global) => Ok(Placement::Home),
        (Sharding::By { column: a, .. }, Sharding::By { column: b, .. }) if a == b => Ok(Placement::Local),
        (Sharding::By { column: a, .. }, Sharding::By { column: b, .. }) => Err(format!(
            "the related rows are sharded by {b}, the parent's by {a}: they are not on one shard together"
        )),
        (Sharding::Global, Sharding::By { column, .. }) => Err(format!(
            "the related rows are sharded by {column} and the parent is global: they are spread over every shard"
        )),
        (_, Sharding::Unsharded) | (Sharding::Unsharded, _) => {
            Err("a model marked shard = \"unsharded\" cannot be read through a router".to_string())
        }
        (_, Sharding::Unroutable(why)) | (Sharding::Unroutable(why), _) => Err(why.to_string()),
    }
}
