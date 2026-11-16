//! One cell of any type, owned.

use std::fmt;

use crate::from_sql::{FromSql, array_elements};
use crate::params::{ArrayRef, Param};
use crate::row::tag;
use crate::to_sql::ToSql;
use crate::types::{Date, Interval, Json, Numeric, Time, Timestamp, Uuid};

/// A value of whatever type a cell held, for code that does not know the
/// shape of a result in advance (a console, an export, [`Row::values`]).
///
/// [`Row::values`]: crate::Row::values
#[derive(Clone, PartialEq, Debug)]
#[non_exhaustive]
pub enum Value {
    Null,
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    U64(u64),
    /// An integer wider than 64 bits (`HUGEINT`, `UHUGEINT`), or an unsigned
    /// one past `u64`.
    I128(i128),
    F32(f32),
    F64(f64),
    Numeric(Numeric),
    /// Text. A cell under the text tag that is not UTF-8 is [`Value::Bytes`].
    Text(String),
    Bytes(Vec<u8>),
    Uuid(Uuid),
    Timestamp(Timestamp),
    Date(Date),
    Time(Time),
    Interval(Interval),
    Json(Json),
    Array(Vec<Value>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// A cell, by its tag. Refuses what cannot be read without guessing (a
    /// `NUMERIC` sent without its scale) and tags this client does not know.
    pub(crate) fn decode(t: u8, raw: &[u8]) -> Result<Value, String> {
        Ok(match t {
            tag::NULL => Value::Null,
            tag::BOOL => Value::Bool(bool::from_sql(t, raw)?),
            tag::I8 | tag::I16 => Value::I16(i16::from_sql(t, raw)?),
            tag::I32 | tag::U16 => Value::I32(i32::from_sql(t, raw)?),
            tag::I64 | tag::U32 => Value::I64(i64::from_sql(t, raw)?),
            tag::U8 => Value::I16(i16::from_sql(t, raw)?),
            tag::U64 => Value::U64(u64::from_sql(t, raw)?),
            tag::I128 | tag::U128 => Value::I128(i128::from_sql(t, raw)?),
            tag::F32 => Value::F32(f32::from_sql(t, raw)?),
            tag::F64 => Value::F64(f64::from_sql(t, raw)?),
            tag::DECIMAL => Value::Numeric(Numeric::from_sql(t, raw)?),
            tag::UUID => Value::Uuid(Uuid::from_sql(t, raw)?),
            tag::DATETIME | tag::TIMESTAMP => Value::Timestamp(Timestamp::from_sql(t, raw)?),
            tag::DATE => Value::Date(Date::from_sql(t, raw)?),
            tag::TIME => Value::Time(Time::from_sql(t, raw)?),
            tag::INTERVAL => Value::Interval(Interval::from_sql(t, raw)?),
            tag::BYTEA => Value::Bytes(raw.to_vec()),
            tag::JSON | tag::JSONTEXT => Value::Json(Json::from_sql(t, raw)?),
            tag::ARRAY => Value::Array(
                array_elements(t, raw)?
                    .into_iter()
                    .map(|e| e.map_or(Value::Null, Value::Text))
                    .collect(),
            ),
            tag::STRING | tag::CHAR => match std::str::from_utf8(raw) {
                Ok(s) => Value::Text(s.to_string()),
                Err(_) => Value::Bytes(raw.to_vec()),
            },
            other => return Err(format!("type tag {other} has no decoder in this client")),
        })
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            Value::Json(j) => Some(j.as_str()),
            Value::Numeric(n) => Some(n.as_str()),
            _ => None,
        }
    }

    /// Any integer that fits `i64`.
    pub fn as_i64(&self) -> Option<i64> {
        match *self {
            Value::I16(v) => Some(v.into()),
            Value::I32(v) => Some(v.into()),
            Value::I64(v) => Some(v),
            Value::U64(v) => i64::try_from(v).ok(),
            Value::I128(v) => i64::try_from(v).ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::F32(v) => Some((*v).into()),
            Value::F64(v) => Some(*v),
            Value::Numeric(n) => n.as_str().parse().ok(),
            other => other.as_i64().map(|v| v as f64),
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            Value::Text(s) => Some(s.as_bytes()),
            _ => None,
        }
    }
}

/// As `psql` would print it: text unquoted, `NULL` for a null, bytes as
/// `\x…`, arrays as `{…}`.
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("NULL"),
            Value::Bool(b) => f.write_str(if *b { "t" } else { "f" }),
            Value::I16(v) => write!(f, "{v}"),
            Value::I32(v) => write!(f, "{v}"),
            Value::I64(v) => write!(f, "{v}"),
            Value::U64(v) => write!(f, "{v}"),
            Value::I128(v) => write!(f, "{v}"),
            Value::F32(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::Numeric(v) => write!(f, "{v}"),
            Value::Text(v) => f.write_str(v),
            Value::Bytes(b) => {
                f.write_str("\\x")?;
                b.iter().try_for_each(|b| write!(f, "{b:02x}"))
            }
            Value::Uuid(v) => write!(f, "{v}"),
            Value::Timestamp(v) => write!(f, "{v}"),
            Value::Date(v) => write!(f, "{v}"),
            Value::Time(v) => write!(f, "{v}"),
            Value::Interval(v) => write!(f, "{v}"),
            Value::Json(v) => write!(f, "{v}"),
            Value::Array(items) => {
                f.write_str("{")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{item}")?;
                }
                f.write_str("}")
            }
        }
    }
}

impl ToSql for Value {
    fn to_param(&self) -> Param<'_> {
        match self {
            Value::Null => Param::Null,
            Value::Bool(v) => Param::Bool(*v),
            Value::I16(v) => Param::I16(*v),
            Value::I32(v) => Param::I32(*v),
            Value::I64(v) => Param::I64(*v),
            Value::U64(v) => Param::U64(*v),
            Value::I128(v) => Param::I128(*v),
            Value::F32(v) => Param::F32(*v),
            Value::F64(v) => Param::F64(*v),
            Value::Numeric(n) => Param::Numeric(n.as_str()),
            Value::Text(s) => Param::Text(s),
            Value::Bytes(b) => Param::Bytes(b),
            Value::Uuid(v) => Param::Uuid(*v),
            Value::Timestamp(v) => Param::Timestamp(*v),
            Value::Date(v) => Param::Date(*v),
            Value::Time(v) => Param::Time(*v),
            Value::Interval(v) => Param::Interval(*v),
            Value::Json(j) => Param::Json(j.as_str()),
            Value::Array(items) => Param::Array(ArrayRef::new(items)),
        }
    }
}

macro_rules! value_from {
    ($($t:ty => $variant:ident),* $(,)?) => {$(
        impl From<$t> for Value {
            fn from(v: $t) -> Self {
                Value::$variant(v)
            }
        }
    )*};
}
value_from!(
    bool => Bool, i16 => I16, i32 => I32, i64 => I64, u64 => U64, i128 => I128, f32 => F32,
    f64 => F64, Numeric => Numeric, String => Text, Vec<u8> => Bytes, Uuid => Uuid,
    Timestamp => Timestamp, Date => Date, Time => Time, Interval => Interval, Json => Json,
);

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_string())
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        v.map_or(Value::Null, Into::into)
    }
}
