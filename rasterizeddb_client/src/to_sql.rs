//! Rust values as statement parameters.

use std::borrow::Cow;
use std::sync::Arc;

use crate::params::{ArrayRef, Param};
use crate::types::{Date, Interval, Json, Numeric, Time, Timestamp, Uuid};

/// A value that can fill a `$n` placeholder.
///
/// The conversion borrows: text and bytes are not copied, and nothing is
/// allocated until the statement is rendered.
///
/// `Sync`, so that a parameter list (`&[&dyn ToSql]`) can be held across an
/// `.await` in a task that must be `Send`.
pub trait ToSql: Sync {
    /// The value as a parameter, borrowing from `self`.
    fn to_param(&self) -> Param<'_>;
}

/// `&[&dyn ToSql]` from a list of values, for the methods that take one
/// ([`Client::fetch_all`](crate::Client::fetch_all) and the rest):
///
/// ```
/// # use rasterizeddb_client::params;
/// let name = String::from("ada");
/// let age: Option<i32> = None;
/// let list = params![name, age, 42i64];
/// assert_eq!(list.len(), 3);
/// ```
#[macro_export]
macro_rules! params {
    () => {
        &[] as &[&dyn $crate::ToSql]
    };
    ($($value:expr),+ $(,)?) => {
        &[$(&$value as &dyn $crate::ToSql),+] as &[&dyn $crate::ToSql]
    };
}

impl ToSql for Param<'_> {
    fn to_param(&self) -> Param<'_> {
        *self
    }
}

impl<T: ToSql + ?Sized> ToSql for &T {
    fn to_param(&self) -> Param<'_> {
        (**self).to_param()
    }
}

impl<T: ToSql + ?Sized> ToSql for Box<T> {
    fn to_param(&self) -> Param<'_> {
        (**self).to_param()
    }
}

impl<T: ToSql + Send + ?Sized> ToSql for Arc<T> {
    fn to_param(&self) -> Param<'_> {
        (**self).to_param()
    }
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_param(&self) -> Param<'_> {
        self.as_ref().map_or(Param::Null, ToSql::to_param)
    }
}

macro_rules! copy_to_sql {
    ($($t:ty => |$v:ident| $e:expr),* $(,)?) => {$(
        impl ToSql for $t {
            fn to_param(&self) -> Param<'_> {
                let $v = *self;
                $e
            }
        }
    )*};
}

copy_to_sql!(
    bool => |v| Param::Bool(v),
    i8 => |v| Param::I16(v.into()),
    i16 => |v| Param::I16(v),
    i32 => |v| Param::I32(v),
    i64 => |v| Param::I64(v),
    isize => |v| Param::I64(v as i64),
    u16 => |v| Param::I32(v.into()),
    u32 => |v| Param::I64(v.into()),
    u64 => |v| Param::U64(v),
    i128 => |v| Param::I128(v),
    usize => |v| Param::U64(v as u64),
    f32 => |v| Param::F32(v),
    f64 => |v| Param::F64(v),
    Uuid => |v| Param::Uuid(v),
    Timestamp => |v| Param::Timestamp(v),
    Date => |v| Param::Date(v),
    Time => |v| Param::Time(v),
    Interval => |v| Param::Interval(v),
    std::time::SystemTime => |v| Param::Timestamp(v.into()),
    std::time::Duration => |v| Param::Interval(v.into()),
);

impl ToSql for str {
    fn to_param(&self) -> Param<'_> {
        Param::Text(self)
    }
}

impl ToSql for String {
    fn to_param(&self) -> Param<'_> {
        Param::Text(self)
    }
}

impl ToSql for Cow<'_, str> {
    fn to_param(&self) -> Param<'_> {
        Param::Text(self)
    }
}

/// Bytes go as `bytea`. A `[u8]` is never an array of numbers: there is no
/// `ToSql` for `u8`, so `Vec<u8>` and `&[u8]` always mean `bytea`.
impl ToSql for [u8] {
    fn to_param(&self) -> Param<'_> {
        Param::Bytes(self)
    }
}

impl ToSql for Vec<u8> {
    fn to_param(&self) -> Param<'_> {
        Param::Bytes(self)
    }
}

impl<const N: usize> ToSql for [u8; N] {
    fn to_param(&self) -> Param<'_> {
        Param::Bytes(self)
    }
}

impl ToSql for Numeric {
    fn to_param(&self) -> Param<'_> {
        Param::Numeric(self.as_str())
    }
}

impl ToSql for Json {
    fn to_param(&self) -> Param<'_> {
        Param::Json(self.as_str())
    }
}

/// An array literal, for an array column or `= ANY($1)`. Not for `u8`, whose
/// sequences are bytes (see [`ToSql for [u8]`](#impl-ToSql-for-%5Bu8%5D)).
macro_rules! array_to_sql {
    ($($t:ty),* $(,)?) => {$(
        impl ToSql for [$t] {
            fn to_param(&self) -> Param<'_> {
                Param::Array(ArrayRef::new(self))
            }
        }
        impl ToSql for Vec<$t> {
            fn to_param(&self) -> Param<'_> {
                Param::Array(ArrayRef::new(self))
            }
        }
        impl<const N: usize> ToSql for [$t; N] {
            fn to_param(&self) -> Param<'_> {
                Param::Array(ArrayRef::new(self))
            }
        }
    )*};
}

array_to_sql!(
    bool,
    i8,
    i16,
    i32,
    i64,
    u16,
    u32,
    u64,
    f32,
    f64,
    String,
    &str,
    Uuid,
    Timestamp,
    Date,
    Time,
    Interval,
    Numeric,
    Json,
    crate::Value,
    Option<bool>,
    Option<i16>,
    Option<i32>,
    Option<i64>,
    Option<u64>,
    Option<f32>,
    Option<f64>,
    Option<String>,
    Option<&str>,
    Option<Uuid>,
    Option<Timestamp>,
    Option<Date>,
    Option<Numeric>,
    Vec<i16>,
    Vec<i32>,
    Vec<i64>,
    Vec<f64>,
    Vec<String>,
);

#[cfg(feature = "uuid")]
impl ToSql for uuid::Uuid {
    fn to_param(&self) -> Param<'_> {
        Param::Uuid(Uuid(*self.as_bytes()))
    }
}

#[cfg(feature = "chrono")]
mod chrono_impls {
    use super::*;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike};

    impl<Tz: TimeZone> ToSql for DateTime<Tz>
    where
        DateTime<Tz>: Sync,
    {
        fn to_param(&self) -> Param<'_> {
            Param::Timestamp(Timestamp::from_micros(self.timestamp_micros()))
        }
    }

    /// A wall-clock time with no zone: written as the same wall-clock time,
    /// for a `timestamp` column.
    impl ToSql for NaiveDateTime {
        fn to_param(&self) -> Param<'_> {
            Param::Timestamp(Timestamp::from_micros(self.and_utc().timestamp_micros()))
        }
    }

    impl ToSql for NaiveDate {
        fn to_param(&self) -> Param<'_> {
            let days = self.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            Param::Date(Date::from_unix_days(days.num_days() as i32))
        }
    }

    impl ToSql for NaiveTime {
        fn to_param(&self) -> Param<'_> {
            let micros = self.num_seconds_from_midnight() as i64 * 1_000_000
                + (self.nanosecond() as i64 / 1_000).min(999_999);
            Param::Time(Time::from_micros(micros).expect("a NaiveTime is within a day"))
        }
    }
}
