use std::fmt::Display;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
#[repr(u8)]
pub enum DbType {
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    BOOL,
    CHAR,
    STRING,
    DATETIME,
    /// RFC 4122 UUID, stored as its 16 octets in network order. Equality is
    /// byte equality and `memcmp` reproduces canonical-text ordering, so it
    /// needs no comparator of its own.
    UUID,
    /// IPv4/IPv6 host or network address, stored as
    /// `[family][address (16, big-endian)][prefix bits]`. Sorts by family,
    /// then address, then netmask.
    INET,
    /// Arbitrary bytes. Stored through the same variable-width mechanism as
    /// `STRING`, and distinguished from it only so the wire layer reports
    /// `bytea` rather than `text`.
    BYTEA,
    /// A one-dimensional array, stored as PostgreSQL's binary array
    /// representation.
    ///
    /// The element type is carried *inside* the value — the format begins with
    /// the element's OID — so one storage type covers arrays of anything
    /// without the schema having to record an element type per column.
    ARRAY,
    /// Exact fixed-point decimal: an `i128` of units scaled by
    /// [`DECIMAL_SCALE`](crate::core::value_codec::DECIMAL_SCALE) decimal
    /// places, little-endian. One fixed scale keeps it comparable as a plain
    /// `i128`, which is what lets it use the ordinary comparison, fingerprint
    /// and zone-map paths.
    DECIMAL,
    /// A JSON document, for *reporting* only.
    ///
    /// Nothing stores this. A JSON column holds a `UUID` referencing the row
    /// its document was shredded into, and the wire layer substitutes this when
    /// describing the column so a client is told `jsonb` rather than the
    /// storage type. Reported as `uuid` a driver refused the column outright;
    /// reported as `text` it refused it again, this time as "Rust type
    /// Option<serde_json::Value> (as SQL type JSONB) is not compatible with SQL
    /// type TEXT".
    JSON,
    /// A JSON document, stored as bytes — see [`crate::core::jsonb`].
    ///
    /// Variable-width and physically a `STRING`, so the row layout, the
    /// pointer file and every compaction path handle it already; what differs
    /// is only what the bytes mean. Unlike [`DbType::JSON`] this one is real
    /// storage: a column declared `JSONB` holds its document rather than a
    /// reference to a table the document was taken apart into.
    JSONB,
    /// A span of time, stored as PostgreSQL's binary layout: `i64` microseconds,
    /// `i32` days, `i32` months, big-endian. The three are kept apart because a
    /// month is not a fixed number of days; see [`crate::core::pg_types`].
    INTERVAL,
    /// A six-octet MAC address.
    MACADDR,
    /// An eight-octet (EUI-64) MAC address. Distinct from [`DbType::MACADDR`]
    /// rather than a width flag on it, because a column advertises one type OID
    /// and the two have different ones.
    MACADDR8,
    /// A fixed-width bit string, `BIT(n)`.
    BIT,
    /// A varying bit string, `BIT VARYING(n)`.
    VARBIT,
    /// A calendar date, `i32` days from 2000-01-01, big-endian.
    ///
    /// Split out of [`DbType::DATETIME`] rather than folded into it: a `DATE`
    /// column that advertised `timestamptz` was decoded by a client into a
    /// timestamp type, and a driver asking for a `NaiveDate` refused the column
    /// outright.
    DATE,
    /// A time of day, `i64` microseconds since midnight, big-endian.
    TIME,
    /// A time of day with a UTC offset: microseconds then an `i32` offset in
    /// seconds west of Greenwich, both big-endian.
    TIMETZ,
    /// Geometric point, two `f64`.
    POINT,
    /// Geometric line, the coefficients of `Ax + By + C = 0`.
    LINE,
    /// Geometric line segment, two endpoints.
    LSEG,
    /// Geometric box, two corners normalised so the first is the upper right.
    BOX,
    /// Geometric path, open or closed, of any number of points.
    PATH,
    /// Geometric polygon.
    POLYGON,
    /// Geometric circle: centre and radius.
    CIRCLE,
    /// `int4range`.
    INT4RANGE,
    /// `int8range`.
    INT8RANGE,
    /// `numrange`.
    NUMRANGE,
    /// `tsrange`.
    TSRANGE,
    /// `tstzrange`.
    TSTZRANGE,
    /// `daterange`.
    DATERANGE,
    /// Sentinel / UNKNOWN type. Used for forward-compat and internal meta records.
    UNKNOWN,
    NULL,
    START,
    END,
}

impl Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self))
    }
}

impl DbType {
    /// The type the storage layer sees.
    ///
    /// `BYTEA` and `ARRAY` are variable-width and are stored through exactly
    /// the same mechanism as `STRING` — a pointer and a length in the fixed
    /// part of the row, bytes at the end. Mapping them here means the row
    /// writer, the fetch path and the in-place-update path need no knowledge of
    /// them at all; only the wire layer, which reports the declared type, tells
    /// them apart.
    #[inline]
    pub fn physical(&self) -> DbType {
        match self {
            // Variable-width and carried by the same pointer-and-length
            // mechanism as a string. A bit string, a path, a polygon and every
            // range vary in length for the same reason an array does, so they
            // reach storage the same way and only the declared type tells them
            // apart on the wire.
            DbType::BYTEA
            | DbType::ARRAY
            | DbType::JSONB
            | DbType::BIT
            | DbType::VARBIT
            | DbType::PATH
            | DbType::POLYGON
            | DbType::INT4RANGE
            | DbType::INT8RANGE
            | DbType::NUMRANGE
            | DbType::TSRANGE
            | DbType::TSTZRANGE
            | DbType::DATERANGE => DbType::STRING,
            other => other.clone(),
        }
    }

    /// Whether values of this type vary in length.
    #[inline]
    pub fn is_variable_width(&self) -> bool {
        matches!(
            self,
            DbType::STRING
                | DbType::BYTEA
                | DbType::ARRAY
                | DbType::JSONB
                | DbType::BIT
                | DbType::VARBIT
                | DbType::PATH
                | DbType::POLYGON
                | DbType::INT4RANGE
                | DbType::INT8RANGE
                | DbType::NUMRANGE
                | DbType::TSRANGE
                | DbType::TSTZRANGE
                | DbType::DATERANGE
        )
    }

    #[inline(always)]
    pub fn to_byte(&self) -> u8 {
        match self {
            DbType::I8 => 1,
            DbType::I16 => 2,
            DbType::I32 => 3,
            DbType::I64 => 4,
            DbType::I128 => 5,
            DbType::U8 => 6,
            DbType::U16 => 7,
            DbType::U32 => 8,
            DbType::U64 => 9,
            DbType::U128 => 10,
            DbType::F32 => 11,
            DbType::F64 => 12,
            DbType::CHAR => 13,
            DbType::STRING => 14,
            DbType::DATETIME => 15,
            DbType::BOOL => 16,
            DbType::UUID => 17,
            DbType::INET => 18,
            DbType::DECIMAL => 19,
            DbType::BYTEA => 20,
            DbType::ARRAY => 21,
            DbType::JSONB => 22,
            DbType::INTERVAL => 24,
            DbType::MACADDR => 25,
            DbType::MACADDR8 => 26,
            DbType::BIT => 27,
            DbType::VARBIT => 28,
            DbType::DATE => 29,
            DbType::TIME => 30,
            DbType::TIMETZ => 31,
            DbType::POINT => 32,
            DbType::LINE => 33,
            DbType::LSEG => 34,
            DbType::BOX => 35,
            DbType::PATH => 36,
            DbType::POLYGON => 37,
            DbType::CIRCLE => 38,
            DbType::INT4RANGE => 39,
            DbType::INT8RANGE => 40,
            DbType::NUMRANGE => 41,
            DbType::TSRANGE => 42,
            DbType::TSTZRANGE => 43,
            DbType::DATERANGE => 44,
            DbType::UNKNOWN => 252,
            // Never written to disk: it exists only so the wire layer can say
            // `jsonb`, and so a rebuilt document keeps its JSON-ness while it
            // travels as a row.
            //
            // It had shared 252 with UNKNOWN under a comment claiming the byte
            // round-tripped. It did not — 252 decodes as UNKNOWN — and a row
            // carrying a rebuilt document through a serialise/parse pair
            // (materialising a derived table does exactly that) arrived with
            // no type, so the value had no literal form and the statement
            // failed. 23 is free and nothing on disk can hold it.
            DbType::JSON => 23,
            DbType::NULL => 253,
            DbType::START => 254,
            DbType::END => 255,
        }
    }

    #[track_caller]
    #[inline(always)]
    pub fn from_byte(byte: u8) -> DbType {
        match byte {
            1 => DbType::I8,
            2 => DbType::I16,
            3 => DbType::I32,
            4 => DbType::I64,
            5 => DbType::I128,
            6 => DbType::U8,
            7 => DbType::U16,
            8 => DbType::U32,
            9 => DbType::U64,
            10 => DbType::U128,
            11 => DbType::F32,
            12 => DbType::F64,
            13 => DbType::CHAR,
            14 => DbType::STRING,
            15 => DbType::DATETIME,
            16 => DbType::BOOL,
            17 => DbType::UUID,
            18 => DbType::INET,
            19 => DbType::DECIMAL,
            20 => DbType::BYTEA,
            21 => DbType::ARRAY,
            22 => DbType::JSONB,
            24 => DbType::INTERVAL,
            25 => DbType::MACADDR,
            26 => DbType::MACADDR8,
            27 => DbType::BIT,
            28 => DbType::VARBIT,
            29 => DbType::DATE,
            30 => DbType::TIME,
            31 => DbType::TIMETZ,
            32 => DbType::POINT,
            33 => DbType::LINE,
            34 => DbType::LSEG,
            35 => DbType::BOX,
            36 => DbType::PATH,
            37 => DbType::POLYGON,
            38 => DbType::CIRCLE,
            39 => DbType::INT4RANGE,
            40 => DbType::INT8RANGE,
            41 => DbType::NUMRANGE,
            42 => DbType::TSRANGE,
            43 => DbType::TSTZRANGE,
            44 => DbType::DATERANGE,
            23 => DbType::JSON,
            252 => DbType::UNKNOWN,
            253 => DbType::NULL,
            254 => DbType::START,
            255 => DbType::END,
            _ => panic!("Invalid byte type: {}", byte),
        }
    }

    #[track_caller]
    #[inline(always)]
    pub fn get_size(&self) -> u32 {
        match self {
            DbType::I8 => 1,
            DbType::I16 => 2,
            DbType::I32 => 4,
            DbType::I64 => 8,
            DbType::I128 => 16,
            DbType::U8 => 1,
            DbType::U16 => 2,
            DbType::U32 => 4,
            DbType::U64 => 8,
            DbType::U128 => 16,
            DbType::F32 => 4,
            DbType::F64 => 8,
            DbType::BOOL => 1,
            DbType::CHAR => 4,
            DbType::STRING => 4 + 8,
            DbType::UUID => crate::core::value_codec::UUID_LEN as u32,
            DbType::INET => crate::core::value_codec::INET_LEN as u32,
            DbType::DECIMAL => 16,
            // Variable-width, like STRING: an 8-byte pointer and a 4-byte
            // length live in the row, and the bytes themselves follow it.
            DbType::BYTEA
            | DbType::ARRAY
            | DbType::JSONB
            | DbType::BIT
            | DbType::VARBIT
            | DbType::PATH
            | DbType::POLYGON
            | DbType::INT4RANGE
            | DbType::INT8RANGE
            | DbType::NUMRANGE
            | DbType::TSRANGE
            | DbType::TSTZRANGE
            | DbType::DATERANGE => 4 + 8,
            DbType::INTERVAL => crate::core::pg_types::INTERVAL_LEN as u32,
            DbType::MACADDR => crate::core::pg_types::MACADDR_LEN as u32,
            DbType::MACADDR8 => crate::core::pg_types::MACADDR8_LEN as u32,
            DbType::DATE => crate::core::pg_types::DATE_LEN as u32,
            DbType::TIME => crate::core::pg_types::TIME_LEN as u32,
            DbType::TIMETZ => crate::core::pg_types::TIMETZ_LEN as u32,
            DbType::POINT => crate::core::pg_types::POINT_LEN as u32,
            DbType::LSEG | DbType::BOX => crate::core::pg_types::LSEG_LEN as u32,
            DbType::LINE | DbType::CIRCLE => crate::core::pg_types::LINE_LEN as u32,
            DbType::UNKNOWN => 0,
            // Nothing stores it; a JSON column's slot is its UUID reference.
            DbType::JSON => crate::core::value_codec::UUID_LEN as u32,
            DbType::NULL => 1,
            DbType::START => 1,
            DbType::END => 1,
            DbType::DATETIME => 8,
        }
    }

    pub fn pg_oid(&self) -> u32 {
        match self {
            DbType::BOOL => 16,
            // A `SMALLINT` column must advertise `int2`. Advertising `int4`
            // made a client decoding into `i16` reject the column outright —
            // "Rust type Option<i16> (as SQL type INT2) is not compatible with
            // SQL type INT4" — though the value on the wire was correct.
            // PostgreSQL has no one-byte integer, so `I8` rides along in the
            // narrowest type it does have.
            DbType::I8 | DbType::I16 => 21, // int2
            DbType::I32 => 23,              // int4
            DbType::I64 => 20,              // int8
            // Every unsigned value below 2^32 fits `int8`.
            DbType::U8 | DbType::U16 | DbType::U32 => 20,
            // Wider than `bigint`, so `numeric` at scale 0 — PostgreSQL's own
            // choice for such integers. Advertised as `int8`, a `UBIGINT`
            // above `i64::MAX` reached a binary client as a negative number.
            DbType::U64 | DbType::I128 | DbType::U128 => 1700,
            DbType::F32 => 700,
            DbType::F64 => 701,
            DbType::STRING => 25,     // TEXT
            DbType::DATETIME => 1184, // timestamptz
            DbType::UUID => 2950,
            DbType::INET => 869,
            DbType::DECIMAL => 1700, // numeric
            DbType::JSON => 3802,    // jsonb
            DbType::JSONB => 3802,   // jsonb, and this one is really stored
            DbType::BYTEA => 17,
            // `text[]`. The element type is in the value, but a column has to
            // advertise one OID, and text is what the schema declares.
            DbType::ARRAY => 1009,
            DbType::INTERVAL => 1186,
            DbType::MACADDR => 829,
            DbType::MACADDR8 => 774,
            DbType::BIT => 1560,
            DbType::VARBIT => 1562,
            DbType::DATE => 1082,
            DbType::TIME => 1083,
            DbType::TIMETZ => 1266,
            DbType::POINT => 600,
            DbType::LINE => 628,
            DbType::LSEG => 601,
            DbType::BOX => 603,
            DbType::PATH => 602,
            DbType::POLYGON => 604,
            DbType::CIRCLE => 718,
            DbType::INT4RANGE => 3904,
            DbType::NUMRANGE => 3906,
            DbType::TSRANGE => 3908,
            DbType::TSTZRANGE => 3910,
            DbType::DATERANGE => 3912,
            DbType::INT8RANGE => 3926,
            _ => 25, // fallback TEXT
        }
    }

    pub fn pg_type_size(&self) -> i16 {
        match self {
            DbType::BOOL => 1,
            // Advertised as `int2`, so it declares that type's width.
            DbType::I8 => 2,
            DbType::I16 => 2,
            DbType::I32 => 4,
            DbType::I64 => 8,
            // Advertised as `int8`, so they declare its width.
            DbType::U8 | DbType::U16 | DbType::U32 => 8,
            // `numeric` is a varlena.
            DbType::U64 | DbType::I128 | DbType::U128 => -1,
            DbType::F32 => 4,
            DbType::F64 => 8,
            DbType::STRING => -1,
            DbType::DATETIME => 8,
            DbType::UUID => 16,
            // Postgres models `inet` as a varlena, so its advertised length is
            // variable even though we store it fixed-width.
            DbType::INET => -1,
            DbType::DECIMAL => -1,
            DbType::BYTEA | DbType::ARRAY => -1,
            DbType::INTERVAL => crate::core::pg_types::INTERVAL_LEN as i16,
            DbType::MACADDR => crate::core::pg_types::MACADDR_LEN as i16,
            DbType::MACADDR8 => crate::core::pg_types::MACADDR8_LEN as i16,
            DbType::DATE => crate::core::pg_types::DATE_LEN as i16,
            DbType::TIME => crate::core::pg_types::TIME_LEN as i16,
            DbType::TIMETZ => crate::core::pg_types::TIMETZ_LEN as i16,
            DbType::POINT => crate::core::pg_types::POINT_LEN as i16,
            DbType::LSEG | DbType::BOX => crate::core::pg_types::LSEG_LEN as i16,
            DbType::LINE | DbType::CIRCLE => crate::core::pg_types::LINE_LEN as i16,
            _ => -1,
        }
    }
}
