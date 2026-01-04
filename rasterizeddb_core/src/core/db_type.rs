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
            DbType::UNKNOWN => 252,
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
            DbType::UNKNOWN => 0,
            DbType::NULL => 1,
            DbType::START => 1,
            DbType::END => 1,
            DbType::DATETIME => 8,
        }
    }

    pub fn pg_oid(&self) -> u32 {
        match self {
            DbType::BOOL => 16,
            DbType::I8 | DbType::I16 | DbType::I32 => 23, // int4
            DbType::I64 => 20, // int8
            DbType::U8 | DbType::U16 | DbType::U32 | DbType::U64 => 20, // best-effort
            DbType::F32 => 700,
            DbType::F64 => 701,
            DbType::STRING => 25, // TEXT
            DbType::DATETIME => 1114, // timestamp
            _ => 25, // fallback TEXT
        }
    }

    pub fn pg_type_size(&self) -> i16 {
        match self {
            DbType::BOOL => 1,
            DbType::I8 => 1,
            DbType::I16 => 2,
            DbType::I32 => 4,
            DbType::I64 => 8,
            DbType::U8 => 1,
            DbType::U16 => 2,
            DbType::U32 => 4,
            DbType::U64 => 8,
            DbType::F32 => 4,
            DbType::F64 => 8,
            DbType::STRING => -1,
            DbType::DATETIME => 8,
            _ => -1,
        }
    }
}
