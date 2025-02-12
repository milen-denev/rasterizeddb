use std::fmt::Display;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
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
    CHAR,
    STRING,
    DATETIME,
    TBD,
    NULL,
    START,
    END
}

impl Display for DbType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:?}", self))
    }
}

impl DbType {
    pub fn to_byte(&self) -> u8{
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
            DbType::TBD => 252,
            DbType::NULL => 253,
            DbType::START => 254,
            DbType::END => 255
        }
    }

    #[track_caller]
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
            253 => DbType::NULL,
            254 => DbType::START,
            255 => DbType::END,
            _ => panic!("Invalid byte type: {}", byte)
        }
    }

    #[track_caller]
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
            DbType::CHAR => 4,
            DbType::STRING => panic!("STRING type doesn't have a set size of bytes."),
            DbType::TBD => panic!("TBD (To Be Determined) is a temporary type."),
            DbType::NULL => 1,
            DbType::START => 1,
            DbType::END => 1,
            DbType::DATETIME => todo!()
        }
    }
}