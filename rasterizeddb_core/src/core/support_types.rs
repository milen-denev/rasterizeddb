use smallvec::SmallVec;

use crate::core::row::row::Row;

pub enum ReturnResult {
    Rows(SmallVec<[Row; 32]>),
    HtmlView(String),
}

#[derive(Debug)]
#[repr(u8)]
pub enum QueryExecutionResult {
    Ok = 0,
    RowsAffected(u64) = 1,
    RowsResult(Box<Vec<u8>>) = 2,
    Error(String) = 3,
}
