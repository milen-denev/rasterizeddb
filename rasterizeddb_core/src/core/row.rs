#[derive(Debug)]
pub struct Row {
    pub id: u64,
    pub length: u32,
    pub columns_data: Vec<u8>
}

#[derive(Debug)]
pub struct InsertRow {
    pub columns_data: Vec<u8>
}