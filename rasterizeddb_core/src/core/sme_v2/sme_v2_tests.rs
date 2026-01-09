use std::{
    collections::HashMap,
    future::Future,
    io::SeekFrom,
    sync::Mutex,
};

use rclite::Arc;

use crate::core::storage_providers::traits::{IOResult, StorageIO};

use super::{
    rule_store::CorrelationRuleStore,
    rules::{NumericCorrelationRule, NumericRuleOp, NumericScalar, RowRange},
};

#[derive(Clone)]
struct MemFile {
    fs: Arc<Mutex<HashMap<String, Arc<Mutex<Vec<u8>>>>>>,
    name: String,
    hash: u32,
}

impl MemFile {
    fn new_root(name: &str) -> Self {
        Self {
            fs: Arc::new(Mutex::new(HashMap::new())),
            name: name.to_string(),
            hash: 0xBADC0DEu32,
        }
    }

    fn with_name(&self, name: String) -> Self {
        {
            let mut fs = self.fs.lock().unwrap();
            fs.entry(name.clone())
                .or_insert_with(|| Arc::new(Mutex::new(Vec::new())));
        }
        Self {
            fs: Arc::clone(&self.fs),
            name,
            hash: self.hash,
        }
    }

    fn buf(&self) -> Arc<Mutex<Vec<u8>>> {
        let fs = self.fs.lock().unwrap();
        fs.get(&self.name)
            .cloned()
            .unwrap_or_else(|| Arc::new(Mutex::new(Vec::new())))
    }
}

impl StorageIO for MemFile {
    async fn verify_data(&self, position: u64, buffer: &[u8]) -> bool {
        let buf = self.buf();
        let buf = buf.lock().unwrap();
        let start = position as usize;
        let end = start.saturating_add(buffer.len());
        if end > buf.len() {
            return false;
        }
        &buf[start..end] == buffer
    }

    async fn write_data(&self, position: u64, buffer: &[u8]) {
        let buf = self.buf();
        let mut buf = buf.lock().unwrap();
        let start = position as usize;
        let end = start + buffer.len();
        if buf.len() < end {
            buf.resize(end, 0);
        }
        buf[start..end].copy_from_slice(buffer);
    }

    async fn write_data_seek(&self, seek: SeekFrom, buffer: &[u8]) {
        let start = match seek {
            SeekFrom::Start(s) => s as usize,
            SeekFrom::Current(_) | SeekFrom::End(_) => {
                panic!("MemFile only supports SeekFrom::Start")
            }
        };

        let buf = self.buf();
        let mut buf = buf.lock().unwrap();
        let end = start + buffer.len();
        if buf.len() < end {
            buf.resize(end, 0);
        }
        buf[start..end].copy_from_slice(buffer);
    }

    async fn read_data_into_buffer(&self, position: &mut u64, buffer: &mut [u8]) -> IOResult<()> {
        let buf = self.buf();
        let buf = buf.lock().unwrap();
        let start = *position as usize;
        let end = start.saturating_add(buffer.len());
        if end > buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read out of bounds",
            ));
        }
        buffer.copy_from_slice(&buf[start..end]);
        *position += buffer.len() as u64;
        Ok(())
    }

    async fn read_vectored(&self, reads: &mut [(u64, &mut [u8])]) -> IOResult<()> {
        for (pos, buf) in reads {
            self.read_data_into_buffer(pos, buf).await?;
        }
        Ok(())
    }

    async fn append_data(&self, buffer: &[u8], _immediate: bool) {
        let buf = self.buf();
        let mut buf = buf.lock().unwrap();
        buf.extend_from_slice(buffer);
    }

    async fn get_len(&self) -> u64 {
        let buf = self.buf();
        let buf = buf.lock().unwrap();
        buf.len() as u64
    }

    fn exists(_location: &str, _table_name: &str) -> bool {
        true
    }

    async fn create_temp(&self) -> Self {
        let temp_name = format!("{}.tmp", self.name);
        self.with_name(temp_name)
    }

    async fn swap_temp(&self, temp_io_sync: &mut Self) {
        let buf_a = self.buf();
        let buf_b = temp_io_sync.buf();

        let mut a = buf_a.lock().unwrap();
        let mut b = buf_b.lock().unwrap();
        std::mem::swap(&mut *a, &mut *b);
    }

    fn get_location(&self) -> Option<String> {
        None
    }

    async fn create_new(&self, name: String) -> Self {
        self.with_name(name)
    }

    fn drop_io(&self) {
        // No-op for tests.
    }

    fn get_hash(&self) -> u32 {
        self.hash
    }

    fn start_service(&self) -> impl Future<Output = ()> + Send + Sync {
        async {}
    }

    fn get_name(&self) -> String {
        self.name.clone()
    }
}

#[tokio::test]
async fn writes_header_and_loads_numeric_rules_for_column() {
    let root = Arc::new(MemFile::new_root("root"));
    let io = CorrelationRuleStore::open_rules_io(root, "people").await;

    let col_age: u64 = 0;
    let col_name: u64 = 1;

    let age_rules = vec![
        NumericCorrelationRule {
            column_schema_id: col_age,
            column_type: crate::core::db_type::DbType::I64,
            op: NumericRuleOp::LessThan,
            value: NumericScalar::Signed(25),
            ranges: smallvec::smallvec![
                RowRange::from_row_id_range_inclusive(200, 500),
                RowRange::from_row_id_range_inclusive(800, 1200),
            ],
        },
        NumericCorrelationRule {
            column_schema_id: col_age,
            column_type: crate::core::db_type::DbType::I64,
            op: NumericRuleOp::GreaterThan,
            value: NumericScalar::Signed(20),
            ranges: smallvec::smallvec![
                RowRange::from_row_id_range_inclusive(150, 250),
                RowRange::from_row_id_range_inclusive(2300, 3200),
            ],
        },
    ];

    // "name" column is non-numeric in practice; test still uses a numeric rule format.
    let name_rules = vec![NumericCorrelationRule {
        column_schema_id: col_name,
        column_type: crate::core::db_type::DbType::U64,
        op: NumericRuleOp::LessThan,
        value: NumericScalar::Unsigned(123),
        ranges: smallvec::smallvec![RowRange::from_row_id_range_inclusive(1, 2)],
    }];

    CorrelationRuleStore::save_numeric_rules_atomic::<_>(
        io.clone(),
        &[(col_age, age_rules.clone()), (col_name, name_rules.clone())],
    )
    .await
    .unwrap();

    let header = CorrelationRuleStore::try_load_header_v2(io.clone())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(header.numeric_columns.len(), 2);
    assert_eq!(header.string_columns.len(), 0);

    let loaded_age = CorrelationRuleStore::load_numeric_rules_for_column::<_>(
        io.clone(),
        &header,
        col_age,
    )
    .await
    .unwrap();
    assert_eq!(loaded_age, age_rules);

    let loaded_name = CorrelationRuleStore::load_numeric_rules_for_column::<_>(io, &header, col_name)
        .await
        .unwrap();
    assert_eq!(loaded_name, name_rules);
}
