use std::{
    collections::HashMap,
    future::Future,
    io::SeekFrom,
    path::Path,
    sync::{Arc, Mutex},
};

use smallvec::{smallvec, SmallVec};

use crate::core::{
    db_type::DbType,
    row::{row_pointer::RowPointer, schema::SchemaField},
    sme::{
        rule_store::SemanticRuleStore,
        rules::{f64_bits_to_16le, u128_to_16le, SemanticRule, SemanticRuleOp},
        semantic_mapping_engine::SemanticMappingEngine,
    },
    storage_providers::file_sync::LocalStorageProvider,
    storage_providers::traits::{IOResult, StorageIO},
    tokenizer::query_tokenizer::tokenize,
};

fn init_test_globals() {
    // RowPointerIterator depends on this in unit tests.
    crate::BATCH_SIZE.get_or_init(|| 128);
}

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
            hash: 0xC0FFEEu32,
        }
    }

    fn with_name(&self, name: String) -> Self {
        // Ensure file exists.
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

    async fn read_data_into_buffer(
        &self,
        position: &mut u64,
        buffer: &mut [u8],
    ) -> IOResult<()> {
        let buf = self.buf();
        let buf = buf.lock().unwrap();
        let start = *position as usize;
        let end = start.saturating_add(buffer.len());
        if end > buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "read out of bounds: {}..{} (len={})",
                    start,
                    end,
                    buf.len()
                ),
            ));
        }
        buffer.copy_from_slice(&buf[start..end]);
        *position += buffer.len() as u64;
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

fn bitset256_from_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    for &b in bytes {
        let idx = b as usize;
        out[idx >> 3] |= 1u8 << (idx & 7);
    }
    out
}

fn bitset256_to_lower_upper(bitset: [u8; 32]) -> ([u8; 16], [u8; 16]) {
    let mut lo = [0u8; 16];
    let mut hi = [0u8; 16];
    lo.copy_from_slice(&bitset[0..16]);
    hi.copy_from_slice(&bitset[16..32]);
    (lo, hi)
}

fn metric_u64_to_lower_upper(lo: u64, hi: u64) -> ([u8; 16], [u8; 16]) {
    let mut lower = [0u8; 16];
    let mut upper = [0u8; 16];
    lower[0..8].copy_from_slice(&lo.to_le_bytes());
    upper[0..8].copy_from_slice(&hi.to_le_bytes());
    (lower, upper)
}

fn string_metrics_from_bytes(bytes: &[u8]) -> (u64, u64) {
    if bytes.is_empty() {
        return (0, 0);
    }

    // max run length
    let mut run_max: u64 = 1;
    let mut run: u64 = 1;
    for i in 1..bytes.len() {
        if bytes[i] == bytes[i - 1] {
            run += 1;
        } else {
            run_max = run_max.max(run);
            run = 1;
        }
    }
    run_max = run_max.max(run);

    // max count of any single byte
    let mut freq = [0u16; 256];
    for &b in bytes {
        freq[b as usize] += 1;
    }
    let max_count = freq.into_iter().map(|v| v as u64).max().unwrap_or(0);

    (run_max, max_count)
}

fn rules_for_string_row(col_schema_id: u64, row_id: u64, s: &str, rule_id_base: u64) -> Vec<SemanticRule> {
    let bytes = s.as_bytes();
    let len = bytes.len() as u64;

    let first_set = if let Some(&b) = bytes.first() {
        let mut bs = [0u8; 32];
        bs[(b as usize) >> 3] |= 1u8 << ((b as usize) & 7);
        bs
    } else {
        [0u8; 32]
    };
    let last_set = if let Some(&b) = bytes.last() {
        let mut bs = [0u8; 32];
        bs[(b as usize) >> 3] |= 1u8 << ((b as usize) & 7);
        bs
    } else {
        [0u8; 32]
    };

    let charset = bitset256_from_bytes(bytes);
    let (max_run, max_count) = string_metrics_from_bytes(bytes);

    let (f_lo, f_hi) = bitset256_to_lower_upper(first_set);
    let (l_lo, l_hi) = bitset256_to_lower_upper(last_set);
    let (c_lo, c_hi) = bitset256_to_lower_upper(charset);
    let (len_lo, len_hi) = metric_u64_to_lower_upper(len, len);
    let (run_lo, run_hi) = metric_u64_to_lower_upper(0, max_run);
    let (cnt_lo, cnt_hi) = metric_u64_to_lower_upper(0, max_count);

    vec![
        SemanticRule {
            rule_id: rule_id_base + 0,
            column_schema_id: col_schema_id,
            column_type: DbType::STRING,
            op: SemanticRuleOp::StrFirstByteSet,
            reserved: [0u8; 6],
            lower: f_lo,
            upper: f_hi,
            start_row_id: row_id,
            end_row_id: row_id,
        },
        SemanticRule {
            rule_id: rule_id_base + 1,
            column_schema_id: col_schema_id,
            column_type: DbType::STRING,
            op: SemanticRuleOp::StrLastByteSet,
            reserved: [0u8; 6],
            lower: l_lo,
            upper: l_hi,
            start_row_id: row_id,
            end_row_id: row_id,
        },
        SemanticRule {
            rule_id: rule_id_base + 2,
            column_schema_id: col_schema_id,
            column_type: DbType::STRING,
            op: SemanticRuleOp::StrCharSet,
            reserved: [0u8; 6],
            lower: c_lo,
            upper: c_hi,
            start_row_id: row_id,
            end_row_id: row_id,
        },
        SemanticRule {
            rule_id: rule_id_base + 3,
            column_schema_id: col_schema_id,
            column_type: DbType::STRING,
            op: SemanticRuleOp::StrLenRange,
            reserved: [0u8; 6],
            lower: len_lo,
            upper: len_hi,
            start_row_id: row_id,
            end_row_id: row_id,
        },
        SemanticRule {
            rule_id: rule_id_base + 4,
            column_schema_id: col_schema_id,
            column_type: DbType::STRING,
            op: SemanticRuleOp::StrMaxRunLen,
            reserved: [0u8; 6],
            lower: run_lo,
            upper: run_hi,
            start_row_id: row_id,
            end_row_id: row_id,
        },
        SemanticRule {
            rule_id: rule_id_base + 5,
            column_schema_id: col_schema_id,
            column_type: DbType::STRING,
            op: SemanticRuleOp::StrMaxCharCount,
            reserved: [0u8; 6],
            lower: cnt_lo,
            upper: cnt_hi,
            start_row_id: row_id,
            end_row_id: row_id,
        },
    ]
}

fn pointer_for_id(id: u64) -> RowPointer {
    #[cfg(not(feature = "enable_long_row"))]
    {
        crate::core::row::row_pointer::RowPointer {
            id,
            length: 0,
            position: 0,
            deleted: false,
            writing_data: crate::core::row::row_pointer::WritingData {
                total_columns_size: 0,
                total_strings_size: 0,
            },
        }
    }

    #[cfg(feature = "enable_long_row")]
    {
        crate::core::row::row_pointer::RowPointer {
            id: id as u128,
            length: 0,
            position: 0,
            deleted: false,
            checksum: 0,
            cluster: 0,
            deleted_at: 0,
            created_at: 0,
            updated_at: 0,
            version: 0,
            is_active: true,
            writing_data: crate::core::row::row_pointer::WritingData {
                total_columns_size: 0,
                total_strings_size: 0,
            },
        }
    }
}

async fn run_sme_query_with_tokens(
    table: &str,
    schema: &[SchemaField],
    tokens: &SmallVec<[crate::core::tokenizer::query_tokenizer::Token; 36]>,
    rules: &[SemanticRule],
    meta: Option<crate::core::sme::rule_store::ScanMeta>,
    pointer_ids_in_file_order: &[u64],
) -> Option<Vec<u64>> {
    init_test_globals();

    let root = MemFile::new_root("root");

    let rules_io = SemanticRuleStore::open_rules_io(Arc::new(root.clone()), table).await;
    SemanticRuleStore::save_rules_atomic_with_meta(rules_io.clone(), meta, rules)
        .await
        .expect("save_rules_atomic_with_meta failed");

    // Sanity-check: header must be readable.
    let header = crate::core::sme::rule_store::SemanticRuleStore::try_load_header_v2(rules_io)
        .await
        .expect("try_load_header_v2 errored")
        .expect("expected rules header (file empty?)");
    assert!(!header.ranges.is_empty() || rules.is_empty());

    // Sanity-check: a freshly opened handle (like the SME uses) must see the bytes.
    let reopened = root
        .create_new(format!("{}_rules.db", table))
        .await;
    let reopened_len = reopened.get_len().await;
    assert!(
        reopened_len > 0 || rules.is_empty(),
        "re-opened rules file has len=0; table={table}"
    );

    let pointers_io = Arc::new(root.create_new("pointers.db".to_string()).await);
    for id in pointer_ids_in_file_order.iter().copied() {
        let p = pointer_for_id(id);
        let block = p.into_memory_block();
        pointers_io.append_data(block.into_slice(), true).await;
    }

    let sme = SemanticMappingEngine::new();
    let candidates = sme
        .get_or_build_candidates_for_table_tokens(
            table,
            tokens,
            schema,
            Arc::new(root.clone()),
            pointers_io,
        )
        .await?;

    Some(
        candidates
            .iter()
            .map(|p| {
                #[cfg(not(feature = "enable_long_row"))]
                {
                    p.id
                }

                #[cfg(feature = "enable_long_row")]
                {
                    p.id as u64
                }
            })
            .collect(),
    )
}

async fn run_sme_query(
    table: &str,
    schema: &SmallVec<[SchemaField; 20]>,
    query: &str,
    rules: &[SemanticRule],
    meta: Option<crate::core::sme::rule_store::ScanMeta>,
    pointer_ids_in_file_order: &[u64],
) -> Option<Vec<u64>> {
    let tokens = tokenize(query, schema);
    run_sme_query_with_tokens(table, schema.as_slice(), &tokens, rules, meta, pointer_ids_in_file_order).await
}

#[tokio::test]
async fn sme_regression_mixed_and_or_query() {
    init_test_globals();
    // Schema (write_order must match tokenizer + SME mapping).
    let schema: SmallVec<[SchemaField; 20]> = smallvec![
        SchemaField::new("id".to_string(), DbType::U64, 8, 0, 0, true),
        SchemaField::new("salary".to_string(), DbType::F64, 8, 0, 1, false),
        SchemaField::new("age".to_string(), DbType::U32, 4, 0, 2, false),
        SchemaField::new("name".to_string(), DbType::STRING, 0, 0, 3, false),
    ];

    // Synthetic dataset: (id, salary, age, name)
    let rows = vec![
        (1039u64, 250000.13f64, 30u32, "Alice"),
        (1041u64, 200000.13f64, 25u32, "Bob"),
        (1042u64, 199999.00f64, 25u32, "Bob"),
        (1049u64, 300000.00f64, 28u32, "Carl"),
        (1050u64, 300000.00f64, 28u32, "Carl"),
        (90001u64, 1.0f64, 29u32, "John"),
        (90002u64, 1.0f64, 29u32, "Bob"),
    ];

    let expected: Vec<u64> = vec![1041, 1049, 90001];

    // Build semantic rules (per-row tight windows).
    let mut rules: Vec<SemanticRule> = Vec::new();
    let mut rid: u64 = 1;

    for (id, salary, age, name) in rows.iter().copied() {
        // id column (schema idx 0)
        rules.push(SemanticRule {
            rule_id: rid,
            column_schema_id: 0,
            column_type: DbType::U64,
            op: SemanticRuleOp::BetweenInclusive,
            reserved: [0u8; 6],
            lower: u128_to_16le(id as u128),
            upper: u128_to_16le(id as u128),
            start_row_id: id,
            end_row_id: id,
        });
        rid += 1;

        // salary column (schema idx 1)
        let bits = salary.to_bits();
        rules.push(SemanticRule {
            rule_id: rid,
            column_schema_id: 1,
            column_type: DbType::F64,
            op: SemanticRuleOp::BetweenInclusive,
            reserved: [0u8; 6],
            lower: f64_bits_to_16le(bits),
            upper: f64_bits_to_16le(bits),
            start_row_id: id,
            end_row_id: id,
        });
        rid += 1;

        // age column (schema idx 2)
        rules.push(SemanticRule {
            rule_id: rid,
            column_schema_id: 2,
            column_type: DbType::U32,
            op: SemanticRuleOp::BetweenInclusive,
            reserved: [0u8; 6],
            lower: u128_to_16le(age as u128),
            upper: u128_to_16le(age as u128),
            start_row_id: id,
            end_row_id: id,
        });
        rid += 1;

        // name column (schema idx 3)
        let mut sr = rules_for_string_row(3, id, name, rid);
        rid += sr.len() as u64;
        rules.append(&mut sr);
    }

    let root = MemFile::new_root("root");
    let table = "people";

    // Persist rules using the same APIs production uses.
    let rules_io = SemanticRuleStore::open_rules_io(Arc::new(root.clone()), table).await;
    SemanticRuleStore::save_rules_atomic_with_meta(rules_io, None, &rules)
        .await
        .expect("save rules");

    // Persist pointers (intentionally out-of-order to validate the ordering fix).
    let pointers_io = Arc::new(root.create_new("pointers.db".to_string()).await);
    let order = vec![90001u64, 1049, 1041, 1039, 90002, 1050, 1042];
    for id in order {
        let p = pointer_for_id(id);
        let block = p.into_memory_block();
        pointers_io.append_data(block.into_slice(), true).await;
    }

    let q = "id > 1040 AND id < 1050 AND salary >= 200000.13 OR age = 29 AND id > 90000 AND name STARTSWITH 'J'";
    let tokens = tokenize(q, &schema);

    let sme = SemanticMappingEngine::new();
    let candidates = sme
        .get_or_build_candidates_for_table_tokens(
            table,
            &tokens,
            schema.as_slice(),
            Arc::new(root.clone()),
            pointers_io,
        )
        .await
        .expect("expected SME candidates");

    let got: Vec<u64> = candidates
        .iter()
        .map(|p| {
            #[cfg(not(feature = "enable_long_row"))]
            {
                p.id
            }

            #[cfg(feature = "enable_long_row")]
            {
                p.id as u64
            }
        })
        .collect();

    // This is the critical invariant: SME must not produce false negatives.
    for id in expected.iter().copied() {
        assert!(got.contains(&id), "missing expected id {id}; got={got:?}");
    }

    // With per-row (tight) rules, we should also avoid obvious false positives.
    // (If this assertion ever becomes flaky due to intentional SME conservatism changes,
    //  keeping only the superset assertion above is still valuable.)
    assert_eq!(
        {
            let mut x = got.clone();
            x.sort_unstable();
            x
        },
        {
            let mut x = expected.clone();
            x.sort_unstable();
            x
        }
    );
}

#[tokio::test]
async fn sme_parentheses_are_unsupported_returns_none() {
    init_test_globals();
    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "id".to_string(),
        DbType::U64,
        8,
        0,
        0,
        true,
    )];

    let root = MemFile::new_root("root");
    let pointers_io = Arc::new(root.create_new("pointers.db".to_string()).await);

    let q = "(id > 1)";
    let tokens = tokenize(q, &schema);

    let sme = SemanticMappingEngine::new();
    let got = sme
        .get_or_build_candidates_for_table_tokens(
            "t",
            &tokens,
            schema.as_slice(),
            Arc::new(root.clone()),
            pointers_io,
        )
        .await;

    assert!(got.is_none());
}

#[tokio::test]
async fn sme_lossy_float_to_int_returns_none() {
    init_test_globals();
    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "age".to_string(),
        DbType::U64,
        8,
        0,
        0,
        false,
    )];

    let root = MemFile::new_root("root");
    let pointers_io = Arc::new(root.create_new("pointers.db".to_string()).await);

    // Fractional float literal; integer column should force SME fallback.
    let q = "age = 12.5";
    let tokens = tokenize(q, &schema);

    let sme = SemanticMappingEngine::new();
    let got = sme
        .get_or_build_candidates_for_table_tokens(
            "t",
            &tokens,
            schema.as_slice(),
            Arc::new(root.clone()),
            pointers_io,
        )
        .await;

    assert!(got.is_none());
}

#[tokio::test]
async fn sme_numeric_comparisons_and_flipped_u64() {
    use crate::core::tokenizer::query_tokenizer::Token;

    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "id".to_string(),
        DbType::U64,
        8,
        0,
        0,
        true,
    )];

    // Sanity-check tokenizer shape for the simplest case.
    let t = tokenize("id = 3", &schema);
    assert!(
        matches!(t.as_slice(), [Token::Ident(_), Token::Op(op), Token::Number(_)] if op == "=" || op == "=="),
        "unexpected tokens for 'id = 3': {t:?}"
    );

    // Tight per-row rules for ids 1..=5
    let mut rules = Vec::new();
    let mut rid = 1u64;
    for id in 1u64..=5 {
        rules.push(SemanticRule {
            rule_id: rid,
            column_schema_id: 0,
            column_type: DbType::U64,
            op: SemanticRuleOp::BetweenInclusive,
            reserved: [0u8; 6],
            lower: u128_to_16le(id as u128),
            upper: u128_to_16le(id as u128),
            start_row_id: id,
            end_row_id: id,
        });
        rid += 1;
    }

    // Random pointer file order to ensure we don't rely on monotonic ids.
    let ptrs = [3, 1, 5, 2, 4];

    let table = "t";

    let cases: Vec<(&str, Vec<u64>)> = vec![
        ("id = 3", vec![3]),
        ("id > 3", vec![4, 5]),
        ("id >= 3", vec![3, 4, 5]),
        ("id < 3", vec![1, 2]),
        ("id <= 3", vec![1, 2, 3]),
        ("id != 3", vec![1, 2, 4, 5]),
        ("id <> 3", vec![1, 2, 4, 5]),
        // Flipped comparisons: Number Op Ident
        ("3 < id", vec![4, 5]),
        ("3 <= id", vec![3, 4, 5]),
        ("3 > id", vec![1, 2]),
        ("3 >= id", vec![1, 2, 3]),
        ("3 = id", vec![3]),
        ("3 != id", vec![1, 2, 4, 5]),
    ];

    for (q, mut expected) in cases {
        let mut got = run_sme_query(table, &schema, q, &rules, None, &ptrs)
            .await
            .unwrap_or_else(|| panic!("SME returned None for supported numeric query: {q}"));
        got.sort_unstable();
        expected.sort_unstable();
        assert_eq!(got, expected, "query={q}");
    }
}

#[tokio::test]
async fn sme_or_and_precedence_matches_transformer_semantics() {
    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "id".to_string(),
        DbType::U64,
        8,
        0,
        0,
        true,
    )];

    let mut rules = Vec::new();
    for (rid, id) in (1u64..=3).zip(1u64..=3) {
        rules.push(SemanticRule {
            rule_id: rid,
            column_schema_id: 0,
            column_type: DbType::U64,
            op: SemanticRuleOp::BetweenInclusive,
            reserved: [0u8; 6],
            lower: u128_to_16le(id as u128),
            upper: u128_to_16le(id as u128),
            start_row_id: id,
            end_row_id: id,
        });
    }

    let ptrs = [1, 2, 3];

    // OR splits AND-groups: (id=1) OR (id=2 AND id=3)
    // The second group is impossible, so result must be {1}.
    let mut got = run_sme_query("t", &schema, "id = 1 OR id = 2 AND id = 3", &rules, None, &ptrs)
        .await
        .unwrap();
    got.sort_unstable();
    assert_eq!(got, vec![1]);

    // (id=2) OR (id=3 AND id=3) => {2,3}
    let mut got = run_sme_query("t", &schema, "id = 2 OR id = 3 AND id = 3", &rules, None, &ptrs)
        .await
        .unwrap();
    got.sort_unstable();
    assert_eq!(got, vec![2, 3]);
}

#[tokio::test]
async fn sme_string_ops_via_tokenizer_ident_form() {
    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "name".to_string(),
        DbType::STRING,
        0,
        0,
        0,
        false,
    )];

    // Rows: 1=John, 2=Alice, 3=Jo
    let mut rules = Vec::new();
    let mut rid = 1u64;
    for (row_id, name) in [(1u64, "John"), (2u64, "Alice"), (3u64, "Jo")] {
        let mut sr = rules_for_string_row(0, row_id, name, rid);
        rid += sr.len() as u64;
        rules.append(&mut sr);
    }

    let ptrs = [2, 1, 3];

    let mut got = run_sme_query("t", &schema, "name STARTSWITH 'Jo'", &rules, None, &ptrs)
        .await
        .unwrap();
    got.sort_unstable();
    assert_eq!(got, vec![1, 3]);

    let mut got = run_sme_query("t", &schema, "name ENDSWITH 'n'", &rules, None, &ptrs)
        .await
        .unwrap();
    got.sort_unstable();
    assert_eq!(got, vec![1]);

    let mut got = run_sme_query("t", &schema, "name CONTAINS 'li'", &rules, None, &ptrs)
        .await
        .unwrap();
    got.sort_unstable();
    assert_eq!(got, vec![2]);
}

#[tokio::test]
async fn sme_string_ops_via_op_token_form() {
    use crate::core::tokenizer::query_tokenizer::Token;

    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "name".to_string(),
        DbType::STRING,
        0,
        0,
        0,
        false,
    )];

    let mut rules = Vec::new();
    let mut rid = 1u64;
    for (row_id, name) in [(1u64, "John"), (2u64, "Alice"), (3u64, "Jo")] {
        let mut sr = rules_for_string_row(0, row_id, name, rid);
        rid += sr.len() as u64;
        rules.append(&mut sr);
    }

    let ptrs = [3, 2, 1];

    // name STARTSWITH 'Jo' expressed as Ident Op StringLit
    let tokens: SmallVec<[Token; 36]> = smallvec![
        Token::Ident(("name".to_string(), DbType::STRING, 0)),
        Token::Op("STARTSWITH".to_string()),
        Token::StringLit("Jo".to_string()),
    ];

    let mut got = run_sme_query_with_tokens("t", schema.as_slice(), &tokens, &rules, None, &ptrs)
        .await
        .unwrap();
    got.sort_unstable();
    assert_eq!(got, vec![1, 3]);
}

#[tokio::test]
async fn sme_string_not_equals_is_conservative_match_all() {
    use crate::core::tokenizer::query_tokenizer::Token;

    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "name".to_string(),
        DbType::STRING,
        0,
        0,
        0,
        false,
    )];

    let t = tokenize("name != 'John'", &schema);
    assert!(
        matches!(t.as_slice(), [Token::Ident(_), Token::Op(op), Token::StringLit(_)] if op == "!="),
        "unexpected tokens for string != query: {t:?}"
    );

    let mut rules = Vec::new();
    let mut rid = 1u64;
    for (row_id, name) in [(1u64, "John"), (2u64, "Alice"), (3u64, "Jo")] {
        let mut sr = rules_for_string_row(0, row_id, name, rid);
        rid += sr.len() as u64;
        rules.append(&mut sr);
    }

    let ptrs = [1, 2, 3];

    let mut got = run_sme_query("t", &schema, "name != 'John'", &rules, None, &ptrs)
        .await
        .unwrap_or_else(|| panic!("SME returned None for supported string query: name != 'John'"));
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3]);

    let mut got = run_sme_query("t", &schema, "name <> 'John'", &rules, None, &ptrs)
        .await
        .unwrap_or_else(|| panic!("SME returned None for supported string query: name <> 'John'"));
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3]);
}

#[tokio::test]
async fn sme_missing_rules_for_referenced_column_returns_none() {
    let schema: SmallVec<[SchemaField; 20]> = smallvec![
        SchemaField::new("id".to_string(), DbType::U64, 8, 0, 0, true),
        SchemaField::new("age".to_string(), DbType::U32, 4, 0, 1, false),
    ];

    // Only id rules exist.
    let rules = vec![SemanticRule {
        rule_id: 1,
        column_schema_id: 0,
        column_type: DbType::U64,
        op: SemanticRuleOp::BetweenInclusive,
        reserved: [0u8; 6],
        lower: u128_to_16le(1),
        upper: u128_to_16le(1),
        start_row_id: 1,
        end_row_id: 1,
    }];

    let ptrs = [1u64];

    let got = run_sme_query("t", &schema, "age = 29", &rules, None, &ptrs).await;
    assert!(got.is_none());
}

#[tokio::test]
async fn sme_string_column_without_string_rules_returns_none() {
    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "name".to_string(),
        DbType::STRING,
        0,
        0,
        0,
        false,
    )];

    // A bogus "numeric-style" rule for a string column: should not be considered usable.
    let rules = vec![SemanticRule {
        rule_id: 1,
        column_schema_id: 0,
        column_type: DbType::STRING,
        op: SemanticRuleOp::BetweenInclusive,
        reserved: [0u8; 6],
        lower: [0u8; 16],
        upper: [0u8; 16],
        start_row_id: 1,
        end_row_id: 1,
    }];

    let ptrs = [1u64];
    let got = run_sme_query("t", &schema, "name = 'A'", &rules, None, &ptrs).await;
    assert!(got.is_none());
}

#[tokio::test]
async fn sme_meta_appended_rows_included_when_no_rule_matches() {
    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "id".to_string(),
        DbType::U64,
        8,
        0,
        0,
        true,
    )];

    // Rules only cover ids 1..=3.
    let mut rules = Vec::new();
    for (rid, id) in (1u64..=3).zip(1u64..=3) {
        rules.push(SemanticRule {
            rule_id: rid,
            column_schema_id: 0,
            column_type: DbType::U64,
            op: SemanticRuleOp::BetweenInclusive,
            reserved: [0u8; 6],
            lower: u128_to_16le(id as u128),
            upper: u128_to_16le(id as u128),
            start_row_id: id,
            end_row_id: id,
        });
    }

    // Pointer file contains rows up to id=5.
    let ptrs = [5, 1, 4, 2, 3];

    let meta = Some(crate::core::sme::rule_store::ScanMeta {
        pointers_len_bytes: 0,
        last_row_id: 3,
    });

    // Query matches nothing in existing rules, but rows appended after last_row_id should still be included.
    let mut got = run_sme_query("t", &schema, "id = 999", &rules, meta, &ptrs)
        .await
        .unwrap();
    got.sort_unstable();
    assert_eq!(got, vec![4, 5]);
}

#[tokio::test]
async fn sme_unsupported_operator_returns_none_manual_tokens() {
    use crate::core::tokenizer::query_tokenizer::{NumericValue, Token};

    let schema: SmallVec<[SchemaField; 20]> = smallvec![SchemaField::new(
        "id".to_string(),
        DbType::U64,
        8,
        0,
        0,
        true,
    )];

    let rules = vec![SemanticRule {
        rule_id: 1,
        column_schema_id: 0,
        column_type: DbType::U64,
        op: SemanticRuleOp::BetweenInclusive,
        reserved: [0u8; 6],
        lower: u128_to_16le(1),
        upper: u128_to_16le(1),
        start_row_id: 1,
        end_row_id: 1,
    }];

    let ptrs = [1u64];

    let tokens: SmallVec<[Token; 36]> = smallvec![
        Token::Ident(("id".to_string(), DbType::U64, 0)),
        Token::Op("+".to_string()),
        Token::Number(NumericValue::U64(1)),
    ];

    let got = run_sme_query_with_tokens("t", schema.as_slice(), &tokens, &rules, None, &ptrs).await;
    assert!(got.is_none());
}

#[tokio::test]
async fn sme_query_returns_all_25_rows_user_dataset() {
    init_test_globals();

    // Schema (write_order must match tokenizer + SME mapping).
    let schema: SmallVec<[SchemaField; 20]> = smallvec![
        SchemaField::new("id".to_string(), DbType::U64, 8, 0, 0, true),
        SchemaField::new("salary".to_string(), DbType::F64, 8, 0, 1, false),
        SchemaField::new("age".to_string(), DbType::U32, 4, 0, 2, false),
        SchemaField::new("name".to_string(), DbType::STRING, 0, 0, 3, false),
    ];

    // Provided dataset: (id, salary, age, name)
    let rows: Vec<(u64, f64, u32, &str)> = vec![
        (1044, 209668.22, 75, "Kelly Williams"),
        (90185, 45998.07, 29, "Joshua Carter"),
        (90436, 49454.043, 29, "Joe Moore"),
        (90511, 31897.578, 29, "Judith Carroll"),
        (91119, 133764.1, 29, "Jeff Cook"),
        (91673, 21267.32, 29, "John Price"),
        (91694, 46089.152, 29, "Julia Stevens"),
        (91762, 64261.234, 29, "Janet Diaz"),
        (92741, 167181.6, 29, "Jason Cox"),
        (92759, 237961.44, 29, "James Bell"),
        (93405, 194462.84, 29, "Joseph Gordon"),
        (93667, 119983.414, 29, "James Snyder"),
        (94250, 133077.11, 29, "John Nguyen"),
        (94376, 228660.0, 29, "Jason Gray"),
        (94512, 56581.414, 29, "John Morris"),
        (94688, 114960.44, 29, "Jessica Thomas"),
        (94709, 79877.64, 29, "Janet Moore"),
        (95220, 109789.55, 29, "Jeff White"),
        (95708, 94938.56, 29, "Jessica Fields"),
        (96106, 98871.016, 29, "Jonathan Carroll"),
        (96353, 99434.89, 29, "Janet West"),
        (97087, 65428.18, 29, "Joseph Diaz"),
        (97134, 155217.36, 29, "Jessica Nelson"),
        (98534, 115571.92, 29, "John Chavez"),
        (99304, 217673.4, 29, "Jennifer Anderson"),
    ];

    let expected: Vec<u64> = rows.iter().map(|(id, _, _, _)| *id).collect();

    // Build per-row (tight) semantic rules.
    let mut rules: Vec<SemanticRule> = Vec::new();
    let mut rid: u64 = 1;

    for (id, salary, age, name) in rows.iter().copied() {
        rules.push(SemanticRule {
            rule_id: rid,
            column_schema_id: 0,
            column_type: DbType::U64,
            op: SemanticRuleOp::BetweenInclusive,
            reserved: [0u8; 6],
            lower: u128_to_16le(id as u128),
            upper: u128_to_16le(id as u128),
            start_row_id: id,
            end_row_id: id,
        });
        rid += 1;

        let bits = salary.to_bits();
        rules.push(SemanticRule {
            rule_id: rid,
            column_schema_id: 1,
            column_type: DbType::F64,
            op: SemanticRuleOp::BetweenInclusive,
            reserved: [0u8; 6],
            lower: f64_bits_to_16le(bits),
            upper: f64_bits_to_16le(bits),
            start_row_id: id,
            end_row_id: id,
        });
        rid += 1;

        rules.push(SemanticRule {
            rule_id: rid,
            column_schema_id: 2,
            column_type: DbType::U32,
            op: SemanticRuleOp::BetweenInclusive,
            reserved: [0u8; 6],
            lower: u128_to_16le(age as u128),
            upper: u128_to_16le(age as u128),
            start_row_id: id,
            end_row_id: id,
        });
        rid += 1;

        let mut sr = rules_for_string_row(3, id, name, rid);
        rid += sr.len() as u64;
        rules.append(&mut sr);
    }

    // Random-ish pointer file order.
    let ptrs: Vec<u64> = vec![
        99304, 90185, 94688, 1044, 97134, 94250, 96106, 97087, 98534, 95708, 93405, 91762, 90511,
        91673, 91694, 92759, 94709, 95220, 90436, 96353, 91119, 94376, 94512, 92741, 93667,
    ];

    let q = "id > 1040 AND id < 1050 AND salary >= 200000.13 OR age = 29 AND id > 90000 AND name STARTSWITH 'J'";

    let mut got = run_sme_query("employees", &schema, q, &rules, None, &ptrs)
        .await
        .unwrap_or_else(|| panic!("SME returned None for supported mixed query: {q}"));
    got.sort_unstable();

    let mut expected_sorted = expected;
    expected_sorted.sort_unstable();

    assert_eq!(got, expected_sorted);
}

// Temporary: loads real rules generated by the production rules pipeline.
// This test is ignored by default because it depends on a local file path.
#[cfg(windows)]
#[tokio::test]
#[ignore]
async fn sme_temp_load_production_employees_rules_and_validate_25_ids() {
    init_test_globals();

    let rules_path = r"G:\Databases\Production\employees_rules.db";
    if !Path::new(rules_path).exists() {
        panic!("Missing rules file at {rules_path}. Update the path or provide the file to run this ignored test.");
    }

    // Open the production rules file.
    let rules_io = Arc::new(LocalStorageProvider::new(
        r"G:\Databases\Production",
        Some("employees_rules.db"),
    )
    .await);

    let (meta, rules) = SemanticRuleStore::load_meta_and_rules(rules_io)
        .await
        .expect("failed to load meta+rules from production rules file");
    assert!(!rules.is_empty(), "production rules file loaded 0 rules");

    // IMPORTANT: schema indices (write_order) must match how the production rules were generated.
    // This matches the employees schema used in test_client's commented CREATE TABLE:
    // 0=id, 1=name, 2=job_title, 3=salary, 4=department, 5=age
    let schema: SmallVec<[SchemaField; 20]> = smallvec![
        SchemaField::new("id".to_string(), DbType::U128, 16, 0, 0, true),
        SchemaField::new("name".to_string(), DbType::STRING, 0, 0, 1, false),
        SchemaField::new("job_title".to_string(), DbType::STRING, 0, 0, 2, false),
        SchemaField::new("salary".to_string(), DbType::F32, 4, 0, 3, false),
        SchemaField::new("department".to_string(), DbType::STRING, 0, 0, 4, false),
        SchemaField::new("age".to_string(), DbType::I32, 4, 0, 5, false),
    ];

    // Pointer ids from the 25-row dataset (file order doesn't matter).
    let ptrs: Vec<u64> = vec![
        1044, 90185, 90436, 90511, 91119, 91673, 91694, 91762, 92741, 92759, 93405, 93667, 94250,
        94376, 94512, 94688, 94709, 95220, 95708, 96106, 96353, 97087, 97134, 98534, 99304,
    ];

    let q = "id > 1040 AND id < 1050 AND salary >= 200000.13 OR age = 29 AND id > 90000 AND name STARTSWITH 'J'";

    let mut got = run_sme_query("employees", &schema, q, &rules, meta, &ptrs)
        .await
        .unwrap_or_else(|| panic!("SME returned None for query using production rules: {q}"));
    got.sort_unstable();

    let mut expected = ptrs.clone();
    expected.sort_unstable();

    assert_eq!(
        got, expected,
        "production rules caused SME to drop ids (false negatives) for query={q}"
    );
}
