//! Compressed storage (`core::compression`, docs/compression.md): the codec,
//! its parsers, dictionaries and their sidecar, and the engine driven through
//! the executor as the server drives it, with a reopen where durability is the
//! question. Every engine test reads back what it wrote and compares it with
//! the text it sent: a compressed value is only right if it reads back as the
//! value, through every path a query can take to it.

use crate::core::compression::{self as c, Codec, MAX_LEVEL, MIN_LEVEL, DEFAULT_LEVEL, MIN_VALUE_BYTES};
use crate::core::database::Database;
use crate::core::row::row::{Row, vec_into_rows};
use crate::core::rql::executor::execute;
use crate::core::rql::lexer_s1::recognize_query_purpose;
use rclite::Arc;

// ------------------------------------------------------------------ codec

#[test]
fn methods_parse_as_postgresql_writes_them() {
    assert_eq!(c::parse_method("zstd"), Ok(Some(DEFAULT_LEVEL)));
    assert_eq!(c::parse_method("ZSTD(19)"), Ok(Some(19)));
    assert_eq!(c::parse_method(" zstd ( 1 ) "), Ok(Some(1)));
    assert_eq!(c::parse_method("'zstd(22)'"), Ok(Some(22)));
    assert_eq!(c::parse_method("lz4"), Ok(Some(DEFAULT_LEVEL)));
    assert_eq!(c::parse_method("pglz"), Ok(Some(DEFAULT_LEVEL)));
    assert_eq!(c::parse_method("default"), Ok(None));
    assert_eq!(c::parse_method("'none'"), Ok(None));
    assert_eq!(c::parse_method("uncompressed"), Ok(None));
    for bad in ["zstd(0)", "zstd(23)", "zstd(-1)", "zstd(x)", "brotli", "zstd(3", "lz4(1)", ""] {
        assert!(c::parse_method(bad).is_err(), "{bad:?} was accepted");
    }
}

#[test]
fn a_clause_is_found_among_constraints() {
    let clause = |text: &str| c::clause_in(&text.split_whitespace().collect::<Vec<_>>());
    assert_eq!(clause("NOT NULL COMPRESSION zstd"), Some(Ok(Some(DEFAULT_LEVEL))));
    assert_eq!(clause("COMPRESSION zstd(12) NOT NULL"), Some(Ok(Some(12))));
    assert_eq!(clause("COMPRESSION zstd (12) UNIQUE"), Some(Ok(Some(12))));
    assert_eq!(clause("compression zstd( 7 )"), Some(Ok(Some(7))));
    assert_eq!(clause("COMPRESSION lz4,"), Some(Ok(Some(DEFAULT_LEVEL))));
    assert_eq!(clause("COMPRESSION default"), Some(Ok(None)));
    assert_eq!(clause("NOT NULL"), None);
    assert!(matches!(clause("COMPRESSION"), Some(Err(_))));
    assert!(matches!(clause("COMPRESSION zstd(99)"), Some(Err(_))));
}

#[test]
fn method_names_round_trip() {
    for level in MIN_LEVEL..=MAX_LEVEL {
        let name = c::method_name(Some(level)).unwrap();
        assert_eq!(c::parse_method(&name), Ok(Some(level)), "{name}");
    }
    assert_eq!(c::method_name(Some(DEFAULT_LEVEL)).as_deref(), Some("zstd"));
    assert_eq!(c::method_name(None), None);
}

#[test]
fn storage_options_name_row_compression_or_are_ignored() {
    assert_eq!(c::row_compression_option("row_compression = 'zstd(9)'"), Ok(Some(Some(9))));
    assert_eq!(c::row_compression_option("fillfactor = 70, row_compression = zstd"), Ok(Some(Some(DEFAULT_LEVEL))));
    assert_eq!(c::row_compression_option("row_compression"), Ok(Some(None)), "RESET names it bare");
    assert_eq!(c::row_compression_option("row_compression = none"), Ok(Some(None)));
    assert_eq!(c::row_compression_option("fillfactor=70, autovacuum_enabled=false, toast.autovacuum_enabled = off"), Ok(None));
    assert!(c::row_compression_option("row_compression = brotli").is_err());
    assert_eq!(c::row_compression_option("bogus = 1"), Err("unrecognized parameter \"bogus\"".to_string()));
}

fn json_like(len: usize, salt: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 64);
    let mut i = 0usize;
    while out.len() < len {
        out.extend_from_slice(format!("{{\"sku\":\"item-{}\",\"qty\":{},\"note\":\"repeated\"}},", i + salt, i % 7).as_bytes());
        i += 1;
    }
    out.truncate(len);
    out
}

fn noise_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64 ^ seed;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

#[test]
fn short_values_are_never_compressed() {
    let codec = Codec { level: DEFAULT_LEVEL, dictionary: None };
    assert_eq!(c::compress(&vec![b'a'; MIN_VALUE_BYTES - 1], &codec), None);
}

#[test]
fn a_frame_preamble_over_the_cap_is_refused_before_allocating() {
    let mut frame = vec![c::CODEC_ZSTD];
    frame.extend_from_slice(&u32::MAX.to_le_bytes());
    let err = c::decompress(&frame).unwrap_err();
    assert!(err.to_string().contains("too large"), "{err}");
}

#[test]
fn unknown_codecs_and_short_frames_are_refused() {
    assert!(c::decompress(&[9, 1, 0, 0, 0, 0]).unwrap_err().to_string().contains("codec"));
    assert!(c::decompress(&[]).is_err());
    assert!(c::decompress(&[c::CODEC_ZSTD, 1, 0]).is_err());
    assert!(c::decompress(&[c::CODEC_ZSTD_DICT, 1, 0, 0, 0, 1, 0]).is_err());
}

#[cfg(feature = "storage_compression")]
mod with_the_codec {
    use super::*;

    #[test]
    fn values_round_trip_at_every_level() {
        let raw = json_like(20_000, 0);
        for level in MIN_LEVEL..=MAX_LEVEL {
            let frame = c::compress(&raw, &Codec { level, dictionary: None }).expect("repetitive text compresses");
            assert_eq!(frame[0], c::CODEC_ZSTD);
            assert!(frame.len() < raw.len() / 4, "level {level}: {} of {}", frame.len(), raw.len());
            assert_eq!(c::decompress(&frame).unwrap().into_slice(), &raw[..], "level {level}");
        }
    }

    #[test]
    fn incompressible_values_are_stored_as_they_are() {
        let codec = Codec { level: MAX_LEVEL, dictionary: None };
        assert_eq!(c::compress(&noise_bytes(4096, 1), &codec), None);
    }

    #[test]
    fn a_truncated_or_altered_frame_errors() {
        let raw = json_like(10_000, 3);
        let frame = c::compress(&raw, &Codec { level: DEFAULT_LEVEL, dictionary: None }).unwrap();
        assert!(c::decompress(&frame[..frame.len() - 1]).is_err(), "truncated");
        let mut longer = frame.clone();
        longer[1..5].copy_from_slice(&10_001u32.to_le_bytes());
        assert!(c::decompress(&longer).is_err(), "a raw length that is not the frame's");
        // The content checksum: a flipped byte in the body is an error, never
        // another value.
        let mut flipped = frame.clone();
        let last = flipped.len() - 6;
        flipped[last] ^= 0x40;
        match c::decompress(&flipped) {
            Err(_) => {}
            Ok(block) => assert_eq!(block.into_slice(), &raw[..], "a flip decoded to other bytes"),
        }
    }

    /// A dictionary trained from many short, similar values pays where
    /// compressing each alone does not, is registered by its id, and a frame
    /// made with it names it and decodes with it.
    #[test]
    fn a_trained_dictionary_pays_and_round_trips() {
        let values: Vec<Vec<u8>> = (0..600)
            .map(|i| format!("{{\"user\":\"u{i:05}\",\"event\":\"page_view\",\"path\":\"/catalog/item/{}\",\"agent\":\"Mozilla/5.0\"}}", i * 7).into_bytes())
            .collect();
        let samples: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
        let dictionary = c::train(&samples, DEFAULT_LEVEL).expect("short similar values train a dictionary that pays");
        assert_ne!(dictionary.id, 0);
        assert!(c::dictionary(dictionary.id).is_some(), "registered");
        let with = Codec { level: DEFAULT_LEVEL, dictionary: Some(dictionary.clone()) };
        let plain = Codec { level: DEFAULT_LEVEL, dictionary: None };
        let mut saved = 0usize;
        for v in &values[..50] {
            let frame = c::compress(v, &with).expect("a dictionary frame of a short value saves bytes");
            assert_eq!(frame[0], c::CODEC_ZSTD_DICT);
            assert_eq!(u32::from_le_bytes(frame[5..9].try_into().unwrap()), dictionary.id);
            assert_eq!(c::decompress(&frame).unwrap().into_slice(), &v[..]);
            saved += c::compress(v, &plain).map_or(v.len(), |f| f.len()) - frame.len();
        }
        assert!(saved > 0);
        // Too few samples train nothing.
        assert!(c::train(&samples[..5], DEFAULT_LEVEL).is_none());
    }

    #[test]
    fn a_registered_id_is_not_taken_by_other_bytes() {
        let a = Arc::new(c::Dictionary { id: 0x7fff_0001, bytes: b"first".to_vec() });
        let b = Arc::new(c::Dictionary { id: 0x7fff_0001, bytes: b"second".to_vec() });
        assert!(c::register(a.clone()).is_ok());
        assert!(c::register(a).is_ok(), "the same bytes again");
        assert!(c::register(b).is_err());
    }

    #[test]
    fn a_frame_naming_an_unknown_dictionary_errors() {
        let mut frame = vec![c::CODEC_ZSTD_DICT];
        frame.extend_from_slice(&100u32.to_le_bytes());
        frame.extend_from_slice(&0x7eee_eeeeu32.to_le_bytes());
        frame.extend_from_slice(&[0u8; 16]);
        assert!(c::decompress(&frame).unwrap_err().to_string().contains("dictionary"));
    }
}

#[cfg(not(feature = "storage_compression"))]
#[test]
fn a_build_without_the_feature_refuses_a_frame_and_compresses_nothing() {
    let frame = [c::CODEC_ZSTD, 5, 0, 0, 0, 1, 2, 3];
    assert_eq!(c::decompress(&frame).unwrap_err().kind(), std::io::ErrorKind::Unsupported);
    assert_eq!(c::compress(&json_like(1000, 0), &Codec { level: DEFAULT_LEVEL, dictionary: None }), None);
    assert!(c::train(&[&b"x"[..]; 100], DEFAULT_LEVEL).is_none());
}

/// The dictionary sidecar reads every whole record, steps over a torn one
/// (zeros, a cut record, a bad checksum) and still finds the records after.
#[test]
fn the_dictionary_sidecar_steps_over_torn_records() {
    let one = c::Dictionary { id: 11, bytes: b"first dictionary".to_vec() };
    let two = c::Dictionary { id: 22, bytes: b"second".to_vec() };
    let mut file = c::sidecar::record(&one);
    file.extend_from_slice(&[0u8; 40]);
    let mut cut = c::sidecar::record(&two);
    cut.truncate(cut.len() - 2);
    file.extend_from_slice(&cut);
    let mut bad = c::sidecar::record(&two);
    let at = bad.len() - 1;
    bad[at] ^= 1;
    file.extend_from_slice(&bad);
    file.extend_from_slice(&c::sidecar::record(&two));
    let read = c::sidecar::parse(&file);
    assert_eq!(read.iter().map(|d| (d.id, d.bytes.clone())).collect::<Vec<_>>(), vec![(11, one.bytes), (22, two.bytes)]);
    assert!(c::sidecar::parse(&[]).is_empty());
}

// ------------------------------------------------------------------ engine

fn init_globals() {
    let _ = crate::MAX_PERMITS_THREADS.set(4);
    let _ = crate::BATCH_SIZE.set(1024);
}

async fn open(dir: &std::path::Path) -> Arc<Database> {
    Arc::new(Database::new_named(&dir.to_string_lossy(), false, "compressiontests").await)
}

async fn run(database: &Arc<Database>, sql: &str) -> Vec<u8> {
    let purpose = recognize_query_purpose(sql).unwrap_or_else(|| panic!("unrecognised: {}", sql));
    execute(purpose, database.clone()).await
}

fn is_error(response: &[u8]) -> bool {
    response.first() == Some(&3)
}

fn message(response: &[u8]) -> String {
    String::from_utf8_lossy(response.get(1..).unwrap_or_default()).into_owned()
}

async fn ok(database: &Arc<Database>, sql: &str) {
    let response = run(database, sql).await;
    if is_error(&response) {
        panic!("`{}` failed: {}", &sql[..sql.len().min(300)], message(&response));
    }
}

async fn refused(database: &Arc<Database>, sql: &str) -> String {
    let response = run(database, sql).await;
    assert!(is_error(&response), "`{}` was accepted", sql);
    message(&response)
}

async fn rows(database: &Arc<Database>, sql: &str) -> Vec<Row> {
    let response = run(database, sql).await;
    if is_error(&response) {
        panic!("`{}` failed: {}", &sql[..sql.len().min(300)], message(&response));
    }
    vec_into_rows(&response[1..]).expect("a decodable result set")
}

fn text(row: &Row, column: usize) -> String {
    String::from_utf8(row.columns[column].data.into_slice().to_vec()).expect("UTF-8 text")
}

fn int(row: &Row, column: usize) -> i64 {
    let bytes = row.columns[column].data.into_slice();
    match bytes.len() {
        4 => i32::from_le_bytes(bytes.try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap()),
        n => panic!("an integer cell of {} bytes", n),
    }
}

async fn ints(database: &Arc<Database>, sql: &str) -> Vec<i64> {
    rows(database, sql).await.iter().map(|r| int(r, 0)).collect()
}

/// `(id, body)` of every row with a body, by id.
async fn bodies(database: &Arc<Database>, table: &str) -> Vec<(i64, String)> {
    rows(database, &format!("SELECT id, body FROM {table} WHERE body IS NOT NULL ORDER BY id"))
        .await
        .iter()
        .map(|r| (int(r, 0), text(r, 1)))
        .collect()
}

/// A long value that compresses well, distinct per `n`.
fn document(n: usize) -> String {
    let mut out = format!("{{\"id\": {n}, \"items\": [");
    for i in 0..200 {
        out.push_str(&format!("{{\"sku\": \"item-{i}\", \"qty\": {}, \"note\": \"repeated text here\"}},", i % 7));
    }
    out.push_str("]}");
    out
}

/// A value zstd cannot shorten: printable, without repetition, no quotes.
fn noise(n: usize, len: usize) -> String {
    noise_bytes(len, n as u64).into_iter().map(|b| (b'0' + b % 75) as char).filter(|c| *c != '\'' && *c != '\\').collect()
}

/// Non-ASCII text, long enough to compress.
fn unicode(n: usize) -> String {
    format!("ŽŠĐĆČ — κείμενο {n} — 日本語のテキスト ").repeat(20)
}

/// The size of table `table`'s row file under `dir`, plain or partitioned
/// (every `<stem>.db` holding rows).
fn rows_file_len(dir: &std::path::Path, table: &str) -> u64 {
    let name = format!("{table}.db");
    let mut pending = vec![dir.to_path_buf()];
    let mut total = 0;
    while let Some(at) = pending.pop() {
        for entry in std::fs::read_dir(&at).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|f| f.to_string_lossy() == name) {
                // `fs::metadata` opens the file: on Windows a directory
                // entry's size is stale (0) for a file the engine holds open.
                total += std::fs::metadata(&path).unwrap().len();
            }
        }
    }
    assert!(total > 0, "no {name} under {}", dir.display());
    total
}

/// Every file under `dir` whose name ends in `suffix`.
fn files_ending(dir: &std::path::Path, suffix: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(at) = pending.pop() {
        for entry in std::fs::read_dir(&at).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|f| f.to_string_lossy().ends_with(suffix)) {
                out.push(path);
            }
        }
    }
    out
}

fn flags(dir: &std::path::Path) -> String {
    std::fs::read_to_string(dir.join(crate::core::database::incompat::FILE_NAME)).unwrap_or_default()
}

fn policy_of(database: &Arc<Database>, table: &str) -> c::StoragePolicy {
    if let Some(pt) = database.partitioned_table(table) {
        return pt.shell().schema.compression.clone();
    }
    database.table_for_tests(table).expect("table").schema.compression.clone()
}

fn level_of(database: &Arc<Database>, table: &str, column: &str) -> Option<i32> {
    let t = database.table_for_tests(table).expect("table");
    let field = t.schema.fields.iter().find(|f| f.name == column && !f.is_deleted).expect("column");
    c::level_of(&t.schema.compression, field.write_order)
}

/// A catalogue relation (`pg_class`, `pg_attribute`) built as pgwire builds
/// it for a statement that reads it: the name of its temporary relation.
async fn catalogue(database: &Arc<Database>, view: &str) -> String {
    crate::core::rql::catalog_views::build_alone(view, database, crate::core::mvcc::transaction::FROZEN_TX, None)
        .await
        .unwrap()
}

/// `SHOW TABLE STATS t` as `metric -> value`.
async fn stats(database: &Arc<Database>, table: &str) -> Vec<(String, String)> {
    let statement = crate::core::introspect::parse(&format!("SHOW TABLE STATS {table}")).unwrap().unwrap();
    let fields = crate::core::introspect::result_fields(&statement).unwrap();
    let out = crate::core::introspect::execute(&statement, database).await;
    assert_eq!(out.first(), Some(&2u8), "{}", String::from_utf8_lossy(&out));
    let metric = fields.iter().position(|(n, _)| n == "metric").unwrap();
    let value = fields.iter().position(|(n, _)| n == "value").unwrap();
    vec_into_rows(&out[1..]).unwrap().iter().map(|r| (text(r, metric), text(r, value))).collect()
}

fn stat<'a>(stats: &'a [(String, String)], metric: &str) -> Option<&'a str> {
    stats.iter().find(|(m, _)| m == metric).map(|(_, v)| v.as_str())
}

/// A log line's message: a request repeating its path, as real ones do.
fn event_message(n: usize) -> String {
    let x = n * 13;
    format!(
        "GET /catalog/item/{x} HTTP/1.1 200 OK agent Mozilla/5.0 (X11; Linux x86_64) \
         referer https://shop.example.com/catalog/item/{x} via /catalog/item/{x}"
    )
}

/// Rows of a few strings, each too short to compress alone; a row's tail,
/// all of them together, does.
fn event_row(n: usize) -> String {
    format!("({n}, 'page_view', 'web-frontend-{}', '{}', 'ok')", n % 3, event_message(n))
}

fn event_values(n: usize) -> (String, String, String) {
    (format!("web-frontend-{}", n % 3), event_message(n), "ok".to_string())
}

async fn events(database: &Arc<Database>, table: &str) -> Vec<(i64, String, String, String)> {
    rows(database, &format!("SELECT id, src, msg, status FROM {table} ORDER BY id"))
        .await
        .iter()
        .map(|r| (int(r, 0), text(r, 1), text(r, 2), text(r, 3)))
        .collect()
}

fn expected_events(ids: impl Iterator<Item = usize>) -> Vec<(i64, String, String, String)> {
    ids.map(|n| {
        let (src, msg, status) = event_values(n);
        (n as i64, src, msg, status)
    })
    .collect()
}

/// Per-column compression: the values written, read back through a full
/// scan, a point lookup, a filter on the compressed column, LIKE, a sort on
/// it with LIMIT (the top-N key cells), an aggregate, and after a reopen;
/// the row file far smaller than the text it holds.
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_values_read_back_as_written() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(&db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT COMPRESSION zstd(9), tag TEXT, n BIGINT)").await;
    assert_eq!(level_of(&db, "docs", "body"), Some(9));
    assert_eq!(level_of(&db, "docs", "tag"), None);
    assert!(flags(dir.path()).contains("compressed_values_v1"), "the directory is fenced");

    let mut expected: Vec<(i64, String)> = Vec::new();
    let mut raw_bytes = 0usize;
    for n in 0..60usize {
        let body = match n % 5 {
            0 | 1 => document(n),
            2 => noise(n, 3000),
            3 => unicode(n),
            // Short: stored as it is.
            _ => format!("short {n}"),
        };
        raw_bytes += body.len();
        ok(&db, &format!("INSERT INTO docs (id, body, tag, n) VALUES ({n}, '{body}', 't{}', {n})", n % 3)).await;
        expected.push((n as i64, body));
    }
    // At the threshold and one under it.
    for (id, len) in [(200usize, MIN_VALUE_BYTES), (201, MIN_VALUE_BYTES - 1)] {
        let body = "a".repeat(len);
        ok(&db, &format!("INSERT INTO docs (id, body, n) VALUES ({id}, '{body}', 0)")).await;
        expected.push((id as i64, body));
    }
    ok(&db, "INSERT INTO docs (id, body, tag, n) VALUES (100, '', 'empty', 0)").await;
    expected.push((100, String::new()));
    ok(&db, "INSERT INTO docs (id, tag, n) VALUES (101, 'null', 0)").await;
    expected.sort();

    let stored = rows_file_len(dir.path(), "docs") as usize;
    assert!(stored < raw_bytes / 2, "{stored} bytes stored for {raw_bytes} of text");

    assert_eq!(bodies(&db, "docs").await, expected);
    assert_eq!(ints(&db, "SELECT id FROM docs WHERE body IS NULL").await, vec![101]);

    let one = rows(&db, "SELECT body FROM docs WHERE id = 5").await;
    assert_eq!(text(&one[0], 0), document(5));
    assert_eq!(ints(&db, &format!("SELECT id FROM docs WHERE body = '{}'", document(6))).await, vec![6]);
    assert_eq!(ints(&db, &format!("SELECT id FROM docs WHERE body = '{}'", unicode(8))).await, vec![8]);
    let like = ints(&db, "SELECT id FROM docs WHERE body LIKE '{\"id\": 1%' ORDER BY id").await;
    assert_eq!(like, vec![1, 10, 11, 15, 16]);
    let top = rows(&db, "SELECT id, body FROM docs WHERE id < 60 ORDER BY body DESC, id LIMIT 3").await;
    let mut sorted: Vec<&(i64, String)> = expected.iter().filter(|(id, _)| *id < 60).collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    assert_eq!(
        top.iter().map(|r| (int(r, 0), text(r, 1))).collect::<Vec<_>>(),
        sorted[..3].iter().map(|e| (*e).clone()).collect::<Vec<_>>()
    );
    let longest = rows(&db, "SELECT MAX(LENGTH(body)) FROM docs").await;
    assert_eq!(int(&longest[0], 0) as usize, expected.iter().map(|(_, b)| b.chars().count()).max().unwrap());

    // UPDATE: a longer value (relocated), the other column only, a value
    // replaced by another; DELETE.
    ok(&db, &format!("UPDATE docs SET body = '{}' WHERE id = 0", document(1000))).await;
    expected.iter_mut().find(|(id, _)| *id == 0).unwrap().1 = document(1000);
    ok(&db, "UPDATE docs SET tag = 'x' WHERE id = 1").await;
    ok(&db, &format!("UPDATE docs SET body = '{}' WHERE id = 5", unicode(500))).await;
    expected.iter_mut().find(|(id, _)| *id == 5).unwrap().1 = unicode(500);
    ok(&db, "DELETE FROM docs WHERE id = 2").await;
    expected.retain(|(id, _)| *id != 2);
    assert_eq!(bodies(&db, "docs").await, expected);

    let s = stats(&db, "docs").await;
    assert_eq!(stat(&s, "compressed_columns"), Some("body=zstd(9)"));
    assert_eq!(stat(&s, "row_compression"), Some("none"));
    assert!(stat(&s, "compressed_values").unwrap().parse::<u64>().unwrap() >= 30);
    let raw: u64 = stat(&s, "compression_raw_bytes").unwrap().parse().unwrap();
    let held: u64 = stat(&s, "compression_stored_bytes").unwrap().parse().unwrap();
    assert!(held * 4 < raw, "{held} of {raw}");

    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(level_of(&db, "docs", "body"), Some(9), "the setting survives a reopen");
    assert_eq!(bodies(&db, "docs").await, expected, "after a reopen");
    ok(&db, "VACUUM FULL docs").await;
    assert_eq!(bodies(&db, "docs").await, expected, "after compaction moved the rows");
}

/// `SET COMPRESSION` applies to values written from then on; a table holds
/// both kinds and reads both. `default` stops it, a rename keeps it, a
/// dropped column's name does not pass it on.
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_compression_applies_to_new_values() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(&db, "CREATE TABLE t (id INT PRIMARY KEY, body TEXT)").await;
    assert!(!flags(dir.path()).contains("compressed_values_v1"), "nothing compresses by default");
    assert!(policy_of(&db, "t").is_empty());
    assert_eq!(stat(&stats(&db, "t").await, "compressed_columns"), None, "no rows for a table that does not compress");
    ok(&db, &format!("INSERT INTO t (id, body) VALUES (1, '{}')", document(1))).await;
    let plain = rows_file_len(dir.path(), "t");

    ok(&db, "ALTER TABLE t ALTER COLUMN body SET COMPRESSION zstd(19)").await;
    assert_eq!(level_of(&db, "t", "body"), Some(19));
    assert!(flags(dir.path()).contains("compressed_values_v1"));
    ok(&db, &format!("INSERT INTO t (id, body) VALUES (2, '{}')", document(2))).await;
    let grown = rows_file_len(dir.path(), "t") - plain;
    assert!(grown < plain / 4, "a compressed row of {grown} bytes beside a plain one of {plain}");

    ok(&db, "ALTER TABLE t RENAME COLUMN body TO doc").await;
    assert_eq!(level_of(&db, "t", "doc"), Some(19), "a rename keeps the setting");
    // `default` is the method's name here, not a DEFAULT clause.
    ok(&db, "ALTER TABLE t ALTER COLUMN doc SET COMPRESSION default").await;
    assert_eq!(level_of(&db, "t", "doc"), None);
    ok(&db, &format!("INSERT INTO t (id, doc) VALUES (3, '{}')", document(3))).await;
    let read: Vec<String> = rows(&db, "SELECT doc FROM t ORDER BY id").await.iter().map(|r| text(r, 0)).collect();
    assert_eq!(read, vec![document(1), document(2), document(3)]);

    ok(&db, "ALTER TABLE t ALTER COLUMN doc SET COMPRESSION lz4").await;
    assert_eq!(level_of(&db, "t", "doc"), Some(DEFAULT_LEVEL));
    ok(&db, "ALTER TABLE t DROP COLUMN doc").await;
    ok(&db, "ALTER TABLE t ADD COLUMN doc TEXT").await;
    assert_eq!(level_of(&db, "t", "doc"), None, "a dropped column's setting does not pass on");

    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(ints(&db, "SELECT id FROM t ORDER BY id").await, vec![1, 2, 3]);
}

/// An ALTER that rewrites every row keeps the setting and the values.
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rewrite_keeps_compression_and_values() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(&db, "CREATE TABLE r (id INT PRIMARY KEY, body TEXT COMPRESSION zstd, n INT)").await;
    for n in 0..10 {
        ok(&db, &format!("INSERT INTO r (id, body, n) VALUES ({n}, '{}', {n})", document(n))).await;
    }
    ok(&db, "ALTER TABLE r ALTER COLUMN n TYPE BIGINT").await;
    assert_eq!(level_of(&db, "r", "body"), Some(DEFAULT_LEVEL));
    assert_eq!(bodies(&db, "r").await, (0..10).map(|n| (n as i64, document(n))).collect::<Vec<_>>());
}

/// A UNIQUE compressed column still finds its duplicates and its index
/// still finds its rows: both compare values, never stored bytes.
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_unique_compressed_column_refuses_duplicates() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(&db, "CREATE TABLE u (id INT PRIMARY KEY, body TEXT UNIQUE COMPRESSION zstd(3))").await;
    ok(&db, &format!("INSERT INTO u (id, body) VALUES (1, '{}')", document(7))).await;
    refused(&db, &format!("INSERT INTO u (id, body) VALUES (2, '{}')", document(7))).await;
    ok(&db, &format!("INSERT INTO u (id, body) VALUES (2, '{}')", document(8))).await;
    ok(&db, &format!("INSERT INTO u (id, body) VALUES (3, '{}') ON CONFLICT DO NOTHING", document(8))).await;
    ok(&db, "CREATE INDEX u_body ON u (body)").await;
    assert_eq!(ints(&db, &format!("SELECT id FROM u WHERE body = '{}'", document(8))).await, vec![2]);
    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(ints(&db, &format!("SELECT id FROM u WHERE body = '{}'", document(7))).await, vec![1]);
}

/// Row compression: many rows of short strings, none of which compresses
/// alone, stored as one frame per row's tail; every read path returns the
/// values; the fixed-width columns are read in place (prefilter, folds).
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_compression_stores_each_tail_as_one_frame() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    let create = |name: &str, with: &str| {
        format!("CREATE TABLE {name} (id INT PRIMARY KEY, kind TEXT, src TEXT, msg TEXT, status TEXT, n BIGINT){with}")
    };
    ok(&db, &create("plain_ev", "")).await;
    ok(&db, &create("ev", " WITH (fillfactor = 90, row_compression = 'zstd(6)')")).await;
    let policy = policy_of(&db, "ev");
    assert_eq!(policy.row.as_ref().map(|r| r.level), Some(6));
    assert!(policy.columns.is_empty());
    for table in ["plain_ev", "ev"] {
        for chunk in (0..400usize).collect::<Vec<_>>().chunks(100) {
            let values: Vec<String> = chunk
                .iter()
                .map(|n| event_row(*n).replace(", 'ok')", &format!(", 'ok', {n})")))
                .collect();
            ok(&db, &format!("INSERT INTO {table} (id, kind, src, msg, status, n) VALUES {}", values.join(", "))).await;
        }
    }
    let (plain, packed) = (rows_file_len(dir.path(), "plain_ev"), rows_file_len(dir.path(), "ev"));
    assert!(packed < plain, "row compression stored {packed} bytes against {plain}");
    let tails: u64 = stat(&stats(&db, "ev").await, "compressed_row_tails").unwrap().parse().unwrap();
    // A tail that does not shrink by an eighth is stored as it is.
    assert!(tails >= 300, "only {tails} of 400 tails were stored as a frame");

    let expected = expected_events(0..400);
    assert_eq!(events(&db, "ev").await, expected);
    // A point read of one string column, a filter on one, a sort with LIMIT
    // (top-N key cells over tail values), a fixed-width filter and fold.
    let one = rows(&db, "SELECT msg FROM ev WHERE id = 77").await;
    assert_eq!(text(&one[0], 0), event_values(77).1);
    assert_eq!(ints(&db, "SELECT id FROM ev WHERE src = 'web-frontend-2' AND id < 12 ORDER BY id").await, vec![2, 5, 8, 11]);
    let top = rows(&db, "SELECT id, msg FROM ev ORDER BY msg DESC, id LIMIT 4").await;
    let mut sorted = expected.clone();
    sorted.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(&b.0)));
    assert_eq!(
        top.iter().map(|r| (int(r, 0), text(r, 1))).collect::<Vec<_>>(),
        sorted[..4].iter().map(|e| (e.0, e.2.clone())).collect::<Vec<_>>()
    );
    assert_eq!(int(&rows(&db, "SELECT COUNT(*) FROM ev WHERE n >= 100").await[0], 0), 300);
    assert_eq!(int(&rows(&db, "SELECT MAX(n) FROM ev WHERE src = 'web-frontend-1'").await[0], 0), 397);
    let groups = rows(&db, "SELECT src, COUNT(*) FROM ev GROUP BY src ORDER BY src").await;
    assert_eq!(groups.iter().map(|r| (text(r, 0), int(r, 1))).collect::<Vec<_>>(), vec![
        ("web-frontend-0".to_string(), 134),
        ("web-frontend-1".to_string(), 133),
        ("web-frontend-2".to_string(), 133),
    ]);

    // UPDATE in place (no transaction, not longer), relocated (longer), a
    // fixed-width cell only; NULL and empty strings in a tail.
    ok(&db, "UPDATE ev SET status = 'no' WHERE id = 3").await;
    ok(&db, &format!("UPDATE ev SET msg = '{}' WHERE id = 4", "longer message ".repeat(30))).await;
    ok(&db, "UPDATE ev SET n = 9999 WHERE id = 5").await;
    ok(&db, "INSERT INTO ev (id, kind, src, msg, status, n) VALUES (1000, NULL, '', 'tail with a null and an empty string beside it', 'ok', 0)").await;
    let mut expected = expected;
    expected[3].3 = "no".to_string();
    expected[4].2 = "longer message ".repeat(30);
    expected.push((1000, String::new(), "tail with a null and an empty string beside it".to_string(), "ok".to_string()));
    assert_eq!(events(&db, "ev").await, expected);
    assert_eq!(ints(&db, "SELECT n FROM ev WHERE id = 5").await, vec![9999]);
    assert_eq!(ints(&db, "SELECT id FROM ev WHERE kind IS NULL").await, vec![1000]);

    let s = stats(&db, "ev").await;
    assert_eq!(stat(&s, "row_compression"), Some("zstd(6)"));
    assert!(stat(&s, "compressed_row_tails").unwrap().parse::<u64>().unwrap() > tails);
    let class = catalogue(&db, "pg_class").await;
    let options = rows(&db, &format!("SELECT reloptions::text FROM {class} WHERE relname = 'ev'")).await;
    assert!(text(&options[0], 0).contains("row_compression=zstd(6)"), "{}", text(&options[0], 0));

    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(events(&db, "ev").await, expected, "after a reopen");
    ok(&db, "DELETE FROM ev WHERE id < 200").await;
    ok(&db, "VACUUM FULL ev").await;
    expected.retain(|e| e.0 >= 200);
    assert_eq!(events(&db, "ev").await, expected, "after compaction moved the rows");
}

/// `SET (row_compression)` on a populated table: old rows stay as they are,
/// new rows are tails; `RESET` stops it; every row reads.
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_and_reset_row_compression_on_a_populated_table() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(&db, "CREATE TABLE ev (id INT PRIMARY KEY, kind TEXT, src TEXT, msg TEXT, status TEXT)").await;
    let insert = |range: std::ops::Range<usize>| {
        let values: Vec<String> = range.map(event_row).collect();
        format!("INSERT INTO ev (id, kind, src, msg, status) VALUES {}", values.join(", "))
    };
    ok(&db, &insert(0..50)).await;
    ok(&db, "ALTER TABLE ev SET (row_compression = 'zstd')").await;
    assert_eq!(policy_of(&db, "ev").row.map(|r| r.level), Some(DEFAULT_LEVEL));
    ok(&db, &insert(50..100)).await;
    ok(&db, "ALTER TABLE ev RESET (row_compression)").await;
    assert!(policy_of(&db, "ev").row.is_none());
    ok(&db, &insert(100..150)).await;
    // Ignored options are accepted; an unknown one is refused.
    ok(&db, "ALTER TABLE ev SET (fillfactor = 80)").await;
    refused(&db, "ALTER TABLE ev SET (bogus = 1)").await;
    assert_eq!(events(&db, "ev").await, expected_events(0..150));
}

/// `RECOMPRESS` rewrites rows stored before a setting under it (and plain
/// again after it is dropped), trains dictionaries from the values, keeps
/// them in the sidecar, and every value reads back the same, before and
/// after a reopen.
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recompress_rewrites_old_rows_and_trains_dictionaries() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(&db, "CREATE TABLE logs (id INT PRIMARY KEY, body TEXT, kind TEXT, src TEXT, msg TEXT, status TEXT)").await;
    // Short, similar JSON documents: what a dictionary is for.
    let doc = |n: usize| {
        format!(
            "{{\"user\":\"u{n:05}\",\"event\":\"page_view\",\"path\":\"/catalog/item/{}\",\"agent\":\"Mozilla/5.0 (X11; Linux x86_64)\"}}",
            n * 7
        )
    };
    for chunk in (0..800usize).collect::<Vec<_>>().chunks(200) {
        let values: Vec<String> = chunk
            .iter()
            .map(|n| event_row(*n).replacen('(', &format!("('{}', ", doc(*n)), 1))
            .collect();
        ok(&db, &format!("INSERT INTO logs (body, id, kind, src, msg, status) VALUES {}", values.join(", "))).await;
    }
    let plain = rows_file_len(dir.path(), "logs");
    let expected_docs: Vec<(i64, String)> = (0..800).map(|n| (n as i64, doc(n))).collect();
    assert_eq!(bodies(&db, "logs").await, expected_docs);

    ok(&db, "ALTER TABLE logs ALTER COLUMN body SET COMPRESSION zstd(9)").await;
    assert_eq!(rows_file_len(dir.path(), "logs"), plain, "SET COMPRESSION rewrites nothing");
    ok(&db, "ALTER TABLE logs RECOMPRESS").await;
    let policy = policy_of(&db, "logs");
    let body = policy.columns.iter().find(|c| c.codec.level == 9).expect("the body column still compresses");
    let dictionary = body.codec.dictionary.clone().expect("short similar documents trained a dictionary");
    assert!(rows_file_len(dir.path(), "logs") < plain, "RECOMPRESS shrank the rows");
    assert_eq!(bodies(&db, "logs").await, expected_docs);
    assert_eq!(stat(&stats(&db, "logs").await, "compressed_columns"), Some("body=zstd(9) +dictionary"));
    // The dictionary is in the table's own sidecar, by the id its marker names.
    let held: Vec<u32> = files_ending(dir.path(), "logs_schema_ZD.db")
        .iter()
        .flat_map(|f| c::sidecar::parse(&std::fs::read(f).unwrap()))
        .map(|d| d.id)
        .collect();
    assert!(held.contains(&dictionary.id), "{held:?} lacks {:#x}", dictionary.id);

    // Row compression over the same rows, by VACUUM (FULL, RECOMPRESS).
    ok(&db, "ALTER TABLE logs SET (row_compression = 'zstd(3)')").await;
    ok(&db, "VACUUM (FULL, RECOMPRESS) logs").await;
    assert!(policy_of(&db, "logs").row.is_some());
    assert_eq!(bodies(&db, "logs").await, expected_docs);
    assert_eq!(
        events(&db, "logs").await,
        expected_events(0..800),
        "the other strings of each tail"
    );

    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(bodies(&db, "logs").await, expected_docs, "after a reopen");

    // Dropping both and recompressing stores every row plain again.
    ok(&db, "ALTER TABLE logs RESET (row_compression)").await;
    ok(&db, "ALTER TABLE logs ALTER COLUMN body SET COMPRESSION default").await;
    ok(&db, "ALTER TABLE logs RECOMPRESS").await;
    assert!(policy_of(&db, "logs").is_empty());
    assert_eq!(bodies(&db, "logs").await, expected_docs);
    assert_eq!(events(&db, "logs").await, expected_events(0..800));
}

/// A partitioned table compresses in every partition, by column and by row,
/// and reads back across them.
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partitioned_table_compresses_in_every_partition() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(
        &db,
        "CREATE TABLE pev (id INT NOT NULL, kind TEXT, src TEXT, msg TEXT, status TEXT, body TEXT COMPRESSION zstd(5)) \
         PARTITION BY HASH (id) PARTITIONS 4 WITH (row_compression = 'zstd')",
    )
    .await;
    let policy = policy_of(&db, "pev");
    assert!(policy.row.is_some() && policy.columns.len() == 1);
    let values: Vec<String> =
        (0..200).map(|n| event_row(n).replace(", 'ok')", &format!(", 'ok', '{}')", document(n)))).collect();
    ok(&db, &format!("INSERT INTO pev (id, kind, src, msg, status, body) VALUES {}", values.join(", "))).await;
    let expected: Vec<(i64, String)> = (0..200).map(|n| (n as i64, document(n))).collect();
    assert_eq!(bodies(&db, "pev").await, expected);
    assert_eq!(events(&db, "pev").await, expected_events(0..200));
    ok(&db, "ALTER TABLE pev RECOMPRESS").await;
    assert_eq!(bodies(&db, "pev").await, expected);
    drop(db);
    let db = open(dir.path()).await;
    assert_eq!(events(&db, "pev").await, expected_events(0..200), "after a reopen");
}

/// What the client's migrator reads: `pg_attribute.attcompression` by
/// column, through the query it sends.
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn attcompression_names_each_columns_method() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(&db, "CREATE TABLE m (id INT PRIMARY KEY, a TEXT COMPRESSION zstd, b JSONB COMPRESSION zstd(19), c TEXT)").await;
    let (class, attribute) = (catalogue(&db, "pg_class").await, catalogue(&db, "pg_attribute").await);
    let oid = int(&rows(&db, &format!("SELECT oid FROM {class} WHERE relname = 'm'")).await[0], 0);
    let read = rows(&db, &format!("SELECT attname, attcompression FROM {attribute} WHERE attrelid = {oid}")).await;
    let mut methods: Vec<(String, String)> = read.iter().map(|r| (text(r, 0), text(r, 1))).collect();
    methods.retain(|(name, _)| ["id", "a", "b", "c"].contains(&name.as_str()));
    methods.sort();
    assert_eq!(
        methods,
        vec![
            ("a".to_string(), "zstd".to_string()),
            ("b".to_string(), "zstd(19)".to_string()),
            ("c".to_string(), String::new()),
            ("id".to_string(), String::new()),
        ]
    );
    let options = rows(&db, &format!("SELECT reloptions IS NULL FROM {class} WHERE relname = 'm'")).await;
    assert_eq!(options[0].columns[0].data.into_slice(), &[1], "no row compression: NULL reloptions");
}

/// A restored directory, laid down without the flag, is fenced again from
/// its compression markers.
#[cfg(feature = "storage_compression")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restored_directory_is_fenced_from_its_markers() {
    use crate::core::database::incompat;
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(&db, "CREATE TABLE plain_t (id INT PRIMARY KEY, body TEXT)").await;
    assert!(incompat::directories_needing_compressed_values(dir.path()).unwrap().is_empty());
    ok(&db, "CREATE TABLE f (id INT PRIMARY KEY, body TEXT COMPRESSION zstd)").await;
    drop(db);
    assert_eq!(incompat::directories_needing_compressed_values(dir.path()).unwrap(), vec![dir.path().to_path_buf()]);
    std::fs::remove_file(dir.path().join(incompat::FILE_NAME)).unwrap();
    incompat::fence_restored_directory(dir.path()).unwrap();
    assert!(flags(dir.path()).contains("compressed_values_v1"));
}

/// What is refused, as PostgreSQL refuses it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_compression_clauses_are_refused() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    let e = refused(&db, "CREATE TABLE a (id INT COMPRESSION zstd)").await;
    assert!(e.contains("does not support compression"), "{e}");
    let e = refused(&db, "CREATE TABLE a (id INT, body TEXT COMPRESSION zstd(23))").await;
    assert!(e.contains("out of range"), "{e}");
    let e = refused(&db, "CREATE TABLE a (id INT, body TEXT COMPRESSION brotli)").await;
    assert!(e.contains("invalid compression method"), "{e}");
    let e = refused(&db, "CREATE TABLE a (id INT, body TEXT) WITH (row_compression = 'zstd(30)')").await;
    assert!(e.contains("out of range"), "{e}");
    let e = refused(&db, "CREATE TABLE a (id INT, body TEXT) WITH (nonsense = 1)").await;
    assert!(e.contains("unrecognized parameter"), "{e}");
    ok(&db, "CREATE TABLE b (id INT, body TEXT)").await;
    let e = refused(&db, "ALTER TABLE b ALTER COLUMN id SET COMPRESSION zstd").await;
    assert!(e.contains("does not support compression"), "{e}");
    let e = refused(&db, "ALTER TABLE b ALTER COLUMN body SET COMPRESSION zstd(0)").await;
    assert!(e.contains("out of range"), "{e}");
    // `default` on a fixed-width column asks for nothing and is accepted.
    ok(&db, "ALTER TABLE b ALTER COLUMN id SET COMPRESSION default").await;
    // pg_dump's storage options load.
    ok(&db, "CREATE TABLE d (id INT) WITH (fillfactor = 70, autovacuum_enabled = false)").await;
}

/// A build without the feature accepts the clauses, stores values as they
/// are, and never fences the directory.
#[cfg(not(feature = "storage_compression"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_the_feature_clauses_are_accepted_and_ignored() {
    init_globals();
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path()).await;
    ok(&db, "CREATE TABLE d (id INT PRIMARY KEY, body TEXT COMPRESSION zstd(9)) WITH (row_compression = 'zstd')").await;
    ok(&db, "ALTER TABLE d ALTER COLUMN body SET COMPRESSION lz4").await;
    ok(&db, "ALTER TABLE d SET (row_compression = 'zstd(4)')").await;
    assert!(policy_of(&db, "d").is_empty());
    assert!(!flags(dir.path()).contains("compressed_values_v1"));
    ok(&db, &format!("INSERT INTO d (id, body) VALUES (1, '{}')", document(1))).await;
    assert_eq!(bodies(&db, "d").await, vec![(1, document(1))]);
    ok(&db, "ALTER TABLE d RECOMPRESS").await;
    assert_eq!(bodies(&db, "d").await, vec![(1, document(1))]);
}

/// `ALTER TABLE … ADD COLUMN … COMPRESSION` takes effect, on an empty table
/// and on one with rows (whose add rewrites the table), with the clause
/// before or after a `DEFAULT`. It was accepted and dropped: the column
/// stored its values uncompressed and no catalogue said otherwise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_column_takes_its_compression() {
    let dir = tempfile::tempdir().unwrap();
    let database = open(dir.path()).await;
    ok(&database, "CREATE TABLE full_t (id INT PRIMARY KEY)").await;
    ok(&database, "INSERT INTO full_t VALUES (1)").await;
    ok(&database, "ALTER TABLE full_t ADD COLUMN body TEXT DEFAULT '' COMPRESSION zstd(9)").await;
    ok(&database, "ALTER TABLE full_t ADD COLUMN memo TEXT COMPRESSION zstd DEFAULT 'x'").await;
    ok(&database, "CREATE TABLE empty_t (id INT PRIMARY KEY)").await;
    ok(&database, "ALTER TABLE empty_t ADD COLUMN body TEXT COMPRESSION zstd(4)").await;
    assert!(refused(&database, "ALTER TABLE empty_t ADD COLUMN n INT COMPRESSION zstd").await.contains("does not support compression"));

    let full = stats(&database, "full_t").await;
    let listed = stat(&full, "compressed_columns").unwrap_or_default().to_string();
    assert!(listed.contains("body=zstd(9)") && listed.contains("memo=zstd"), "{listed}");
    assert_eq!(stat(&stats(&database, "empty_t").await, "compressed_columns"), Some("body=zstd(4)"));

    // A value written now is stored compressed, and reads back as written.
    let body = "abc".repeat(400);
    ok(&database, &format!("INSERT INTO full_t (id, body) VALUES (2, '{body}')")).await;
    let after = stats(&database, "full_t").await;
    assert_eq!(stat(&after, "compressed_values"), Some("1"), "{after:?}");
    assert_eq!(bodies_of(&database).await, vec![(1, String::new()), (2, body)]);
}

async fn bodies_of(database: &Arc<Database>) -> Vec<(i64, String)> {
    let rows = rows(database, "SELECT id, body FROM full_t ORDER BY id").await;
    rows.iter().map(|r| (int(r, 0), text(r, 1))).collect()
}
