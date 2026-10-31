//! A legacy `CHAR` column: the four-byte type builds up to f89ed7ad made
//! for a bare `CHAR` or `CHAR(1)` (`DbType::CHAR`), before `character(n)`
//! was a type of its own.
//!
//! Every write path stored the value's UTF-8, cut at four bytes and padded
//! with zeros, and every reader decoded the four bytes as a little-endian
//! code point. So `'é'` (`c3 a9 00 00`) read back as U+A9C3 `'꧃'`, `'€'` as
//! the number 11305698, `'ab'` as `'扡'`; `c = 'é'` compared the literal's
//! code point and found nothing; and `'abcde'` was stored as `'abcd'` with
//! the statement reporting success. Nothing of a value of at most four
//! bytes — any one character — was lost on the way in, so it now reads back
//! as written. New writes are held to `character(1)`, which a bare `CHAR`
//! is in PostgreSQL, and `ALTER COLUMN … TYPE character(1)` (or text)
//! converts the column, which was refused.
//!
//! No statement creates such a column now (`CHAR` is `character(1)`), so
//! the tests make one as an older build left it: a `UINT4` column, whose
//! cells are the same four bytes, holding the integers whose little-endian
//! bytes are what that build stored, then retyped to `CHAR` in its field
//! record. The values match what f89ed7ad's binary stores, byte for byte
//! (`.claude/reviews/c5/` has the dump).
//!
//! Every expectation for a value PostgreSQL can hold is PostgreSQL 17's
//! answer for a `char` column; the two-character values only an older build
//! could store read back as stored.

use rasterizeddb_compat::TestServer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn client(server: &TestServer) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(connection);
    client
}

/// The rows of `sql` (simple protocol, text), one string per row with the
/// cells joined by `|` and NULL as `∅`, or the SQLSTATE it failed with.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Result<Vec<String>, String> {
    match client.simple_query(sql).await {
        Ok(messages) => Ok(messages
            .iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => Some(
                    (0..r.len())
                        .map(|i| r.get(i).map(|v| format!("[{v}]")).unwrap_or_else(|| "∅".to_string()))
                        .collect::<Vec<_>>()
                        .join("|"),
                ),
                _ => None,
            })
            .collect()),
        Err(e) => Err(e.code().map(|c| c.code().to_string()).unwrap_or_else(|| e.to_string())),
    }
}

/// Runs each `(sql, want)` and reports every answer that differs. `want` is
/// the rows joined by `;`, or `ERR <sqlstate>`.
async fn check(client: &tokio_postgres::Client, cases: &[(&str, &str)]) -> Vec<String> {
    let mut wrong = Vec::new();
    for (sql, want) in cases {
        let got = match rows(client, sql).await {
            Ok(rows) => rows.join(";"),
            Err(code) => format!("ERR {code}"),
        };
        if got != *want {
            wrong.push(format!("{sql}\n    got  {got}\n    want {want}"));
        }
    }
    wrong
}

/// The `(type OID, atttypmod)` of each column `sql` returns, from the
/// RowDescription over a bare v3 connection.
async fn described(server: &TestServer, sql: &str) -> Vec<(u32, i32)> {
    async fn message(s: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        let tag = s.read_u8().await.expect("tag");
        let len = s.read_u32().await.expect("length") as usize;
        let mut body = vec![0u8; len - 4];
        s.read_exact(&mut body).await.expect("body");
        (tag, body)
    }
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", server.port())).await.expect("connect");
    let mut startup = 196608u32.to_be_bytes().to_vec();
    for part in ["user", "rasterized", "database", rasterizeddb_core::core::database::DEFAULT_DATABASE_NAME] {
        startup.extend_from_slice(part.as_bytes());
        startup.push(0);
    }
    startup.push(0);
    let mut frame = ((startup.len() + 4) as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&startup);
    s.write_all(&frame).await.unwrap();
    while message(&mut s).await.0 != b'Z' {}
    let mut query = sql.as_bytes().to_vec();
    query.push(0);
    let mut frame = vec![b'Q'];
    frame.extend_from_slice(&((query.len() + 4) as u32).to_be_bytes());
    frame.extend_from_slice(&query);
    s.write_all(&frame).await.unwrap();
    let mut out = Vec::new();
    loop {
        let (tag, body) = message(&mut s).await;
        match tag {
            b'T' => {
                let n = u16::from_be_bytes([body[0], body[1]]) as usize;
                let mut i = 2;
                for _ in 0..n {
                    i += body[i..].iter().position(|b| *b == 0).unwrap() + 1 + 6;
                    let oid = u32::from_be_bytes(body[i..i + 4].try_into().unwrap());
                    i += 6;
                    let typmod = i32::from_be_bytes(body[i..i + 4].try_into().unwrap());
                    i += 6;
                    out.push((oid, typmod));
                }
            }
            b'E' => panic!("{sql}: {}", String::from_utf8_lossy(&body)),
            b'Z' => break,
            _ => {}
        }
    }
    out
}

/// The integer whose little-endian bytes are what an older build stored
/// for `text` in a `CHAR` cell: its UTF-8, cut at four bytes, zero-padded.
fn stored_by_older_build(text: &str) -> u32 {
    let mut cell = [0u8; 4];
    let n = text.len().min(4);
    cell[..n].copy_from_slice(&text.as_bytes()[..n]);
    u32::from_le_bytes(cell)
}

/// A file-backed database holding `lc (id INT, c CHAR, k CHAR UNIQUE, after
/// INT)` as an older build left it, `rows` being `(id, c, k)` as that build
/// was asked to store them (`None` is NULL). Dropped with its directory.
struct LegacyDb {
    root: std::path::PathBuf,
}

impl Drop for LegacyDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl LegacyDb {
    async fn make(name: &str, rows: &[(i32, Option<&str>, Option<&str>)]) -> LegacyDb {
        let root = std::env::temp_dir().join(format!("legacy_char_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let db = LegacyDb { root };
        {
            let server = TestServer::start_file_backed(&db.root_text()).await.expect("server");
            let c = client(&server).await;
            c.batch_execute("CREATE TABLE lc (id INT, c UINT4, k UINT4 UNIQUE, after INT)").await.expect("create");
            let cell = |v: &Option<&str>| v.map(|v| stored_by_older_build(v).to_string()).unwrap_or_else(|| "NULL".to_string());
            for chunk in rows.chunks(500) {
                let tuples: Vec<String> = chunk
                    .iter()
                    .map(|(id, value, key)| format!("({id}, {}, {}, {})", cell(value), cell(key), id * 10))
                    .collect();
                c.batch_execute(&format!("INSERT INTO lc VALUES {}", tuples.join(", ")))
                    .await
                    .expect("insert");
            }
            // Into the files, so the log does not replay the records over the
            // edit below.
            rasterizeddb_core::core::wal::checkpoint_all().expect("checkpoint");
        }
        // The field records of `c` and `k`, retyped from UINT4 (8) to the
        // legacy CHAR (13): the record an older build wrote for `CHAR`.
        let fields = db.root.join("lc_schema_FIELDS.db");
        let mut bytes = std::fs::read(&fields).expect("the field records");
        let mut retyped = 0;
        for record in bytes.chunks_mut(153) {
            let name_end = record[..129].iter().position(|b| *b == 0).unwrap_or(129);
            if matches!(&record[..name_end], b"c" | b"k") {
                assert_eq!(u16::from_le_bytes([record[129], record[130]]), 8, "a UINT4 record");
                record[129..131].copy_from_slice(&13u16.to_le_bytes());
                retyped += 1;
            }
        }
        assert_eq!(retyped, 2, "both columns retyped");
        std::fs::write(&fields, &bytes).expect("write the field records");
        // And the marker f89ed7ad recorded for every `CHAR` column: a
        // `character(n)` marker with no length, which makes a check added on
        // it cast the column `::bpchar`, as that binary's own checks were.
        let typ = db.root.join("lc_schema_TYP.db");
        let mut records = std::fs::read(&typ).unwrap_or_default();
        for column in ["c", "k"] {
            let mut record = vec![0u8; 134];
            let name = format!("\u{1}bpchar:{column}");
            record[..name.len()].copy_from_slice(name.as_bytes());
            records.extend_from_slice(&record);
        }
        std::fs::write(&typ, &records).expect("write the marker records");
        db
    }

    fn root_text(&self) -> String {
        self.root.to_string_lossy().into_owned()
    }

    async fn open(&self) -> TestServer {
        TestServer::start_file_backed(&self.root_text()).await.expect("open")
    }
}

/// What older builds stored reads back as written; a comparison finds it;
/// a new write is held to `character(1)` and stored so that it reads back,
/// after a restart too.
///
/// Mutations: the wire decoding a code point again (every non-ASCII value
/// reads wrong, `'€'` as a number); `legacy_char_literal` as the code
/// point (`c = 'é'` finds nothing); `legacy_char_assign` accepting any text
/// that fits (`'xyz'` stored, `'abcde'` as `'abcd'`); an expression reading
/// the raw cell (`CASE WHEN c = 'é'` is false).
#[tokio::test(flavor = "multi_thread")]
async fn a_legacy_char_column_reads_and_writes_its_text() {
    let db = LegacyDb::make(
        "rw",
        &[
            (1, Some("A"), Some("a")),
            (2, Some("é"), Some("b")),
            (3, Some("€"), Some("c")),
            (4, Some("😀"), Some("d")),
            (5, Some("ab"), Some("e")),
            // Cut to 'abcd' by the older build's write: lost there.
            (6, Some("abcde"), Some("f")),
            (7, Some(" "), Some("g")),
            (8, None, None),
        ],
    )
    .await;
    {
        let server = db.open().await;
        let c = client(&server).await;
        let wrong = check(
            &c,
            &[
                (
                    "SELECT id, c, c || '|', length(c), upper(c), c IS NULL FROM lc ORDER BY id",
                    "[1]|[A]|[A|]|[1]|[A]|[f];[2]|[é]|[é|]|[1]|[É]|[f];[3]|[€]|[€|]|[1]|[€]|[f];\
                     [4]|[😀]|[😀|]|[1]|[😀]|[f];[5]|[ab]|[ab|]|[2]|[AB]|[f];[6]|[abcd]|[abcd|]|[4]|[ABCD]|[f];\
                     [7]|[ ]|[|]|[0]|[]|[f];[8]|∅|∅|∅|∅|[t]",
                ),
                ("SELECT id FROM lc WHERE c = 'é'", "[2]"),
                ("SELECT id FROM lc WHERE c = '😀'", "[4]"),
                ("SELECT id FROM lc WHERE c = 'ab'", "[5]"),
                ("SELECT id FROM lc WHERE c IN ('é', '€') ORDER BY id", "[2];[3]"),
                (
                    "SELECT id, CASE WHEN c = 'é' THEN 'e' WHEN c = '😀' THEN 'yes' ELSE 'no' END, coalesce(c, 'x') = 'é' FROM lc WHERE id < 5 ORDER BY id",
                    "[1]|[no]|[f];[2]|[e]|[t];[3]|[no]|[f];[4]|[yes]|[f]",
                ),
                ("SELECT id FROM lc WHERE c LIKE 'é%'", "[2]"),
                ("SELECT max(c) FROM lc WHERE id < 5", "[😀]"),
                ("SELECT c, count(*) FROM lc WHERE id IN (1, 2, 4) GROUP BY c ORDER BY c", "[A]|[1];[é]|[1];[😀]|[1]"),
                ("SELECT a.id, b.c FROM lc a JOIN lc b ON a.id = b.id WHERE a.id IN (2, 3) ORDER BY a.id", "[2]|[é];[3]|[€]"),
                ("WITH w AS (SELECT id, c FROM lc) SELECT c FROM w WHERE id IN (2, 5) ORDER BY id", "[é];[ab]"),
                // New writes: one character, the rest spaces; '' is ' '.
                ("INSERT INTO lc VALUES (20, 'ö', 'h', 200)", ""),
                ("INSERT INTO lc VALUES (21, 'x  ', 'i', 210)", ""),
                ("INSERT INTO lc VALUES (22, '', 'j', 220)", ""),
                ("INSERT INTO lc VALUES (23, 'xyz', 'l', 230)", "ERR 22001"),
                ("INSERT INTO lc VALUES (23, 'abcde', 'l', 230)", "ERR 22001"),
                ("UPDATE lc SET c = 'ab' WHERE id = 1", "ERR 22001"),
                ("UPDATE lc SET c = c || 'z' WHERE id = 1", "ERR 22001"),
                ("UPDATE lc SET c = 'ß' WHERE id = 1", ""),
                ("UPDATE lc SET c = 5 WHERE id = 3", ""),
                ("UPDATE lc SET c = upper(c) WHERE id = 20", ""),
                ("CREATE TABLE lt (id INT, t TEXT)", ""),
                ("INSERT INTO lt SELECT id, c FROM lc WHERE id IN (2, 4, 5)", ""),
                ("SELECT id, t, length(t) FROM lt ORDER BY id", "[2]|[é]|[1];[4]|[😀]|[1];[5]|[ab]|[2]"),
                ("INSERT INTO lt VALUES (40, 'yy')", ""),
                ("INSERT INTO lc (id, c) SELECT id, t FROM lt WHERE id = 40", "ERR 22001"),
            ],
        )
        .await;
        let mut wrong = wrong;
        // A parameter takes the same way.
        if let Err(e) = c.execute("INSERT INTO lc (id, c, k) VALUES (30, $1, 'p')", &[&"ü"]).await {
            wrong.push(format!("parameter 'ü': {e}"));
        }
        match c.execute("INSERT INTO lc (id, c, k) VALUES (31, $1, 'q')", &[&"üü"]).await {
            Err(e) if e.code().map(|c| c.code()) == Some("22001") => {}
            other => wrong.push(format!("parameter 'üü': {other:?}")),
        }
        // Text on the wire, as it always was.
        let types = described(&server, "SELECT c FROM lc").await;
        if types != vec![(25, -1)] {
            wrong.push(format!("described {types:?}"));
        }
        assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
    }
    // What was written is what was stored.
    let server = db.open().await;
    let c = client(&server).await;
    let wrong = check(
        &c,
        &[
            (
                "SELECT id, c, c || '|' FROM lc WHERE id IN (1, 2, 3, 20, 21, 22, 30) ORDER BY id",
                "[1]|[ß]|[ß|];[2]|[é]|[é|];[3]|[5]|[5|];[20]|[Ö]|[Ö|];[21]|[x]|[x|];[22]|[ ]|[|];[30]|[ü]|[ü|]",
            ),
            ("SELECT count(*) FROM lc WHERE id IN (23, 31)", "[0]"),
            ("SELECT id FROM lc WHERE c = 'x' OR c = 'ü' ORDER BY id", "[21];[30]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// `ALTER COLUMN … TYPE character(1)`, `text` and `varchar(n)` convert a
/// legacy column, as PostgreSQL converts a `char` column; they were refused
/// as a rewrite not supported. A value the new type cannot hold refuses the
/// change (22001), as do two keys it makes one (23505), and the table is
/// left as it was.
///
/// Mutations: `retype_legacy_char` keeping the legacy bytes (the converted
/// values read as their four bytes); its unique-key test skipped (`' '` and
/// `''` become one key of a UNIQUE column); the ALTER arm refusing again.
#[tokio::test(flavor = "multi_thread")]
async fn alter_type_converts_a_legacy_char_column() {
    let db = LegacyDb::make(
        "alter",
        &[
            (1, Some("A"), Some("a")),
            (2, Some("é"), Some(" ")),
            (3, Some("😀"), Some("")),
            (4, Some("ab"), Some("d")),
            (5, Some("x "), None),
            (6, None, Some("f")),
        ],
    )
    .await;
    {
        let server = db.open().await;
        let c = client(&server).await;
        let wrong = check(
            &c,
            &[
                // 'ab' is two characters.
                ("ALTER TABLE lc ALTER COLUMN c TYPE character(1)", "ERR 22001"),
                ("SELECT c FROM lc WHERE id = 4", "[ab]"),
                ("ALTER TABLE lc ALTER COLUMN c TYPE varchar(1)", "ERR 22001"),
                ("DELETE FROM lc WHERE id = 4", ""),
                ("ALTER TABLE lc ALTER COLUMN c TYPE character(1)", ""),
                ("SELECT id, c, c || '|', length(c) FROM lc ORDER BY id", "[1]|[A]|[A|]|[1];[2]|[é]|[é|]|[1];[3]|[😀]|[😀|]|[1];[5]|[x]|[x|]|[1];[6]|∅|∅|∅"),
                ("SELECT id FROM lc WHERE c = 'x  '", "[5]"),
                ("INSERT INTO lc VALUES (7, 'yy', 'g', 70)", "ERR 22001"),
                // ' ' and '' were two keys; as character(1) they are one.
                ("ALTER TABLE lc ALTER COLUMN k TYPE character(1)", "ERR 23505"),
                ("SELECT id, k || '|' FROM lc WHERE id IN (2, 3) ORDER BY id", "[2]|[|];[3]|[|]"),
                ("ALTER TABLE lc ALTER COLUMN k TYPE text", "ERR 23505"),
                ("UPDATE lc SET k = 'c' WHERE id = 3", ""),
                ("ALTER TABLE lc ALTER COLUMN k TYPE text", ""),
                ("SELECT id, k || '|' FROM lc ORDER BY id", "[1]|[a|];[2]|[|];[3]|[c|];[5]|∅;[6]|[f|]"),
                ("INSERT INTO lc VALUES (8, 'z', 'a', 80)", "ERR 23505"),
                ("INSERT INTO lc VALUES (8, 'z', 'long text', 80)", ""),
            ],
        )
        .await;
        let mut wrong = wrong;
        let types = described(&server, "SELECT c, k FROM lc").await;
        if types != vec![(1042, 5), (25, -1)] {
            wrong.push(format!("described {types:?}"));
        }
        assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
    }
    let server = db.open().await;
    let c = client(&server).await;
    let wrong = check(
        &c,
        &[
            ("SELECT id, c, k FROM lc ORDER BY id", "[1]|[A]|[a];[2]|[é]|[];[3]|[😀]|[c];[5]|[x]|∅;[6]|∅|[f];[8]|[z]|[long text]"),
            ("SELECT id FROM lc WHERE c = 'é'", "[2]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A check on a legacy column is enforced, and an `ALTER … TYPE` that would
/// leave it unenforced is refused rather than reported done. The check here
/// is stored as f89ed7ad stored one on a `CHAR` column, `c::bpchar <> 'q'`,
/// which is enforced over `character(n)` and kept unenforced over text
/// (`compile_check`): `ALTER … TYPE text` used to succeed, and the next
/// `INSERT 'q'` went in. The same holds for a `character(n)` column's
/// `pg_dump`-spelled check; one without a `::bpchar` stays enforced over
/// text, as in PostgreSQL. PostgreSQL 17 keeps each check through the ALTER
/// and refuses every `'q'`, `'XY'` and `'zz'` below.
///
/// Mutation: `checks_stay_enforced` returning `Ok` (each ALTER to text
/// succeeds and the next violating INSERT is stored).
#[tokio::test(flavor = "multi_thread")]
async fn a_type_change_never_leaves_a_check_unenforced() {
    let db = LegacyDb::make("check", &[(1, Some("é"), Some("a")), (2, Some("x "), Some("b"))]).await;
    let server = db.open().await;
    let c = client(&server).await;
    let wrong = check(
        &c,
        &[
            ("ALTER TABLE lc ADD CONSTRAINT lc_c_check CHECK (c <> 'q')", ""),
            ("INSERT INTO lc (id, c) VALUES (3, 'q')", "ERR 23514"),
            ("ALTER TABLE lc ALTER COLUMN c TYPE text", "ERR 0A000"),
            ("ALTER TABLE lc ALTER COLUMN c TYPE varchar(3)", "ERR 0A000"),
            ("INSERT INTO lc (id, c) VALUES (3, 'q')", "ERR 23514"),
            ("ALTER TABLE lc ALTER COLUMN c TYPE character(1)", ""),
            ("INSERT INTO lc (id, c) VALUES (3, 'q')", "ERR 23514"),
            ("INSERT INTO lc (id, c) VALUES (3, 'q  ')", "ERR 23514"),
            ("ALTER TABLE lc ALTER COLUMN c TYPE text", "ERR 0A000"),
            ("INSERT INTO lc (id, c) VALUES (3, 'q')", "ERR 23514"),
            ("SELECT id, c FROM lc ORDER BY id", "[1]|[é];[2]|[x]"),
            // A character(n) column's own checks, as pg_dump writes one and
            // as one is written by hand.
            (
                "CREATE TABLE m (id INT, tag CHAR(2) CHECK (tag = ANY (ARRAY['AB'::bpchar, 'CD'::bpchar])), \
                 t2 CHAR(2) CHECK (t2 <> 'zz'))",
                "",
            ),
            ("INSERT INTO m VALUES (1, 'AB', 'ok')", ""),
            ("ALTER TABLE m ALTER COLUMN tag TYPE text", "ERR 0A000"),
            ("INSERT INTO m VALUES (2, 'XY', 'ok')", "ERR 23514"),
            ("ALTER TABLE m ALTER COLUMN t2 TYPE text", ""),
            ("INSERT INTO m VALUES (3, 'CD', 'zz')", "ERR 23514"),
            ("SELECT id, tag, t2 FROM m ORDER BY id", "[1]|[AB]|[ok]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// Every comparison reads a legacy cell as the text it shows, in
/// `character(1)`'s terms: trailing spaces do not count and `''` is `' '`.
/// An older build stored one value several ways — `''` as four zeros, `' '`
/// and `'a '` with their spaces, `'aé€'` cut to `61 c3 a9 e2` — and the
/// comparator read the four bytes as a `u32`: `c BETWEEN 'a' AND 'z'` missed
/// `'b'`, `LIKE '%b'` matched nothing, `c = 'a'` missed the `'a '` cell
/// showing `'a'`, `c = ''` missed the zero cell, the two blanks grouped
/// apart, `c = 'hello'` was refused and `c = true` answered nothing.
/// Expectations for one-character values are PostgreSQL 17's for a `char`
/// column under `COLLATE "C"` (the engine's text order); the cells only an
/// older build could write answer as the text they show.
///
/// Mutations: `legacy_char_operand` returning `None` (the comparisons reach
/// the `u32` comparator: ranges and patterns wrong); `legacy_char_text`
/// keeping trailing spaces (`'a '`, `' '` unreachable, groups split);
/// the group key left raw (`''` and `' '` two groups).
#[tokio::test(flavor = "multi_thread")]
async fn a_legacy_char_cell_compares_as_the_text_it_shows() {
    let rows: Vec<(i32, Option<&str>, Option<&str>)> = vec![
        (1, Some("A"), Some("A")),
        (2, Some("é"), Some("x")),
        (3, Some("€"), None),
        (4, Some("😀"), None),
        (5, Some("ab"), Some("ab")),
        (6, Some("abcde"), None),
        (7, Some(""), None),
        (8, None, None),
        (9, Some(" "), None),
        (10, Some("a "), Some("a")),
        (11, Some("z"), None),
        (12, Some("B"), None),
        (13, Some("aé€"), None),
        (14, Some("a"), None),
        (15, Some("b"), None),
    ];
    let db = LegacyDb::make("text", &rows).await;
    let server = db.open().await;
    let c = client(&server).await;
    let ids = |sql: &str| format!("SELECT id FROM lc WHERE {sql} ORDER BY id");
    let cases: Vec<(String, &str)> = vec![
        (
            "SELECT id, c, c || '|', length(c) FROM lc ORDER BY id".to_string(),
            "[1]|[A]|[A|]|[1];[2]|[é]|[é|]|[1];[3]|[€]|[€|]|[1];[4]|[😀]|[😀|]|[1];[5]|[ab]|[ab|]|[2];\
             [6]|[abcd]|[abcd|]|[4];[7]|[ ]|[|]|[0];[8]|∅|∅|∅;[9]|[ ]|[|]|[0];[10]|[a]|[a|]|[1];\
             [11]|[z]|[z|]|[1];[12]|[B]|[B|]|[1];[13]|[aé]|[aé|]|[2];[14]|[a]|[a|]|[1];[15]|[b]|[b|]|[1]",
        ),
        (ids("c = ''"), "[7,9]"),
        (ids("c = ' '"), "[7,9]"),
        (ids("c = 'a'"), "[10,14]"),
        (ids("c = 'a '"), "[10,14]"),
        (ids("c = 'aé'"), "[13]"),
        (ids("c = 'hello'"), "∅"),
        (ids("c <> 'hello'"), "[1,2,3,4,5,6,7,9,10,11,12,13,14,15]"),
        (ids("c IN ('é', 'hello')"), "[2]"),
        (ids("c IN ('a ', ' ')"), "[7,9,10,14]"),
        (ids("c BETWEEN 'a' AND 'z'"), "[5,6,10,11,13,14,15]"),
        (ids("c < 'b'"), "[1,5,6,7,9,10,12,13,14]"),
        (ids("c > 'z'"), "[2,3,4]"),
        (ids("c LIKE 'a%'"), "[5,6,10,13,14]"),
        (ids("c NOT LIKE 'a%'"), "[1,2,3,4,7,9,11,12,15]"),
        (ids("c LIKE '%b'"), "[5,15]"),
        (ids("c LIKE '_'"), "[1,2,3,4,7,9,10,11,12,14,15]"),
        (ids("c LIKE ' '"), "[7,9]"),
        (ids("c ILIKE 'A%'"), "[1,5,6,10,13,14]"),
        (ids("c SIMILAR TO '(a|b)%'"), "[5,6,10,13,14,15]"),
        (ids("c IS NULL"), "[8]"),
        (ids("c = k"), "[1,5,10]"),
        (ids("CASE WHEN c = ' ' THEN true ELSE false END"), "[7,9]"),
        ("SELECT c, count(*) FROM lc GROUP BY c ORDER BY c".to_string(),
         "[ ]|[2];[A]|[1];[B]|[1];[a]|[2];[ab]|[1];[abcd]|[1];[aé]|[1];[b]|[1];[z]|[1];[é]|[1];[€]|[1];[😀]|[1];∅|[1]"),
        ("SELECT count(DISTINCT c) FROM lc".to_string(), "[12]"),
        ("SELECT max(c), min(c) FROM lc".to_string(), "[😀]|[ ]"),
        ("SELECT id FROM lc ORDER BY c, id LIMIT 4".to_string(), "[7];[9];[1];[12]"),
    ];
    // `[7,9]` is the rows `[7];[9]`, and `∅` none.
    let cases: Vec<(String, String)> = cases
        .into_iter()
        .map(|(s, w)| {
            let w = if w == "∅" {
                String::new()
            } else if w.starts_with('[') && w.ends_with(']') && w[1..w.len() - 1].bytes().all(|b| b.is_ascii_digit() || b == b',') {
                w[1..w.len() - 1].split(',').map(|id| format!("[{id}]")).collect::<Vec<_>>().join(";")
            } else {
                w.to_string()
            };
            (s, w)
        })
        .collect();
    let cases: Vec<(&str, &str)> = cases.iter().map(|(s, w)| (s.as_str(), w.as_str())).collect();
    let wrong = check(&c, &cases).await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// The same over 3000 rows, with the filter on (its threshold lowered to
/// take the table), and through an index on the column: neither the filter's
/// zone bounds nor its fingerprints nor the index hold the cell's text, so
/// none of them may prune a comparison of it. Byte order: `'a'` and `'ab'`
/// are below `'b'`, so `c < 'b'` is 1000; it was 500.
///
/// Mutation: `sarg` pruning a legacy `CHAR` predicate again, together with
/// `legacy_char_operand` returning `None`.
#[tokio::test(flavor = "multi_thread")]
async fn a_large_legacy_char_table_answers_by_text_with_the_filter_and_an_index() {
    rasterizeddb_core::filter_settings().update_global(|o| o.min_table_rows = Some(100));
    let values = ["a", "ab", "b", "ba", "c", "é"];
    let rows: Vec<(i32, Option<&str>, Option<&str>)> =
        (0..3000).map(|i| (i, Some(values[(i % 6) as usize]), None)).collect();
    let db = LegacyDb::make("large", &rows).await;
    let server = db.open().await;
    let c = client(&server).await;
    let count = |sql: &str| format!("SELECT count(*) FROM lc WHERE {sql}");
    let shapes = [
        ("c < 'b'", "[1000]"),
        ("c BETWEEN 'a' AND 'b'", "[1500]"),
        ("c > 'c'", "[500]"),
        ("c LIKE 'a%'", "[1000]"),
        ("c NOT LIKE 'a%'", "[2000]"),
        ("c LIKE '%b'", "[1000]"),
        ("c ILIKE 'A%'", "[1000]"),
        ("c SIMILAR TO '(a|b)%'", "[2000]"),
        ("c LIKE '_'", "[2000]"),
        ("c = 'ab'", "[500]"),
        ("c = 'a  '", "[500]"),
        ("c IN ('ba', 'é')", "[1000]"),
    ];
    let mut wrong = Vec::new();
    for round in ["plain", "indexed"] {
        if round == "indexed" {
            c.batch_execute("CREATE INDEX lc_c ON lc (c)").await.expect("index");
        }
        let cases: Vec<(String, &str)> = shapes.iter().map(|(s, w)| (count(s), *w)).collect();
        let cases: Vec<(&str, &str)> = cases.iter().map(|(s, w)| (s.as_str(), *w)).collect();
        wrong.extend(check(&c, &cases).await.into_iter().map(|w| format!("{round}: {w}")));
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
