//! `character(n)` as a real type, and casts that keep their type modifiers.
//!
//! A `character(n)` column used to be text with a marker beside it, so
//! `'b'`, `'b '` and `'b  '` were three primary keys, `k = 'a'` missed a
//! stored `'a  '`, and every check reading the column was kept unenforced.
//! Now the declared length is recorded (`type_modifier::TextTypmod`), every
//! value is held to it and stored without its trailing spaces, every
//! literal compared with the column is read the same way, and the value is
//! shown padded. Casts apply their modifiers: `'abcd'::varchar(2)` is
//! `'ab'` and `ts::timestamp(0)` rounds.
//!
//! Every expectation here is PostgreSQL 17's answer to the same statement.

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

/// The `(type OID, atttypmod)` of each column `sql` returns, read from the
/// RowDescription over a bare v3 connection: no client library shows the
/// type modifier.
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

/// `'b'`, `'b '` and `'b  '` are one `character(3)` value, so one key: to
/// the primary key, a UNIQUE column, a composite UNIQUE, the ON CONFLICT
/// arbiter (DO NOTHING and DO UPDATE), the multi-row check for a second
/// update of one row (21000) and a prepared statement's parameter. Each was
/// a second row before.
///
/// Mutations: `assign_bytes` keeping the trailing spaces (every 23505 here
/// becomes a row); `stored_key_bytes` leaving the key as written (the 21000
/// becomes two rows).
#[tokio::test(flavor = "multi_thread")]
async fn a_character_key_is_one_value_however_it_is_padded() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE ck (k CHAR(3) PRIMARY KEY, v INT); \
         CREATE TABLE cu (id INT PRIMARY KEY, k CHAR(4) UNIQUE); \
         CREATE TABLE cc (a CHAR(2), b INT, UNIQUE (a, b))",
    )
    .await
    .expect("setup");
    let mut wrong = check(
        &c,
        &[
            ("INSERT INTO ck VALUES ('b', 1)", ""),
            ("INSERT INTO ck VALUES ('b ', 2)", "ERR 23505"),
            ("INSERT INTO ck VALUES ('b  ', 3)", "ERR 23505"),
            ("INSERT INTO ck VALUES ('a', 6), ('a ', 7)", "ERR 23505"),
            ("INSERT INTO ck VALUES ('', 8)", ""),
            ("INSERT INTO ck VALUES ('   ', 9)", "ERR 23505"),
            ("INSERT INTO ck VALUES ('x', 9) ON CONFLICT (k) DO UPDATE SET v = 90", ""),
            ("INSERT INTO ck VALUES ('x  ', 10) ON CONFLICT (k) DO UPDATE SET v = excluded.v", ""),
            ("SELECT k, v FROM ck WHERE k = 'x'", "[x  ]|[10]"),
            ("INSERT INTO ck VALUES ('y', 11), ('y ', 12) ON CONFLICT (k) DO UPDATE SET v = excluded.v", "ERR 21000"),
            ("INSERT INTO ck VALUES ('z', 13) ON CONFLICT (k) DO NOTHING", ""),
            ("INSERT INTO ck VALUES ('z  ', 14) ON CONFLICT (k) DO NOTHING", ""),
            ("INSERT INTO ck VALUES ('z ', 15) ON CONFLICT DO NOTHING", ""),
            ("UPDATE ck SET k = 'z ' WHERE k = 'x'", "ERR 23505"),
            ("SELECT k, v FROM ck ORDER BY k", "[   ]|[8];[b  ]|[1];[x  ]|[10];[z  ]|[13]"),
            ("SELECT count(*), count(DISTINCT k) FROM ck", "[4]|[4]"),
            ("INSERT INTO cu VALUES (1, 'ab')", ""),
            ("INSERT INTO cu VALUES (2, 'ab ')", "ERR 23505"),
            ("INSERT INTO cu VALUES (3, 'ab  ')", "ERR 23505"),
            ("INSERT INTO cc VALUES ('a', 1)", ""),
            ("INSERT INTO cc VALUES ('a ', 1)", "ERR 23505"),
            ("INSERT INTO cc VALUES ('a ', 2)", ""),
            ("SELECT a || '|', b FROM cc ORDER BY b", "[a|]|[1];[a|]|[2]"),
        ],
    )
    .await;
    // A bound parameter is a value like any other.
    match c.execute("INSERT INTO ck VALUES ($1, $2)", &[&"b  ", &50i32]).await {
        Err(e) if e.code().map(|c| c.code()) == Some("23505") => {}
        other => wrong.push(format!("parameter 'b  ': {other:?}")),
    }
    let found: Vec<i32> = c
        .query("SELECT v FROM ck WHERE k = $1", &[&"b "])
        .await
        .expect("parameter lookup")
        .iter()
        .map(|r| r.get(0))
        .collect();
    if found != vec![1] {
        wrong.push(format!("WHERE k = $1 ('b '): {found:?}"));
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A `character(n)` value compares without its trailing spaces wherever it
/// is compared — `=`, `<>`, `IN`, `BETWEEN`, `<`, a join with another width
/// or with text, `IN (SELECT …)`, a correlated `EXISTS`, a `CASE` — matches
/// a pattern padded (`LIKE`, `ILIKE`, `SIMILAR TO`), reads as text without
/// them (`||`, `length`, `::text`, `upper`), and is shown padded, on the
/// wire as `bpchar` (OID 1042, atttypmod n + 4) in text and in binary.
///
/// Mutations: `canonicalize_bpchar_literals` doing nothing (`k = 'b  '`
/// finds nothing); `padded_for_pattern` returning `None` (`LIKE 'b'`
/// matches); the `BPCHAR` arm of `reported_type_of` (OID 25, unpadded).
#[tokio::test(flavor = "multi_thread")]
async fn a_character_value_compares_and_shows_as_postgresql() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE cv (k CHAR(3), v INT); \
         INSERT INTO cv VALUES ('b', 1), ('abc', 5), ('', 8), ('a', 6), (NULL, 7); \
         CREATE TABLE cj (k CHAR(5), t TEXT, w INT); \
         INSERT INTO cj VALUES ('b', 'b', 1), ('abc', 'abc ', 2), ('a ', 'a', 3)",
    )
    .await
    .expect("setup");
    let mut wrong = check(
        &c,
        &[
            ("SELECT k, v FROM cv ORDER BY v", "[b  ]|[1];[abc]|[5];[a  ]|[6];∅|[7];[   ]|[8]"),
            (
                "SELECT k || '|', length(k), octet_length(k), char_length(k) FROM cv WHERE v <> 7 ORDER BY v",
                "[b|]|[1]|[3]|[1];[abc|]|[3]|[3]|[3];[a|]|[1]|[3]|[1];[|]|[0]|[3]|[0]",
            ),
            ("SELECT v FROM cv WHERE k = 'b'", "[1]"),
            ("SELECT v FROM cv WHERE k = 'b  '", "[1]"),
            ("SELECT v FROM cv WHERE k = 'b    '", "[1]"),
            ("SELECT v FROM cv WHERE 'b ' = k", "[1]"),
            ("SELECT v FROM cv WHERE k <> 'b ' ORDER BY v", "[5];[6];[8]"),
            ("SELECT v FROM cv WHERE k IN ('b ', 'abc') ORDER BY v", "[1];[5]"),
            ("SELECT v FROM cv WHERE k NOT IN ('b ', 'abc') ORDER BY v", "[6];[8]"),
            ("SELECT v FROM cv WHERE k > 'a ' ORDER BY v", "[1];[5]"),
            ("SELECT v FROM cv WHERE k BETWEEN 'a' AND 'b ' ORDER BY v", "[1];[5];[6]"),
            ("SELECT v FROM cv WHERE k = ''", "[8]"),
            ("SELECT v FROM cv WHERE k = '  '", "[8]"),
            ("SELECT v FROM cv WHERE k LIKE 'b' ORDER BY v", ""),
            ("SELECT v FROM cv WHERE k LIKE 'b  ' ORDER BY v", "[1]"),
            ("SELECT v FROM cv WHERE k LIKE 'b%' ORDER BY v", "[1]"),
            ("SELECT v FROM cv WHERE k LIKE '%  ' ORDER BY v", "[1];[6];[8]"),
            ("SELECT v FROM cv WHERE k LIKE '_' ORDER BY v", ""),
            ("SELECT v FROM cv WHERE k NOT LIKE 'a%' ORDER BY v", "[1];[8]"),
            ("SELECT v FROM cv WHERE k ILIKE 'B__' ORDER BY v", "[1]"),
            ("SELECT v FROM cv WHERE k SIMILAR TO 'a  ' ORDER BY v", "[6]"),
            (
                "SELECT data_type FROM information_schema.columns WHERE table_name = 'cv' AND column_name = 'k'",
                "[character]",
            ),
            (
                "SELECT k::text || '|', k::varchar || '|', upper(k) || '|' FROM cv WHERE v < 7 ORDER BY v",
                "[b|]|[b|]|[B|];[abc|]|[abc|]|[ABC|];[a|]|[a|]|[A|]",
            ),
            ("SELECT k FROM cv ORDER BY k DESC", "∅;[b  ];[abc];[a  ];[   ]"),
            ("SELECT k, count(*) FROM cv GROUP BY k ORDER BY k", "[   ]|[1];[a  ]|[1];[abc]|[1];[b  ]|[1];∅|[1]"),
            ("SELECT min(k), max(k) FROM cv", "[   ]|[b  ]"),
            ("SELECT cv.v, cj.w FROM cv JOIN cj ON cv.k = cj.k ORDER BY cv.v", "[1]|[1];[5]|[2];[6]|[3]"),
            ("SELECT cv.k, cj.k FROM cv JOIN cj ON cv.k = cj.k ORDER BY cv.v", "[b  ]|[b    ];[abc]|[abc  ];[a  ]|[a    ]"),
            ("SELECT cv.v, cj.w FROM cv JOIN cj ON cv.k = cj.t ORDER BY cv.v", "[1]|[1];[6]|[3]"),
            ("SELECT v FROM cv WHERE k IN (SELECT k FROM cj) ORDER BY v", "[1];[5];[6]"),
            ("SELECT v FROM cv WHERE EXISTS (SELECT 1 FROM cj WHERE cj.k = cv.k) ORDER BY v", "[1];[5];[6]"),
            ("SELECT v, CASE WHEN k = 'a ' THEN 'y' ELSE 'n' END FROM cv ORDER BY v", "[1]|[n];[5]|[n];[6]|[y];[7]|[n];[8]|[n]"),
            ("SELECT v, COALESCE(k, 'zz') || '|' FROM cv WHERE v > 5 ORDER BY v", "[6]|[a|];[7]|[zz|];[8]|[|]"),
            ("SELECT k FROM cv WHERE v = 1 UNION ALL SELECT k FROM cv WHERE v = 6", "[b  ];[a  ]"),
            // `character` UNION `text` is `bpchar` with no length: a text
            // branch's value is shown as it is.
            ("SELECT k FROM cv WHERE v = 99 UNION ALL SELECT t FROM cj WHERE w = 1", "[b]"),
            ("UPDATE cv SET v = v + 100 WHERE k = 'b  ' RETURNING k, k || '|'", "[b  ]|[b|]"),
            ("DELETE FROM cv WHERE k = 'abc   ' RETURNING k", "[abc]"),
        ],
    )
    .await;
    // `bpchar` on the wire, with its length, as both a single-relation and
    // a join select describe it.
    for (sql, want) in [
        ("SELECT k, v FROM cv", vec![(1042, 7), (23, -1)]),
        ("SELECT k || '' AS t, k FROM cv", vec![(25, -1), (1042, 7)]),
        ("SELECT cv.k, cj.k FROM cv JOIN cj ON cv.k = cj.k", vec![(1042, 7), (1042, 9)]),
        ("UPDATE cv SET v = v WHERE v = 6 RETURNING k", vec![(1042, 7)]),
        ("SELECT k FROM cv UNION ALL SELECT k FROM cv", vec![(1042, 7)]),
        ("SELECT k FROM cv UNION ALL SELECT t FROM cj", vec![(1042, -1)]),
    ] {
        let got = described(&server, sql).await;
        if got != want {
            wrong.push(format!("{sql}: described {got:?}, want {want:?}"));
        }
    }
    // Binary (the extended protocol, tokio-postgres): `bpcharsend` is the
    // padded text.
    let statement = c.prepare("SELECT k FROM cv WHERE v = 6").await.expect("prepare");
    if statement.columns()[0].type_() != &tokio_postgres::types::Type::BPCHAR {
        wrong.push(format!("prepared type: {:?}", statement.columns()[0].type_()));
    }
    let k: String = c.query_one(&statement, &[]).await.expect("binary").get(0);
    if k != "a  " {
        wrong.push(format!("binary value: {k:?}"));
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A value past a declared `character(n)` or `character varying(n)` length
/// is an error (22001), unless every character past it is a space, which is
/// cut: on every write path — a literal, an expression, a DEFAULT, a bound
/// parameter, `INSERT … SELECT`, `UPDATE` with a literal and with an
/// expression, and `ON CONFLICT DO UPDATE`. Each stored the value whole.
///
/// Mutation: `TextTypmod::assign` keeping the trailing spaces (the
/// `character(3)` values come back unstripped). The table-level
/// `hold_text_lengths` is the backstop behind the encoders: disabled alone it
/// changes no answer here, every path tested being held before it.
#[tokio::test(flavor = "multi_thread")]
async fn a_declared_length_is_held_on_every_write_path() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE cl (id INT PRIMARY KEY, s VARCHAR(3), c CHAR(3), d CHAR(2) DEFAULT 'q  ', e VARCHAR(2)); \
         CREATE TABLE cd (id INT PRIMARY KEY, e VARCHAR(2) DEFAULT 'abc')",
    )
    .await
    .expect("setup");
    let mut wrong = check(
        &c,
        &[
            ("INSERT INTO cl (id, s, c) VALUES (1, 'abc', 'abc')", ""),
            ("INSERT INTO cl (id, s, c) VALUES (2, 'abcd', 'a')", "ERR 22001"),
            ("INSERT INTO cl (id, s, c) VALUES (3, 'ab   ', 'ab   ')", ""),
            ("INSERT INTO cl (id, s, c) VALUES (4, 'a', 'abcd')", "ERR 22001"),
            ("INSERT INTO cl (id, s) VALUES (5, 'x' || 'yzw')", "ERR 22001"),
            ("INSERT INTO cl (id, c) VALUES (6, 'x' || 'y  ')", ""),
            ("INSERT INTO cl (id, s, c, e) VALUES (7, 'q', 'q', 'ok')", ""),
            ("INSERT INTO cl SELECT 8, 'longer', 'x', 'ok'", "ERR 22001"),
            ("INSERT INTO cl SELECT 9, 'x', 'longer', 'ok'", "ERR 22001"),
            ("UPDATE cl SET s = 'wxyz' WHERE id = 1", "ERR 22001"),
            ("UPDATE cl SET s = s || 'zz' WHERE id = 1", "ERR 22001"),
            ("UPDATE cl SET c = c || 'zz' WHERE id = 1", "ERR 22001"),
            ("UPDATE cl SET c = 'q' || '  ' WHERE id = 1", ""),
            (
                "INSERT INTO cl (id, s, c, e) VALUES (1, 'a', 'a', 'ok') ON CONFLICT (id) DO UPDATE SET c = 'toolong'",
                "ERR 22001",
            ),
            (
                "INSERT INTO cl (id, s, c, e) VALUES (1, 'a', 'a', 'ok') ON CONFLICT (id) DO UPDATE SET s = excluded.s || 'b  '",
                "",
            ),
            (
                "SELECT id, s || '|', c || '|', c, d || '|', e FROM cl ORDER BY id",
                "[1]|[ab |]|[q|]|[q  ]|[q|]|∅;[3]|[ab |]|[ab|]|[ab ]|[q|]|∅;[6]|∅|[xy|]|[xy ]|[q|]|∅;[7]|[q|]|[q|]|[q  ]|[q|]|[ok]",
            ),
        ],
    )
    .await;
    // The DEFAULT 'abc' of a VARCHAR(2) is past it when it is used.
    match rows(&c, "INSERT INTO cd (id) VALUES (1)").await {
        Err(code) if code == "22001" => {}
        other => wrong.push(format!("DEFAULT 'abc' into VARCHAR(2): {other:?}")),
    }
    for (param, want) in [("abcdef", "22001"), ("ab  ", "ok")] {
        let got = match c.execute("INSERT INTO cl (id, s, e) VALUES (20 + length($1::text), $1, 'ok')", &[&param]).await {
            Ok(_) => "ok".to_string(),
            Err(e) => e.code().map(|c| c.code().to_string()).unwrap_or_else(|| e.to_string()),
        };
        if got != want {
            wrong.push(format!("parameter {param:?}: {got}, want {want}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// Over thousands of rows a `character(n)` column is answered through its
/// B-tree, hash and UNIQUE indexes and the filter index exactly as an
/// unindexed twin answers, with literals written padded and not; and after a
/// restart, when the length is read back from its record and the indexes
/// are rebuilt from the stored values.
///
/// Mutation: `canonicalize_bpchar_literals` doing nothing (the padded
/// literals find nothing, through the indexes and the twin alike).
#[tokio::test(flavor = "multi_thread")]
async fn character_indexes_answer_like_an_unindexed_twin() {
    let root = std::env::temp_dir().join(format!("char_typmod_ix_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let root_text = root.to_string_lossy().into_owned();
    let mut server = TestServer::start_file_backed(&root_text).await.expect("server");
    let mut c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE ix (id INT PRIMARY KEY, k CHAR(4), h CHAR(4), u CHAR(6) UNIQUE); \
         CREATE INDEX ix_k ON ix (k); \
         CREATE INDEX ix_h ON ix USING hash (h); \
         CREATE TABLE tw (id INT PRIMARY KEY, k CHAR(4), h CHAR(4), u CHAR(6))",
    )
    .await
    .expect("setup");
    // `k` cycles through 20 short codes, written with and without trailing
    // spaces; `u` is unique, written padded on odd ids.
    let code = |i: i32| format!("c{}", i % 20);
    for chunk in 0..8 {
        let values: Vec<String> = (0..500)
            .map(|j| {
                let i = chunk * 500 + j;
                let pad = if i % 3 == 0 { "  " } else if i % 3 == 1 { " " } else { "" };
                let unique = format!("u{}{}", i, if i % 2 == 1 { " " } else { "" });
                format!("({i}, '{}{pad}', '{}{pad}', '{unique}')", code(i), code(i))
            })
            .collect();
        for table in ["ix", "tw"] {
            c.batch_execute(&format!("INSERT INTO {table} VALUES {}", values.join(",")))
                .await
                .unwrap_or_else(|e| panic!("insert {table}: {e}"));
        }
    }
    let model = |pred: &dyn Fn(i32) -> bool| (0..4000).filter(|i| pred(*i)).count().to_string();
    let n = |c: i32| move |i: i32| i % 20 == c;
    let cases: Vec<(String, String)> = vec![
        ("k = 'c7'".into(), model(&n(7))),
        ("k = 'c7  '".into(), model(&n(7))),
        ("h = 'c7 '".into(), model(&n(7))),
        ("h = 'c13'".into(), model(&n(13))),
        ("k IN ('c1 ', 'c2', 'c3   ')".into(), model(&|i| (1..=3).contains(&(i % 20)))),
        ("k > 'c1 ' AND k < 'c2'".into(), model(&|i| (10..20).contains(&(i % 20)))),
        ("h BETWEEN 'c3' AND 'c4  '".into(), model(&|i| matches!(i % 20, 3 | 4))),
        ("u = 'u101'".into(), "1".into()),
        ("u = 'u100 '".into(), "1".into()),
        ("u = 'u3999  '".into(), "1".into()),
        ("k = 'c7' AND h = 'c7   '".into(), model(&n(7))),
        ("k = 'zz '".into(), "0".into()),
    ];
    for round in 0..2 {
        let mut wrong = Vec::new();
        for (predicate, want) in &cases {
            for table in ["ix", "tw"] {
                let sql = format!("SELECT count(*) FROM {table} WHERE {predicate}");
                match rows(&c, &sql).await {
                    Ok(r) if r == vec![format!("[{want}]")] => {}
                    other => wrong.push(format!("{sql}: {other:?}, want {want}")),
                }
            }
        }
        // A key padded differently is still a duplicate of the one stored.
        match rows(&c, "INSERT INTO ix VALUES (9999, 'c1', 'c1', 'u7  ')").await {
            Err(code) if code == "23505" => {}
            other => wrong.push(format!("duplicate 'u7  ': {other:?}")),
        }
        assert!(wrong.is_empty(), "round {round}: {} wrong:\n{}", wrong.len(), wrong.join("\n"));
        drop(c);
        drop(server);
        server = TestServer::start_file_backed(&root_text).await.expect("restart");
        c = client(&server).await;
    }
}

/// Casts keep their type modifiers, in the select list and in `WHERE`:
/// `varchar(n)` and `character(n)` cut (and `character(n)` pads and compares
/// without trailing spaces), `timestamp(p)`, `time(p)` and `interval(p)`
/// round half away from zero, on PostgreSQL's epoch. They were plain casts:
/// `WHERE s::varchar(2) = 'ab'` matched only 'ab' itself. (A `timestamp` is
/// shown with `+00` here, as this engine shows every timestamp; the instants
/// are PostgreSQL's.)
///
/// Mutation: `apply_cast_typmod` returning the value unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn casts_keep_their_type_modifiers() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE tc (id INT PRIMARY KEY, t TIMESTAMP, s TEXT, tm TIME, iv INTERVAL); \
         INSERT INTO tc VALUES (1, '2024-01-01 10:00:00.6', 'abcd', '10:00:00.56', '1 second 600 milliseconds'), \
         (2, '2024-01-01 10:00:00.4', 'ab', '10:00:00.44', '400 milliseconds'), \
         (3, '2024-01-01 10:00:01', 'ac', '23:59:59.5', '-1.5 seconds'), \
         (4, '1999-12-31 23:59:59.5', 'a  ', '10:00:00', '1 day 00:00:00.567')",
    )
    .await
    .expect("setup");
    let wrong = check(
        &c,
        &[
            (
                "SELECT id, s::varchar(2), s::char(3) || '|', s::char(3), s::character varying(1) FROM tc ORDER BY id",
                "[1]|[ab]|[abc|]|[abc]|[a];[2]|[ab]|[ab|]|[ab ]|[a];[3]|[ac]|[ac|]|[ac ]|[a];[4]|[a ]|[a|]|[a  ]|[a]",
            ),
            (
                "SELECT id, t::timestamp(0), tm::time(1), iv::interval(0) FROM tc ORDER BY id",
                "[1]|[2024-01-01 10:00:01+00]|[10:00:00.6]|[00:00:02];\
                 [2]|[2024-01-01 10:00:00+00]|[10:00:00.4]|[00:00:00];\
                 [3]|[2024-01-01 10:00:01+00]|[23:59:59.5]|[-00:00:02];\
                 [4]|[1999-12-31 23:59:59+00]|[10:00:00]|[1 day 00:00:01]",
            ),
            ("SELECT id, tm::time(0), t::timestamp(2) FROM tc WHERE id = 3", "[3]|[24:00:00]|[2024-01-01 10:00:01+00]"),
            ("SELECT id FROM tc WHERE s::varchar(2) = 'ab' ORDER BY id", "[1];[2]"),
            ("SELECT id FROM tc WHERE CAST(s AS varchar(1)) = 'a' ORDER BY id", "[1];[2];[3];[4]"),
            ("SELECT id FROM tc WHERE s::char(3) = 'ab' ORDER BY id", "[2]"),
            ("SELECT id FROM tc WHERE s::char(5) = 'a ' ORDER BY id", "[4]"),
            ("SELECT id FROM tc WHERE s::char(5) IN ('ab ', 'ac') ORDER BY id", "[2];[3]"),
            ("SELECT id FROM tc WHERE s::char(3) LIKE 'ab ' ORDER BY id", "[2]"),
            ("SELECT id FROM tc WHERE s::varchar(3) LIKE 'ab_' ORDER BY id", "[1]"),
            ("SELECT id FROM tc WHERE t::timestamp(0) = '2024-01-01 10:00:01' ORDER BY id", "[1];[3]"),
            ("SELECT id FROM tc WHERE t::timestamp(0) < '2000-01-01' ORDER BY id", "[4]"),
            ("UPDATE tc SET s = s::varchar(1) WHERE id = 1 RETURNING s", "[a]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// `ALTER COLUMN … TYPE` to and from `character(n)` and `character
/// varying(n)` brings the stored values to the new type, as PostgreSQL's
/// rewrite does: past the length 22001, two keys that become one 23505,
/// `character(n)` values stored and compared without their trailing spaces
/// and shown padded, and to text without them. Each was a marker change
/// that left 'ab ' and 'ab' apart under `character(3)`.
///
/// Mutation: `text_type_change` writing the marker without the rewrite
/// (`s = 'ab'` finds one row of two).
#[tokio::test(flavor = "multi_thread")]
async fn alter_type_to_and_from_character_rewrites_as_postgresql() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE ca (id INT PRIMARY KEY, s TEXT, c CHAR(4), u TEXT UNIQUE); \
         INSERT INTO ca VALUES (1, 'ab ', 'xy', 'p'), (2, 'ab', 'xy  ', 'p ')",
    )
    .await
    .expect("setup");
    let wrong = check(
        &c,
        &[
            ("ALTER TABLE ca ALTER COLUMN s TYPE CHAR(3)", ""),
            ("SELECT id, s, s || '|' FROM ca ORDER BY id", "[1]|[ab ]|[ab|];[2]|[ab ]|[ab|]"),
            ("SELECT id FROM ca WHERE s = 'ab' ORDER BY id", "[1];[2]"),
            ("ALTER TABLE ca ALTER COLUMN c TYPE TEXT", ""),
            ("SELECT id, c || '|' FROM ca ORDER BY id", "[1]|[xy|];[2]|[xy|]"),
            ("ALTER TABLE ca ALTER COLUMN s TYPE CHAR(1)", "ERR 22001"),
            ("ALTER TABLE ca ALTER COLUMN s TYPE CHAR(5)", ""),
            ("SELECT id, s, s || '|' FROM ca ORDER BY id", "[1]|[ab   ]|[ab|];[2]|[ab   ]|[ab|]"),
            ("ALTER TABLE ca ALTER COLUMN s TYPE VARCHAR(5)", ""),
            ("SELECT id, s || '|' FROM ca ORDER BY id", "[1]|[ab|];[2]|[ab|]"),
            ("ALTER TABLE ca ALTER COLUMN s TYPE VARCHAR(1)", "ERR 22001"),
            ("ALTER TABLE ca ALTER COLUMN u TYPE CHAR(2)", "ERR 23505"),
            ("UPDATE ca SET u = 'q' WHERE id = 2", ""),
            ("ALTER TABLE ca ALTER COLUMN u TYPE CHAR(2)", ""),
            ("INSERT INTO ca VALUES (3, 'x', 'x', 'q ')", "ERR 23505"),
            ("INSERT INTO ca VALUES (4, 'x', 'x', 'abc')", "ERR 22001"),
            ("SELECT id, u FROM ca ORDER BY id", "[1]|[p ];[2]|[q ]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A `character(n)` column an older binary created is text with a marker
/// and no length: its values are stored as written, `'b'` and `'b '` may
/// both be keys, and its checks were kept unenforced. It opens as it was —
/// text semantics, checks unenforced, shown as text — rather than being
/// read half as one type and half as the other (a stored `'b '` that
/// `k = 'b '` no longer found). `ALTER COLUMN … TYPE character(n)` gives it
/// a length, converting the values; it refuses 23505 while two keys would
/// become one, and a row its now-enforced check is false for (23514).
///
/// The older binary's files are made by writing its marker record — a
/// `character(n)` marker with no length — into a TEXT column's sidecar.
///
/// Mutation: `text_typmod_of` reading a marker with no length as
/// `character(1)` (the `'b '` key is no longer found).
#[tokio::test(flavor = "multi_thread")]
async fn a_character_column_an_older_binary_created_keeps_text_semantics() {
    let root = std::env::temp_dir().join(format!("char_typmod_legacy_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let root_text = root.to_string_lossy().into_owned();
    {
        let server = TestServer::start_file_backed(&root_text).await.expect("server");
        let c = client(&server).await;
        c.batch_execute(
            "CREATE TABLE lg (k TEXT PRIMARY KEY, v INT, t TEXT, CONSTRAINT lg_t_check CHECK (t::bpchar IN ('AB', 'CD'))); \
             INSERT INTO lg VALUES ('b', 1, 'AB'), ('b ', 2, 'AB '), ('c  ', 3, 'ZZ')",
        )
        .await
        .expect("setup");
    }
    // The older binary's records: `character(n)` with no length, for `k` and `t`.
    let typ = root.join("lg_schema_TYP.db");
    let mut bytes = std::fs::read(&typ).unwrap_or_default();
    for column in ["k", "t"] {
        let mut record = vec![0u8; 134];
        let name = format!("\u{1}bpchar:{column}");
        record[..name.len()].copy_from_slice(name.as_bytes());
        bytes.extend_from_slice(&record);
    }
    std::fs::write(&typ, &bytes).expect("write the marker records");

    let server = TestServer::start_file_backed(&root_text).await.expect("reopen");
    let c = client(&server).await;
    let mut wrong = check(
        &c,
        &[
            ("SELECT k || '|', v FROM lg ORDER BY v", "[b|]|[1];[b |]|[2];[c  |]|[3]"),
            ("SELECT v FROM lg WHERE k = 'b ' ORDER BY v", "[2]"),
            ("INSERT INTO lg VALUES ('b  ', 4, 'XX')", ""),
            ("ALTER TABLE lg ALTER COLUMN k TYPE CHAR(3)", "ERR 23505"),
            ("DELETE FROM lg WHERE v IN (2, 4)", ""),
            ("ALTER TABLE lg ALTER COLUMN k TYPE CHAR(3)", ""),
            ("SELECT k, k || '|', v FROM lg ORDER BY v", "[b  ]|[b|]|[1];[c  ]|[c|]|[3]"),
            ("SELECT v FROM lg WHERE k = 'c' ORDER BY v", "[3]"),
            ("INSERT INTO lg VALUES ('c', 9, 'AB')", "ERR 23505"),
            ("ALTER TABLE lg ALTER COLUMN t TYPE CHAR(4)", "ERR 23514"),
            ("UPDATE lg SET t = 'CD' WHERE v = 3", ""),
            ("ALTER TABLE lg ALTER COLUMN t TYPE CHAR(4)", ""),
            ("INSERT INTO lg VALUES ('e', 10, 'XX')", "ERR 23514"),
            ("INSERT INTO lg VALUES ('e', 10, 'AB  ')", ""),
        ],
    )
    .await;
    let described = described(&server, "SELECT k, t FROM lg").await;
    if described != vec![(1042, 7), (1042, 8)] {
        wrong.push(format!("described {described:?}"));
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
