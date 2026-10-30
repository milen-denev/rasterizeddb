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

/// A value typed text — a text column's value substituted from a subquery,
/// a parameter declared text, a `::text` cast — compares with a
/// `character(n)` column as text, exactly: `'ab  '` is not `'ab'`. Only an
/// untyped literal, and a `character varying` value, compare blank-padded.
/// Reading every string blank-padded updated rows PostgreSQL leaves alone.
///
/// Mutations: `subquery_literal` never typing the value (`IN (SELECT t …)`
/// takes 'ab  ' for 'ab'); `spaced_text_literal` false (`'ab  '::text`
/// matches).
#[tokio::test(flavor = "multi_thread")]
async fn a_text_typed_value_compares_with_character_exactly() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE c (id INT PRIMARY KEY, a CHAR(4), v INT); \
         CREATE TABLE d (id INT PRIMARY KEY, t TEXT, b VARCHAR(6), k CHAR(6)); \
         INSERT INTO c VALUES (1, 'ab', 0), (2, 'abc', 0), (3, 'x', 0); \
         INSERT INTO d VALUES (1, 'ab  ', 'ab  ', 'ab'), (2, 'abc', 'abc ', 'abc'), (3, 'x ', 'x', 'x')",
    )
    .await
    .expect("setup");
    let mut wrong = Vec::new();
    for (sql, want) in [
        ("UPDATE c SET v = 1 WHERE EXISTS (SELECT 1 FROM d WHERE d.t = c.a) RETURNING id", "[2]"),
        ("UPDATE c SET v = 2 WHERE a IN (SELECT t FROM d) RETURNING id", "[2]"),
        ("UPDATE c SET v = 3 WHERE a = (SELECT t FROM d WHERE id = 1) RETURNING id", ""),
        ("UPDATE c SET v = 3 WHERE a = (SELECT t FROM d WHERE id = 2) RETURNING id", "[2]"),
        ("UPDATE c SET v = 4 WHERE EXISTS (SELECT 1 FROM d WHERE d.b = c.a) RETURNING id", "[1];[2];[3]"),
        ("UPDATE c SET v = 5 WHERE a IN (SELECT b FROM d) RETURNING id", "[1];[2];[3]"),
        ("UPDATE c SET v = 6 WHERE EXISTS (SELECT 1 FROM d WHERE d.k = c.a) RETURNING id", "[1];[2];[3]"),
        ("UPDATE c SET v = 7 WHERE a IN (SELECT t || '' FROM d) RETURNING id", "[2]"),
        ("UPDATE c SET v = 9 WHERE a = 'ab  '::text RETURNING id", ""),
        ("UPDATE c SET v = 9 WHERE a = 'ab  '::varchar RETURNING id", "[1]"),
        ("UPDATE c SET v = 9 WHERE a = 'ab  ' RETURNING id", "[1]"),
        // An IN list's items take the column's type, typed or not.
        ("UPDATE c SET v = 10 WHERE a IN ('ab  '::text, 'x') RETURNING id", "[1];[3]"),
        ("DELETE FROM c WHERE a NOT IN (SELECT t FROM d WHERE t IS NOT NULL) RETURNING id", "[1];[3]"),
    ] {
        c.batch_execute("BEGIN").await.expect("begin");
        let got = match rows(&c, sql).await {
            Ok(mut r) => {
                r.sort();
                r.join(";")
            }
            Err(code) => format!("ERR {code}"),
        };
        c.batch_execute("ROLLBACK").await.expect("rollback");
        if got != want {
            wrong.push(format!("{sql}\n    got  {got}\n    want {want}"));
        }
    }
    // Parameters: declared text (as sqlx and tokio-postgres bind a string)
    // compares exactly; `$1::bpchar`, and a parameter whose type is left to
    // the column, blank-padded.
    use tokio_postgres::types::Type;
    for (sql, types, want) in [
        ("SELECT id FROM c WHERE a = $1", vec![Type::TEXT], 0usize),
        ("SELECT id FROM c WHERE a = $1::text", vec![], 0),
        ("SELECT id FROM c WHERE a = $1::bpchar", vec![], 1),
        ("SELECT id FROM c WHERE a = $1", vec![], 1),
    ] {
        let statement = c.prepare_typed(sql, &types).await.expect("prepare");
        let found = c.query(&statement, &[&"ab  "]).await.map(|r| r.len());
        if found.as_ref().ok() != Some(&want) {
            wrong.push(format!("{sql} {types:?} with 'ab  ': {found:?}, want {want} rows"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// `character(n)` against `character varying` compares blank-padded, as
/// PostgreSQL resolves `bpchar = varchar` to `bpchar`: a join, `UPDATE …
/// FROM`, `DELETE … USING` and a comparison of two columns of one row. The
/// varying side's trailing spaces were part of the key.
///
/// Mutations: `KeyColumn::blank_padded` never set (the join misses 'ab  ');
/// `column_text_kind` answering `None` (the in-row comparison misses).
#[tokio::test(flavor = "multi_thread")]
async fn character_against_varchar_compares_blank_padded() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE c (id INT PRIMARY KEY, a CHAR(4), b VARCHAR(6), v INT); \
         CREATE TABLE d (id INT PRIMARY KEY, t TEXT, b VARCHAR(6), u VARCHAR); \
         INSERT INTO c VALUES (1, 'ab', 'ab  ', 0), (2, 'abc', 'abc', 0), (3, 'x', 'y', 0); \
         INSERT INTO d VALUES (1, 'ab  ', 'ab  ', 'ab '), (2, 'abc', 'abc ', 'abc'), (3, 'x ', 'x', 'x  ')",
    )
    .await
    .expect("setup");
    let mut wrong = Vec::new();
    for (sql, want) in [
        ("SELECT c.id FROM c JOIN d ON c.a = d.b", "[1];[2];[3]"),
        ("SELECT c.id FROM c JOIN d ON c.a = d.u", "[1];[2];[3]"),
        ("SELECT c.id FROM c JOIN d ON c.a = d.t", "[2]"),
        ("SELECT c.id FROM c JOIN d ON d.b = c.a WHERE d.id < 3", "[1];[2]"),
        ("UPDATE c SET v = 1 FROM d WHERE d.b = c.a RETURNING c.id", "[1];[2];[3]"),
        ("UPDATE c SET v = 1 FROM d WHERE d.t = c.a RETURNING c.id", "[2]"),
        ("DELETE FROM c USING d WHERE d.b = c.a AND d.id = 1 RETURNING c.id", "[1]"),
        ("DELETE FROM c USING d WHERE d.t = c.a AND d.id = 1 RETURNING c.id", ""),
        ("SELECT id FROM c WHERE a = b", "[1];[2]"),
        ("SELECT id FROM c WHERE b = a AND id > 1", "[2]"),
        ("SELECT id FROM c WHERE a <> b", "[3]"),
    ] {
        c.batch_execute("BEGIN").await.expect("begin");
        let got = match rows(&c, sql).await {
            Ok(mut r) => {
                r.sort();
                r.join(";")
            }
            Err(code) => format!("ERR {code}"),
        };
        c.batch_execute("ROLLBACK").await.expect("rollback");
        if got != want {
            wrong.push(format!("{sql}\n    got  {got}\n    want {want}"));
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A `character(n)` value keeps its type through a CTE, a derived table, a
/// `CASE`, a `COALESCE` and a `UNION` with text (PostgreSQL resolves the
/// column to `bpchar`): compared without trailing spaces, and a UNION's
/// 'ab' and 'ab  ' are one row. Through a relation it was read as text.
///
/// Mutations: `sql_type_of` without `BPCHAR` (the CTE reads text: 0 rows);
/// `common_bpchar` answering `None` (the `COALESCE` comparison misses);
/// `widened_branch_integers` without the `BPCHAR` mark (the UNION keeps 5).
#[tokio::test(flavor = "multi_thread")]
async fn character_keeps_its_type_through_relations_and_expressions() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE c (id INT PRIMARY KEY, a CHAR(4)); \
         CREATE TABLE d (id INT PRIMARY KEY, t TEXT); \
         INSERT INTO c VALUES (1, 'ab'), (2, 'abc'), (3, 'x'), (4, NULL); \
         INSERT INTO d VALUES (1, 'ab  '), (2, 'abc'), (3, 'x ')",
    )
    .await
    .expect("setup");
    let wrong = check(
        &c,
        &[
            ("WITH w AS (SELECT a FROM c) SELECT count(*) FROM w WHERE a = 'ab  '", "[1]"),
            ("WITH w AS (SELECT a FROM c) SELECT a FROM w WHERE id IS NULL OR a = 'x'", "ERR 42703"),
            ("SELECT count(*) FROM (SELECT a FROM c) s WHERE a = 'abc '", "[1]"),
            ("SELECT s.a FROM (SELECT a FROM c WHERE id = 1) s", "[ab  ]"),
            ("SELECT id FROM c WHERE CASE WHEN id > 0 THEN a END = 'ab  ' ORDER BY id", "[1]"),
            ("SELECT id FROM c WHERE COALESCE(a, 'zz') = 'zz  ' ORDER BY id", "[4]"),
            ("SELECT id, CASE WHEN id < 3 THEN a ELSE 'q' END = 'q ' FROM c ORDER BY id", "[1]|[f];[2]|[f];[3]|[t];[4]|[t]"),
            ("SELECT count(*) FROM (SELECT z FROM (SELECT COALESCE(a, 'q') AS z FROM c) s1 WHERE z = 'ab  ') s2", "[1]"),
            ("SELECT count(*) FROM (SELECT a FROM c UNION SELECT t FROM d) s", "[4]"),
            ("SELECT count(*) FROM (SELECT t FROM d UNION SELECT a FROM c) s", "[6]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A `timestamp(p)`, `timestamptz(p)`, `time(p)` or `interval(p)` column
/// rounds what it is given to `p` fractional digits, on every write path, as
/// PostgreSQL's assignment does; and a timestamp written with more than six
/// fractional digits rounds to the microsecond (ties to even), not cuts. It
/// stored the value whole, so the column disagreed with its own cast.
///
/// Mutation: `assign_bytes` leaving a time value as it is.
#[tokio::test(flavor = "multi_thread")]
async fn a_time_column_rounds_to_its_declared_precision() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE tp (id INT PRIMARY KEY, t TIMESTAMP(0), tz TIMESTAMPTZ(2), tm TIME(1), iv INTERVAL(0), raw TIMESTAMP)",
    )
    .await
    .expect("setup");
    let wrong = check(
        &c,
        &[
            (
                "INSERT INTO tp VALUES (1, '2024-01-01 10:00:00.5', '2024-01-01 10:00:00.125+00', '10:00:00.56', '1.5 seconds', '2024-01-01 10:00:00.4999995')",
                "",
            ),
            ("INSERT INTO tp SELECT 2, raw, raw, '23:59:59.96', '-1.5 seconds', raw FROM tp WHERE id = 1", ""),
            ("UPDATE tp SET t = '1999-12-31 23:59:59.5' WHERE id = 2", ""),
            (
                "SELECT id, t, tz, tm, iv, raw FROM tp ORDER BY id",
                // `t` and `raw` are TIMESTAMP (without time zone), shown with
                // no offset as PostgreSQL shows them; `tz` is a timestamptz.
                "[1]|[2024-01-01 10:00:01]|[2024-01-01 10:00:00.13+00]|[10:00:00.6]|[00:00:02]|[2024-01-01 10:00:00.5];\
                 [2]|[1999-12-31 23:59:59]|[2024-01-01 10:00:00.5+00]|[24:00:00]|[-00:00:02]|[2024-01-01 10:00:00.5]",
            ),
            ("SELECT id FROM tp WHERE t = t::timestamp(0) AND raw::timestamp(0) = '2024-01-01 10:00:01' ORDER BY id", "[1];[2]"),
            ("UPDATE tp SET tm = '10:00:00.64' WHERE id = 1 RETURNING tm", "[10:00:00.6]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// Runs each `(sql, want)` inside a transaction it rolls back, so each
/// starts from the same rows, and reports every answer that differs. `want`
/// is the rows sorted and joined by `;`, or `ERR <sqlstate>`.
async fn check_each_rolled_back(client: &tokio_postgres::Client, cases: &[(&str, &str)]) -> Vec<String> {
    let mut wrong = Vec::new();
    for (sql, want) in cases {
        client.batch_execute("BEGIN").await.expect("begin");
        let got = match rows(client, sql).await {
            Ok(mut r) => {
                r.sort();
                r.join(";")
            }
            Err(code) => format!("ERR {code}"),
        };
        client.batch_execute("ROLLBACK").await.expect("rollback");
        if got != *want {
            wrong.push(format!("{sql}\n    got  {got}\n    want {want}"));
        }
    }
    wrong
}

/// A value typed text compares with `character(n)` exactly however it is
/// spelled: a `CAST`, a parenthesised `::text`, the `text '…'` typed literal,
/// and a string any function or operator computes (`lower`, `||`, `NULLIF`,
/// `COALESCE`). Each was folded to a bare literal and read blank-padded,
/// so `'ab  '` matched the stored `'ab'`; a cast to `varchar` or to
/// `character(n)` compares blank-padded and still does.
///
/// Mutation: the constant fold writing every string bare again (each typed
/// case matches row 1).
#[tokio::test(flavor = "multi_thread")]
async fn a_typed_text_value_spelled_any_way_compares_exactly() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute("CREATE TABLE c (id INT PRIMARY KEY, a CHAR(4), v INT); INSERT INTO c VALUES (1, 'ab', 0), (2, 'abc', 0), (3, 'x', 0)")
        .await
        .expect("setup");
    let wrong = check_each_rolled_back(
        &c,
        &[
            ("SELECT id FROM c WHERE a = CAST('ab  ' AS TEXT)", ""),
            ("SELECT id FROM c WHERE CAST('ab  ' AS text) = a", ""),
            ("SELECT id FROM c WHERE a = ('ab  ')::text", ""),
            ("SELECT id FROM c WHERE a = text 'ab  '", ""),
            ("SELECT id FROM c WHERE text 'ab  ' = a", ""),
            ("SELECT id FROM c WHERE a = lower('AB  ')", ""),
            ("SELECT id FROM c WHERE a = ('ab' || '  ')", ""),
            ("SELECT id FROM c WHERE a = NULLIF('ab  ', 'z')", ""),
            ("SELECT id FROM c WHERE COALESCE('ab  '::text, 'z') = a", ""),
            ("UPDATE c SET v = 1 WHERE a = CAST('ab  ' AS TEXT) RETURNING id", ""),
            ("DELETE FROM c WHERE a = CAST('ab  ' AS TEXT) RETURNING id", ""),
            // Blank-padded: a cast to `varchar` or through `character(n)`,
            // a computed value with no trailing space, an untyped literal.
            ("SELECT id FROM c WHERE a = CAST('ab  ' AS VARCHAR)", "[1]"),
            ("SELECT id FROM c WHERE a = 'ab  '::char(4)::text", "[1]"),
            ("SELECT id FROM c WHERE a = lower('AB')", "[1]"),
            ("SELECT id FROM c WHERE a = 'ab  '", "[1]"),
            ("UPDATE c SET v = 1 WHERE a = 'ab  ' RETURNING id", "[1]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// An upsert whose key arrives typed text finds and updates the row it
/// conflicted with: a parameter bound as text (what sqlx and Npgsql send for
/// a string), a `::text` literal and a `::varchar(5)` one, on the primary key
/// and on a UNIQUE column. The conflict was found on the stored key and the
/// DO UPDATE then looked the row up by the value as written, which text
/// compares with exactly: no row, no error, the write lost. A foreign key's
/// violation shows such a key without its cast.
///
/// Mutation: `stored_text_literal` answering `None` (every DO UPDATE here
/// returns no row and the final rows keep their old values).
#[tokio::test(flavor = "multi_thread")]
async fn an_upsert_keyed_by_typed_text_updates_the_row() {
    use tokio_postgres::types::Type;
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE k2 (k CHAR(3) PRIMARY KEY, u CHAR(4) UNIQUE, v INT); \
         CREATE TABLE ch (id INT PRIMARY KEY, pk CHAR(3) REFERENCES k2 (k)); \
         INSERT INTO k2 VALUES ('a', 'x', 1), ('b', 'y', 2)",
    )
    .await
    .expect("setup");
    let mut wrong = Vec::new();
    let text = [Type::TEXT, Type::TEXT];
    for (sql, params, want) in [
        (
            "INSERT INTO k2 VALUES ($1, $2, 4) ON CONFLICT (k) DO UPDATE SET v = 40 RETURNING k, v",
            ["a  ", "zz"],
            "a  |40",
        ),
        (
            "INSERT INTO k2 VALUES ($1, $2, 6) ON CONFLICT (u) DO UPDATE SET v = 60 RETURNING k, v",
            ["q", "x   "],
            "a  |60",
        ),
    ] {
        let statement = c.prepare_typed(sql, &text).await.expect("prepare");
        let got = match c.query(&statement, &[&params[0], &params[1]]).await {
            Ok(rows) => rows
                .iter()
                .map(|r| format!("{}|{}", r.get::<_, String>(0), r.get::<_, i32>(1)))
                .collect::<Vec<_>>()
                .join(";"),
            Err(e) => format!("ERR {e}"),
        };
        if got != want {
            wrong.push(format!("{sql} {params:?}\n    got  {got}\n    want {want}"));
        }
    }
    wrong.extend(
        check(
            &c,
            &[
                ("INSERT INTO k2 VALUES ('b  '::text, 'm', 11) ON CONFLICT (k) DO UPDATE SET v = 110 RETURNING k, v", "[b  ]|[110]"),
                (
                    "INSERT INTO k2 VALUES ('a '::varchar(5), 'm2', 12) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v RETURNING k, v",
                    "[a  ]|[12]",
                ),
                ("SELECT k, u, v FROM k2 ORDER BY k", "[a  ]|[x   ]|[12];[b  ]|[y   ]|[110]"),
            ],
        )
        .await,
    );
    let statement = c.prepare_typed("INSERT INTO ch VALUES (3, $1)", &[Type::TEXT]).await.expect("prepare");
    match c.execute(&statement, &[&"zz "]).await {
        Ok(n) => wrong.push(format!("a key no parent holds was written ({n} rows)")),
        Err(e) => {
            let message = e.as_db_error().map(|d| d.message().to_string()).unwrap_or_else(|| e.to_string());
            if !message.contains("(zz )") || message.contains("::") {
                wrong.push(format!("23503 shows the key as written: {message}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// `COALESCE`, `CASE`, `LEAST` and `GREATEST` over a `character(n)` column
/// and a text one take the type of the first that has one, as PostgreSQL's
/// common type does, a `CASE`'s `ELSE` ahead of its `THEN`s: `bpchar` when
/// that is the `character(n)` value, compared blank-padded, its text values
/// without their trailing spaces; text when it is the text one. Mixed
/// branches were read as text whichever came first.
///
/// Mutation: `blank_pad_text_branches` doing nothing (`COALESCE(a, t)`
/// misses row 2); reading a `CASE`'s `THEN`s first (the `CASE … ELSE t`
/// case matches as `bpchar`).
#[tokio::test(flavor = "multi_thread")]
async fn mixed_character_and_text_branches_take_the_first_branchs_type() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE s1 (id INT PRIMARY KEY, a CHAR(4), t TEXT); \
         INSERT INTO s1 VALUES (1, 'ab', 'ab  '), (2, NULL, 'ab  '), (3, 'ab', NULL)",
    )
    .await
    .expect("setup");
    let wrong = check(
        &c,
        &[
            ("SELECT id FROM s1 WHERE COALESCE(a, t) = 'ab' ORDER BY id", "[1];[2];[3]"),
            ("SELECT id FROM s1 WHERE COALESCE(a, t, 'zz') = 'ab  ' ORDER BY id", "[1];[2];[3]"),
            ("SELECT id FROM s1 WHERE COALESCE(t, a) = 'ab' ORDER BY id", "[3]"),
            ("SELECT id FROM s1 WHERE CASE WHEN id > 0 THEN a ELSE t END = 'ab  ' ORDER BY id", ""),
            ("SELECT id FROM s1 WHERE CASE WHEN id > 0 THEN t ELSE a END = 'ab' ORDER BY id", "[1];[2]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A `character varying` column on the left of a `character(n)` one
/// compares blank-padded too: `b IN (SELECT k …)`, and the recheck an
/// `UPDATE … FROM` and a `DELETE … USING` run on each row they write. The
/// subquery's values and the joined key became untyped literals, which a
/// varying column compares with exactly, so 'ab  ' missed 'ab' (the mirrored
/// forms, with the `character(n)` column on the left, were right).
///
/// Mutations: `varying_in_operand` answering `None` (the IN misses row 1);
/// `pad_varying_beside_padded` returning its terms (the UPDATE and DELETE
/// write nothing for row 1).
#[tokio::test(flavor = "multi_thread")]
async fn a_varying_column_beside_character_reads_blank_padded() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE v1 (id INT PRIMARY KEY, b VARCHAR(6), v INT); \
         CREATE TABLE v2 (id INT PRIMARY KEY, k CHAR(6)); \
         INSERT INTO v1 VALUES (1, 'ab  ', 0), (2, 'abc', 0), (3, 'zz', 0); \
         INSERT INTO v2 VALUES (1, 'ab'), (2, 'abc')",
    )
    .await
    .expect("setup");
    let wrong = check_each_rolled_back(
        &c,
        &[
            ("SELECT id FROM v1 WHERE b IN (SELECT k FROM v2)", "[1];[2]"),
            ("SELECT id FROM v1 WHERE v1.b NOT IN (SELECT k FROM v2)", "[3]"),
            ("UPDATE v1 SET v = 1 FROM v2 WHERE v1.b = v2.k RETURNING v1.id", "[1];[2]"),
            ("DELETE FROM v1 USING v2 WHERE v1.b = v2.k RETURNING v1.id", "[1];[2]"),
            // The mirrored forms, which were right.
            ("SELECT id FROM v2 WHERE k IN (SELECT b FROM v1)", "[1];[2]"),
            ("UPDATE v1 SET v = 1 FROM v2 WHERE v2.k = v1.b RETURNING v1.id", "[1];[2]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// `RETURNING` shows a value assigned from a text-typed parameter as the
/// column holds it: a `character(4)` padded to four, a `varchar(4)` with
/// the spaces past four cut. It showed the parameter as sent.
///
/// Mutation: the RETURNING overlay skipping `held_to_text_length`.
#[tokio::test(flavor = "multi_thread")]
async fn returning_shows_a_text_typed_value_as_the_column_holds_it() {
    use tokio_postgres::types::Type;
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute("CREATE TABLE r1 (id INT PRIMARY KEY, a CHAR(4), b VARCHAR(4)); INSERT INTO r1 VALUES (1, 'a', 'a')")
        .await
        .expect("setup");
    let mut wrong = Vec::new();
    for (sql, value, want) in [
        ("UPDATE r1 SET a = $1 WHERE id = 1 RETURNING a", "qq   ", "qq  "),
        ("UPDATE r1 SET b = $1 WHERE id = 1 RETURNING b", "xy    ", "xy  "),
    ] {
        let statement = c.prepare_typed(sql, &[Type::TEXT]).await.expect("prepare");
        let got = c.query_one(&statement, &[&value]).await.map(|r| r.get::<_, String>(0));
        if got.as_deref().ok() != Some(want) {
            wrong.push(format!("{sql} with {value:?}: {got:?}, want {want:?}"));
        }
    }
    let stored = rows(&c, "SELECT a, b FROM r1").await;
    if stored != Ok(vec!["[qq  ]|[xy  ]".to_string()]) {
        wrong.push(format!("stored {stored:?}"));
    }
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// An interval column declared with fields keeps only them, as PostgreSQL's
/// assignment cuts it: `interval hour to minute` drops the seconds, `day`
/// the time, `year` the months past whole years, and `day to second(0)` and
/// `minute to second(1)` round the seconds to their precision. The fields
/// were ignored, so every value was kept whole; `ALTER … TYPE` to such a
/// type rewrites the stored values.
///
/// Mutation: `restrict_interval_fields` doing nothing (every cut value is
/// kept whole).
#[tokio::test(flavor = "multi_thread")]
async fn interval_fields_restrict_what_a_column_keeps() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE w4 (id INT PRIMARY KEY, a INTERVAL HOUR TO MINUTE, b INTERVAL DAY TO SECOND(0), \
             y INTERVAL YEAR, d INTERVAL DAY, m INTERVAL MINUTE TO SECOND(1)); \
         INSERT INTO w4 VALUES (1, '1 day 02:03:04.5', '1 day 02:03:04.5', '1 year 5 mons 3 days', '3 days 04:05:06', '02:03:04.56'); \
         INSERT INTO w4 (id, a) VALUES (2, '-02:03:59'); \
         CREATE TABLE w5 (id INT PRIMARY KEY, iv INTERVAL); \
         INSERT INTO w5 VALUES (1, '1 day 02:03:04.5')",
    )
    .await
    .expect("setup");
    let wrong = check(
        &c,
        &[
            (
                "SELECT id, a, b, y, d, m FROM w4 ORDER BY id",
                "[1]|[1 day 02:03:00]|[1 day 02:03:05]|[1 year]|[3 days]|[02:03:04.6];[2]|[-02:03:00]|∅|∅|∅|∅",
            ),
            ("UPDATE w4 SET a = '05:06:07.8' WHERE id = 1", ""),
            ("SELECT a FROM w4 WHERE id = 1", "[05:06:00]"),
            ("ALTER TABLE w5 ALTER COLUMN iv TYPE INTERVAL HOUR TO MINUTE", ""),
            ("SELECT iv FROM w5", "[1 day 02:03:00]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// `LIKE … ESCAPE` over a `character(n)` column, which matches its padded
/// value: the escape character is read as PostgreSQL reads it, for `NOT
/// LIKE` and `ILIKE` too. The padded match left the `ESCAPE` clause where
/// nothing read it, and every such statement was refused (42601).
///
/// Mutation: `like_without_escape` answering `None` (each case is 42601).
#[tokio::test(flavor = "multi_thread")]
async fn like_escape_reads_a_character_column_padded() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE c (id INT PRIMARY KEY, a CHAR(4), b VARCHAR(6)); \
         INSERT INTO c VALUES (1, 'a%b', 'a%b'), (2, 'ab', 'ab  '), (3, 'a_b', 'a_b'), (4, NULL, NULL), (5, 'x', 'x ')",
    )
    .await
    .expect("setup");
    let wrong = check(
        &c,
        &[
            ("SELECT id FROM c WHERE a LIKE 'a!%b%' ESCAPE '!' ORDER BY id", "[1]"),
            // Padded to four, 'a%b' is 'a%b ', which the pattern does not end.
            ("SELECT id FROM c WHERE a LIKE 'a!%b' ESCAPE '!' ORDER BY id", ""),
            ("SELECT id FROM c WHERE a NOT LIKE 'a!%b%' ESCAPE '!' ORDER BY id", "[2];[3];[5]"),
            ("SELECT id FROM c WHERE a ILIKE 'A!%B%' ESCAPE '!' ORDER BY id", "[1]"),
            ("SELECT id FROM c WHERE a LIKE 'a#_b ' ESCAPE '#' ORDER BY id", "[3]"),
            ("SELECT id FROM c WHERE a LIKE 'a%b%' ESCAPE '' ORDER BY id", "[1];[2];[3]"),
            ("SELECT id FROM c WHERE a LIKE 'a\\%b%' ORDER BY id", "[1]"),
            ("SELECT id FROM c WHERE b LIKE 'a!%b%' ESCAPE '!' ORDER BY id", "[1]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// An `IN` list over a `character(n)` column is `bpchar` as a whole, an item
/// computed from strings included (`'ab' || '  '`, `lower('AB  ')`): it is
/// read without its trailing spaces, as the plain literals beside it are.
/// Such an item was text and compared with the column exactly, so `IN`
/// missed the row, `NOT IN` kept it and a `DELETE` left it.
///
/// Mutation: the list's computed items not trimmed (`spell_out_list_or_range`).
#[tokio::test(flavor = "multi_thread")]
async fn an_in_list_over_character_reads_computed_items_padded() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE c (id INT PRIMARY KEY, a CHAR(4), b VARCHAR(6), t TEXT); \
         INSERT INTO c VALUES (1, 'a%b', 'a%b', 'a%b'), (2, 'ab', 'ab  ', 'ab  '), (3, 'a_b', 'a_b', 'a_b'), (4, NULL, NULL, NULL), (5, 'x', 'x ', 'x')",
    )
    .await
    .expect("setup");
    let wrong = check_each_rolled_back(
        &c,
        &[
            ("SELECT id FROM c WHERE a IN ('ab' || '  ', 'x')", "[2];[5]"),
            ("SELECT id FROM c WHERE a NOT IN ('ab' || '  ', 'x')", "[1];[3]"),
            ("SELECT id FROM c WHERE a IN ('ab  ', 'x' || ' ')", "[2];[5]"),
            ("SELECT id FROM c WHERE a IN (lower('AB  '), 'q')", "[2]"),
            ("DELETE FROM c WHERE a IN ('x' || ' ', 'a%b' || '') RETURNING id", "[1];[5]"),
            // Over text and varchar the items stay text.
            ("SELECT id FROM c WHERE b IN ('ab' || '  ', 'x')", "[2]"),
            ("SELECT id FROM c WHERE t IN ('ab' || '  ', 'x')", "[2];[5]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// `text 'x'` (and `varchar 'x'`, `char 'x'`, `character varying 'x'`) is
/// the value `x`, as `'x'::text` is: written by an INSERT, an UPDATE and an
/// upsert's key. Taken whole, `text 'ab  '` was too long for a
/// `character(4)` (22001), an upsert keyed by it failed, and `SET s = text
/// 'zz '` stored the ten characters `text 'zz '`.
///
/// Mutation: `typed_text_literal` answering `None` (each write fails or
/// stores the spelling).
#[tokio::test(flavor = "multi_thread")]
async fn a_typed_text_literal_is_its_value_on_every_write() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE tt (id INT PRIMARY KEY, s TEXT, c CHAR(4), v VARCHAR(3)); \
         CREATE TABLE cc (k CHAR(3), n INT, v INT, CONSTRAINT cc_pk PRIMARY KEY (k, n)); \
         INSERT INTO cc VALUES ('a', 1, 1)",
    )
    .await
    .expect("setup");
    let wrong = check(
        &c,
        &[
            ("INSERT INTO tt VALUES (1, text 'ab  ', text 'ab  ', varchar 'x ')", ""),
            ("INSERT INTO tt VALUES (2, 'q', char 'cd', character varying 'yz   ')", ""),
            ("UPDATE tt SET s = text 'zz ' WHERE id = 2 RETURNING s", "[zz ]"),
            ("SELECT id, s, c, v FROM tt ORDER BY id", "[1]|[ab  ]|[ab  ]|[x ];[2]|[zz ]|[cd  ]|[yz ]"),
            (
                "INSERT INTO cc VALUES (text 'a  ', 1, 25) ON CONFLICT (k, n) DO UPDATE SET v = EXCLUDED.v RETURNING k, n, v",
                "[a  ]|[1]|[25]",
            ),
            (
                "INSERT INTO cc VALUES (text 'b  ', 2, 26), (text 'b', 2, 27) ON CONFLICT (k, n) DO UPDATE SET v = EXCLUDED.v",
                "ERR 21000",
            ),
            ("SELECT k, n, v FROM cc ORDER BY k", "[a  ]|[1]|[25]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// A pattern over `CAST(t AS CHAR(n))` reads the padded value, as one over
/// `t::char(n)` does; and a `character varying` column keeps its type in a
/// CTE or a derived table, so it compares blank-padded with a `character(n)`
/// one there as in its table. Both read text before.
///
/// Mutations: `padded_for_pattern` not reading `CAST(` (the LIKE misses
/// row 2); the relation's varying column declared TEXT (`a = b` misses
/// rows 2 and 5).
#[tokio::test(flavor = "multi_thread")]
async fn a_cast_pattern_and_a_relations_varying_column_read_as_postgresql() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE c (id INT PRIMARY KEY, a CHAR(4), b VARCHAR(6), t TEXT); \
         INSERT INTO c VALUES (1, 'a%b', 'a%b', 'a%b'), (2, 'ab', 'ab  ', 'ab  '), (3, 'a_b', 'a_b', 'a_b'), (4, NULL, NULL, NULL), (5, 'x', 'x ', 'x')",
    )
    .await
    .expect("setup");
    let wrong = check(
        &c,
        &[
            ("SELECT id FROM c WHERE CAST(t AS CHAR(4)) LIKE 'ab  ' ORDER BY id", "[2]"),
            ("SELECT id FROM c WHERE t::char(4) LIKE 'ab  ' ORDER BY id", "[2]"),
            ("WITH w AS (SELECT id, a, b FROM c) SELECT id FROM w WHERE a = b ORDER BY id", "[1];[2];[3];[5]"),
            ("WITH w AS (SELECT id, a, b FROM c) SELECT id FROM w WHERE b = a ORDER BY id", "[1];[2];[3];[5]"),
            ("SELECT id FROM (SELECT id, a, b FROM c) s WHERE s.a = s.b ORDER BY id", "[1];[2];[3];[5]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// PostgreSQL types an `IN` list as one array only when two or more of its
/// items are not column references; then the list is `bpchar` over a
/// `character(n)` column and a computed item is read blank-padded. With
/// fewer, `a IN (x, …)` is `a = x OR …`, each item at its own type, so a
/// single computed or typed text item compares exactly, as `=` does. Every
/// list over a `character(n)` column was read blank-padded, and a one-item
/// `IN`, `NOT IN`, `UPDATE` and `DELETE` matched rows PostgreSQL does not.
///
/// Mutation: the array rule ignoring the item count (`spell_out_list_or_range`
/// pads a lone computed item).
#[tokio::test(flavor = "multi_thread")]
async fn a_one_item_in_list_compares_as_equals_does() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE d (id INT PRIMARY KEY, a CHAR(5), t TEXT, n INT); \
         INSERT INTO d VALUES (1, 'ab', 'ab  ', 1), (2, 'x', 'x', 2), (3, 'abc', 'zz', 3)",
    )
    .await
    .expect("setup");
    let wrong = check_each_rolled_back(
        &c,
        &[
            ("SELECT id FROM d WHERE a IN ('ab' || '  ')", ""),
            ("SELECT id FROM d WHERE a NOT IN ('ab' || '  ')", "[1];[2];[3]"),
            ("SELECT id FROM d WHERE a IN ('ab  '::text)", ""),
            ("SELECT id FROM d WHERE a IN (CAST('ab  ' AS TEXT))", ""),
            ("SELECT id FROM d WHERE a IN (text 'ab  ')", ""),
            ("SELECT id FROM d WHERE a IN ('ab' || '  ', t)", "[2]"),
            ("SELECT id FROM d WHERE a IN (t, 'x' || ' ')", "[2]"),
            ("SELECT id FROM d WHERE (a IN ('ab' || '  ')) OR id = 3", "[3]"),
            ("UPDATE d SET n = 100 WHERE a IN ('ab' || '  ') RETURNING id", ""),
            ("DELETE FROM d WHERE a IN ('x' || ' ') RETURNING id", ""),
            ("DELETE FROM d WHERE a NOT IN ('abc' || ' ') RETURNING id", "[1];[2];[3]"),
            // Two items or more that are not columns: one bpchar array.
            ("SELECT id FROM d WHERE a IN ('ab' || '  ', 'x' || ' ')", "[1];[2]"),
            ("SELECT id FROM d WHERE a IN ('ab  ', 'x ')", "[1];[2]"),
            ("SELECT id FROM d WHERE a IN ('ab  ')", "[1]"),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}

/// An `IN` item that reads a column anywhere (`t || ''`, `lower(t)`,
/// `d.b`) is outside PostgreSQL's common-typed array: it is compared on its
/// own, as `a = item`, and only the column-free items count toward the
/// array. Counted as constants, two such items made the whole list
/// `bpchar` and every `t || ''` matched blank-padded. A `character varying`
/// item beside a `character(n)` operand, or the other way round, compares
/// blank-padded, as `a = b` does; it compared exactly.
///
/// Mutations: `reads_a_column` seeing only a bare column (the `t || ''`
/// lists match rows 1 and 3); the column item compared without the
/// varying side trimmed (`a IN (b)` misses rows 1, 3 and 5).
#[tokio::test(flavor = "multi_thread")]
async fn an_in_item_reading_a_column_compares_on_its_own() {
    let server = TestServer::start().await.expect("server");
    let c = client(&server).await;
    c.batch_execute(
        "CREATE TABLE d (id INT PRIMARY KEY, a CHAR(5), t TEXT, n INT); \
         INSERT INTO d VALUES (1, 'ab', 'ab  ', 1), (2, 'x', 'x', 2), (3, 'q', 'q ', 3); \
         CREATE TABLE e (id INT PRIMARY KEY, a CHAR(5), t TEXT, b VARCHAR(6), n INT); \
         INSERT INTO e VALUES (1, 'ab', 'ab  ', 'ab ', 1), (2, 'x', 'x', 'x', 2), (3, 'abc', 'zz', 'abc  ', 3), \
             (4, NULL, NULL, NULL, 4), (5, 'it''s', 'it''s ', 'it''s', 5)",
    )
    .await
    .expect("setup");
    let wrong = check_each_rolled_back(
        &c,
        &[
            ("SELECT id FROM d WHERE a IN (t || '', 'x')", "[2]"),
            ("SELECT id FROM d WHERE a IN (lower(t), 'x')", "[2]"),
            ("SELECT id FROM d WHERE a IN (t || '', 'x' || '')", "[2]"),
            ("SELECT id FROM d WHERE a IN (d.t || '', 'x')", "[2]"),
            ("SELECT id FROM d WHERE a IN (CAST(t AS TEXT), 'x')", "[2]"),
            // A column's name in a string is a string: two constants, one array.
            ("SELECT id FROM d WHERE a IN ('t' || ' ', 'q' || '  ')", "[3]"),
            ("SELECT id FROM d WHERE a NOT IN (t || '', 'zz')", "[1];[3]"),
            ("UPDATE d SET n = 0 WHERE a IN (t || '', 'zz') RETURNING id", "[2]"),
            ("DELETE FROM d WHERE a IN (lower(t), 'zz') RETURNING id", "[2]"),
            ("SELECT id FROM e WHERE a IN (t, b)", "[1];[2];[3];[5]"),
            ("SELECT id FROM e WHERE a IN (b)", "[1];[2];[3];[5]"),
            ("SELECT id FROM e WHERE a IN (t, b, 'x')", "[1];[2];[3];[5]"),
            ("SELECT id FROM e WHERE a IN (e.t, e.b)", "[1];[2];[3];[5]"),
            ("SELECT id FROM e WHERE a NOT IN (t, b)", ""),
            ("SELECT id FROM e WHERE b IN (a, 'zz')", "[1];[2];[3];[5]"),
            ("SELECT id FROM e WHERE t IN (a, 'zz')", "[2];[3]"),
            ("SELECT id FROM e WHERE a IN (t)", "[2]"),
            ("SELECT id FROM e WHERE a IN ('ab  '::text, 'x', t)", "[1];[2]"),
            ("SELECT id FROM e WHERE a IN (t, 'ab' || '  ', 'x' || ' ')", "[1];[2]"),
            ("SELECT id FROM e WHERE a IN ('it''s' || ' ', NULL)", "[5]"),
            ("SELECT id FROM e WHERE a IN (text 'ab  ', 'x')", "[1];[2]"),
            ("SELECT id FROM e WHERE a IN (varchar 'ab ')", "[1]"),
            ("UPDATE e SET n = 100 WHERE a IN (t, b) RETURNING id", "[1];[2];[3];[5]"),
            ("DELETE FROM e WHERE a IN ('ab  '::text, t) RETURNING id", "[2]"),
            ("DELETE FROM e WHERE a NOT IN (b, 'zz') RETURNING id", ""),
        ],
    )
    .await;
    assert!(wrong.is_empty(), "{} wrong:\n{}", wrong.len(), wrong.join("\n"));
}
