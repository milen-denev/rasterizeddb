//! What drivers and `psql` read and send beyond plain statements: the
//! catalogue (`pg_type`, `pg_attribute`, `pg_constraint`, `pg_index` and the
//! functions over them), `COPY` in its three formats, and the SQL spellings
//! their catalogue queries use. Each was refused: Npgsql could not connect,
//! `\d` failed, and COPY was a syntax error. Expected answers are PostgreSQL
//! 17's (`.claude/reviews/pgdiff`, `.claude/reviews/drivers`).

use bytes::Bytes;
use futures_util::{SinkExt, TryStreamExt};
use rasterizeddb_compat::TestServer;
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;

async fn rows(c: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    c.simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(
                (0..r.len())
                    .map(|i| r.get(i).unwrap_or("NULL").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect()
}

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c.batch_execute(
        "CREATE TABLE items (id BIGSERIAL PRIMARY KEY, name VARCHAR(40) NOT NULL DEFAULT 'x', \
         price NUMERIC(10,2) CHECK (price > 0), tags TEXT[], UNIQUE (name)); \
         CREATE INDEX items_price ON items (price); \
         CREATE TABLE kids (id BIGINT PRIMARY KEY, item_id BIGINT REFERENCES items(id) ON DELETE CASCADE)",
    )
    .await
    .expect("setup");
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn the_catalogue_describes_tables_as_postgresql_does() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;

    // Built-in types by their PostgreSQL oids, arrays naming their elements
    // and `array_recv`, as Npgsql reads them when it connects.
    assert_eq!(
        rows(&c, "SELECT t.typname, e.typname, p.proname FROM pg_type t JOIN pg_type e ON e.oid = t.typelem \
                  LEFT JOIN pg_proc p ON p.oid = t.typreceive WHERE t.oid = 1007")
        .await,
        ["_int4|int4|array_recv"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT typname FROM pg_type WHERE oid IN (20, 1043, 3802) ORDER BY oid"
        )
        .await,
        ["int8", "varchar", "jsonb"]
    );

    // Columns, as psql's `\d` reads them: types with their modifiers, NOT
    // NULL, and defaults through a correlated subquery, laid out over lines.
    let columns = rows(
        &c,
        "SELECT a.attname,\n  pg_catalog.format_type(a.atttypid, a.atttypmod),\n  \
         (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true)\n   FROM pg_catalog.pg_attrdef d\n   \
         WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef),\n  a.attnotnull\n\
         FROM pg_catalog.pg_attribute a\nWHERE a.attrelid = 'items'::regclass AND a.attnum > 0 AND NOT a.attisdropped\n\
         ORDER BY a.attnum",
    )
    .await;
    assert_eq!(
        columns,
        [
            "id|bigint|nextval('items_id_seq')|t",
            "name|character varying(40)|'x'|t",
            "price|numeric(10,2)|NULL|f",
            "tags|text[]|NULL|f",
        ]
    );

    // Indexes and constraints, with their definitions.
    assert_eq!(
        rows(
            &c,
            "SELECT c2.relname, i.indisprimary, i.indisunique, pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) \
             FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i \
             WHERE c.oid = 'items'::regclass AND c.oid = i.indrelid AND i.indexrelid = c2.oid ORDER BY c2.relname",
        )
        .await,
        [
            "items_name_key|f|t|CREATE UNIQUE INDEX \"items_name_key\" ON public.\"items\" USING btree (\"name\")",
            "items_pkey|t|t|CREATE UNIQUE INDEX \"items_pkey\" ON public.\"items\" USING btree (\"id\")",
            "items_price|f|f|CREATE INDEX \"items_price\" ON public.\"items\" USING btree (\"price\")",
        ]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT conname, contype, pg_catalog.pg_get_constraintdef(oid, true), conrelid::pg_catalog.regclass \
             FROM pg_catalog.pg_constraint WHERE confrelid = 'items'::regclass",
        )
        .await,
        ["kids_item_id_fkey|f|FOREIGN KEY (\"item_id\") REFERENCES \"items\"(\"id\") ON DELETE CASCADE|kids"]
    );
    // psql's own spellings: OPERATOR(pg_catalog.~), COLLATE, visibility.
    assert_eq!(
        rows(
            &c,
            "SELECT c.relname, pg_catalog.pg_get_userbyid(c.relowner) FROM pg_catalog.pg_class c \
             LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname OPERATOR(pg_catalog.~) '^(items)$' COLLATE pg_catalog.default \
             AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 1",
        )
        .await,
        ["items|postgres"]
    );
    // A query over a relation that holds no rows here answers with none,
    // whatever its select list computes (psql's policies, publications).
    assert!(
        rows(
            &c,
            "SELECT pol.polname, CASE WHEN pol.polroles = '{0}' THEN NULL ELSE pg_catalog.array_to_string(array(select \
             rolname from pg_catalog.pg_roles where oid = any (pol.polroles) order by 1),',') END \
             FROM pg_catalog.pg_policy pol WHERE pol.polrelid = 'items'::regclass ORDER BY 1",
        )
        .await
        .is_empty()
    );
    // Over the extended protocol, a join of catalogue relations is described
    // before its rows (it was NoData, then DataRows).
    let described = c
        .query("SELECT t.typname, n.nspname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.oid = $1", &[&20i64])
        .await
        .expect("extended catalogue join");
    assert_eq!(described.len(), 1);
    assert_eq!(described[0].get::<_, String>(0), "int8");
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_moves_rows_in_every_format() {
    let server = TestServer::start().await.expect("server");
    let c = connect(&server).await;

    let sink = c
        .copy_in("COPY items (id, name, price) FROM STDIN")
        .await
        .expect("copy in");
    futures_util::pin_mut!(sink);
    sink.send(Bytes::from_static(b"1\tone\t1.50\n2\ttab\\there\t\\N\n"))
        .await
        .expect("send");
    assert_eq!(sink.finish().await.expect("finish"), 2);

    let sink = c
        .copy_in("COPY items (id, name, tags) FROM STDIN WITH (FORMAT csv, HEADER)")
        .await
        .expect("csv in");
    futures_util::pin_mut!(sink);
    sink.send(Bytes::from_static(
        b"id,name,tags\n3,\"a,b\",\"{x,y}\"\n4,\"q\"\"t\",\n",
    ))
    .await
    .expect("send");
    assert_eq!(sink.finish().await.expect("finish"), 2);

    let sink = c
        .copy_in("COPY items (id, name, tags) FROM STDIN (FORMAT binary)")
        .await
        .expect("binary in");
    let writer = BinaryCopyInWriter::new(sink, &[Type::INT8, Type::VARCHAR, Type::TEXT_ARRAY]);
    futures_util::pin_mut!(writer);
    writer
        .as_mut()
        .write(&[&5i64, &"five", &vec!["b".to_string(), "c d".to_string()]])
        .await
        .expect("row");
    assert_eq!(writer.finish().await.expect("finish"), 1);

    assert_eq!(
        rows(&c, "SELECT id, name, price, tags FROM items ORDER BY id").await,
        [
            "1|one|1.50|NULL",
            "2|tab\there|NULL|NULL",
            "3|a,b|NULL|{x,y}",
            "4|q\"t|NULL|NULL",
            "5|five|NULL|{b,\"c d\"}"
        ]
    );

    let out: Vec<Bytes> = c
        .copy_out("COPY (SELECT id, name FROM items WHERE id <= 3 ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER)")
        .await
        .expect("copy out")
        .try_collect()
        .await
        .expect("read");
    assert_eq!(
        out.concat(),
        b"id,name\n1,one\n2,tab\there\n3,\"a,b\"\n".to_vec()
    );
    let out: Vec<Bytes> = c
        .copy_out("COPY items (id, name) TO STDOUT")
        .await
        .expect("text out")
        .try_collect()
        .await
        .expect("read");
    assert!(
        out.concat().starts_with(b"1\tone\n2\ttab\\there\n"),
        "{:?}",
        String::from_utf8_lossy(&out.concat())
    );

    // A row that fails leaves none of the copy behind (one statement).
    let sink = c
        .copy_in("COPY items (id, name) FROM STDIN")
        .await
        .expect("copy in");
    futures_util::pin_mut!(sink);
    sink.send(Bytes::from_static(b"6\tsix\n1\tduplicate\n"))
        .await
        .expect("send");
    let e = sink
        .finish()
        .await
        .expect_err("a duplicate key fails the copy");
    assert_eq!(e.code().map(|s| s.code()), Some("23505"), "{e}");
    assert_eq!(
        rows(&c, "SELECT count(*) FROM items WHERE id = 6").await,
        ["0"]
    );

    // A file is the server's: refused.
    let e = c
        .simple_query("COPY items FROM '/etc/passwd'")
        .await
        .expect_err("file");
    assert_eq!(e.code().map(|s| s.code()), Some("0A000"));
}

#[tokio::test(flavor = "multi_thread")]
async fn join_conditions_read_bare_names_and_lines() {
    let server = TestServer::start().await.expect("server");
    let (c, conn) = tokio_postgres::connect(&server.url(), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(conn);
    c.batch_execute(
        "CREATE TABLE ja (id BIGINT, an TEXT); CREATE TABLE jb (bid BIGINT, aid BIGINT, bn TEXT); \
         INSERT INTO ja VALUES (1,'x'),(2,'y'),(3,'w'); INSERT INTO jb VALUES (10,1,'p'),(11,2,'q'),(12,9,'r')",
    )
    .await
    .expect("setup");
    assert_eq!(
        rows(&c, "SELECT an, bn FROM ja JOIN jb ON id = aid ORDER BY bn").await,
        ["x|p", "y|q"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT an, bn FROM ja LEFT JOIN jb ON id = aid ORDER BY an"
        )
        .await,
        ["w|NULL", "x|p", "y|q"]
    );
    assert_eq!(
        rows(
            &c,
            "SELECT an, bn FROM ja FULL JOIN jb ON id = aid ORDER BY an, bn"
        )
        .await,
        ["w|NULL", "x|p", "y|q", "NULL|r"]
    );
    let e = c
        .simple_query("SELECT count(*) FROM ja a JOIN ja b ON id = id")
        .await
        .expect_err("ambiguous");
    assert_eq!(e.code().map(|s| s.code()), Some("42702"));
    // A CASE ending its line, as drivers lay their queries out.
    assert_eq!(
        rows(&c, "SELECT CASE\nWHEN bn = 'p' THEN an\nELSE 'z'\nEND FROM ja JOIN jb ON id = aid ORDER BY CASE\nWHEN bn = 'p' THEN 0 ELSE 1 END").await,
        ["x", "z"]
    );
}

/// A partitioned table is PostgreSQL's partitioned parent: relkind `'p'`, a
/// `pg_partitioned_table` row, `pg_get_partkeydef`, and the queries `psql`
/// 18's `\d` and `\d+` send for one, each written as `psql` writes it
/// (`'…'::pg_catalog.regclass`, `pg_partition_ancestors(oid)` in a `FROM`
/// list, `attstorage`). `\d` of a partitioned table failed: relkind `'r'`
/// hid what it was, and once `'p'` the foreign-key query named a function
/// the engine did not have. Its partitions are not relations a client can
/// name, so `pg_inherits` stays empty.
#[tokio::test(flavor = "multi_thread")]
async fn a_partitioned_table_is_described_as_a_partitioned_parent() {
    // On files: an in-memory database refuses partitioned tables.
    struct OnDisk(std::path::PathBuf);
    impl Drop for OnDisk {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let root = OnDisk(std::env::temp_dir().join(format!("driver_catalog_partitioned_{}", std::process::id())));
    let _ = std::fs::remove_dir_all(&root.0);
    std::fs::create_dir_all(&root.0).expect("a database directory");
    let server = TestServer::start_file_backed(&root.0.to_string_lossy()).await.expect("server");
    let c = connect(&server).await;
    c.batch_execute(
        "CREATE TABLE ev (tenant_id BIGINT NOT NULL, grp INT NOT NULL, id BIGINT NOT NULL, v TEXT, \
         PRIMARY KEY (tenant_id, grp, id)) PARTITION BY HASH (tenant_id, grp) PARTITIONS 8",
    )
    .await
    .expect("create");

    assert_eq!(
        rows(&c, "SELECT relname, relkind, relam FROM pg_class WHERE relname IN ('ev', 'items') ORDER BY relname").await,
        ["ev|p|0", "items|r|2"]
    );
    assert_eq!(
        rows(&c, "SELECT c.relname, p.partstrat, p.partnatts, p.partdefid FROM pg_partitioned_table p \
                  JOIN pg_class c ON c.oid = p.partrelid")
        .await,
        ["ev|h|2|0"]
    );
    assert_eq!(
        rows(&c, "SELECT pg_get_partkeydef(c.oid), pg_get_partkeydef(i.oid) FROM pg_class c, pg_class i \
                  WHERE c.relname = 'ev' AND i.relname = 'items'")
        .await,
        ["HASH (tenant_id, grp)|NULL"]
    );
    assert!(rows(&c, "SELECT * FROM pg_inherits").await.is_empty());

    // psql 18's `\d ev`: its oid, then the foreign keys through
    // `pg_partition_ancestors`, which only a partitioned table's `\d` sends.
    let oid = rows(&c, "SELECT oid FROM pg_class WHERE relname = 'ev'").await.remove(0);
    let fks = format!(
        "SELECT conrelid = '{oid}'::pg_catalog.regclass AS sametable,\n       conname,\n       \
         pg_catalog.pg_get_constraintdef(oid, true) AS condef,\n       conrelid::pg_catalog.regclass AS ontable\n  \
         FROM pg_catalog.pg_constraint,\n       pg_catalog.pg_partition_ancestors('{oid}')\n \
         WHERE conrelid = relid AND contype = 'f' AND conparentid = 0\nORDER BY sametable DESC, conname;"
    );
    assert!(rows(&c, &fks).await.is_empty(), "ev has no foreign key");
    c.batch_execute("CREATE TABLE ev_ref (item_id BIGINT NOT NULL REFERENCES items (id)) PARTITION BY HASH (item_id) PARTITIONS 4")
        .await
        .expect("a partitioned table with a foreign key");
    let ref_oid = rows(&c, "SELECT oid FROM pg_class WHERE relname = 'ev_ref'").await.remove(0);
    let ref_fks = rows(&c, &fks.replace(&oid, &ref_oid)).await;
    assert_eq!(ref_fks.len(), 1, "{ref_fks:?}");
    assert!(ref_fks[0].starts_with("t|ev_ref_item_id_fkey|") && ref_fks[0].ends_with("|ev_ref"), "{ref_fks:?}");

    // `\d+`: storage and compression, for a partitioned table and a plain one.
    for (table, storage) in [("ev", ["plain", "plain", "plain", "extended"].as_slice()), ("items", &[])] {
        let toid = rows(&c, &format!("SELECT oid FROM pg_class WHERE relname = '{table}'")).await.remove(0);
        let got = rows(
            &c,
            &format!(
                "SELECT a.attname, a.attstorage, a.attcompression AS attcompression,\n  \
                 CASE WHEN a.attstattarget=-1 THEN NULL ELSE a.attstattarget END AS attstattarget\n\
                 FROM pg_catalog.pg_attribute a\nWHERE a.attrelid = '{toid}' AND a.attnum > 0 AND NOT a.attisdropped\n\
                 ORDER BY a.attnum;"
            ),
        )
        .await;
        assert!(!got.is_empty(), "{table}");
        for (i, want) in storage.iter().enumerate() {
            let code = if *want == "plain" { "p" } else { "x" };
            assert!(got[i].split('|').nth(1) == Some(code), "{table} column {i}: {got:?}");
        }
    }
}
