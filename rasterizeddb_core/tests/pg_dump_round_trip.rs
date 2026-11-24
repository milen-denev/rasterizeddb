//! PostgreSQL's `pg_dump` against this server (`pgwire::pg_dump`), through
//! the real binary.
//!
//! `catalogue_answers_dump_every_partitioned_row` pins what `pg_dump` reads
//! and needs no PostgreSQL installed: its catalogue queries, as `pg_dump` 18
//! sends them to a server reporting 16.3, answered in its session and only
//! there; a partitioned table answered as the table its rows are read from,
//! with its partitioning as a post-data conversion index; and the
//! statements a dump restores with, run here.
//!
//! `real_pg_dump_round_trips_plain_and_partitioned_tables` runs the real
//! `pg_dump`, `pg_restore` and `psql` when they are installed (on `PATH`, or
//! in the directory `RDB_PG_BIN` names) and is skipped, saying so, when they
//! are not: a dump in the plain and the custom format, each restored into a
//! fresh database here, every table compared row for row with the source.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

type Rows = Vec<Vec<Option<String>>>;

/// A minimal simple-query client: trust login, `Q`, rows until `Z`, and
/// `COPY` both ways.
struct Pg(TcpStream);

impl Pg {
    fn connect(port: u16, database: &str) -> Pg {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
        let mut pg = Pg(s);
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        for (k, v) in [("user", "rasterized"), ("database", database)] {
            body.extend_from_slice(k.as_bytes());
            body.push(0);
            body.extend_from_slice(v.as_bytes());
            body.push(0);
        }
        body.push(0);
        let mut msg = ((body.len() + 4) as i32).to_be_bytes().to_vec();
        msg.extend_from_slice(&body);
        pg.0.write_all(&msg).unwrap();
        pg.until_ready().expect("login");
        pg
    }

    fn read_msg(&mut self) -> (u8, Vec<u8>) {
        let mut head = [0u8; 5];
        self.0.read_exact(&mut head).expect("message");
        let len = i32::from_be_bytes(head[1..5].try_into().unwrap()) as usize - 4;
        let mut body = vec![0u8; len];
        self.0.read_exact(&mut body).expect("body");
        (head[0], body)
    }

    fn send(&mut self, kind: u8, payload: &[u8]) {
        let mut msg = vec![kind];
        msg.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
        msg.extend_from_slice(payload);
        self.0.write_all(&msg).unwrap();
    }

    /// Rows (a `COPY … TO`'s lines as one-cell rows) until ReadyForQuery;
    /// `Err` with the server's message on an error.
    fn until_ready(&mut self) -> Result<Rows, String> {
        let mut rows = Vec::new();
        let mut error = None;
        loop {
            let (kind, body) = self.read_msg();
            match kind {
                b'D' => {
                    let n = i16::from_be_bytes(body[0..2].try_into().unwrap()) as usize;
                    let mut at = 2;
                    let mut row = Vec::with_capacity(n);
                    for _ in 0..n {
                        let len = i32::from_be_bytes(body[at..at + 4].try_into().unwrap());
                        at += 4;
                        if len < 0 {
                            row.push(None);
                        } else {
                            row.push(Some(String::from_utf8_lossy(&body[at..at + len as usize]).into_owned()));
                            at += len as usize;
                        }
                    }
                    rows.push(row);
                }
                b'd' => rows.push(vec![Some(String::from_utf8_lossy(&body).into_owned())]),
                b'E' => {
                    let text = String::from_utf8_lossy(&body).into_owned();
                    let m = text.split('\0').find_map(|f| f.strip_prefix('M')).unwrap_or("error").to_string();
                    error = Some(m);
                }
                b'Z' => return error.map_or(Ok(rows), Err),
                _ => {}
            }
        }
    }

    fn query(&mut self, sql: &str) -> Result<Rows, String> {
        let mut payload = sql.as_bytes().to_vec();
        payload.push(0);
        self.send(b'Q', &payload);
        self.until_ready()
    }

    fn rows(&mut self, sql: &str) -> Rows {
        self.query(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
    }

    /// `COPY … FROM STDIN` with `data`, text format.
    fn copy_in(&mut self, sql: &str, data: &str) -> Result<Rows, String> {
        let mut payload = sql.as_bytes().to_vec();
        payload.push(0);
        self.send(b'Q', &payload);
        let (kind, body) = self.read_msg();
        if kind != b'G' {
            let text = String::from_utf8_lossy(&body).into_owned();
            let _ = self.until_ready();
            return Err(format!("{sql}: no CopyInResponse ({}): {text}", kind as char));
        }
        self.send(b'd', data.as_bytes());
        self.send(b'c', &[]);
        self.until_ready()
    }
}

fn cell(rows: &Rows, row: usize, column: usize) -> String {
    rows.get(row).and_then(|r| r.get(column)).cloned().flatten().unwrap_or_else(|| "NULL".to_string())
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Cleanup(PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// A server on a fresh directory under /var/tmp (/tmp is a tmpfs here),
/// removed with it.
fn start(name: &str) -> (Server, u16, Cleanup) {
    let dir = PathBuf::from(format!("/var/tmp/{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("data")).unwrap();
    let cleanup = Cleanup(dir.clone());
    // `free_port` lets the port go before the server binds it, and the tests
    // here start their servers in parallel: two can be handed one port. The
    // one that loses the bind exits, and a readiness check that only connects
    // would then hand this test the other test's server. So the port counts
    // only while this test's own server is alive once it answers; otherwise
    // the start is retried on a new port.
    for _attempt in 0..5 {
        let port = free_port();
        let child = Command::new(env!("CARGO_BIN_EXE_rasterizeddb_core"))
            .arg("--location")
            .arg(dir.join("data"))
            .args(["--protocol", "pgwire", "--pg-tls", "disable", "--pg-auth", "trust"])
            .args(["--pg-port", &port.to_string(), "--log-level", "error"])
            .env_remove("RDB_PARTITION_MAX_OPEN")
            .env_remove("RASTERIZEDDB_INDEX_RESIDENCY")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the server");
        let mut server = Server(child);
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut exited = false;
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            if server.0.try_wait().expect("poll the server").is_some() {
                exited = true;
                break;
            }
            assert!(Instant::now() < deadline, "the server did not start");
            std::thread::sleep(Duration::from_millis(50));
        }
        // Answered: still ours only if our server is still running (a
        // server that lost the bind exits right after trying it).
        if !exited {
            std::thread::sleep(Duration::from_millis(200));
            exited = server.0.try_wait().expect("poll the server").is_some();
        }
        if !exited {
            return (server, port, cleanup);
        }
    }
    panic!("the server did not start on any of five ports");
}

/// Every common type with NULLs, an enum type among them; a SERIAL, an
/// identity column, an explicit sequence a default reads; PRIMARY KEY,
/// UNIQUE, FOREIGN KEY, CHECK, DEFAULT; a secondary index; a
/// hash-partitioned table of 2,001 rows; a view.
const SCHEMA: &[&str] = &[
    "CREATE SEQUENCE order_no_seq START 1000 INCREMENT 5",
    "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
    "CREATE TABLE feel (id INT PRIMARY KEY, m mood, ms mood[])",
    "INSERT INTO feel VALUES (1, 'sad', '{sad,happy}'), (2, NULL, NULL), (3, 'happy', '{}')",
    "CREATE TABLE customers (id SERIAL PRIMARY KEY, email TEXT NOT NULL UNIQUE, name VARCHAR(100), \
     active BOOLEAN DEFAULT true, score NUMERIC(10,2) CHECK (score >= 0))",
    "CREATE TABLE orders (id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
     order_no BIGINT DEFAULT nextval('order_no_seq'), customer_id INT REFERENCES customers(id), \
     amount NUMERIC(12,2) NOT NULL, placed DATE)",
    "CREATE INDEX orders_customer_idx ON orders (customer_id)",
    "CREATE TABLE typed (id INT PRIMARY KEY, s SMALLINT, b BIGINT, r REAL, d DOUBLE PRECISION, n NUMERIC, \
     t TEXT, c CHAR(5), bo BOOLEAN, dt DATE, ts TIMESTAMP, tstz TIMESTAMPTZ, iv INTERVAL, u UUID, \
     j JSON, jb JSONB, ba BYTEA, arr INT[])",
    "CREATE TABLE ev (tenant_id INT NOT NULL, id BIGINT NOT NULL, payload TEXT, amount NUMERIC(10,2), \
     PRIMARY KEY (tenant_id, id)) PARTITION BY HASH (tenant_id) PARTITIONS 8",
    "CREATE VIEW active_customers AS SELECT id, email FROM customers WHERE active",
    "INSERT INTO customers (email, name, active, score) VALUES ('a@x.org', 'Alice', true, 12.50), \
     ('b@x.org', 'Bob O''Brien', false, 0), ('c@x.org', NULL, NULL, NULL), \
     ('d@x.org', 'Ζωή ünïcödé 🎉', true, 99999999.99), ('e@x.org', E'tab\\there\\nline\\\\back', true, 1.01)",
    "INSERT INTO orders (customer_id, amount, placed) VALUES (1, 10.00, '2024-01-01'), (1, 20.50, NULL), \
     (2, 0.01, '2024-02-29'), (NULL, 1234567890.12, '1999-12-31')",
    "INSERT INTO typed VALUES (1, 1, 3, 1.5, 2.25, 3.14159265358979, 'text', 'ab', true, '2024-01-01', \
     '2024-01-01 12:34:56.789', '2024-01-01 12:34:56.789+02', '1 day 02:03:04', \
     'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"a\": 1}', '{\"k\": \"v\", \"n\": null}', '\\x0001ff', '{1,2,3}'), \
     (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL), \
     (3, -32768, 9223372036854775807, -0.5, 1e100, -0.000001, 'quote '' \"dq\" \\ back', 'abcde', false, \
     '1970-01-01', '2099-12-31 23:59:59', '1970-01-01 00:00:00+00', '-1 days', \
     '00000000-0000-0000-0000-000000000000', '[]', '[]', '\\x', '{-1,0,NULL}')",
    "INSERT INTO ev (tenant_id, id, payload, amount) \
     SELECT g % 37, g, 'payload ' || g, (g % 1000) / 100.0 FROM generate_series(1, 2000) g",
    "INSERT INTO ev (tenant_id, id, payload, amount) VALUES (999, 1, NULL, NULL)",
];

/// Each relation's rows in key order, as text: what a dump must carry.
const COMPARED: &[(&str, &str)] = &[
    ("customers", "SELECT * FROM customers ORDER BY id"),
    ("orders", "SELECT * FROM orders ORDER BY id"),
    ("typed", "SELECT * FROM typed ORDER BY id"),
    ("ev", "SELECT * FROM ev ORDER BY tenant_id, id"),
    ("active_customers", "SELECT * FROM active_customers ORDER BY id"),
    ("feel", "SELECT * FROM feel ORDER BY id"),
];

fn create_schema(pg: &mut Pg) {
    for sql in SCHEMA {
        pg.query(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    assert_eq!(cell(&pg.rows("SELECT count(*) FROM ev"), 0, 0), "2001");
}

/// `getTables` as `pg_dump` 18 sends it to a server reporting 16.3.
const GET_TABLES: &str = "SELECT c.tableoid, c.oid, c.relname, c.relnamespace, c.relkind, c.reltype, c.relowner, \
c.relchecks, c.relhasindex, c.relhasrules, c.relpages, c.reltuples, c.relallvisible, 0 AS relallfrozen, \
c.relhastriggers, c.relpersistence, c.reloftype, c.relacl, acldefault(CASE WHEN c.relkind = 'S' THEN 's'::\"char\" \
ELSE 'r'::\"char\" END, c.relowner) AS acldefault, CASE WHEN c.relkind = 'f' THEN (SELECT ftserver FROM \
pg_catalog.pg_foreign_table WHERE ftrelid = c.oid) ELSE 0 END AS foreignserver, c.relfrozenxid, tc.relfrozenxid AS \
tfrozenxid, tc.oid AS toid, tc.relpages AS toastpages, tc.reloptions AS toast_reloptions, d.refobjid AS owning_tab, \
d.refobjsubid AS owning_col, tsp.spcname AS reltablespace, false AS relhasoids, c.relispopulated, c.relreplident, \
c.relrowsecurity, c.relforcerowsecurity, c.relminmxid, tc.relminmxid AS tminmxid, \
array_remove(array_remove(c.reloptions,'check_option=local'),'check_option=cascaded') AS reloptions, CASE WHEN \
'check_option=local' = ANY (c.reloptions) THEN 'LOCAL'::text WHEN 'check_option=cascaded' = ANY (c.reloptions) THEN \
'CASCADED'::text ELSE NULL END AS checkoption, am.amname, (d.deptype = 'i') IS TRUE AS is_identity_sequence, \
c.relispartition AS ispartition \nFROM pg_class c\nLEFT JOIN pg_depend d ON (c.relkind = 'S' AND d.classid = \
'pg_class'::regclass AND d.objid = c.oid AND d.objsubid = 0 AND d.refclassid = 'pg_class'::regclass AND d.deptype IN \
('a', 'i'))\nLEFT JOIN pg_tablespace tsp ON (tsp.oid = c.reltablespace)\nLEFT JOIN pg_am am ON (c.relam = am.oid)\n\
LEFT JOIN pg_class tc ON (c.reltoastrelid = tc.oid AND tc.relkind = 't' AND c.relkind <> 'p')\nWHERE c.relkind IN \
('r', 'S', 'v', 'c', 'm', 'f', 'p')\nORDER BY c.oid";

/// `getIndexes`, for the tables `OIDS` names.
const GET_INDEXES: &str = "SELECT t.tableoid, t.oid, i.indrelid, t.relname AS indexname, t.relpages, t.reltuples, \
t.relallvisible, 0 AS relallfrozen, pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef, i.indkey, \
i.indisclustered, c.contype, c.conname, c.condeferrable, c.condeferred, c.tableoid AS contableoid, c.oid AS conoid, \
pg_catalog.pg_get_constraintdef(c.oid, false) AS condef, CASE WHEN i.indexprs IS NOT NULL THEN (SELECT \
pg_catalog.array_agg(attname ORDER BY attnum)  FROM pg_catalog.pg_attribute   WHERE attrelid = i.indexrelid) ELSE \
NULL END AS indattnames, (SELECT spcname FROM pg_catalog.pg_tablespace s WHERE s.oid = t.reltablespace) AS \
tablespace, t.reloptions AS indreloptions, i.indisreplident, inh.inhparent AS parentidx, i.indnkeyatts AS \
indnkeyatts, i.indnatts AS indnatts, (SELECT pg_catalog.array_agg(attnum ORDER BY attnum)   FROM \
pg_catalog.pg_attribute   WHERE attrelid = i.indexrelid AND     attstattarget >= 0) AS indstatcols, (SELECT \
pg_catalog.array_agg(attstattarget ORDER BY attnum)   FROM pg_catalog.pg_attribute   WHERE attrelid = i.indexrelid \
AND     attstattarget >= 0) AS indstatvals, i.indnullsnotdistinct, NULL AS conperiod FROM \
unnest('{OIDS}'::pg_catalog.oid[]) AS src(tbloid)\nJOIN pg_catalog.pg_index i ON (src.tbloid = i.indrelid) JOIN \
pg_catalog.pg_class t ON (t.oid = i.indexrelid) JOIN pg_catalog.pg_class t2 ON (t2.oid = i.indrelid) LEFT JOIN \
pg_catalog.pg_constraint c ON (i.indrelid = c.conrelid AND i.indexrelid = c.conindid AND c.contype IN ('p','u','x')) \
LEFT JOIN pg_catalog.pg_inherits inh ON (inh.inhrelid = indexrelid) WHERE (i.indisvalid OR t2.relkind = 'p') AND \
i.indisready ORDER BY i.indrelid, indexname";

/// `getTableAttrs`' defaults, for `OIDS`.
const GET_DEFAULTS: &str = "SELECT a.tableoid, a.oid, adrelid, adnum, pg_catalog.pg_get_expr(adbin, adrelid) AS adsrc\n\
FROM unnest('{OIDS}'::pg_catalog.oid[]) AS src(tbloid)\nJOIN pg_catalog.pg_attrdef a ON (src.tbloid = a.adrelid)\n\
ORDER BY a.adrelid, a.adnum";

/// The value of `column` in the row of `rows` (described by `names`) whose
/// `key` column is `value`.
fn field(rows: &Rows, names: &[&str], key: &str, value: &str, column: &str) -> String {
    let k = names.iter().position(|n| *n == key).unwrap();
    let c = names.iter().position(|n| *n == column).unwrap();
    let row = rows
        .iter()
        .find(|r| r[k].as_deref() == Some(value))
        .unwrap_or_else(|| panic!("no row with {key} = {value}: {rows:?}"));
    row[c].clone().unwrap_or_else(|| "NULL".to_string())
}

const TABLE_COLUMNS: &[&str] = &[
    "tableoid", "oid", "relname", "relnamespace", "relkind", "reltype", "relowner", "relchecks", "relhasindex",
    "relhasrules", "relpages", "reltuples", "relallvisible", "relallfrozen", "relhastriggers", "relpersistence",
    "reloftype", "relacl", "acldefault", "foreignserver", "relfrozenxid", "tfrozenxid", "toid", "toastpages",
    "toast_reloptions", "owning_tab", "owning_col", "reltablespace", "relhasoids", "relispopulated", "relreplident",
    "relrowsecurity", "relforcerowsecurity", "relminmxid", "tminmxid", "reloptions", "checkoption", "amname",
    "is_identity_sequence", "ispartition",
];

#[test]
fn catalogue_answers_dump_every_partitioned_row() {
    let (_server, port, _cleanup) = start("pg_dump_catalogue");
    let mut pg = Pg::connect(port, "postgres");
    create_schema(&mut pg);

    // Any other session sees the partitioned parent PostgreSQL would show,
    // and pg_dump's own statement shape is not answered for it.
    assert_eq!(pg.rows("SELECT relkind FROM pg_class WHERE relname = 'ev'"), vec![vec![Some("p".to_string())]]);
    assert!(pg.rows("SELECT * FROM pg_inherits").is_empty());

    // pg_dump's session: `synchronize_seqscans` off, then a REPEATABLE READ
    // block, as `setup_connection` opens it.
    let mut dump = Pg::connect(port, "postgres");
    assert_eq!(dump.rows("SELECT pg_catalog.pg_is_in_recovery()"), vec![vec![Some("f".to_string())]]);
    for sql in [
        "SELECT pg_catalog.set_config('search_path', '', false);",
        "SET DATESTYLE = ISO",
        "SET extra_float_digits TO 3",
        "SET synchronize_seqscans TO off",
        "SET row_security = off",
        "BEGIN",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
    ] {
        dump.rows(sql);
    }
    let tables = dump.rows(GET_TABLES);
    assert!(tables.iter().all(|r| r.len() == TABLE_COLUMNS.len()), "{tables:?}");
    let oids: Vec<i64> = tables.iter().map(|r| r[1].as_deref().unwrap().parse().unwrap()).collect();
    assert!(oids.windows(2).all(|w| w[0] < w[1]), "pg_dump reads the relations in oid order: {oids:?}");
    let get = |name: &str, column: &str| field(&tables, TABLE_COLUMNS, "relname", name, column);
    // The partitioned table is the table its rows are read from, so its data
    // step is `COPY ev TO stdout` over every partition.
    assert_eq!(get("ev", "relkind"), "r");
    assert_eq!(get("ev", "relhasindex"), "t");
    assert_eq!(get("ev", "ispartition"), "f");
    assert_eq!(get("customers", "relkind"), "r");
    assert_eq!(get("customers", "relchecks"), "1");
    assert_eq!(get("active_customers", "relkind"), "v");
    // Sequences: a SERIAL's and an identity column's belong to their columns;
    // a sequence created on its own to nothing.
    assert_eq!(get("customers_id_seq", "relkind"), "S");
    assert_eq!(get("customers_id_seq", "owning_tab"), get("customers", "oid"));
    assert_eq!(get("customers_id_seq", "owning_col"), "1");
    assert_eq!(get("customers_id_seq", "is_identity_sequence"), "f");
    assert_eq!(get("orders_id_seq", "owning_tab"), get("orders", "oid"));
    assert_eq!(get("orders_id_seq", "is_identity_sequence"), "t");
    assert_eq!(get("order_no_seq", "owning_tab"), "NULL");

    // The partition key as a post-data conversion, its partition count kept.
    let ev = get("ev", "oid");
    let indexes = dump.rows(&GET_INDEXES.replace("OIDS", &ev));
    let defs: Vec<String> = indexes.iter().map(|r| r[8].clone().unwrap_or_default()).collect();
    assert!(
        defs.contains(&"CREATE INDEX ev_partition_key ON public.ev USING partition (tenant_id) WITH (partitions = 8)".to_string()),
        "{defs:?}"
    );
    let key = indexes.iter().find(|r| r[3].as_deref() == Some("ev_partition_key")).unwrap();
    assert_eq!((key[9].as_deref(), key[23].as_deref(), key[11].as_deref()), (Some("1"), Some("1"), None));
    let pkey = indexes.iter().find(|r| r[3].as_deref() == Some("ev_pkey")).unwrap();
    assert_eq!(pkey[11].as_deref(), Some("p"));

    // Defaults name their sequences schema-qualified, as PostgreSQL's
    // deparser does under the empty search_path a restore runs with.
    let customers = get("customers", "oid");
    let orders = get("orders", "oid");
    let defaults = dump.rows(&GET_DEFAULTS.replace("OIDS", &format!("{customers},{orders}")));
    let texts: Vec<String> = defaults.iter().map(|r| r[4].clone().unwrap_or_default()).collect();
    assert!(texts.contains(&"nextval('public.customers_id_seq'::regclass)".to_string()), "{texts:?}");
    assert!(texts.contains(&"nextval('public.order_no_seq'::regclass)".to_string()), "{texts:?}");
    // No default for the identity column: pg_dump writes ADD GENERATED.
    assert_eq!(texts.len(), 3, "{texts:?}");

    // A sequence's value: a SERIAL's is the one its next insert would seed
    // from, even before any insert since the server started.
    assert_eq!(
        dump.rows("SELECT last_value, is_called FROM public.customers_id_seq"),
        vec![vec![Some("5".to_string()), Some("t".to_string())]]
    );
    let view = get("active_customers", "oid");
    assert_eq!(
        cell(&dump.rows(&format!("SELECT pg_catalog.pg_get_viewdef('{view}'::pg_catalog.oid) AS viewdef")), 0, 0),
        "SELECT id, email FROM public.customers WHERE active;"
    );
    // An aggregate over a catalogue with no rows is one row of zeros.
    assert_eq!(
        dump.rows("SELECT count(*) FROM pg_subscription WHERE subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())"),
        vec![vec![Some("0".to_string())]]
    );
    // The data step reads every partition.
    let copied = dump.rows("COPY public.ev (tenant_id, id, payload, amount) TO stdout;");
    assert_eq!(copied.len(), 2001);
    dump.rows("COMMIT");

    // What a dump restores with: ADD GENERATED AS IDENTITY (it was ignored),
    // a COPY of the identity column's values, the default naming a
    // qualified sequence, the conversion on a qualified name.
    pg.rows("CREATE DATABASE restored");
    let mut r = Pg::connect(port, "restored");
    for sql in [
        "CREATE SEQUENCE public.n_seq START WITH 10 INCREMENT BY 10",
        "CREATE TABLE public.t (id BIGINT NOT NULL, n BIGINT DEFAULT nextval('public.n_seq'::regclass), k INT NOT NULL)",
        "ALTER TABLE public.t ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (\n    SEQUENCE NAME public.t_id_seq\n    \
         START WITH 1\n    INCREMENT BY 1\n    NO MINVALUE\n    NO MAXVALUE\n    CACHE 1\n)",
    ] {
        r.rows(sql);
    }
    assert!(r.query("ALTER TABLE public.t ALTER COLUMN k ADD GENERATED ALWAYS AS IDENTITY (SEQUENCE NAME other_seq)").is_err());
    assert!(r.query("ALTER TABLE public.t ALTER COLUMN id DROP IDENTITY").is_err(), "refused, not ignored");
    r.copy_in("COPY public.t (id, n, k) FROM stdin;", "1\t10\t1\n2\t20\t2\n7\t30\t3\n").expect("COPY of identity values");
    r.rows("SELECT pg_catalog.setval('public.t_id_seq', 7, true)");
    r.rows("SELECT pg_catalog.setval('public.n_seq', 30, true)");
    r.rows("CREATE INDEX t_partition_key ON public.t USING partition (k) WITH (partitions = 4)");
    assert_eq!(r.rows("SELECT relkind FROM pg_class WHERE relname = 't'"), vec![vec![Some("p".to_string())]]);
    assert_eq!(
        r.rows("INSERT INTO t (k) VALUES (4) RETURNING id, n"),
        vec![vec![Some("8".to_string()), Some("40".to_string())]]
    );
    assert_eq!(cell(&r.rows("SELECT count(*) FROM t"), 0, 0), "4");
}

/// The directory holding `pg_dump`, `pg_restore` and `psql`: `RDB_PG_BIN`,
/// or the one `PATH` finds `pg_dump` in.
fn pg_bin() -> Option<PathBuf> {
    let has_all = |dir: &Path| ["pg_dump", "pg_restore", "psql"].iter().all(|b| dir.join(b).is_file());
    if let Some(dir) = std::env::var_os("RDB_PG_BIN").map(PathBuf::from) {
        return has_all(&dir).then_some(dir);
    }
    std::env::split_paths(&std::env::var_os("PATH")?).find(|d| has_all(d))
}

/// Runs `command` to its end or for `limit`, whichever is first; its output
/// on failure.
fn run(mut command: Command, limit: Duration) -> Result<(), String> {
    let mut child = command.stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().map_err(|e| e.to_string())?;
    let mut out = child.stdout.take().unwrap();
    let mut err = child.stderr.take().unwrap();
    let readers = (
        std::thread::spawn(move || {
            let mut s = String::new();
            let _ = out.read_to_string(&mut s);
            s
        }),
        std::thread::spawn(move || {
            let mut s = String::new();
            let _ = err.read_to_string(&mut s);
            s
        }),
    );
    let deadline = Instant::now() + limit;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            break Some(status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let (out, err) = (readers.0.join().unwrap_or_default(), readers.1.join().unwrap_or_default());
    match status {
        Some(s) if s.success() && !err.contains("ERROR") => Ok(()),
        Some(s) => Err(format!("{command:?}: {s}\n{out}\n{err}")),
        None => Err(format!("{command:?}: still running after {limit:?}\n{out}\n{err}")),
    }
}

#[test]
fn real_pg_dump_round_trips_plain_and_partitioned_tables() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipped: no pg_dump, pg_restore and psql on PATH or in RDB_PG_BIN");
        return;
    };
    let version = Command::new(bin.join("pg_dump")).arg("--version").output().expect("pg_dump --version");
    let version = String::from_utf8_lossy(&version.stdout).to_string();
    let major: u32 = version
        .split_whitespace()
        .find_map(|w| w.split('.').next().and_then(|m| m.parse().ok()))
        .unwrap_or(0);
    if major < 16 {
        eprintln!("skipped: {version} is older than the 16.3 this server reports");
        return;
    }
    let (_server, port, cleanup) = start("pg_dump_real");
    let mut pg = Pg::connect(port, "postgres");
    create_schema(&mut pg);
    let source: Vec<Rows> = COMPARED.iter().map(|(_, sql)| pg.rows(sql)).collect();
    let tool = |name: &str| {
        let mut c = Command::new(bin.join(name));
        c.args(["-h", "127.0.0.1", "-p", &port.to_string(), "-U", "rasterized", "-w"]).env("PGCONNECT_TIMEOUT", "30");
        c
    };
    let plain = cleanup.0.join("dump.sql");
    let custom = cleanup.0.join("dump.custom");
    for (format, file) in [("-Fp", &plain), ("-Fc", &custom)] {
        let mut c = tool("pg_dump");
        c.args([format, "--no-owner", "-d", "postgres", "-f"]).arg(file);
        run(c, Duration::from_secs(300)).unwrap_or_else(|e| panic!("pg_dump {format}: {e}"));
    }
    let text = std::fs::read_to_string(&plain).unwrap();
    // Every partitioned row is in the dump, and the partitioning with it.
    let ev_rows = text
        .split("COPY public.ev (")
        .nth(1)
        .and_then(|rest| rest.split("\n\\.\n").next())
        .map(|block| block.lines().count() - 1)
        .unwrap_or(0);
    assert_eq!(ev_rows, 2001, "the dump's COPY of ev");
    assert!(text.contains("CREATE INDEX ev_partition_key ON public.ev USING partition (tenant_id) WITH (partitions = 8);"));

    for (database, restore) in [("from_plain", "plain"), ("from_custom", "custom")] {
        pg.rows(&format!("CREATE DATABASE {database}"));
        let mut c = if restore == "plain" {
            let mut c = tool("psql");
            c.args(["-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", database, "-f"]).arg(&plain);
            c
        } else {
            let mut c = tool("pg_restore");
            c.args(["--no-owner", "--exit-on-error", "-d", database]).arg(&custom);
            c
        };
        c.env("PGOPTIONS", "");
        run(c, Duration::from_secs(300)).unwrap_or_else(|e| panic!("restore {restore}: {e}"));
        let mut r = Pg::connect(port, database);
        for ((name, sql), want) in COMPARED.iter().zip(&source) {
            assert_eq!(&r.rows(sql), want, "{name} restored from the {restore} dump");
        }
        // Partitioned again, by the same key.
        assert_eq!(
            r.rows("SELECT c.relkind, pg_get_partkeydef(c.oid) FROM pg_class c WHERE c.relname = 'ev'"),
            vec![vec![Some("p".to_string()), Some("HASH (tenant_id)".to_string())]]
        );
        // Every counter carries on past the restored rows.
        assert_eq!(cell(&r.rows("INSERT INTO customers (email) VALUES ('new@x.org') RETURNING id"), 0, 0), "6");
        assert_eq!(
            r.rows("INSERT INTO orders (amount) VALUES (1) RETURNING id, order_no"),
            vec![vec![Some("5".to_string()), Some("1020".to_string())]]
        );
    }
}

/// The `pms` layout: composite keys on and onto hash-partitioned tables.
const PMS: &[&str] = &[
    "CREATE TABLE \"case\" (domain_id BIGINT NOT NULL, id BIGINT NOT NULL, PRIMARY KEY (domain_id, id))",
    "CREATE TABLE step (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, id BIGINT NOT NULL, \
     PRIMARY KEY (domain_id, case_id, id)) PARTITION BY HASH (domain_id, case_id) PARTITIONS 4",
    "CREATE TABLE field (domain_id BIGINT NOT NULL, case_id BIGINT NOT NULL, step_id BIGINT NOT NULL, \
     id BIGINT NOT NULL, PRIMARY KEY (domain_id, case_id, step_id, id)) PARTITION BY HASH (domain_id, case_id) PARTITIONS 4",
    "ALTER TABLE field ADD CONSTRAINT field_step_fk FOREIGN KEY (domain_id, case_id, step_id) \
     REFERENCES step (domain_id, case_id, id) ON DELETE CASCADE",
    "ALTER TABLE step ADD CONSTRAINT step_case_fk FOREIGN KEY (domain_id, case_id) REFERENCES \"case\" (domain_id, id)",
    "CREATE TABLE note (n INT PRIMARY KEY, domain_id BIGINT, case_id BIGINT, step_id BIGINT, \
     FOREIGN KEY (domain_id, case_id, step_id) REFERENCES step MATCH FULL)",
    "INSERT INTO \"case\" VALUES (1, 10), (1, 11), (2, 10), (-3, 7)",
    "INSERT INTO step VALUES (1, 10, 100), (1, 10, 101), (2, 10, 100), (-3, 7, 1)",
    "INSERT INTO field VALUES (1, 10, 100, 1), (1, 10, 101, 1), (-3, 7, 1, 1)",
    "INSERT INTO note VALUES (1, 1, 10, 100), (2, NULL, NULL, NULL)",
];

/// A dump of the `pms` layout restores whole: the partitioned tables are
/// dumped plain and converted by their post-data `CREATE INDEX … USING
/// partition`, and the composite keys, also post-data, are added onto and
/// from them; enforced afterwards, both sides, from both dump formats.
#[test]
fn real_pg_dump_round_trips_composite_keys_on_partitioned_tables() {
    let Some(bin) = pg_bin() else {
        eprintln!("skipped: no pg_dump, pg_restore and psql on PATH or in RDB_PG_BIN");
        return;
    };
    let (_server, port, cleanup) = start("pg_dump_cfk");
    let mut pg = Pg::connect(port, "postgres");
    for sql in PMS {
        pg.query(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let tool = |name: &str| {
        let mut c = Command::new(bin.join(name));
        c.args(["-h", "127.0.0.1", "-p", &port.to_string(), "-U", "rasterized", "-w"]).env("PGCONNECT_TIMEOUT", "30");
        c
    };
    let plain = cleanup.0.join("pms.sql");
    let custom = cleanup.0.join("pms.custom");
    for (format, file) in [("-Fp", &plain), ("-Fc", &custom)] {
        let mut c = tool("pg_dump");
        c.args([format, "--no-owner", "-d", "postgres", "-f"]).arg(file);
        run(c, Duration::from_secs(300)).unwrap_or_else(|e| panic!("pg_dump {format}: {e}"));
    }
    let text = std::fs::read_to_string(&plain).unwrap();
    let at = |needle: &str| text.find(needle).unwrap_or_else(|| panic!("the dump lacks `{needle}`:\n{text}"));
    let converts = at("CREATE INDEX step_partition_key ON public.step USING partition (domain_id, case_id) WITH (partitions = 4);");
    at("CREATE INDEX field_partition_key ON public.field USING partition (domain_id, case_id) WITH (partitions = 4);");
    // Every name quoted (`pg_get_constraintdef`'s documented divergence).
    let key = at("ADD CONSTRAINT field_step_fk FOREIGN KEY (\"domain_id\", \"case_id\", \"step_id\") REFERENCES public.step(\"domain_id\", \"case_id\", \"id\") ON DELETE CASCADE;");
    at("ADD CONSTRAINT step_case_fk FOREIGN KEY (\"domain_id\", \"case_id\") REFERENCES public.\"case\"(\"domain_id\", \"id\");");
    at("REFERENCES public.step(\"domain_id\", \"case_id\", \"id\") MATCH FULL;");
    // pg_dump's order: indexes, then foreign keys.
    assert!(converts < key, "the conversion comes before the keys");

    for (database, restore) in [("pms_plain", "plain"), ("pms_custom", "custom")] {
        pg.rows(&format!("CREATE DATABASE {database}"));
        let mut c = if restore == "plain" {
            let mut c = tool("psql");
            c.args(["-X", "-q", "-v", "ON_ERROR_STOP=1", "-d", database, "-f"]).arg(&plain);
            c
        } else {
            let mut c = tool("pg_restore");
            c.args(["--no-owner", "--exit-on-error", "-d", database]).arg(&custom);
            c
        };
        c.env("PGOPTIONS", "");
        run(c, Duration::from_secs(300)).unwrap_or_else(|e| panic!("restore {restore}: {e}"));
        let mut r = Pg::connect(port, database);
        assert_eq!(
            r.rows("SELECT relname, relkind FROM pg_class WHERE relname IN ('case', 'step', 'field') ORDER BY relname"),
            vec![
                vec![Some("case".to_string()), Some("r".to_string())],
                vec![Some("field".to_string()), Some("p".to_string())],
                vec![Some("step".to_string()), Some("p".to_string())],
            ],
            "{restore}"
        );
        assert_eq!(
            r.rows("SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE contype = 'f' ORDER BY conname"),
            vec![
                vec![
                    Some("field_step_fk".to_string()),
                    Some("FOREIGN KEY (\"domain_id\", \"case_id\", \"step_id\") REFERENCES \"step\"(\"domain_id\", \"case_id\", \"id\") ON DELETE CASCADE".to_string())
                ],
                vec![
                    Some("note_domain_id_case_id_step_id_fkey".to_string()),
                    Some("FOREIGN KEY (\"domain_id\", \"case_id\", \"step_id\") REFERENCES \"step\"(\"domain_id\", \"case_id\", \"id\") MATCH FULL".to_string())
                ],
                vec![
                    Some("step_case_fk".to_string()),
                    Some("FOREIGN KEY (\"domain_id\", \"case_id\") REFERENCES \"case\"(\"domain_id\", \"id\")".to_string())
                ],
            ],
            "{restore}"
        );
        assert_eq!(cell(&r.rows("SELECT count(*) FROM field"), 0, 0), "3");
        for sql in [
            "INSERT INTO field VALUES (1, 10, 102, 1)",
            "INSERT INTO step VALUES (2, 11, 1)",
            "INSERT INTO note VALUES (3, 1, NULL, 100)",
            "DELETE FROM \"case\" WHERE domain_id = 1 AND id = 10",
        ] {
            let e = r.query(sql).expect_err(sql);
            assert!(e.contains("foreign key"), "{restore}: {sql}: {e}");
        }
        r.rows("DELETE FROM step WHERE domain_id = -3");
        assert_eq!(cell(&r.rows("SELECT count(*) FROM field"), 0, 0), "2", "{restore}: cascaded");
    }
}
