//! Startup settings saved by `SET CONFIG`, through the real binary: saved for
//! the next start, applied by it, shown as pending until then, beaten by a
//! flag, and never a reason not to start.
//!
//! A server process per step, because the precedence lives in `main`, which
//! an in-process server never runs.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// A minimal simple-query client: trust login, `Q`, rows until `Z`.
struct Pg(TcpStream);

impl Pg {
    fn connect(port: u16) -> Pg {
        let s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        let mut pg = Pg(s);
        let mut body = Vec::new();
        body.extend_from_slice(&196608i32.to_be_bytes());
        for (k, v) in [("user", "rasterized"), ("database", "postgres")] {
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

    /// Rows until ReadyForQuery; `Err` with the server's message on an error.
    fn until_ready(&mut self) -> Result<Vec<Vec<Option<String>>>, String> {
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

    fn query(&mut self, sql: &str) -> Result<Vec<Vec<Option<String>>>, String> {
        let mut msg = vec![b'Q'];
        msg.extend_from_slice(&((sql.len() + 5) as i32).to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);
        self.0.write_all(&msg).unwrap();
        self.until_ready()
    }

    /// `(value, pending, source)` of `section.key` in SHOW CONFIG.
    fn config(&mut self, section: &str, key: &str) -> (String, String, String) {
        let rows = self.query("SHOW CONFIG").expect("SHOW CONFIG");
        let row = rows
            .iter()
            .find(|r| r[0].as_deref() == Some(section) && r[1].as_deref() == Some(key))
            .unwrap_or_else(|| panic!("{section}.{key} is not in SHOW CONFIG"));
        assert_eq!(row.len(), 9, "SHOW CONFIG has pending, options and source");
        let get = |i: usize| row[i].clone().unwrap_or_default();
        (get(2), get(6), get(8))
    }
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn start(dir: &str, extra: &[&str]) -> (Server, u16) {
    start_with_env(dir, extra, &[])
}

fn start_with_env(dir: &str, extra: &[&str], env: &[(&str, &str)]) -> (Server, u16) {
    let port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_rasterizeddb_core"))
        .args(["--location", dir, "--protocol", "pgwire", "--pg-tls", "disable", "--pg-auth", "trust"])
        .args(["--pg-port", &port.to_string(), "--log-level", "error"])
        .args(extra)
        .env_remove("RDB_PARTITION_MAX_OPEN")
        .envs(env.iter().copied())
        // The suite may be run with every index paged; this test is about
        // where a value comes from, so the default must be the default.
        .env_remove("RASTERIZEDDB_INDEX_RESIDENCY")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the server");
    let server = Server(child);
    let deadline = Instant::now() + Duration::from_secs(60);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "the server did not start");
        std::thread::sleep(Duration::from_millis(50));
    }
    (server, port)
}

#[test]
fn a_saved_setting_waits_for_the_next_start_and_a_flag_beats_it() {
    // A real directory: /tmp is a tmpfs here, and the file must outlive
    // each server process.
    let dir = format!("/var/tmp/startup_settings_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(dir.clone());

    {
        let (_server, port) = start(&dir, &[]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("server", "batch_size"), ("10000".into(), "".into(), "default".into()));
        let reply = pg.query("SET CONFIG server.batch_size = 5000").expect("saved");
        assert!(format!("{reply:?}").contains("next start"), "{reply:?}");
        // Saved, not applied: the running value is unchanged and the saved
        // one is pending.
        assert_eq!(pg.config("server", "batch_size"), ("10000".into(), "5000".into(), "default".into()));
        // The index residency is a startup key like any other.
        assert_eq!(pg.config("index", "residency"), ("memory".into(), "".into(), "default".into()));
        pg.query("SET CONFIG index.residency = 'disk'").expect("saved");
        assert!(pg.query("SET CONFIG index.residency = 'tape'").is_err());
        // Refused values are refused now, not at the next start.
        assert!(pg.query("SET CONFIG server.batch_size = lots").is_err());
        assert!(pg.query("SET CONFIG wal.enabled = false").is_err());
        // The other fixes this column set rides with: SET CONFIG syntax.
        let (mode, _, source) = pg.config("replication", "mode");
        assert_eq!(mode, "async");
        assert_eq!(source, "", "a hot key has no startup source");
    }
    {
        // Applied at the next start.
        let (_server, port) = start(&dir, &[]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("server", "batch_size"), ("5000".into(), "".into(), "saved".into()));
        assert_eq!(pg.config("index", "residency"), ("disk".into(), "".into(), "saved".into()));
        // And it does what it says: a table's index pages into `.idxr`.
        pg.query("CREATE TABLE paged_t (id BIGINT PRIMARY KEY, v INT)").expect("create");
        let values: Vec<String> = (1..=2000).map(|i| format!("({i}, {i})")).collect();
        pg.query(&format!("INSERT INTO paged_t (id, v) VALUES {}", values.join(","))).expect("insert");
        assert_eq!(pg.query("SELECT v FROM paged_t WHERE id = 1234").expect("lookup"), vec![vec![Some("1234".into())]]);
        fn has_idxr(dir: &std::path::Path) -> bool {
            std::fs::read_dir(dir).into_iter().flatten().flatten().any(|e| {
                let p = e.path();
                p.extension().is_some_and(|x| x == "idxr") || (p.is_dir() && has_idxr(&p))
            })
        }
        assert!(has_idxr(std::path::Path::new(&dir)), "no paged index directory under {dir}");
    }
    {
        // A flag wins, and the saved value shows as what it would be.
        let (_server, port) = start(&dir, &["--batch-size", "7000"]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("server", "batch_size"), ("7000".into(), "5000".into(), "command line".into()));
        let reply = pg.query("SET CONFIG server.batch_size = 6000").expect("saved");
        assert!(format!("{reply:?}").contains("wins over a saved value"), "{reply:?}");
    }
    {
        // A file that does not parse never keeps the server from starting.
        std::fs::write(format!("{dir}/rasterizeddb.conf"), "server.batch_size = abc\n").unwrap();
        let (_server, port) = start(&dir, &[]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("server", "batch_size").0, "10000");
    }
}

/// A hot key a session changes takes effect at once and is saved, so a
/// restart keeps it (`partition.idle_close_secs` had no flag and reverted at
/// every restart); `cache.size` is a startup key like the others; a flag
/// still wins over either; and a file that does not parse is never replaced.
#[test]
fn a_hot_setting_survives_a_restart_and_cache_size_is_saved() {
    let dir = format!("/var/tmp/startup_settings_hot_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(dir.clone());
    let conf = format!("{dir}/rasterizeddb.conf");

    {
        let (_server, port) = start(&dir, &[]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("partition", "idle_close_secs").2, "default");
        let reply = pg.query("SET CONFIG partition.idle_close_secs = 600").expect("applied");
        assert!(format!("{reply:?}").contains("saved, so it holds after a restart"), "{reply:?}");
        // Applied now, and a hot key has nothing pending.
        assert_eq!(pg.config("partition", "idle_close_secs"), ("600".into(), "".into(), "saved".into()));
        pg.query("SET CONFIG filter.bloom = off").expect("applied");
        pg.query("SET CONFIG auto_compact.dead_fraction = 0.4").expect("applied");
        // A refused value is neither applied nor saved.
        assert!(pg.query("SET CONFIG partition.idle_close_secs = soon").is_err());
        // The keys that act on a database say they are not saved.
        let reply = pg.query("SET CONFIG replication.batch_max_records = 128").expect("applied");
        assert!(format!("{reply:?}").contains("not saved"), "{reply:?}");
        assert_eq!(pg.config("replication", "batch_max_records").2, "", "a database's key has no source");

        assert_eq!(pg.config("cache", "size"), ("100000".into(), "".into(), "default".into()));
        let reply = pg.query("SET CONFIG cache.size = 5000").expect("saved");
        assert!(format!("{reply:?}").contains("next start"), "{reply:?}");
        assert_eq!(pg.config("cache", "size"), ("100000".into(), "5000".into(), "default".into()));
        assert!(pg.query("SET CONFIG cache.size = 0").is_err(), "0 would be an unbounded cache");
    }
    let saved = std::fs::read_to_string(&conf).unwrap();
    assert!(!saved.contains("replication."), "{saved}");
    {
        // Applied by the next start, from the file.
        let (_server, port) = start(&dir, &[]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("partition", "idle_close_secs"), ("600".into(), "".into(), "saved".into()));
        assert_eq!(pg.config("filter", "bloom"), ("false".into(), "".into(), "saved".into()));
        assert_eq!(pg.config("auto_compact", "dead_fraction"), ("0.4".into(), "".into(), "saved".into()));
        assert_eq!(pg.config("cache", "size"), ("5000".into(), "".into(), "saved".into()));
        assert_eq!(pg.config("replication", "batch_max_records").0, "256", "not saved, so the start-up value");
        assert_eq!(pg.config("wal", "synchronous_commit").2, "default");
    }
    {
        // A flag wins over both kinds, and says so.
        let (_server, port) = start(
            &dir,
            &["--filter-bloom", "true", "--cache-size", "7000", "--enable-cache", "true"],
        );
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("filter", "bloom"), ("true".into(), "".into(), "command line".into()));
        assert_eq!(pg.config("cache", "size"), ("7000".into(), "5000".into(), "command line".into()));
        // The keys without a flag still come from the file.
        assert_eq!(pg.config("partition", "idle_close_secs"), ("600".into(), "".into(), "saved".into()));
        let reply = pg.query("SET CONFIG filter.bloom = off").expect("applied");
        assert!(format!("{reply:?}").contains("wins over a saved value"), "{reply:?}");
        assert_eq!(pg.config("filter", "bloom"), ("false".into(), "".into(), "command line".into()));
    }
    {
        // A saved value a key refuses is skipped, and the start goes on.
        std::fs::write(&conf, "partition.idle_close_secs = soon\nfilter.max_f = 14\n").unwrap();
        let (_server, port) = start(&dir, &[]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("partition", "idle_close_secs").2, "default");
        assert_eq!(pg.config("filter", "max_f"), ("14".into(), "".into(), "saved".into()));
    }
    {
        // A file that does not parse: the start goes on without it, and a
        // SET CONFIG applies but refuses to save rather than replace it.
        let text = "partition.idle_close_secs = 600\nthis line is not a setting\n";
        std::fs::write(&conf, text).unwrap();
        let (_server, port) = start(&dir, &[]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("partition", "idle_close_secs").2, "default");
        let reply = pg.query("SET CONFIG partition.idle_close_secs = 700").expect("applied");
        let reply = format!("{reply:?}");
        assert!(reply.contains("not saved") && reply.contains("line 2"), "{reply}");
        assert_eq!(pg.config("partition", "idle_close_secs").0, "700");
        assert!(pg.query("SET CONFIG server.batch_size = 5000").is_err(), "a startup key cannot be saved either");
        assert_eq!(std::fs::read_to_string(&conf).unwrap(), text, "the file is left as it was");
    }
}

/// `partition.max_open` from each of its three places, the flag and the
/// environment winning over the file; and the start raises the open-file
/// limit for it (pms plans 5 tables of 1,024 partitions, past the old fixed
/// ceiling of 4,096).
#[test]
fn partition_max_open_comes_from_a_flag_the_environment_or_the_file() {
    let dir = format!("/var/tmp/startup_settings_max_open_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(dir.clone());

    /// The child's soft and hard open-file limits, from /proc.
    fn nofile(server: &Server) -> Option<(u64, u64)> {
        let limits = std::fs::read_to_string(format!("/proc/{}/limits", server.0.id())).ok()?;
        let line = limits.lines().find(|l| l.starts_with("Max open files"))?;
        let mut n = line.split_whitespace().skip(3).map(|v| v.parse::<u64>().ok());
        Some((n.next()??, n.next()??))
    }

    {
        let (_server, port) = start(&dir, &[]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("partition", "max_open"), ("4096".into(), "".into(), "default".into()));
        let reply = pg.query("SET CONFIG partition.max_open = 6000").expect("saved");
        assert!(format!("{reply:?}").contains("next start"), "{reply:?}");
        assert!(pg.query("SET CONFIG partition.max_open = 3").is_err(), "under the floor of 16");
        assert!(pg.query("SET CONFIG partition.max_open = 99999999").is_err(), "over the ceiling");
    }
    {
        let (server, port) = start(&dir, &[]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("partition", "max_open"), ("6000".into(), "".into(), "saved".into()));
        // Raised for 6,000 partitions of ten descriptors over the reserve,
        // as far as the hard limit allows.
        if let Some((soft, hard)) = nofile(&server) {
            let wanted = 2048 + 6000 * 10;
            assert!(soft >= wanted.min(hard), "soft {soft}, hard {hard}, wanted {wanted}");
        }
    }
    {
        let (_server, port) = start_with_env(&dir, &[], &[("RDB_PARTITION_MAX_OPEN", "8000")]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("partition", "max_open"), ("8000".into(), "6000".into(), "command line".into()));
    }
    {
        // The flag over the environment, and over the file.
        let (_server, port) =
            start_with_env(&dir, &["--partition-max-open", "5120"], &[("RDB_PARTITION_MAX_OPEN", "8000")]);
        let mut pg = Pg::connect(port);
        assert_eq!(pg.config("partition", "max_open"), ("5120".into(), "6000".into(), "command line".into()));
        let reply = pg.query("SET CONFIG partition.max_open = 7000").expect("saved");
        assert!(format!("{reply:?}").contains("wins over a saved value"), "{reply:?}");
    }
    {
        // A value out of range refuses the start rather than run with
        // another one.
        let status = Command::new(env!("CARGO_BIN_EXE_rasterizeddb_core"))
            .args(["--location", &dir, "--partition-max-open", "2"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run the server");
        assert_eq!(status.code(), Some(2));
    }
}
