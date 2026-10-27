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
    let port = free_port();
    let child = Command::new(env!("CARGO_BIN_EXE_rasterizeddb_core"))
        .args(["--location", dir, "--protocol", "pgwire", "--pg-tls", "disable", "--pg-auth", "trust"])
        .args(["--pg-port", &port.to_string(), "--log-level", "error"])
        .args(extra)
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
