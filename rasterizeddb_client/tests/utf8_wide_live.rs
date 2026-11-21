//! A wide ORM row holding multi-byte UTF-8 text, written by `insert` and
//! `update` against a real server.
//!
//! Needs `RDB_SERVER_BIN`, as `sdk_live.rs` does; without it each test says
//! so and passes.
#![cfg(all(feature = "derive", feature = "serde_json"))]
#![cfg(unix)]

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use rasterizeddb_client::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::process::{Child, Command};

struct Server {
    child: Child,
    dir: PathBuf,
    socket: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn server(name: &str) -> Option<Server> {
    let Ok(bin) = std::env::var("RDB_SERVER_BIN") else {
        eprintln!("RDB_SERVER_BIN is not set: skipping {name}");
        return None;
    };
    let dir = std::env::temp_dir().join(format!("rdb_utf8_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("db")).unwrap();
    let socket = dir.join("s.sock");
    let child = Command::new(bin)
        .arg("--location")
        .arg(dir.join("db"))
        .arg("--rdb-socket")
        .arg(&socket)
        .args(["--rdb-tcp", "off", "--rdb-socket-auth", "trust", "--socket-mode", "600"])
        .args(["--log-level", "error"])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start the server");
    let server = Server { child, dir, socket: format!("unix:{}", socket.display()) };
    for _ in 0..600 {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            return Some(server);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the server did not open {} within 60 s", socket.display());
}

async fn connect(server: &Server) -> Client {
    Client::connect(
        &server.socket,
        Options { timeout: Some(Duration::from_secs(30)), ..Options::default() },
    )
    .await
    .expect("connect")
}

fn bounded<F: std::future::Future<Output = ()>>(f: F) -> impl std::future::Future<Output = ()> {
    async move {
        tokio::time::timeout(Duration::from_secs(300), f)
            .await
            .expect("the test did not finish within 300 s")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Button {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    label: String,
    #[serde(default, rename = "labelEn")]
    label_en: Option<String>,
    #[serde(default, rename = "labelDe")]
    label_de: Option<String>,
    icon: String,
    enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Access {
    owner: bool,
    manager: bool,
    waiter: bool,
}

#[derive(Model, Debug, Clone, PartialEq)]
#[rdb(table = "stores")]
struct Store {
    #[rdb(primary_key)]
    id: String,
    name: String,
    #[rdb(index)]
    owner_uid: String,
    active: bool,
    approval_status: String,
    auto_lock: bool,
    grace_days: i32,
    paid_until: String,
    billing_amount_cents: i64,
    manual_locked: bool,
    owner_locked: bool,
    archived: bool,
    #[rdb(json)]
    guest_buttons: Vec<Button>,
    #[rdb(json)]
    basic_guest_buttons: Vec<Button>,
    basic_mode: bool,
    menu_url: String,
    menu_url_en: String,
    menu_url_de: String,
    printing_enabled: bool,
    print_mode: String,
    waiter_ordering_enabled: bool,
    managers_enabled: bool,
    screens_enabled: bool,
    people_read_only: bool,
    screens_read_only: bool,
    #[rdb(json)]
    printer_role_access: Access,
    analytics_enabled: bool,
    #[rdb(json)]
    analytics_modules: Vec<String>,
    #[rdb(default = "0")]
    analytics_addon_price_cents: i64,
    push_owner: bool,
    push_staff: bool,
    push_central: bool,
    board_revision: i64,
    created_at: i64,
}

fn button(id: &str, kind: &str, label: &str, en: &str, de: &str, icon: &str, enabled: bool) -> Button {
    Button {
        id: id.into(),
        kind: kind.into(),
        label: label.into(),
        label_en: Some(en.into()),
        label_de: Some(de.into()),
        icon: icon.into(),
        enabled,
    }
}

fn buttons() -> Vec<Button> {
    vec![
        button("menu", "menu", "Δες μενού", "View menu", "Speisekarte", "📖", false),
        button("water", "request", "Ζητάω νερό", "Water please", "Wasser bitte", "💧", true),
        button("bill", "request", "Θέλω να πληρώσω", "I want to pay", "Ich möchte zahlen", "💳", true),
        button("order", "request", "Θέλω να παραγγείλω", "I want to order", "Ich möchte bestellen", "🍽️", true),
        button("waiter", "request", "Ζητάω σερβιτόρο", "Call the waiter", "Kellner rufen", "🙋", true),
        button("selfOrder", "selfOrder", "Παραγγέλνω μόνος μου", "Order myself", "Selbst bestellen", "🛒", false),
    ]
}

fn store(id: &str, name: &str, guest: Vec<Button>) -> Store {
    Store {
        id: id.into(),
        name: name.into(),
        owner_uid: "owner-1".into(),
        active: true,
        approval_status: "approved".into(),
        auto_lock: false,
        grace_days: 7,
        paid_until: String::new(),
        billing_amount_cents: 0,
        manual_locked: false,
        owner_locked: false,
        archived: false,
        guest_buttons: guest,
        basic_guest_buttons: Vec::new(),
        basic_mode: false,
        menu_url: String::new(),
        menu_url_en: String::new(),
        menu_url_de: String::new(),
        printing_enabled: false,
        print_mode: "auto".into(),
        waiter_ordering_enabled: false,
        managers_enabled: true,
        screens_enabled: true,
        people_read_only: false,
        screens_read_only: false,
        printer_role_access: Access { owner: true, manager: true, waiter: false },
        analytics_enabled: false,
        analytics_modules: vec!["sales".into()],
        analytics_addon_price_cents: 0,
        push_owner: true,
        push_staff: true,
        push_central: false,
        board_revision: 0,
        created_at: 1_763_700_000_000,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wide_row_with_multibyte_text_inserts_and_updates() {
    bounded(async {
        let Some(server) = server("wide").await else {
            return;
        };
        let c = connect(&server).await;
        c.create_table::<Store>().await.unwrap();

        let cases = [
            ("s1", "Δοκιμή", buttons(), ""),
            ("s2", "é", buttons(), ""),
            ("s3", "Δοκιμή", buttons()[..3].to_vec(), ""),
            ("s4", "Probe", buttons(), ""),
            ("s5", "Probe", buttons(), "Δοκιμή"),
        ];
        for (id, name, guest, menu_url) in cases {
            let mut s = store(id, "Probe", Vec::new());
            let inserted = c.insert(&s).await.unwrap_or_else(|e| panic!("insert {id}: {e}"));
            assert_eq!(inserted, s, "{id}");

            s.name = name.into();
            s.guest_buttons = guest;
            s.menu_url = menu_url.into();
            let sql = rasterizeddb_client::orm::update_sql(&s).unwrap();
            let updated = c
                .update(&s)
                .await
                .unwrap_or_else(|e| panic!("update {id}: {e}\n{sql}"));
            assert_eq!(updated, s, "{id}");

            let mut fresh = s.clone();
            fresh.id = format!("{id}-new");
            let inserted = c.insert(&fresh).await.unwrap_or_else(|e| panic!("insert {id}-new: {e}"));
            assert_eq!(inserted, fresh, "{id}-new");
        }
    })
    .await
}

/// Statements that wake the server's text rewrites, each with a multi-byte
/// character put at every position: an error is fine, a handler panic is not.
#[tokio::test(flavor = "multi_thread")]
async fn no_statement_rewrite_cuts_a_multibyte_character() {
    bounded(async {
        let Some(server) = server("sweep").await else {
            return;
        };
        let c = connect(&server).await;
        c.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, a TEXT, b BIGINT, c TEXT)", &[]).await.unwrap();
        c.execute("CREATE TABLE u (b BIGINT)", &[]).await.unwrap();
        c.execute("INSERT INTO t (id, a, b, c) VALUES (1, 'x', 2, 'y')", &[]).await.unwrap();
        const TRIGGERS: &[&str] = &[
            "SELECT * FROM t WHERE b = ANY('{1,2}'::int[]) AND b <> ALL(ARRAY[1,2]::bigint[]) OR a = SOME('{x}'::text[])",
            "UPDATE t SET (a, c) = ('p', 'q') WHERE id = 1 RETURNING a",
            "SELECT sum(b) OVER w, rank() OVER (w ORDER BY c) FROM t WINDOW w AS (PARTITION BY a)",
            "SELECT d.x FROM (SELECT a, b FROM t) AS d(x, y)",
            "TABLE t",
            "WITH x AS MATERIALIZED (SELECT 1 AS k) SELECT * FROM x",
            "SELECT ROW(1, 'a') = ROW(1, 'a'), (b, id) IN (SELECT 1, 2) FROM t",
            "SELECT a || 'x', a ~ 'b', a !~* 'c', a ^@ 'd' FROM t",
            "SELECT * FROM t TABLESAMPLE BERNOULLI (10) REPEATABLE (1)",
            "SELECT DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00+02', INTERVAL '1 day'",
            "SELECT E'a\\nb', U&'d\\0061t', $$q$$ FROM t",
            "SELECT ARRAY(SELECT a FROM t), 't'::regclass, to_regclass('t')",
            "SELECT * FROM t WHERE b > ANY (SELECT b FROM u) AND b < ALL (SELECT b FROM u)",
            "SELECT a, count(*) FROM t WHERE c LIKE 'y%' GROUP BY a HAVING count(*) >= 1 ORDER BY a DESC LIMIT 5",
            "INSERT INTO t (id, a, b, c) VALUES (9, 'all', 3, '[]') ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a RETURNING id",
            "SELECT * FROM t AS l LEFT JOIN LATERAL (SELECT b FROM u WHERE u.b = l.b) AS r ON true",
            "SELECT CASE WHEN a = 'x' THEN 'Call the waiter' ELSE c END, CAST(b AS TEXT) FROM t",
        ];
        let mut panics = Vec::new();
        for base in TRIGGERS {
            for at in (0..=base.len()).filter(|&i| base.is_char_boundary(i)) {
                for ch in ["Δ", "😀"] {
                    let sql = format!("{}{}{}", &base[..at], ch, &base[at..]);
                    if let Err(e) = c.query(&sql, &[]).await
                        && e.to_string().contains("panicked")
                    {
                        panics.push(format!("{sql}: {e}"));
                    }
                }
            }
        }
        assert!(panics.is_empty(), "{} panics, first: {:#?}", panics.len(), &panics[..panics.len().min(10)]);
    })
    .await
}
