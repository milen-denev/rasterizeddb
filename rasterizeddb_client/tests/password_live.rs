//! `Login::Password` against a real server: the SCRAM exchange in
//! `src/scram.rs` is a copy of the server's, and this is what keeps the two
//! in step.
//!
//! Needs `RDB_SERVER_BIN`, as `live.rs` does; without it each test says so and
//! passes.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use rasterizeddb_client::{Client, Login, Options};
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

/// A server whose socket asks for `auth`, with the bootstrap role `alice`
/// holding the password `hunter2`.
async fn server(name: &str, auth: &str) -> Option<Server> {
    let Ok(bin) = std::env::var("RDB_SERVER_BIN") else {
        eprintln!("RDB_SERVER_BIN is not set: skipping {name}");
        return None;
    };
    let dir = std::env::temp_dir().join(format!("rdb_pw_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("db")).unwrap();
    let socket = dir.join("s.sock");
    let child = Command::new(bin)
        .arg("--location")
        .arg(dir.join("db"))
        .arg("--rdb-socket")
        .arg(&socket)
        .args(["--rdb-tcp", "off", "--rdb-socket-auth", auth])
        .args(["--socket-mode", "600", "--rdb-user", "alice"])
        .args(["--log-level", "warn"])
        .env("RASTERIZEDDB_PASSWORD", "hunter2")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("start the server");
    let server = Server {
        child,
        dir,
        socket: format!("unix:{}", socket.display()),
    };
    for _ in 0..600 {
        if std::os::unix::net::UnixStream::connect(&socket).is_ok() {
            return Some(server);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the server did not open {} within 60 s", socket.display());
}

fn login(login: Login) -> Options {
    Options {
        login,
        timeout: Some(Duration::from_secs(30)),
        ..Options::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_password_logs_in_and_a_wrong_one_does_not() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(server) = server("scram", "scram-sha-256").await else {
            return;
        };

        let client = Client::connect(&server.socket, login(Login::password("alice", "hunter2")))
            .await
            .expect("the right password logs in");
        let one: i32 = client.fetch_scalar("SELECT 1", &[]).await.unwrap();
        assert_eq!(one, 1);

        let wrong = Client::connect(&server.socket, login(Login::password("alice", "hunter3")))
            .await
            .err()
            .expect("a wrong password is refused");
        assert!(
            !wrong.to_string().contains("hunter"),
            "the error carries a password: {wrong}"
        );

        let nobody = Client::connect(&server.socket, login(Login::password("bob", "hunter2"))).await;
        assert!(nobody.is_err(), "an unknown role logged in");

        // No login at all: refused at the connect or at the first statement.
        if let Ok(anonymous) = Client::connect(&server.socket, login(Login::Trust)).await {
            let ran = anonymous.fetch_scalar::<i32>("SELECT 1", &[]).await;
            assert!(ran.is_err(), "a server asking for a password ran a statement without one");
        }
    })
    .await
    .expect("the test did not finish within 120 s");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_that_skips_the_exchange_is_refused() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let Some(server) = server("trust", "trust").await else {
            return;
        };
        // The server never proves who it is, so a client holding a password
        // will not talk to it. (A trust listener does not answer the hello at
        // all, so this is the handshake's timeout rather than the client's
        // own refusal; either way nothing is sent.)
        let refused =
            Client::connect(&server.socket, login(Login::password("alice", "hunter2"))).await;
        assert!(refused.is_err(), "a password login accepted a server that asked for nothing");
    })
    .await
    .expect("the test did not finish within 120 s");
}

#[test]
fn debug_never_prints_the_password() {
    let options = login(Login::password("alice", "hunter2"));
    let printed = format!("{options:?}");
    assert!(printed.contains("alice"), "{printed}");
    assert!(!printed.contains("hunter2"), "{printed}");
}
