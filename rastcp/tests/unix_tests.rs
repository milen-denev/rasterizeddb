//! Serving on a unix socket.
//!
//! The same server and the same clients as over TCP, with no TLS: what has to
//! hold is everything that holds over TCP — requests answered, batches in
//! order, many callers on a shared connection, authentication — plus what a
//! socket file brings with it: who may connect, and that a file is neither
//! left behind nor taken from someone else.

#![cfg(unix)]

use std::future::Future;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rastcp::client::TcpClientBuilder;
use rastcp::error::RastcpError;
use rastcp::net::Peer;
use rastcp::server::{TcpServer, TcpServerBuilder};
use rastcp::session::{
    AuthChannel, AuthFuture, ClientAuthenticator, Principal, ServerAuthenticator,
};
use rastcp::tls::ClientAuth;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::sleep;

const TEST_LIMIT: Duration = Duration::from_secs(60);

async fn bounded<F: Future>(test: F) -> F::Output {
    tokio::time::timeout(TEST_LIMIT, test)
        .await
        .expect("the test wedged")
}

/// A socket path of this test's own. Kept short: a unix socket's path has to
/// fit in about a hundred bytes.
fn socket_path(name: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    PathBuf::from(format!(
        "/tmp/rastcp-{}-{}-{}.sock",
        std::process::id(),
        name,
        n
    ))
}

fn address(path: &Path) -> String {
    format!("unix:{}", path.display())
}

/// A server on `path` answering with what `handler` makes of each request.
async fn serve<F, Fut>(
    path: &Path,
    configure: impl FnOnce(TcpServerBuilder) -> TcpServerBuilder,
    handler: F,
) -> (
    Arc<TcpServer>,
    tokio::task::JoinHandle<Result<(), RastcpError>>,
)
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Vec<u8>> + Send + 'static,
{
    let server = Arc::new(
        configure(TcpServerBuilder::unix(path))
            .build()
            .await
            .expect("the server should build"),
    );
    let running = server.clone();
    let task = tokio::spawn(async move { running.run(handler).await });
    (server, task)
}

#[tokio::test]
async fn requests_and_batches_are_answered_over_a_unix_socket() {
    bounded(async {
        let path = socket_path("echo");
        let (server, task) = serve(&path, |b| b, |data| async move { data }).await;
        assert_eq!(server.local_endpoint(), address(&path));
        assert_eq!(
            server.certificate_fingerprint(),
            None,
            "a unix socket presents no certificate"
        );

        let mut client = TcpClientBuilder::new(&address(&path))
            .timeout(Duration::from_secs(5))
            .build()
            .await
            .expect("a client");

        for round in 0..20u8 {
            let request = vec![round; 1 + round as usize * 1000];
            assert_eq!(client.send(request.clone()).await.unwrap(), request);
        }

        // Large ones too: past the read-ahead buffer and the copy limit.
        let large = vec![7u8; 300_000];
        assert_eq!(client.send(large.clone()).await.unwrap(), large);

        let batch: Vec<Vec<u8>> = (0..50u8).map(|i| vec![i; 64]).collect();
        assert!(client.send_batch(&batch).await.unwrap() == batch);

        task.abort();
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_callers_share_one_unix_connection() {
    bounded(async {
        let path = socket_path("mux");
        let (server, task) = serve(&path, |b| b, |data| async move { data }).await;

        let client = TcpClientBuilder::new(&address(&path))
            .timeout(Duration::from_secs(10))
            .build_multiplexed()
            .await
            .expect("a client");

        let mut tasks = Vec::new();
        for caller in 0..32usize {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                for round in 0..100usize {
                    let request: Vec<u8> = (0..(1 + (caller * 37 + round) % 2000))
                        .map(|i| (i + caller * 7 + round) as u8)
                        .collect();
                    let response = client.send(&request).await.unwrap();
                    assert!(
                        response == request,
                        "caller {caller} round {round} got another's answer"
                    );
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(server.active_connections(), 1);
        task.abort();
    })
    .await
}

/// The authenticator sees who the kernel says is calling, and the credential
/// exchange runs with a binding both ends agree on.
#[tokio::test]
async fn an_authenticator_learns_the_callers_user_from_the_kernel() {
    #[derive(Debug)]
    struct NamesTheCaller;

    impl ServerAuthenticator for NamesTheCaller {
        fn authenticate<'a>(
            &'a self,
            channel: &'a mut AuthChannel<'_>,
        ) -> AuthFuture<'a, Principal> {
            Box::pin(async move {
                let client_view = channel.recv().await?;
                assert_eq!(
                    client_view.as_slice(),
                    channel.channel_binding().as_slice(),
                    "the two ends of one unix connection must use the same binding"
                );
                assert_eq!(
                    *channel.channel_binding(),
                    rastcp::session::UNIX_CHANNEL_BINDING
                );

                let Peer::Unix(peer) = channel.peer() else {
                    panic!("a unix caller was reported as {}", channel.peer());
                };
                channel.send(b"ok").await?;
                Ok(Principal::new(format!("uid:{}", peer.uid), false))
            })
        }
    }

    #[derive(Debug)]
    struct SendsBinding;

    impl ClientAuthenticator for SendsBinding {
        fn authenticate<'a>(&'a self, channel: &'a mut AuthChannel<'_>) -> AuthFuture<'a, ()> {
            Box::pin(async move {
                // The client learns who serves the socket the same way.
                assert!(matches!(channel.peer(), Peer::Unix(_)));
                let binding = *channel.channel_binding();
                channel.send(&binding).await?;
                channel.recv().await.map(|_| ())
            })
        }
    }

    bounded(async {
        let path = socket_path("auth");
        let server = Arc::new(
            TcpServerBuilder::unix(&path)
                .authenticator(Arc::new(NamesTheCaller))
                .build()
                .await
                .unwrap(),
        );
        let running = server.clone();
        let task = tokio::spawn(async move {
            running
                .run_with_principal(
                    |principal, _| async move { principal.user.clone().into_bytes() },
                )
                .await
        });

        let mut client = TcpClientBuilder::new(&address(&path))
            .timeout(Duration::from_secs(5))
            .authenticator(Arc::new(SendsBinding))
            .build()
            .await
            .unwrap();

        let me = unsafe { libc::getuid() };
        let said = client.send(b"who am I".to_vec()).await.unwrap();
        assert_eq!(String::from_utf8_lossy(&said), format!("uid:{me}"));

        task.abort();
    })
    .await
}

#[tokio::test]
async fn the_socket_file_has_the_mode_it_was_given_and_goes_with_its_server() {
    bounded(async {
        // The default: its owner only.
        let path = socket_path("mode");
        let server = TcpServerBuilder::unix(&path).build().await.unwrap();
        let metadata = std::fs::symlink_metadata(&path).expect("the socket file");
        assert!(metadata.file_type().is_socket());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        drop(server);
        assert!(!path.exists(), "the socket file outlived its server");

        // Wider, when asked for.
        let path = socket_path("group");
        let server = TcpServerBuilder::unix(&path)
            .unix_mode(0o660)
            .build()
            .await
            .unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o660);
        drop(server);
        assert!(!path.exists());

        // And after serving and shutting down, as a real server ends.
        let path = socket_path("served");
        let (server, task) = serve(&path, |b| b, |data| async move { data }).await;
        let mut client = TcpClientBuilder::new(&address(&path))
            .build()
            .await
            .unwrap();
        assert_eq!(client.send(b"hi".to_vec()).await.unwrap(), b"hi".to_vec());
        server.shutdown();
        assert!(task.await.unwrap().is_ok());
        drop(server);
        assert!(
            !path.exists(),
            "the socket file outlived a server that shut down"
        );
    })
    .await
}

#[tokio::test]
async fn a_socket_left_behind_is_replaced() {
    bounded(async {
        let path = socket_path("stale");

        // What a server that died leaves: a socket file nobody listens on.
        drop(std::os::unix::net::UnixListener::bind(&path).unwrap());
        assert!(path.exists());

        let (_server, task) = serve(&path, |b| b, |data| async move { data }).await;
        let mut client = TcpClientBuilder::new(&address(&path))
            .build()
            .await
            .unwrap();
        assert_eq!(client.send(b"hi".to_vec()).await.unwrap(), b"hi".to_vec());

        task.abort();
    })
    .await
}

#[tokio::test]
async fn a_socket_someone_listens_on_is_not_taken() {
    bounded(async {
        let path = socket_path("live");
        let (_first, task) = serve(
            &path,
            |b| b,
            |data| async move { b"first".to_vec().into_iter().chain(data).collect() },
        )
        .await;

        let second = TcpServerBuilder::unix(&path).build().await;
        assert!(
            second.is_err(),
            "a second server took a socket the first was listening on"
        );

        // And the first still has it.
        let mut client = TcpClientBuilder::new(&address(&path))
            .build()
            .await
            .unwrap();
        assert_eq!(
            client.send(b"!".to_vec()).await.unwrap(),
            b"first!".to_vec()
        );

        task.abort();
    })
    .await
}

#[tokio::test]
async fn a_path_that_is_not_a_socket_is_left_alone() {
    bounded(async {
        let path = socket_path("file");
        std::fs::write(&path, b"somebody's data").unwrap();

        assert!(TcpServerBuilder::unix(&path).build().await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"somebody's data");

        std::fs::remove_file(&path).unwrap();
    })
    .await
}

#[tokio::test]
async fn a_server_does_not_remove_a_socket_that_is_no_longer_its_own() {
    bounded(async {
        let path = socket_path("replaced");
        let old = TcpServerBuilder::unix(&path).build().await.unwrap();

        // The path is taken over while the old server is still about: its
        // file removed and a new server bound in its place, as a restart that
        // does not wait for the old process can do.
        std::fs::remove_file(&path).unwrap();
        let (_new, task) = serve(&path, |b| b, |data| async move { data }).await;

        drop(old);
        assert!(
            path.exists(),
            "the old server removed the new server's socket"
        );

        let mut client = TcpClientBuilder::new(&address(&path))
            .build()
            .await
            .unwrap();
        assert_eq!(client.send(b"hi".to_vec()).await.unwrap(), b"hi".to_vec());

        task.abort();
    })
    .await
}

#[tokio::test]
async fn what_needs_tls_is_refused_rather_than_ignored() {
    bounded(async {
        // A server told to require client certificates would, on a unix
        // socket, serve everyone it was told to keep out.
        let path = socket_path("certs");
        let refused = TcpServerBuilder::unix(&path)
            .client_auth(ClientAuth::RequirePinned(vec![[1u8; 32]]))
            .build()
            .await;
        assert!(refused.is_err());
        assert!(!path.exists());

        // A client asked to check the server's certificate would connect
        // without having checked anything.
        let refused = TcpClientBuilder::new("unix:/tmp/nothing-here.sock")
            .verify_certificate(true)
            .build()
            .await;
        assert!(refused.is_err());
        let refused = TcpClientBuilder::new("unix:/tmp/nothing-here.sock")
            .trust(rastcp::tls::Trust::Pinned(vec![[1u8; 32]]))
            .build()
            .await;
        assert!(refused.is_err());

        // An address with no path is no address.
        assert!(TcpClientBuilder::new("unix:").build().await.is_err());
    })
    .await
}

#[tokio::test]
async fn a_connection_that_does_not_name_the_protocol_is_closed() {
    bounded(async {
        let path = socket_path("preamble");
        let (_server, task) = serve(&path, |b| b, |data| async move { data }).await;

        // Something that is not a rastcp client: an HTTP request, say.
        let mut stranger = tokio::net::UnixStream::connect(&path).await.unwrap();
        stranger.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let mut answer = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(5), stranger.read_to_end(&mut answer))
            .await
            .expect("the server should close the connection, not leave it open");
        // Closed either way: cleanly, or — since the stranger's bytes past
        // the preamble were never read — with a reset, which is what the
        // kernel sends when a socket is closed with unread data in it.
        match read {
            Ok(_) => {}
            Err(e) => assert_eq!(e.kind(), std::io::ErrorKind::ConnectionReset, "got {e}"),
        }
        assert!(
            answer.is_empty(),
            "the server answered a stranger: {answer:?}"
        );

        // And it is still serving those that do.
        let mut client = TcpClientBuilder::new(&address(&path))
            .build()
            .await
            .unwrap();
        assert_eq!(client.send(b"hi".to_vec()).await.unwrap(), b"hi".to_vec());

        task.abort();
    })
    .await
}

#[tokio::test]
async fn an_idle_unix_connection_is_closed_and_replaced_like_a_tcp_one() {
    bounded(async {
        let request_timeout = Duration::from_millis(200);
        let path = socket_path("idle");
        let (server, task) = serve(
            &path,
            |b| b.request_timeout(request_timeout),
            |data| async move { data },
        )
        .await;

        let mut client = TcpClientBuilder::new(&address(&path))
            .timeout(Duration::from_secs(5))
            .reconnect_when_idle_closed(true)
            .build()
            .await
            .unwrap();

        assert_eq!(
            client.send(b"first".to_vec()).await.unwrap(),
            b"first".to_vec()
        );
        sleep(request_timeout * 3).await;
        assert_eq!(
            server.active_connections(),
            0,
            "an idle unix connection was kept"
        );
        assert_eq!(
            client.send(b"second".to_vec()).await.unwrap(),
            b"second".to_vec()
        );

        task.abort();
    })
    .await
}
