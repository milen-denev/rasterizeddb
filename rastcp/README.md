# RasTCP

A secure TCP protocol implementation using Rustls for transferring raw binary data.

## Features

- TLS encryption using Rustls
- Asynchronous I/O with Tokio
- Length-prefixed message framing
- Simple client/server APIs
- Persistent connections
- Automatic self-signed certificate generation

## Usage Example

### Server

```rust
let server = TcpServerBuilder::new("127.0.0.1", 8080).build().await?;

// Runs until `server.shutdown()`; the handler gets a request and returns the reply.
server.run(|request| async move { request }).await?;
```

### Client

```rust
let mut client = TcpClientBuilder::new("127.0.0.1:8080").build().await?;
let reply = client.send(b"hello".to_vec()).await?;
```

## Sending more than one request at a time

A request costs a system call each way on each side, and for a small request
that is most of what it costs. There are two ways to share those calls.

**A batch**, when one caller has several requests in hand. They leave together,
the server handles them in order, and the replies come back together:

```rust
let replies = client.send_batch(&[b"one".to_vec(), b"two".to_vec()]).await?;
```

**A shared connection**, when many callers each have one. `MuxClient` is cheap
to clone and safe to use from any number of tasks; whatever they send while the
connection is busy goes out in one write, and each caller gets its own reply
whenever the server has it, in any order:

```rust
let client = TcpClientBuilder::new("127.0.0.1:8080").build_multiplexed().await?;

let worker = client.clone();
tokio::spawn(async move { worker.send(b"from another task").await });

let reply = client.send(b"hello").await?;
```

A shared connection speaks `rastcp/2`; the server offers it alongside `rastcp/1`
and each client asks for the one it speaks, so both kinds can use one server.

On the server, each request of a shared connection runs as a task of its own.
If the handler never waits and does almost nothing — a cache lookup, say —
`TcpServerBuilder::inline_handlers(true)` has the connection run it directly,
which is about three times cheaper per request. Leave it off for a handler that
does real work: the connection would do that work one request at a time.

`examples/echo_bench.rs` measures all of this.
