//! MySQL packet framing: a 3-byte little-endian length, a sequence id, then
//! the payload. A payload of 2^24-1 bytes or more is split, and the reader
//! joins it back.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

pub(crate) const MAX_PAYLOAD: usize = 0xff_ffff;

/// The largest packet a client may send (`max_allowed_packet`).
pub(crate) const MAX_ALLOWED_PACKET: usize = 64 * 1024 * 1024;

/// A client connection: TCP, TLS over TCP, or the unix socket.
pub(crate) enum MyStream {
    Tcp(tokio::net::TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    /// TLS on the unix socket: MariaDB's client asks for it there too.
    #[cfg(unix)]
    TlsUnix(Box<tokio_rustls::server::TlsStream<tokio::net::UnixStream>>),
    /// Between the plain stream and TLS, during the upgrade.
    Upgrading,
}

impl MyStream {
    pub(crate) fn is_secure(&self) -> bool {
        match self {
            MyStream::Tls(_) => true,
            #[cfg(unix)]
            MyStream::Unix(_) | MyStream::TlsUnix(_) => true,
            _ => false,
        }
    }

    pub(crate) fn is_tls_unix(&self) -> bool {
        #[cfg(unix)]
        {
            matches!(self, MyStream::TlsUnix(_))
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    pub(crate) fn is_unix(&self) -> bool {
        #[cfg(unix)]
        {
            matches!(self, MyStream::Unix(_) | MyStream::TlsUnix(_))
        }
        #[cfg(not(unix))]
        {
            false
        }
    }
}

#[cfg(unix)]
impl std::os::fd::AsFd for MyStream {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        match self {
            MyStream::Tcp(s) => s.as_fd(),
            MyStream::Tls(s) => s.get_ref().0.as_fd(),
            MyStream::Unix(s) => s.as_fd(),
            MyStream::TlsUnix(s) => s.get_ref().0.as_fd(),
            MyStream::Upgrading => unreachable!("a stream is never watched mid-upgrade"),
        }
    }
}

macro_rules! each {
    ($self:ident, $s:ident => $e:expr) => {
        match $self.get_mut() {
            MyStream::Tcp($s) => $e,
            MyStream::Tls($s) => $e,
            #[cfg(unix)]
            MyStream::Unix($s) => $e,
            #[cfg(unix)]
            MyStream::TlsUnix($s) => $e,
            MyStream::Upgrading => Poll::Ready(Err(io::Error::other("stream mid-upgrade"))),
        }
    };
}

impl AsyncRead for MyStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        each!(self, s => Pin::new(s).poll_read(cx, buf))
    }
}

impl AsyncWrite for MyStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        each!(self, s => Pin::new(s).poll_write(cx, buf))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        each!(self, s => Pin::new(s).poll_flush(cx))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        each!(self, s => Pin::new(s).poll_shutdown(cx))
    }
}

/// Reads and writes packets, tracking the sequence id.
pub(crate) struct Packets {
    pub(crate) stream: MyStream,
    rbuf: Vec<u8>,
    rpos: usize,
    out: Vec<u8>,
    pub(crate) seq: u8,
    /// Read exactly a packet at a time, never ahead: during the handshake,
    /// where TLS bytes may follow an SSL request in the same segment.
    pub(crate) exact: bool,
    /// The largest packet accepted: small until the client has logged in, so
    /// an unauthenticated one cannot make the server hold megabytes.
    pub(crate) max_packet: usize,
    /// How many times the output was sent: a [`Mark`] from before a send
    /// cannot be rewound to.
    flushes: u64,
}

/// A point in the output, to take back what was written after it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Mark {
    at: usize,
    seq: u8,
    flushes: u64,
}

/// What a client may send before it has authenticated.
pub(crate) const MAX_LOGIN_PACKET: usize = 64 * 1024;

impl Packets {
    pub(crate) fn new(stream: MyStream) -> Self {
        Packets {
            stream,
            rbuf: Vec::new(),
            rpos: 0,
            out: Vec::with_capacity(1024),
            seq: 0,
            exact: true,
            max_packet: MAX_LOGIN_PACKET,
            flushes: 0,
        }
    }

    pub(crate) fn mark(&self) -> Mark {
        Mark {
            at: self.out.len(),
            seq: self.seq,
            flushes: self.flushes,
        }
    }

    /// The first byte of the first packet written since `mark` (0x00 OK,
    /// 0xff ERR, else a result set's column count), if it is still here.
    pub(crate) fn first_since(&self, mark: Mark) -> Option<u8> {
        if mark.flushes != self.flushes {
            return None;
        }
        self.out.get(mark.at + 4).copied()
    }

    /// Takes back what was written since `mark`; false when it was sent.
    pub(crate) fn rewind(&mut self, mark: Mark) -> bool {
        if mark.flushes != self.flushes || self.out.len() < mark.at {
            return false;
        }
        self.out.truncate(mark.at);
        self.seq = mark.seq;
        true
    }

    async fn fill(&mut self, want: usize) -> io::Result<()> {
        while self.rbuf.len() - self.rpos < want {
            if self.rpos > 0 {
                self.rbuf.drain(..self.rpos);
                self.rpos = 0;
            }
            let have = self.rbuf.len();
            let need = want - have;
            let chunk = if self.exact {
                need
            } else {
                need.max(16 * 1024)
            };
            self.rbuf.resize(have + chunk, 0);
            let n = self.stream.read(&mut self.rbuf[have..]).await;
            match n {
                Ok(0) => {
                    self.rbuf.truncate(have);
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "client closed the connection",
                    ));
                }
                Ok(n) => self.rbuf.truncate(have + n),
                Err(e) => {
                    self.rbuf.truncate(have);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// The next packet's payload, joined if it was split.
    pub(crate) async fn read(&mut self) -> io::Result<Vec<u8>> {
        let mut payload = Vec::new();
        loop {
            self.fill(4).await?;
            let h = &self.rbuf[self.rpos..self.rpos + 4];
            let len = h[0] as usize | (h[1] as usize) << 8 | (h[2] as usize) << 16;
            self.seq = h[3].wrapping_add(1);
            self.rpos += 4;
            if payload.len() + len > self.max_packet {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "packet larger than max_allowed_packet",
                ));
            }
            self.fill(len).await?;
            payload.extend_from_slice(&self.rbuf[self.rpos..self.rpos + len]);
            self.rpos += len;
            if self.rpos == self.rbuf.len() {
                self.rbuf.clear();
                self.rpos = 0;
                if self.rbuf.capacity() > 256 * 1024 {
                    self.rbuf = Vec::new();
                }
            }
            if len < MAX_PAYLOAD {
                return Ok(payload);
            }
        }
    }

    /// Whether unread bytes are buffered (a TLS upgrade must find none).
    pub(crate) fn has_buffered_input(&self) -> bool {
        self.rpos < self.rbuf.len()
    }

    /// Starts a packet; returns where its header is, for [`Packets::end`].
    pub(crate) fn begin(&mut self) -> usize {
        let at = self.out.len();
        self.out.extend_from_slice(&[0, 0, 0, 0]);
        at
    }

    /// The buffer a packet's payload is written into.
    pub(crate) fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.out
    }

    /// Ends the packet begun at `at`: its header is filled in, and a payload
    /// too long for one packet is split.
    pub(crate) fn end(&mut self, at: usize) {
        let len = self.out.len() - at - 4;
        if len < MAX_PAYLOAD {
            self.out[at] = len as u8;
            self.out[at + 1] = (len >> 8) as u8;
            self.out[at + 2] = (len >> 16) as u8;
            self.out[at + 3] = self.seq;
            self.seq = self.seq.wrapping_add(1);
            return;
        }
        let payload: Vec<u8> = self.out.split_off(at + 4);
        self.out.truncate(at);
        let mut chunks = payload.chunks(MAX_PAYLOAD).peekable();
        let mut last_full = false;
        while let Some(chunk) = chunks.next() {
            let n = chunk.len();
            self.out
                .extend_from_slice(&[n as u8, (n >> 8) as u8, (n >> 16) as u8, self.seq]);
            self.seq = self.seq.wrapping_add(1);
            self.out.extend_from_slice(chunk);
            last_full = n == MAX_PAYLOAD && chunks.peek().is_none();
        }
        if last_full {
            self.out.extend_from_slice(&[0, 0, 0, self.seq]);
            self.seq = self.seq.wrapping_add(1);
        }
    }

    /// One whole packet.
    pub(crate) fn packet(&mut self, payload: &[u8]) {
        let at = self.begin();
        self.out.extend_from_slice(payload);
        self.end(at);
    }

    pub(crate) fn pending(&self) -> usize {
        self.out.len()
    }

    pub(crate) async fn flush(&mut self) -> io::Result<()> {
        if !self.out.is_empty() {
            self.flushes += 1;
            self.stream.write_all(&self.out).await?;
            self.out.clear();
            if self.out.capacity() > 1024 * 1024 {
                self.out = Vec::with_capacity(1024);
            }
        }
        self.stream.flush().await
    }
}

// ---------------------------------------------------------------------------
// Encodings
// ---------------------------------------------------------------------------

pub(crate) fn put_lenenc_int(out: &mut Vec<u8>, n: u64) {
    if n < 251 {
        out.push(n as u8);
    } else if n < 1 << 16 {
        out.push(0xfc);
        out.extend_from_slice(&(n as u16).to_le_bytes());
    } else if n < 1 << 24 {
        out.push(0xfd);
        out.extend_from_slice(&(n as u32).to_le_bytes()[..3]);
    } else {
        out.push(0xfe);
        out.extend_from_slice(&n.to_le_bytes());
    }
}

pub(crate) fn put_lenenc_str(out: &mut Vec<u8>, s: &[u8]) {
    put_lenenc_int(out, s.len() as u64);
    out.extend_from_slice(s);
}

/// A reader over a packet's payload.
pub(crate) struct Reader<'a> {
    pub(crate) buf: &'a [u8],
    pub(crate) pos: usize,
}

fn short() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "packet shorter than its contents",
    )
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }
    pub(crate) fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }
    pub(crate) fn u8(&mut self) -> io::Result<u8> {
        let v = *self.buf.get(self.pos).ok_or_else(short)?;
        self.pos += 1;
        Ok(v)
    }
    pub(crate) fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }
    pub(crate) fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
    pub(crate) fn bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let v = self.buf.get(self.pos..self.pos + n).ok_or_else(short)?;
        self.pos += n;
        Ok(v)
    }
    pub(crate) fn rest(&mut self) -> &'a [u8] {
        let v = &self.buf[self.pos.min(self.buf.len())..];
        self.pos = self.buf.len();
        v
    }
    /// A NUL-terminated string (or the rest, if there is no NUL).
    pub(crate) fn cstr(&mut self) -> io::Result<&'a [u8]> {
        let rest = &self.buf[self.pos.min(self.buf.len())..];
        match rest.iter().position(|b| *b == 0) {
            Some(end) => {
                self.pos += end + 1;
                Ok(&rest[..end])
            }
            None => {
                self.pos = self.buf.len();
                Ok(rest)
            }
        }
    }
    pub(crate) fn lenenc_int(&mut self) -> io::Result<u64> {
        let first = self.u8()?;
        Ok(match first {
            0xfc => self.u16()? as u64,
            0xfd => {
                let b = self.bytes(3)?;
                b[0] as u64 | (b[1] as u64) << 8 | (b[2] as u64) << 16
            }
            0xfe => self.u64()?,
            n => n as u64,
        })
    }
    pub(crate) fn lenenc_str(&mut self) -> io::Result<&'a [u8]> {
        let n = self.lenenc_int()? as usize;
        self.bytes(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lenenc_round_trips() {
        for n in [
            0u64,
            250,
            251,
            65535,
            65536,
            (1 << 24) - 1,
            1 << 24,
            u64::MAX,
        ] {
            let mut out = Vec::new();
            put_lenenc_int(&mut out, n);
            assert_eq!(Reader::new(&out).lenenc_int().unwrap(), n);
        }
    }
}
