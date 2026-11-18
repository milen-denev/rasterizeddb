//! TDS (Tabular Data Stream), SQL Server's wire protocol, version 7.4.
//!
//! **Packets.** A message is a run of packets, each an 8-byte header (type,
//! status, big-endian length, SPID, packet id, window) and a payload; the last
//! has the end-of-message bit. The client proposes a packet size at login and
//! the server confirms it.
//!
//! **TLS (TDS 7.x).** The TLS handshake is carried *inside* PRELOGIN packets;
//! once it is done, TLS records go over the socket directly and the TDS
//! packets travel inside them. [`Shim`] does the wrapping during the
//! handshake and steps aside after. TDS 8.0 ("strict") starts with TLS
//! itself, recognised by its first byte.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

pub(crate) const PT_SQL_BATCH: u8 = 0x01;
pub(crate) const PT_RPC: u8 = 0x03;
pub(crate) const PT_REPLY: u8 = 0x04;
pub(crate) const PT_ATTENTION: u8 = 0x06;
pub(crate) const PT_BULK: u8 = 0x07;
pub(crate) const PT_TM: u8 = 0x0e;
pub(crate) const PT_LOGIN7: u8 = 0x10;
pub(crate) const PT_SSPI: u8 = 0x11;
pub(crate) const PT_PRELOGIN: u8 = 0x12;

pub(crate) const ST_EOM: u8 = 0x01;
pub(crate) const ST_RESET_CONNECTION: u8 = 0x08;
pub(crate) const ST_RESET_CONNECTION_SKIP_TRAN: u8 = 0x10;

pub(crate) const DEFAULT_PACKET: usize = 4096;
pub(crate) const MAX_PACKET: usize = 32767;
/// The largest message a client may send.
pub(crate) const MAX_MESSAGE: usize = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// The socket, and TLS inside PRELOGIN
// ---------------------------------------------------------------------------

pub(crate) enum Base {
    Tcp(tokio::net::TcpStream),
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
}

/// The socket under TLS: wraps the handshake's records in PRELOGIN packets
/// until [`Shim::passthrough`] is set, and replays bytes read ahead.
pub(crate) struct Shim {
    pub(crate) base: Base,
    /// Bytes already read off the socket and not yet consumed.
    pending: Vec<u8>,
    pending_pos: usize,
    pub(crate) passthrough: bool,
    /// Payload bytes of the PRELOGIN packet being unwrapped.
    payload_left: usize,
    out: Vec<u8>,
    out_sent: usize,
}

impl Shim {
    pub(crate) fn new(base: Base, prefix: Vec<u8>) -> Self {
        Shim {
            base,
            pending: prefix,
            pending_pos: 0,
            passthrough: true,
            payload_left: 0,
            out: Vec::new(),
            out_sent: 0,
        }
    }

    fn poll_base_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.pending_pos < self.pending.len() {
            let n = (self.pending.len() - self.pending_pos).min(buf.remaining());
            buf.put_slice(&self.pending[self.pending_pos..self.pending_pos + n]);
            self.pending_pos += n;
            if self.pending_pos == self.pending.len() {
                self.pending.clear();
                self.pending_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        match &mut self.base {
            Base::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            Base::Unix(s) => Pin::new(s).poll_read(cx, buf),
        }
    }

    fn poll_base_write(&mut self, cx: &mut Context<'_>, data: &[u8]) -> Poll<io::Result<usize>> {
        match &mut self.base {
            Base::Tcp(s) => Pin::new(s).poll_write(cx, data),
            #[cfg(unix)]
            Base::Unix(s) => Pin::new(s).poll_write(cx, data),
        }
    }

    fn poll_base_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.base {
            Base::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            Base::Unix(s) => Pin::new(s).poll_flush(cx),
        }
    }

    /// Reads exactly `n` bytes (of a PRELOGIN header) from below.
    fn poll_fill(&mut self, cx: &mut Context<'_>, want: usize) -> Poll<io::Result<Vec<u8>>> {
        let mut got = Vec::with_capacity(want);
        while got.len() < want {
            let mut tmp = vec![0u8; want - got.len()];
            let mut rb = ReadBuf::new(&mut tmp);
            match self.poll_base_read(cx, &mut rb) {
                Poll::Ready(Ok(())) => {
                    let n = rb.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "closed during TLS",
                        )));
                    }
                    got.extend_from_slice(rb.filled());
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {
                    // Put back what was read so far, and wait.
                    if !got.is_empty() {
                        let mut rest = got;
                        rest.extend_from_slice(&self.pending[self.pending_pos..]);
                        self.pending = rest;
                        self.pending_pos = 0;
                    }
                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(Ok(got))
    }
}

impl AsyncRead for Shim {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.passthrough {
            return this.poll_base_read(cx, buf);
        }
        // Unwrap PRELOGIN packets — until the client sends TLS records
        // bare: a client whose side of a TLS 1.3 handshake is done (tiberius)
        // sends its Finished unwrapped. A TDS packet starts 0x12 and a TLS
        // record 0x14..=0x17, so the first byte tells them apart.
        while this.payload_left == 0 {
            let first = match this.poll_fill(cx, 1) {
                Poll::Ready(Ok(b)) => b,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            let mut back = first.clone();
            back.extend_from_slice(&this.pending[this.pending_pos..]);
            this.pending = back;
            this.pending_pos = 0;
            if (0x14..=0x17).contains(&first[0]) {
                this.passthrough = true;
                return this.poll_base_read(cx, buf);
            }
            let header = match this.poll_fill(cx, 8) {
                Poll::Ready(Ok(h)) => h,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };
            let len = u16::from_be_bytes([header[2], header[3]]) as usize;
            if len < 8 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bad TDS packet during TLS",
                )));
            }
            this.payload_left = len - 8;
        }
        let want = this.payload_left.min(buf.remaining());
        let mut tmp = vec![0u8; want];
        let mut rb = ReadBuf::new(&mut tmp);
        match this.poll_base_read(cx, &mut rb) {
            Poll::Ready(Ok(())) => {
                let n = rb.filled().len();
                if n == 0 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "closed during TLS",
                    )));
                }
                this.payload_left -= n;
                buf.put_slice(rb.filled());
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl AsyncWrite for Shim {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.passthrough && this.out_sent == this.out.len() {
            return this.poll_base_write(cx, data);
        }
        if this.passthrough {
            // Finish what was wrapped first.
            match Pin::new(&mut *this).poll_flush(cx) {
                Poll::Ready(Ok(())) => return this.poll_base_write(cx, data),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        // One PRELOGIN packet per write, as SQL Server sends them.
        for chunk in data.chunks(DEFAULT_PACKET - 8) {
            let len = (chunk.len() + 8) as u16;
            this.out.extend_from_slice(&[PT_PRELOGIN, ST_EOM]);
            this.out.extend_from_slice(&len.to_be_bytes());
            this.out.extend_from_slice(&[0, 0, 0, 0]);
            this.out.extend_from_slice(chunk);
        }
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while this.out_sent < this.out.len() {
            let out = std::mem::take(&mut this.out);
            let r = this.poll_base_write(cx, &out[this.out_sent..]);
            this.out = out;
            match r {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "write zero",
                    )));
                }
                Poll::Ready(Ok(n)) => this.out_sent += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        this.out.clear();
        this.out_sent = 0;
        this.poll_base_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match &mut this.base {
            Base::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            Base::Unix(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// A client connection, before or after TLS.
pub(crate) enum TdsStream {
    Plain(Shim),
    Tls(Box<tokio_rustls::server::TlsStream<Shim>>),
    Upgrading,
}

impl TdsStream {
    pub(crate) fn is_tls(&self) -> bool {
        matches!(self, TdsStream::Tls(_))
    }

    pub(crate) fn is_unix(&self) -> bool {
        let base = match self {
            TdsStream::Plain(s) => &s.base,
            TdsStream::Tls(t) => &t.get_ref().0.base,
            TdsStream::Upgrading => return false,
        };
        #[cfg(unix)]
        {
            matches!(base, Base::Unix(_))
        }
        #[cfg(not(unix))]
        {
            let _ = base;
            false
        }
    }
}

#[cfg(unix)]
impl std::os::fd::AsFd for TdsStream {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        let base = match self {
            TdsStream::Plain(s) => &s.base,
            TdsStream::Tls(t) => &t.get_ref().0.base,
            TdsStream::Upgrading => unreachable!("never watched mid-upgrade"),
        };
        match base {
            Base::Tcp(s) => s.as_fd(),
            Base::Unix(s) => s.as_fd(),
        }
    }
}

macro_rules! each {
    ($self:ident, $s:ident => $e:expr) => {
        match $self.get_mut() {
            TdsStream::Plain($s) => $e,
            TdsStream::Tls($s) => $e,
            TdsStream::Upgrading => Poll::Ready(Err(io::Error::other("stream mid-upgrade"))),
        }
    };
}

impl AsyncRead for TdsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        each!(self, s => Pin::new(s).poll_read(cx, buf))
    }
}

impl AsyncWrite for TdsStream {
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

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// A whole client message.
pub(crate) struct Message {
    pub(crate) kind: u8,
    pub(crate) status: u8,
    pub(crate) payload: Vec<u8>,
}

/// Reads messages and writes replies.
pub(crate) struct Wire {
    pub(crate) stream: TdsStream,
    rbuf: Vec<u8>,
    rpos: usize,
    out: Vec<u8>,
    /// The reply being built, before it is cut into packets.
    reply: Vec<u8>,
    pub(crate) packet_size: usize,
    packet_id: u8,
    pub(crate) spid: u16,
    /// Reply bytes already cut into packets, for [`Wire::mark`].
    sent: usize,
    /// The largest message accepted: small until the client has logged in.
    pub(crate) max_message: usize,
}

/// What a client may send before it has logged in (PRELOGIN, LOGIN7).
pub(crate) const MAX_LOGIN_MESSAGE: usize = 64 * 1024;

impl Wire {
    pub(crate) fn new(stream: TdsStream) -> Self {
        Wire {
            stream,
            rbuf: Vec::new(),
            rpos: 0,
            out: Vec::new(),
            reply: Vec::with_capacity(1024),
            packet_size: DEFAULT_PACKET,
            packet_id: 1,
            spid: 0,
            sent: 0,
            max_message: MAX_LOGIN_MESSAGE,
        }
    }

    /// Where the next reply byte will be, counted over the whole reply.
    pub(crate) fn mark(&self) -> usize {
        self.sent + self.reply.len()
    }

    /// Clears `bits` in the byte at `at` (a [`Wire::mark`]), if it has not
    /// been sent yet; whether it could.
    pub(crate) fn clear_bits(&mut self, at: usize, bits: u8) -> bool {
        match at.checked_sub(self.sent) {
            Some(i) if i < self.reply.len() => {
                self.reply[i] &= !bits;
                true
            }
            _ => false,
        }
    }

    async fn fill(&mut self, want: usize) -> io::Result<()> {
        while self.rbuf.len() - self.rpos < want {
            if self.rpos > 0 {
                self.rbuf.drain(..self.rpos);
                self.rpos = 0;
            }
            let have = self.rbuf.len();
            let need = want - have;
            self.rbuf.resize(have + need.max(8192), 0);
            match self.stream.read(&mut self.rbuf[have..]).await {
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

    /// Bytes buffered but not consumed (a TLS switch must find none).
    pub(crate) fn take_buffered(&mut self) -> Vec<u8> {
        let rest = self.rbuf[self.rpos..].to_vec();
        self.rbuf.clear();
        self.rpos = 0;
        rest
    }

    /// The first byte the client sent, without consuming it.
    pub(crate) async fn peek_byte(&mut self) -> io::Result<u8> {
        self.fill(1).await?;
        Ok(self.rbuf[self.rpos])
    }

    /// The next whole message.
    pub(crate) async fn read(&mut self) -> io::Result<Message> {
        let mut payload = Vec::new();
        let mut first: Option<(u8, u8)> = None;
        loop {
            self.fill(8).await?;
            let h = &self.rbuf[self.rpos..self.rpos + 8];
            let kind = h[0];
            let status = h[1];
            let len = u16::from_be_bytes([h[2], h[3]]) as usize;
            if len < 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "TDS packet shorter than its header",
                ));
            }
            self.rpos += 8;
            self.fill(len - 8).await?;
            if payload.len() + len - 8 > self.max_message {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "TDS message too large",
                ));
            }
            payload.extend_from_slice(&self.rbuf[self.rpos..self.rpos + len - 8]);
            self.rpos += len - 8;
            if first.is_none() {
                first = Some((kind, status));
            }
            if status & ST_EOM != 0 {
                break;
            }
        }
        if self.rpos == self.rbuf.len() {
            self.rbuf.clear();
            self.rpos = 0;
        }
        let (kind, status) = first.unwrap();
        Ok(Message {
            kind,
            status,
            payload,
        })
    }

    /// Whether a complete attention packet is already buffered.
    pub(crate) fn attention_buffered(&self) -> bool {
        self.rbuf.get(self.rpos) == Some(&PT_ATTENTION)
    }

    /// The reply being built.
    pub(crate) fn reply(&mut self) -> &mut Vec<u8> {
        &mut self.reply
    }

    pub(crate) fn reply_len(&self) -> usize {
        self.reply.len()
    }

    /// Cuts the reply built so far into packets; `last` marks the end of
    /// the message. Full packets are always cut; a partial one only at the
    /// end.
    fn packetize(&mut self, kind: u8, last: bool) {
        let body = self.packet_size - 8;
        let mut start = 0usize;
        let reply = std::mem::take(&mut self.reply);
        while reply.len() - start > body
            || (last && start < reply.len())
            || (last && reply.is_empty() && start == 0)
        {
            let end = (start + body).min(reply.len());
            let eom = last && end == reply.len();
            let len = (end - start + 8) as u16;
            self.out
                .extend_from_slice(&[kind, if eom { ST_EOM } else { 0 }]);
            self.out.extend_from_slice(&len.to_be_bytes());
            self.out.extend_from_slice(&self.spid.to_be_bytes());
            self.out.push(self.packet_id);
            self.out.push(0);
            self.packet_id = self.packet_id.wrapping_add(1);
            self.out.extend_from_slice(&reply[start..end]);
            start = end;
            if eom {
                break;
            }
        }
        self.sent += start;
        self.reply = reply[start..].to_vec();
    }

    /// Writes the packets cut so far. Under TLS each packet is its own
    /// write, so its own TLS record, as SQL Server sends them: SqlClient's
    /// managed network layer reads a record as a packet's worth.
    async fn write_packets(&mut self) -> io::Result<()> {
        if self.out.is_empty() {
            return Ok(());
        }
        if self.stream.is_tls() {
            let mut at = 0usize;
            while at + 8 <= self.out.len() {
                let len = u16::from_be_bytes([self.out[at + 2], self.out[at + 3]]) as usize;
                let end = (at + len).min(self.out.len());
                self.stream.write_all(&self.out[at..end]).await?;
                at = end;
            }
        } else {
            self.stream.write_all(&self.out).await?;
        }
        self.out.clear();
        Ok(())
    }

    /// Sends what is complete of the reply, keeping a partial packet back.
    pub(crate) async fn flush_partial(&mut self, kind: u8) -> io::Result<()> {
        self.packetize(kind, false);
        self.write_packets().await
    }

    /// Ends and sends the reply.
    pub(crate) async fn send(&mut self, kind: u8) -> io::Result<()> {
        self.packetize(kind, true);
        self.sent = 0;
        self.packet_id = 1;
        self.write_packets().await?;
        if self.out.capacity() > 1024 * 1024 {
            self.out = Vec::new();
        }
        if self.reply.capacity() > 1024 * 1024 {
            self.reply = Vec::with_capacity(1024);
        }
        self.stream.flush().await
    }
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

pub(crate) struct Reader<'a> {
    pub(crate) buf: &'a [u8],
    pub(crate) pos: usize,
}

pub(crate) fn short() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "TDS message shorter than its contents",
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
    /// `n` UTF-16 code units.
    pub(crate) fn utf16(&mut self, n: usize) -> io::Result<String> {
        Ok(utf16_text(self.bytes(n * 2)?))
    }
    /// B_VARCHAR: a byte of length, then UTF-16.
    pub(crate) fn b_varchar(&mut self) -> io::Result<String> {
        let n = self.u8()? as usize;
        self.utf16(n)
    }
}

pub(crate) fn utf16_text(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|c| u16::from_le_bytes([c[0], c[1]]))
        .collect();
    String::from_utf16_lossy(&units)
}

/// Skips ALL_HEADERS, returning the transaction descriptor if one is there.
pub(crate) fn all_headers(r: &mut Reader<'_>) -> io::Result<Option<u64>> {
    let start = r.pos;
    let total = r.u32()? as usize;
    if total < 4 || start + total > r.buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad ALL_HEADERS",
        ));
    }
    let mut descriptor = None;
    while r.pos < start + total {
        let header_start = r.pos;
        let len = r.u32()? as usize;
        let kind = r.u16()?;
        if kind == 2 {
            descriptor = Some(r.u64()?);
        }
        r.pos = header_start + len.max(6);
    }
    r.pos = start + total;
    Ok(descriptor)
}

// ---------------------------------------------------------------------------
// PRELOGIN and LOGIN7
// ---------------------------------------------------------------------------

pub(crate) const ENCRYPT_OFF: u8 = 0;
pub(crate) const ENCRYPT_ON: u8 = 1;
pub(crate) const ENCRYPT_NOT_SUP: u8 = 2;
pub(crate) const ENCRYPT_REQ: u8 = 3;

/// The client's PRELOGIN encryption option (`ENCRYPT_OFF` if absent).
pub(crate) fn prelogin_encryption(payload: &[u8]) -> u8 {
    let mut i = 0usize;
    while i + 5 <= payload.len() && payload[i] != 0xff {
        let token = payload[i];
        let offset = u16::from_be_bytes([payload[i + 1], payload[i + 2]]) as usize;
        let len = u16::from_be_bytes([payload[i + 3], payload[i + 4]]) as usize;
        if token == 1 && len >= 1 {
            return payload.get(offset).copied().unwrap_or(ENCRYPT_OFF);
        }
        i += 5;
    }
    ENCRYPT_OFF
}

/// The PRELOGIN reply: version, encryption, no instance, no MARS.
pub(crate) fn prelogin_reply(encryption: u8) -> Vec<u8> {
    // (token, data)
    let options: [(u8, Vec<u8>); 5] = [
        (0, vec![16, 0, 0x0f, 0xa0, 0, 0]),
        (1, vec![encryption]),
        (2, vec![0]),
        (3, vec![]),
        (4, vec![0]),
    ];
    let header_len = options.len() * 5 + 1;
    let mut out = Vec::new();
    let mut data = Vec::new();
    for (token, value) in &options {
        out.push(*token);
        out.extend_from_slice(&((header_len + data.len()) as u16).to_be_bytes());
        out.extend_from_slice(&(value.len() as u16).to_be_bytes());
        data.extend_from_slice(value);
    }
    out.push(0xff);
    out.extend_from_slice(&data);
    out
}

/// What LOGIN7 carries that the server reads.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct Login {
    pub(crate) tds_version: u32,
    pub(crate) packet_size: u32,
    pub(crate) host: String,
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) app: String,
    pub(crate) server: String,
    pub(crate) database: String,
    pub(crate) integrated: bool,
    pub(crate) wants_features: bool,
    pub(crate) read_only_intent: bool,
}

pub(crate) fn parse_login7(p: &[u8]) -> io::Result<Login> {
    let mut r = Reader::new(p);
    let _len = r.u32()?;
    let tds_version = r.u32()?;
    let packet_size = r.u32()?;
    let _prog = r.u32()?;
    let _pid = r.u32()?;
    let _conn = r.u32()?;
    let _flags1 = r.u8()?;
    let flags2 = r.u8()?;
    let type_flags = r.u8()?;
    let flags3 = r.u8()?;
    let _tz = r.u32()?;
    let _lcid = r.u32()?;
    let field = |at: usize| -> io::Result<(usize, usize)> {
        let off =
            u16::from_le_bytes(p.get(at..at + 2).ok_or_else(short)?.try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(p.get(at + 2..at + 4).ok_or_else(short)?.try_into().unwrap())
            as usize;
        Ok((off, len))
    };
    let text = |at: usize| -> io::Result<String> {
        let (off, len) = field(at)?;
        Ok(utf16_text(p.get(off..off + len * 2).ok_or_else(short)?))
    };
    let (poff, plen) = field(44)?;
    let raw = p.get(poff..poff + plen * 2).ok_or_else(short)?;
    let decoded: Vec<u8> = raw
        .iter()
        .map(|b| {
            let x = b ^ 0xa5;
            x.rotate_left(4)
        })
        .collect();
    Ok(Login {
        tds_version,
        packet_size,
        host: text(36)?,
        user: text(40)?,
        password: utf16_text(&decoded),
        app: text(48)?,
        server: text(52)?,
        database: text(68)?,
        integrated: flags2 & 0x80 != 0,
        wants_features: flags3 & 0x10 != 0,
        read_only_intent: type_flags & 0x20 != 0,
    })
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

pub(crate) fn put_b_varchar(out: &mut Vec<u8>, s: &str) {
    let units: Vec<u16> = s.encode_utf16().take(255).collect();
    out.push(units.len() as u8);
    for u in units {
        out.extend_from_slice(&u.to_le_bytes());
    }
}

pub(crate) fn put_us_varchar(out: &mut Vec<u8>, s: &str) {
    let units: Vec<u16> = s.encode_utf16().take(65535).collect();
    out.extend_from_slice(&(units.len() as u16).to_le_bytes());
    for u in units {
        out.extend_from_slice(&u.to_le_bytes());
    }
}

fn utf16_bytes(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() * 2);
    for u in s.encode_utf16() {
        out.extend_from_slice(&u.to_le_bytes());
    }
    out
}

/// `Latin1_General_BIN2`: names and text compare as code points here.
pub(crate) const COLLATION: [u8; 5] = [0x09, 0x04, 0x00, 0x02, 0x00];

/// ENVCHANGE with string values.
pub(crate) fn env_change_text(out: &mut Vec<u8>, kind: u8, new: &str, old: &str) {
    let mut body = vec![kind];
    put_b_varchar(&mut body, new);
    put_b_varchar(&mut body, old);
    out.push(0xe3);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

/// ENVCHANGE with binary values.
pub(crate) fn env_change_bytes(out: &mut Vec<u8>, kind: u8, new: &[u8], old: &[u8]) {
    let mut body = vec![kind, new.len() as u8];
    body.extend_from_slice(new);
    body.push(old.len() as u8);
    body.extend_from_slice(old);
    out.push(0xe3);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

pub(crate) fn login_ack(out: &mut Vec<u8>) {
    let mut body = vec![1u8];
    body.extend_from_slice(&0x7400_0004u32.to_be_bytes());
    put_b_varchar(&mut body, "Microsoft SQL Server");
    body.extend_from_slice(&[16, 0, 0x0f, 0xa0]);
    out.push(0xad);
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

/// An ERROR (0xAA) or INFO (0xAB) token.
pub(crate) fn message(
    out: &mut Vec<u8>,
    error: bool,
    number: i32,
    state: u8,
    class: u8,
    text: &str,
    server: &str,
    line: i32,
) {
    let mut body = Vec::new();
    body.extend_from_slice(&number.to_le_bytes());
    body.push(state);
    body.push(class);
    put_us_varchar(&mut body, text);
    put_b_varchar(&mut body, server);
    put_b_varchar(&mut body, "");
    body.extend_from_slice(&line.to_le_bytes());
    out.push(if error { 0xaa } else { 0xab });
    out.extend_from_slice(&(body.len() as u16).to_le_bytes());
    out.extend_from_slice(&body);
}

pub(crate) const DONE_MORE: u16 = 0x1;
pub(crate) const DONE_ERROR: u16 = 0x2;
pub(crate) const DONE_INXACT: u16 = 0x4;
pub(crate) const DONE_COUNT: u16 = 0x10;
pub(crate) const DONE_ATTN: u16 = 0x20;

pub(crate) const TOKEN_DONE: u8 = 0xfd;
pub(crate) const TOKEN_DONEPROC: u8 = 0xfe;
pub(crate) const TOKEN_DONEINPROC: u8 = 0xff;

pub(crate) fn done(out: &mut Vec<u8>, token: u8, status: u16, command: u16, rows: u64) {
    out.push(token);
    out.extend_from_slice(&status.to_le_bytes());
    out.extend_from_slice(&command.to_le_bytes());
    out.extend_from_slice(&rows.to_le_bytes());
}

pub(crate) fn return_status(out: &mut Vec<u8>, value: i32) {
    out.push(0x79);
    out.extend_from_slice(&value.to_le_bytes());
}

/// RETURNVALUE of an `int` OUTPUT parameter (a prepared handle).
pub(crate) fn return_value_int(out: &mut Vec<u8>, ordinal: u16, name: &str, value: i32) {
    out.push(0xac);
    out.extend_from_slice(&ordinal.to_le_bytes());
    put_b_varchar(out, name);
    out.push(0x01);
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.push(0x26);
    out.push(4);
    out.push(4);
    out.extend_from_slice(&value.to_le_bytes());
}

/// RETURNVALUE of a procedure's OUTPUT parameter, sent as the engine type
/// the client bound it with (`oid`).
pub(crate) fn return_value(out: &mut Vec<u8>, ordinal: u16, name: &str, oid: u32, value: Option<&[u8]>) {
    let ty = column_type(oid, -1).unwrap_or_else(|| {
        let (int, frac) = value
            .map(|v| decimal_shape(&String::from_utf8_lossy(v)))
            .unwrap_or((1, 0));
        let frac = frac.min(38 - int.min(38));
        ColType::Decimal {
            precision: (int + frac).clamp(1, 38) as u8,
            scale: frac as u8,
        }
    });
    out.push(0xac);
    out.extend_from_slice(&ordinal.to_le_bytes());
    put_b_varchar(out, name);
    out.push(0x01);
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    type_info(out, &ty);
    let at = out.len();
    if put_value(out, &ty, value).is_err() {
        out.truncate(at);
        let _ = put_value(out, &ty, None);
    }
}

// ---------------------------------------------------------------------------
// Column types and values
// ---------------------------------------------------------------------------

/// How a result column is sent.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ColType {
    Bit,
    Int(u8),
    Float(u8),
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// NVARCHAR(n) with n in characters, or MAX.
    NVarChar(Option<u16>),
    VarBinaryMax,
    Date,
    DateTime2,
    DateTimeOffset,
    Time,
    Guid,
}

#[derive(Clone, Debug)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) ty: ColType,
    /// The engine's type, for reading its text.
    pub(crate) oid: u32,
}

/// The column an engine column is sent as. Numeric with no declared scale
/// is `None`: its scale is decided from the values (see the caller).
pub(crate) fn column_type(oid: u32, typmod: i32) -> Option<ColType> {
    Some(match oid {
        16 => ColType::Bit,
        21 => ColType::Int(2),
        23 | 26 => ColType::Int(4),
        20 => ColType::Int(8),
        700 => ColType::Float(4),
        701 => ColType::Float(8),
        1700 if typmod >= 4 => {
            let m = typmod - 4;
            let precision = ((m >> 16) & 0xffff).clamp(1, 38) as u8;
            let scale = (m & 0xffff).min(precision as i32) as u8;
            ColType::Decimal { precision, scale }
        }
        1700 => return None,
        1082 => ColType::Date,
        1114 => ColType::DateTime2,
        1184 => ColType::DateTimeOffset,
        1083 | 1266 => ColType::Time,
        17 => ColType::VarBinaryMax,
        2950 => ColType::Guid,
        1043 | 1042 if typmod > 4 && typmod - 4 <= 4000 => {
            ColType::NVarChar(Some((typmod - 4) as u16))
        }
        _ => ColType::NVarChar(None),
    })
}

/// COLMETADATA.
pub(crate) fn col_metadata(out: &mut Vec<u8>, columns: &[Column]) {
    out.push(0x81);
    out.extend_from_slice(&(columns.len() as u16).to_le_bytes());
    for c in columns {
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(&0x0009u16.to_le_bytes());
        type_info(out, &c.ty);
        put_b_varchar(out, &c.name);
    }
}

/// COLMETADATA for "no metadata" (a statement without rows in an RPC).
pub(crate) fn type_info(out: &mut Vec<u8>, ty: &ColType) {
    match ty {
        ColType::Bit => out.extend_from_slice(&[0x68, 1]),
        ColType::Int(n) => out.extend_from_slice(&[0x26, *n]),
        ColType::Float(n) => out.extend_from_slice(&[0x6d, *n]),
        ColType::Decimal { precision, scale } => {
            out.extend_from_slice(&[0x6a, 17, *precision, *scale])
        }
        ColType::NVarChar(n) => {
            out.push(0xe7);
            match n {
                Some(chars) => out.extend_from_slice(&(chars * 2).to_le_bytes()),
                None => out.extend_from_slice(&0xffffu16.to_le_bytes()),
            }
            out.extend_from_slice(&COLLATION);
        }
        ColType::VarBinaryMax => {
            out.push(0xa5);
            out.extend_from_slice(&0xffffu16.to_le_bytes());
        }
        ColType::Date => out.push(0x28),
        ColType::DateTime2 => out.extend_from_slice(&[0x2a, 6]),
        ColType::DateTimeOffset => out.extend_from_slice(&[0x2b, 6]),
        ColType::Time => out.extend_from_slice(&[0x29, 6]),
        ColType::Guid => out.extend_from_slice(&[0x24, 16]),
    }
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    // Days since 1970-01-01 (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Days from 0001-01-01, as TDS dates count.
fn tds_days(y: i64, m: u32, d: u32) -> i64 {
    days_from_civil(y, m, d) - days_from_civil(1, 1, 1)
}

pub(crate) fn civil_from_tds_days(days: i64) -> (i64, u32, u32) {
    let z = days + days_from_civil(1, 1, 1) + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn parse_date(s: &str) -> Option<(i64, u32, u32)> {
    let (neg, s) = match s.strip_suffix(" BC") {
        Some(rest) => (true, rest),
        None => (false, s),
    };
    let mut parts = s.splitn(3, '-');
    let y: i64 = parts.next()?.parse().ok()?;
    let m = parts.next()?.parse().ok()?;
    let d = parts.next()?.get(..2)?.parse().ok()?;
    Some((if neg { 1 - y } else { y }, m, d))
}

/// `HH:MM:SS[.ffffff]` as microseconds.
fn parse_time_micros(s: &str) -> Option<u64> {
    let mut parts = s.splitn(3, ':');
    let h: u64 = parts.next()?.parse().ok()?;
    let m: u64 = parts.next()?.parse().ok()?;
    let rest = parts.next()?;
    let (sec, frac) = rest.split_once('.').unwrap_or((rest, ""));
    let sec: u64 = sec.parse().ok()?;
    let digits: String = frac
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .take(6)
        .collect();
    let micros: u64 = if digits.is_empty() {
        0
    } else {
        format!("{:0<6}", digits).parse().ok()?
    };
    Some(((h * 60 + m) * 60 + sec) * 1_000_000 + micros)
}

/// A time zone suffix (`+02`, `-05:30`) split off a time.
fn split_offset(s: &str) -> (&str, i32) {
    let Some(colon) = s.find(':') else {
        return (s, 0);
    };
    if let Some(at) = s[colon..].rfind(['+', '-']) {
        let at = colon + at;
        let sign = if s.as_bytes()[at] == b'-' { -1 } else { 1 };
        let off = &s[at + 1..];
        let mut parts = off.split(':');
        let h: i32 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        let m: i32 = parts.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        return (&s[..at], sign * (h * 60 + m));
    }
    (s, 0)
}

/// A decimal's text as an unscaled integer at `scale`.
pub(crate) fn decimal_units(text: &str, scale: u8) -> Option<(bool, u128)> {
    let (neg, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if frac.len() > scale as usize && frac[scale as usize..].chars().any(|c| c != '0') {
        // More digits than the column holds: rounded, as SQL Server rounds.
        let keep = &frac[..scale as usize];
        let next = frac.as_bytes()[scale as usize];
        let mut units: u128 = format!("{int}{keep}").parse().ok()?;
        if next >= b'5' {
            units += 1;
        }
        return Some((neg, units));
    }
    let mut padded = frac.chars().take(scale as usize).collect::<String>();
    while padded.len() < scale as usize {
        padded.push('0');
    }
    let all = format!("{int}{padded}");
    let units: u128 = if all.is_empty() { 0 } else { all.parse().ok()? };
    Some((neg, units))
}

/// The digits before and after the point in a decimal's text.
pub(crate) fn decimal_shape(text: &str) -> (usize, usize) {
    let digits = text.trim_start_matches(['-', '+']);
    let (int, frac) = digits.split_once('.').unwrap_or((digits, ""));
    (int.trim_start_matches('0').len().max(1), frac.len())
}

fn put_time(out: &mut Vec<u8>, micros: u64) {
    // Scale 6: units of microseconds, five bytes.
    out.extend_from_slice(&micros.to_le_bytes()[..5]);
}

fn put_days(out: &mut Vec<u8>, days: i64) {
    out.extend_from_slice(&(days as u32).to_le_bytes()[..3]);
}

fn put_plp(out: &mut Vec<u8>, data: &[u8]) {
    out.extend_from_slice(&(data.len() as u64).to_le_bytes());
    if !data.is_empty() {
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
    }
    out.extend_from_slice(&0u32.to_le_bytes());
}

fn decode_bytea_text(value: &[u8]) -> Vec<u8> {
    let Some(hex) = value.strip_prefix(b"\\x") else {
        return value.to_vec();
    };
    let digit = |b: u8| match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    };
    hex.chunks(2)
        .filter(|p| p.len() == 2)
        .map(|p| digit(p[0]) << 4 | digit(p[1]))
        .collect()
}

/// One value of a ROW, from the engine's text.
pub(crate) fn put_value(
    out: &mut Vec<u8>,
    ty: &ColType,
    value: Option<&[u8]>,
) -> Result<(), String> {
    let Some(value) = value else {
        match ty {
            ColType::NVarChar(Some(_)) => out.extend_from_slice(&0xffffu16.to_le_bytes()),
            ColType::NVarChar(None) | ColType::VarBinaryMax => {
                out.extend_from_slice(&u64::MAX.to_le_bytes())
            }
            _ => out.push(0),
        }
        return Ok(());
    };
    let text = || std::str::from_utf8(value).map_err(|_| "a value is not text".to_string());
    let bad = |what: &str| {
        format!(
            "{what} {:?} cannot be sent as a SQL Server value",
            String::from_utf8_lossy(value)
        )
    };
    match ty {
        ColType::Bit => {
            out.push(1);
            out.push(matches!(value, b"t" | b"true" | b"1") as u8);
        }
        ColType::Int(n) => {
            let v: i64 = text()?.parse().map_err(|_| bad("the integer"))?;
            out.push(*n);
            out.extend_from_slice(&v.to_le_bytes()[..*n as usize]);
        }
        ColType::Float(n) => {
            let v: f64 = match text()? {
                "Infinity" => f64::INFINITY,
                "-Infinity" => f64::NEG_INFINITY,
                "NaN" => f64::NAN,
                t => t.parse().map_err(|_| bad("the number"))?,
            };
            out.push(*n);
            if *n == 4 {
                out.extend_from_slice(&(v as f32).to_le_bytes());
            } else {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        ColType::Decimal { precision, scale } => {
            let (neg, units) = decimal_units(text()?, *scale).ok_or_else(|| bad("the decimal"))?;
            let size = match precision {
                1..=9 => 4,
                10..=19 => 8,
                20..=28 => 12,
                _ => 16,
            };
            if size < 16 && units >> (size * 8) != 0 {
                return Err(bad("the decimal"));
            }
            out.push(size as u8 + 1);
            out.push(if neg { 0 } else { 1 });
            out.extend_from_slice(&units.to_le_bytes()[..size]);
        }
        ColType::NVarChar(limit) => {
            let s = std::str::from_utf8(value).map_err(|_| bad("the text"))?;
            let bytes = utf16_bytes(s);
            match limit {
                Some(_) => {
                    out.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                    out.extend_from_slice(&bytes);
                }
                None => put_plp(out, &bytes),
            }
        }
        ColType::VarBinaryMax => put_plp(out, &decode_bytea_text(value)),
        ColType::Date => {
            let (y, m, d) = parse_date(text()?).ok_or_else(|| bad("the date"))?;
            out.push(3);
            put_days(out, tds_days(y, m, d));
        }
        ColType::DateTime2 => {
            let t = text()?;
            let (date, time) = t.split_once(' ').unwrap_or((t, "00:00:00"));
            let (y, m, d) = parse_date(date).ok_or_else(|| bad("the time"))?;
            let micros = parse_time_micros(time).ok_or_else(|| bad("the time"))?;
            out.push(8);
            put_time(out, micros);
            put_days(out, tds_days(y, m, d));
        }
        ColType::DateTimeOffset => {
            let t = text()?;
            let (date, rest) = t.split_once(' ').unwrap_or((t, "00:00:00"));
            let (time, offset) = split_offset(rest);
            let (y, m, d) = parse_date(date).ok_or_else(|| bad("the time"))?;
            let local = parse_time_micros(time).ok_or_else(|| bad("the time"))? as i64;
            // Stored in UTC, with the offset beside it.
            let mut days = tds_days(y, m, d);
            let mut utc = local - offset as i64 * 60_000_000;
            const DAY: i64 = 86_400_000_000;
            while utc < 0 {
                utc += DAY;
                days -= 1;
            }
            while utc >= DAY {
                utc -= DAY;
                days += 1;
            }
            out.push(10);
            put_time(out, utc as u64);
            put_days(out, days);
            out.extend_from_slice(&(offset as i16).to_le_bytes());
        }
        ColType::Time => {
            let (time, _) = split_offset(text()?);
            let micros = parse_time_micros(time).ok_or_else(|| bad("the time"))?;
            out.push(5);
            put_time(out, micros);
        }
        ColType::Guid => {
            let hex: String = text()?.chars().filter(|c| *c != '-').collect();
            if hex.len() != 32 {
                return Err(bad("the uuid"));
            }
            let b: Vec<u8> = (0..16)
                .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or(0))
                .collect();
            out.push(16);
            // The first three groups are little-endian.
            out.extend_from_slice(&[b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6]]);
            out.extend_from_slice(&b[8..]);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// RPC parameters
// ---------------------------------------------------------------------------

/// A parameter's value, as the engine takes it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ParamValue {
    Null,
    Text(String),
    Bytes(Vec<u8>),
}

/// One RPC parameter.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct Param {
    pub(crate) name: String,
    pub(crate) output: bool,
    pub(crate) value: ParamValue,
    /// The engine type the value suits (0: let the engine infer it).
    pub(crate) oid: u32,
}

fn read_plp(r: &mut Reader<'_>) -> io::Result<Option<Vec<u8>>> {
    let total = r.u64()?;
    if total == u64::MAX {
        return Ok(None);
    }
    let mut out = Vec::new();
    loop {
        let n = r.u32()? as usize;
        if n == 0 {
            break;
        }
        out.extend_from_slice(r.bytes(n)?);
    }
    Ok(Some(out))
}

fn format_decimal(neg: bool, units: u128, scale: u8) -> String {
    let digits = units.to_string();
    let s = scale as usize;
    let text = if s == 0 {
        digits
    } else if digits.len() > s {
        format!(
            "{}.{}",
            &digits[..digits.len() - s],
            &digits[digits.len() - s..]
        )
    } else {
        format!("0.{}{}", "0".repeat(s - digits.len()), digits)
    };
    if neg && units != 0 {
        format!("-{text}")
    } else {
        text
    }
}

fn time_text(units: u64, scale: u8) -> String {
    let per_second = 10u64.pow(scale as u32);
    let secs = units / per_second;
    let frac = units % per_second;
    let mut text = format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
    if scale > 0 {
        text.push_str(&format!(".{:0width$}", frac, width = scale as usize));
    }
    text
}

fn date_text(days: i64) -> String {
    let (y, m, d) = civil_from_tds_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

fn time_bytes(scale: u8) -> usize {
    match scale {
        0..=2 => 3,
        3..=4 => 4,
        _ => 5,
    }
}

fn le_uint(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a[..b.len()].copy_from_slice(b);
    u64::from_le_bytes(a)
}

/// Reads one TYPE_INFO and its value.
pub(crate) fn read_typed_value(r: &mut Reader<'_>) -> io::Result<(ParamValue, u32)> {
    let ty = r.u8()?;
    let bad = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported parameter type {ty:#04x}"),
        )
    };
    Ok(match ty {
        // Fixed-length types.
        0x1f => (ParamValue::Null, 0),
        0x30 => (ParamValue::Text(r.u8()?.to_string()), 21),
        0x32 => (
            ParamValue::Text(if r.u8()? != 0 { "true" } else { "false" }.into()),
            16,
        ),
        0x34 => (ParamValue::Text((r.u16()? as i16).to_string()), 21),
        0x38 => (ParamValue::Text((r.u32()? as i32).to_string()), 23),
        0x7f => (ParamValue::Text((r.u64()? as i64).to_string()), 20),
        0x3b => (
            ParamValue::Text(f32::from_le_bytes(r.bytes(4)?.try_into().unwrap()).to_string()),
            700,
        ),
        0x3e => (
            ParamValue::Text(f64::from_le_bytes(r.bytes(8)?.try_into().unwrap()).to_string()),
            701,
        ),
        0x3c | 0x7a => {
            let v = if ty == 0x7a {
                r.u32()? as i32 as i64
            } else {
                let hi = r.u32()? as i32 as i64;
                let lo = r.u32()? as i64;
                (hi << 32) | lo
            };
            (
                ParamValue::Text(format_decimal(v < 0, v.unsigned_abs() as u128, 4)),
                1700,
            )
        }
        0x3d | 0x3a => {
            let (days, ticks) = if ty == 0x3a {
                (r.u16()? as i64, r.u16()? as u64 * 60 * 300)
            } else {
                (r.u32()? as i32 as i64, r.u32()? as u64)
            };
            let micros = ticks * 10_000 / 3;
            let days = days + tds_days(1900, 1, 1);
            (
                ParamValue::Text(format!("{} {}", date_text(days), time_text(micros, 6))),
                1114,
            )
        }
        // Nullable variable-length types: a byte of length.
        0x26 | 0x68 | 0x6d | 0x6e | 0x6f | 0x24 => {
            let _max = r.u8()?;
            let len = r.u8()? as usize;
            if len == 0 {
                return Ok((ParamValue::Null, 0));
            }
            let b = r.bytes(len)?;
            match ty {
                0x26 => {
                    let v = match len {
                        1 => b[0] as i64,
                        2 => i16::from_le_bytes([b[0], b[1]]) as i64,
                        4 => i32::from_le_bytes(b.try_into().unwrap()) as i64,
                        _ => i64::from_le_bytes(b.try_into().map_err(|_| bad())?),
                    };
                    (
                        ParamValue::Text(v.to_string()),
                        if len == 8 { 20 } else { 23 },
                    )
                }
                0x68 => (
                    ParamValue::Text(if b[0] != 0 { "true" } else { "false" }.into()),
                    16,
                ),
                0x6d => {
                    let v = if len == 4 {
                        f32::from_le_bytes(b.try_into().unwrap()) as f64
                    } else {
                        f64::from_le_bytes(b.try_into().map_err(|_| bad())?)
                    };
                    (ParamValue::Text(v.to_string()), 701)
                }
                0x6e => {
                    let v = if len == 4 {
                        i32::from_le_bytes(b.try_into().unwrap()) as i64
                    } else {
                        let hi = i32::from_le_bytes(b[0..4].try_into().unwrap()) as i64;
                        let lo = u32::from_le_bytes(b[4..8].try_into().unwrap()) as i64;
                        (hi << 32) | lo
                    };
                    (
                        ParamValue::Text(format_decimal(v < 0, v.unsigned_abs() as u128, 4)),
                        1700,
                    )
                }
                0x6f => {
                    let (days, ticks) = if len == 4 {
                        (
                            u16::from_le_bytes([b[0], b[1]]) as i64,
                            u16::from_le_bytes([b[2], b[3]]) as u64 * 60 * 300,
                        )
                    } else {
                        (
                            i32::from_le_bytes(b[0..4].try_into().unwrap()) as i64,
                            u32::from_le_bytes(b[4..8].try_into().unwrap()) as u64,
                        )
                    };
                    let micros = ticks * 10_000 / 3;
                    let days = days + tds_days(1900, 1, 1);
                    (
                        ParamValue::Text(format!("{} {}", date_text(days), time_text(micros, 6))),
                        1114,
                    )
                }
                _ => {
                    // GUID: the first three groups little-endian.
                    let g = format!(
                        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{}",
                        b[3],
                        b[2],
                        b[1],
                        b[0],
                        b[5],
                        b[4],
                        b[7],
                        b[6],
                        b[8],
                        b[9],
                        b[10..16]
                            .iter()
                            .map(|x| format!("{x:02x}"))
                            .collect::<String>()
                    );
                    (ParamValue::Text(g), 2950)
                }
            }
        }
        0x6a | 0x6c | 0x37 | 0x3f => {
            let _max = r.u8()?;
            let _precision = r.u8()?;
            let scale = r.u8()?;
            let len = r.u8()? as usize;
            if len == 0 {
                return Ok((ParamValue::Null, 1700));
            }
            let sign = r.u8()?;
            let mut m = [0u8; 16];
            let b = r.bytes(len - 1)?;
            m[..b.len()].copy_from_slice(b);
            (
                ParamValue::Text(format_decimal(sign == 0, u128::from_le_bytes(m), scale)),
                1700,
            )
        }
        0x28 => {
            let len = r.u8()? as usize;
            if len == 0 {
                return Ok((ParamValue::Null, 1082));
            }
            let b = r.bytes(3)?;
            (ParamValue::Text(date_text(le_uint(b) as i64)), 1082)
        }
        0x29 | 0x2a | 0x2b => {
            let scale = r.u8()?;
            let len = r.u8()? as usize;
            if len == 0 {
                return Ok((ParamValue::Null, 0));
            }
            let tb = time_bytes(scale);
            let units = le_uint(r.bytes(tb)?);
            let time = time_text(units, scale);
            match ty {
                0x29 => (ParamValue::Text(time), 1083),
                0x2a => {
                    let days = le_uint(r.bytes(3)?) as i64;
                    (
                        ParamValue::Text(format!("{} {time}", date_text(days))),
                        1114,
                    )
                }
                _ => {
                    let days = le_uint(r.bytes(3)?) as i64;
                    let offset = r.u16()? as i16;
                    let sign = if offset < 0 { '-' } else { '+' };
                    let o = offset.unsigned_abs();
                    // The value is UTC; the offset only says how it was written.
                    let _ = (sign, o);
                    (
                        ParamValue::Text(format!("{} {time}+00", date_text(days))),
                        1184,
                    )
                }
            }
        }
        // Character and binary types: two bytes of maximum length.
        0xa7 | 0xaf | 0xe7 | 0xef | 0xa5 | 0xad => {
            let max = r.u16()?;
            let text = matches!(ty, 0xa7 | 0xaf | 0xe7 | 0xef);
            if text {
                r.bytes(5)?;
            }
            let data = if max == 0xffff {
                read_plp(r)?
            } else {
                let len = r.u16()?;
                if len == 0xffff {
                    None
                } else {
                    Some(r.bytes(len as usize)?.to_vec())
                }
            };
            let Some(data) = data else {
                return Ok((ParamValue::Null, if text { 0 } else { 17 }));
            };
            match ty {
                0xe7 | 0xef => (ParamValue::Text(utf16_text(&data)), 0),
                0xa7 | 0xaf => (
                    ParamValue::Text(String::from_utf8_lossy(&data).into_owned()),
                    0,
                ),
                _ => (ParamValue::Bytes(data), 17),
            }
        }
        // Legacy long types: TEXT, NTEXT, IMAGE.
        0x23 | 0x63 | 0x22 => {
            let _max = r.u32()?;
            if ty != 0x22 {
                r.bytes(5)?;
            }
            let len = r.u32()?;
            if len == u32::MAX {
                return Ok((ParamValue::Null, 0));
            }
            let data = r.bytes(len as usize)?.to_vec();
            match ty {
                0x63 => (ParamValue::Text(utf16_text(&data)), 0),
                0x23 => (
                    ParamValue::Text(String::from_utf8_lossy(&data).into_owned()),
                    0,
                ),
                _ => (ParamValue::Bytes(data), 17),
            }
        }
        // XML: a schema-info byte, then PLP.
        0xf1 => {
            let schema = r.u8()?;
            if schema != 0 {
                return Err(bad());
            }
            match read_plp(r)? {
                None => (ParamValue::Null, 0),
                Some(data) => (ParamValue::Text(utf16_text(&data)), 0),
            }
        }
        _ => return Err(bad()),
    })
}

/// Parses the parameters of one RPC call, from after the option flags.
pub(crate) fn read_params(r: &mut Reader<'_>) -> io::Result<Vec<Param>> {
    let mut params = Vec::new();
    while r.remaining() > 0 {
        // A batch separator ends this call.
        if matches!(r.buf[r.pos], 0x80 | 0xff) {
            break;
        }
        let name = r.b_varchar()?;
        let status = r.u8()?;
        let (value, oid) = read_typed_value(r)?;
        params.push(Param {
            name,
            output: status & 0x01 != 0,
            value,
            oid,
        });
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dates_count_from_year_one() {
        assert_eq!(tds_days(1, 1, 1), 0);
        assert_eq!(tds_days(1900, 1, 1), 693595);
        assert_eq!(civil_from_tds_days(tds_days(2024, 2, 29)), (2024, 2, 29));
        assert_eq!(date_text(tds_days(1815, 12, 10)), "1815-12-10");
    }

    #[test]
    fn decimals() {
        assert_eq!(decimal_units("12.5", 2), Some((false, 1250)));
        assert_eq!(decimal_units("-0.005", 2), Some((true, 1)));
        assert_eq!(format_decimal(false, 1250, 2), "12.50");
        assert_eq!(format_decimal(true, 5, 3), "-0.005");
        assert_eq!(decimal_shape("123.4500"), (3, 4));
    }

    #[test]
    fn login_password_is_unscrambled() {
        // "ab" scrambled: swap nibbles then XOR 0xA5, per UTF-16 byte.
        let scramble = |b: u8| b.rotate_left(4) ^ 0xa5;
        let password: Vec<u8> = "ab"
            .encode_utf16()
            .flat_map(|u| u.to_le_bytes())
            .map(scramble)
            .collect();
        let mut p = vec![0u8; 94];
        p.extend_from_slice(&password);
        p[44..46].copy_from_slice(&94u16.to_le_bytes());
        p[46..48].copy_from_slice(&2u16.to_le_bytes());
        let login = parse_login7(&p).unwrap();
        assert_eq!(login.password, "ab");
    }

    #[test]
    fn values_encode() {
        let mut out = Vec::new();
        put_value(&mut out, &ColType::Int(4), Some(b"-2")).unwrap();
        assert_eq!(out, vec![4, 0xfe, 0xff, 0xff, 0xff]);
        let mut out = Vec::new();
        put_value(
            &mut out,
            &ColType::Decimal {
                precision: 10,
                scale: 2,
            },
            Some(b"19.99"),
        )
        .unwrap();
        assert_eq!(out, vec![9, 1, 0xcf, 0x07, 0, 0, 0, 0, 0, 0]);
        let mut out = Vec::new();
        put_value(
            &mut out,
            &ColType::Guid,
            Some(b"00112233-4455-6677-8899-aabbccddeeff"),
        )
        .unwrap();
        assert_eq!(
            out[..9],
            [16, 0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66]
        );
    }
}
