//! The session's socket, buffered in both directions.
//!
//! # Why
//!
//! Every protocol message used to leave as three `write_all`s and a `flush` on
//! a socket with `TCP_NODELAY`, so a `SELECT` of N rows cost about 3N write
//! syscalls and as many TCP segments; every inbound message was three
//! `read_exact`s. [`WireStream`] gathers output into one buffer and sends it
//! when there is a reason to:
//!
//! - `ReadyForQuery`, an explicit `Flush` (`H`), and every `ErrorResponse` —
//!   the callers flush there;
//! - the buffer passing [`FLUSH_AT`];
//! - **before any read that has to go to the socket.** This is the rule that
//!   makes buffering safe everywhere at once: nothing can wait on the client
//!   while holding output the client is waiting for, whichever path is
//!   reading — the message loop, the SASL exchange, anything added later.
//!
//! Reads are served from an [`READ_BUF`]-byte buffer, so a pipelined
//! Parse/Bind/Describe/Execute/Sync is one read syscall rather than fifteen.
//!
//! # The disconnect watch
//!
//! A duplicate of the socket is peeked during a statement to notice a client
//! that has gone (`super::PeerWatch`). Read-ahead moves pipelined bytes out of
//! the kernel and into this buffer, where `peek` cannot see them, so the
//! stream tells the watch whether it is holding unread bytes. Nothing else
//! reads the socket: the watch only peeks, and every session read comes
//! through here once the stream is wrapped.

use std::{
    io,
    pin::Pin,
    sync::Arc as StdArc,
    task::{Context, Poll, ready},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::PeerWatch;
use crate::core::db_type::DbType;

/// Buffered output is sent once it reaches this size.
pub(super) const FLUSH_AT: usize = 64 * 1024;
/// Output capacity kept between statements. A result past [`FLUSH_AT`] grows
/// the buffer to about twice that; the next full flush shrinks it back to
/// this, so an idle session does not pin what its biggest statement needed,
/// while a session of medium results does not reallocate every statement.
pub(super) const RETAIN: usize = 16 * 1024;
/// Inbound buffer size.
pub(super) const READ_BUF: usize = 8 * 1024;
/// Output capacity an *idle* session keeps — one waiting on its client.
///
/// [`RETAIN`] alone never applied to the common case: a result of many small
/// frames grows `out` by doubling to exactly [`FLUSH_AT`], which is not more
/// than `FLUSH_AT`, so every session that had once returned 64 KiB pinned
/// 64 KiB for the rest of its life. Shrinking happens only when the session
/// is about to wait on the client, and only above [`IDLE_SHRINK_ABOVE`]:
/// a session whose results stay under that never reallocates, and one that
/// returns large results pays a few reallocations per such result — small
/// next to encoding it.
pub(super) const IDLE_RETAIN: usize = 4 * 1024;
/// See [`IDLE_RETAIN`]: the hysteresis band's upper edge.
pub(super) const IDLE_SHRINK_ABOVE: usize = 8 * 1024;
/// Bytes read into a stack buffer when the session has no read buffer. Most
/// requests fit, so a session of ordinary statements never allocates
/// [`READ_BUF`] at all; a larger one allocates it for as long as it keeps the
/// session busy.
const READ_PROBE: usize = 1024;

pub(super) struct WireStream<S> {
    inner: S,
    out: Vec<u8>,
    /// How much of `out` has already reached `inner`.
    out_sent: usize,
    rbuf: Box<[u8]>,
    rpos: usize,
    rlen: usize,
    watch: Option<StdArc<PeerWatch>>,
}

impl<S> WireStream<S> {
    pub(super) fn new(inner: S, watch: Option<StdArc<PeerWatch>>) -> Self {
        Self {
            inner,
            out: Vec::new(),
            out_sent: 0,
            rbuf: Box::default(),
            rpos: 0,
            rlen: 0,
            watch,
        }
    }

    /// Gives back what an idle session does not need: called only when a read
    /// has returned `Pending`, so the session is waiting on its client, all
    /// output has been flushed, and no input is buffered.
    fn release_idle_capacity(&mut self) {
        if self.out.is_empty() && self.out.capacity() > IDLE_SHRINK_ABOVE {
            self.out = Vec::with_capacity(IDLE_RETAIN);
        }
        if self.rpos == self.rlen && !self.rbuf.is_empty() {
            self.rbuf = Box::default();
            self.rpos = 0;
            self.rlen = 0;
        }
    }

    fn note_buffered(&self) {
        if let Some(watch) = &self.watch {
            watch.set_input_buffered(self.rpos < self.rlen);
        }
    }
}

impl<S: AsyncWrite + Unpin> WireStream<S> {
    /// Hands everything buffered to `inner`. Does not flush `inner` itself.
    fn poll_send_out(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.out_sent < self.out.len() {
            let n = ready!(Pin::new(&mut self.inner).poll_write(cx, &self.out[self.out_sent..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "the client socket accepted no bytes",
                )));
            }
            self.out_sent += n;
        }
        self.out.clear();
        self.out_sent = 0;
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for WireStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.out.len() + buf.len() > FLUSH_AT {
            ready!(this.poll_send_out(cx))?;
        }
        if buf.len() >= FLUSH_AT {
            // Too big to be worth copying; `out` is empty, so order holds.
            return Pin::new(&mut this.inner).poll_write(cx, buf);
        }
        this.out.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_send_out(cx))?;
        // A TLS stream holds records until it is flushed itself.
        ready!(Pin::new(&mut this.inner).poll_flush(cx))?;
        if this.out.capacity() > FLUSH_AT {
            this.out = Vec::with_capacity(RETAIN);
        }
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_send_out(cx))?;
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for WireStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.rpos == this.rlen {
            // About to wait on the client: everything owed to it goes first,
            // or both ends could wait on each other.
            ready!(Pin::new(&mut *this).poll_flush(cx))?;
            if dst.remaining() >= READ_BUF {
                // A large payload goes straight into its destination.
                let polled = Pin::new(&mut this.inner).poll_read(cx, dst);
                if polled.is_pending() {
                    this.release_idle_capacity();
                }
                return polled;
            }
            // A buffer cut to the size of one small read is used once: reading
            // into it again would take a large message in small pieces.
            if this.rbuf.len() < READ_BUF {
                this.rbuf = Box::default();
            }
            let n = if this.rbuf.is_empty() {
                // No read buffer: probe with a small one on the stack, and
                // allocate the real one only if the client sent more.
                let mut probe = [0u8; READ_PROBE];
                let mut rb = ReadBuf::new(&mut probe);
                let polled = Pin::new(&mut this.inner).poll_read(cx, &mut rb);
                if polled.is_pending() {
                    this.release_idle_capacity();
                    return Poll::Pending;
                }
                ready!(polled)?;
                let n = rb.filled().len();
                if n == READ_PROBE {
                    let mut full = vec![0u8; READ_BUF].into_boxed_slice();
                    full[..n].copy_from_slice(&probe);
                    this.rbuf = full;
                } else {
                    this.rbuf = probe[..n].to_vec().into_boxed_slice();
                }
                n
            } else {
                let mut rb = ReadBuf::new(&mut this.rbuf);
                let polled = Pin::new(&mut this.inner).poll_read(cx, &mut rb);
                let filled = rb.filled().len();
                if polled.is_pending() {
                    // Waiting: nothing was read, so the buffer holds nothing
                    // owed to anyone and can go.
                    this.release_idle_capacity();
                    return Poll::Pending;
                }
                ready!(polled)?;
                filled
            };
            this.rpos = 0;
            this.rlen = n;
            if n == 0 {
                return Poll::Ready(Ok(()));
            }
        }
        let n = dst.remaining().min(this.rlen - this.rpos);
        dst.put_slice(&this.rbuf[this.rpos..this.rpos + n]);
        this.rpos += n;
        this.note_buffered();
        Poll::Ready(Ok(()))
    }
}

/// Starts a protocol message in `out`: the tag and a length to be filled in by
/// [`finish_message`]. Returns where the message starts.
#[inline]
pub(super) fn begin_message(out: &mut Vec<u8>, tag: u8) -> usize {
    let start = out.len();
    out.push(tag);
    out.extend_from_slice(&[0u8; 4]);
    start
}

/// Writes the length of the message begun at `start`, which is everything
/// after its tag.
#[inline]
pub(super) fn finish_message(out: &mut [u8], start: usize) -> io::Result<()> {
    let len: i32 = (out.len() - start - 1)
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "message too large"))?;
    out[start + 1..start + 5].copy_from_slice(&len.to_be_bytes());
    Ok(())
}

// ---------------------------------------------------------------------------
// cells
// ---------------------------------------------------------------------------

/// Appends the text form of one cell to `out`; `false` means NULL, and then
/// nothing was appended.
///
/// Byte-for-byte [`super::format_column_text_scaled`] (pinned by
/// `cell_tests`), written straight into the row's frame: the common types are
/// rendered here without a `String` per cell, everything else goes through the
/// original.
pub(super) fn encode_cell_text(
    db_type: &DbType,
    bytes: &[u8],
    scale: Option<u32>,
    out: &mut Vec<u8>,
) -> bool {
    use crate::core::value_codec as vc;
    match db_type {
        DbType::NULL | DbType::START | DbType::END => false,
        DbType::STRING | DbType::UNKNOWN | DbType::JSON => {
            match std::str::from_utf8(bytes) {
                Ok(_) => out.extend_from_slice(bytes),
                Err(_) => out.extend_from_slice(String::from_utf8_lossy(bytes).as_bytes()),
            }
            true
        }
        // Stored without its trailing spaces, shown padded to its length.
        DbType::BPCHAR(n) => {
            push_bpchar(bytes, *n, out);
            true
        }
        DbType::DECIMAL => {
            // A value carrying its own display scale (an `AVG` result) wins
            // over the column's, exactly as `format_column_text_scaled` does.
            if let Some((v, Some(own))) = vc::split_decimal_cell(bytes) {
                vc::format_decimal_scaled_into(v, own, out);
                return true;
            }
            let Ok(raw) = <[u8; 16]>::try_from(bytes) else {
                return false;
            };
            let v = i128::from_le_bytes(raw);
            match scale {
                Some(scale) => vc::format_decimal_scaled_into(v, scale, out),
                None => vc::format_decimal_into(v, out),
            }
            true
        }
        DbType::I8 if bytes.len() == 1 => {
            vc::write_i64(out, i64::from(bytes[0] as i8));
            true
        }
        DbType::U8 if bytes.len() == 1 => {
            vc::write_u64(out, u64::from(bytes[0]));
            true
        }
        DbType::I16 if bytes.len() == 2 => {
            vc::write_i64(out, i64::from(i16::from_le_bytes([bytes[0], bytes[1]])));
            true
        }
        DbType::U16 if bytes.len() == 2 => {
            vc::write_u64(out, u64::from(u16::from_le_bytes([bytes[0], bytes[1]])));
            true
        }
        DbType::I32 if bytes.len() == 4 => {
            vc::write_i64(out, i64::from(i32::from_le_bytes(bytes.try_into().unwrap())));
            true
        }
        DbType::U32 if bytes.len() == 4 => {
            vc::write_u64(out, u64::from(u32::from_le_bytes(bytes.try_into().unwrap())));
            true
        }
        DbType::I64 if bytes.len() == 8 => {
            vc::write_i64(out, i64::from_le_bytes(bytes.try_into().unwrap()));
            true
        }
        DbType::U64 if bytes.len() == 8 => {
            vc::write_u64(out, u64::from_le_bytes(bytes.try_into().unwrap()));
            true
        }
        DbType::BOOL => {
            out.push(if bytes.first().copied().unwrap_or(0) != 0 { b't' } else { b'f' });
            true
        }
        DbType::DATETIME => {
            let Ok(raw) = <[u8; 8]>::try_from(bytes) else {
                return false;
            };
            vc::format_timestamp_us_into(i64::from_le_bytes(raw), out);
            out.extend_from_slice(b"+00");
            true
        }
        DbType::BYTEA => {
            vc::format_bytea_into(bytes, out);
            true
        }
        DbType::JSONB => {
            if bytes.is_empty() {
                return true;
            }
            let start = out.len();
            match crate::core::jsonb::render_text_into(bytes, out) {
                Ok(()) => true,
                Err(e) => {
                    out.truncate(start);
                    log::error!("a JSONB value could not be decoded: {}", e);
                    false
                }
            }
        }
        _ => match super::format_column_text_scaled(db_type.clone(), bytes, scale) {
            Some(text) => {
                out.extend_from_slice(text.as_bytes());
                true
            }
            None => false,
        },
    }
}

/// A `character(n)` value as it is shown, text and binary alike: its stored
/// form (no trailing spaces) blank-padded to `n` characters.
fn push_bpchar(bytes: &[u8], n: u16, out: &mut Vec<u8>) {
    let text = String::from_utf8_lossy(bytes);
    out.extend_from_slice(crate::core::rql::type_modifier::pad_to(&text, u32::from(n)).as_bytes());
}

/// Appends the binary form of one cell to `out`; `false` means NULL, and then
/// nothing was appended. Byte-for-byte [`super::format_column_binary`].
pub(super) fn encode_cell_binary(
    db_type: &DbType,
    bytes: &[u8],
    scale: Option<u32>,
    out: &mut Vec<u8>,
) -> bool {
    match db_type {
        DbType::NULL | DbType::START | DbType::END => false,
        DbType::STRING | DbType::UNKNOWN | DbType::BYTEA | DbType::ARRAY => {
            out.extend_from_slice(bytes);
            true
        }
        // `bpcharsend` is the padded text, as the text form is.
        DbType::BPCHAR(n) => {
            push_bpchar(bytes, *n, out);
            true
        }
        // The unsigned widths are advertised as `int8` (`DbType::pg_oid`),
        // so their binary form is eight bytes. Sent at their own width — two
        // or four bytes under an `int8` OID — a binary client refused the
        // column ("error deserializing column").
        DbType::U8 | DbType::U16 | DbType::U32 if matches!(bytes.len(), 1 | 2 | 4) => {
            let mut le = [0u8; 8];
            le[..bytes.len()].copy_from_slice(bytes);
            out.extend_from_slice(&u64::from_le_bytes(le).to_be_bytes());
            true
        }
        DbType::I16 if bytes.len() == 2 => {
            out.extend_from_slice(&[bytes[1], bytes[0]]);
            true
        }
        DbType::I32 | DbType::F32 if bytes.len() == 4 => {
            let raw: [u8; 4] = bytes.try_into().unwrap();
            out.extend_from_slice(&u32::from_le_bytes(raw).to_be_bytes());
            true
        }
        DbType::I64 | DbType::F64 if bytes.len() == 8 => {
            let raw: [u8; 8] = bytes.try_into().unwrap();
            out.extend_from_slice(&u64::from_le_bytes(raw).to_be_bytes());
            true
        }
        // `numeric` at scale 0 (`DbType::pg_oid`); any other width falls
        // through to `format_column_binary`, which reads it as the text
        // path does.
        DbType::U64 if bytes.len() == 8 => {
            let raw: [u8; 8] = bytes.try_into().unwrap();
            crate::core::value_codec::encode_pg_numeric_u128_into(u64::from_le_bytes(raw) as u128, out);
            true
        }
        DbType::I16 | DbType::U8 | DbType::U16 | DbType::I32 | DbType::U32 | DbType::F32
        | DbType::I64 | DbType::F64 => false,
        DbType::BOOL => {
            out.push(u8::from(bytes.first().is_some_and(|b| *b != 0)));
            true
        }
        DbType::UUID => {
            if bytes.len() != crate::core::value_codec::UUID_LEN {
                return false;
            }
            out.extend_from_slice(bytes);
            true
        }
        DbType::DECIMAL => {
            // Own display scale first, as `format_column_binary` does.
            if let Some((v, Some(own))) = crate::core::value_codec::split_decimal_cell(bytes) {
                out.extend_from_slice(&crate::core::value_codec::encode_pg_numeric_with_dscale(v, own));
                return true;
            }
            let Ok(raw) = <[u8; 16]>::try_from(bytes) else {
                return false;
            };
            crate::core::value_codec::encode_pg_numeric_scaled_into(
                i128::from_le_bytes(raw),
                scale.unwrap_or(crate::core::value_codec::DECIMAL_SCALE),
                out,
            );
            true
        }
        DbType::DATETIME => {
            let Ok(raw) = <[u8; 8]>::try_from(bytes) else {
                return false;
            };
            let us = i64::from_le_bytes(raw);
            out.extend_from_slice(
                &us.wrapping_sub(crate::core::value_codec::PG_EPOCH_OFFSET_US)
                    .to_be_bytes(),
            );
            true
        }
        DbType::JSONB => {
            if bytes.is_empty() {
                return false;
            }
            let start = out.len();
            out.push(1u8);
            if crate::core::jsonb::render_text_into(bytes, out).is_err() {
                out.truncate(start);
                return false;
            }
            true
        }
        _ => match super::format_column_binary(db_type.clone(), bytes, scale) {
            Some(value) => {
                out.extend_from_slice(&value);
                true
            }
            None => false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Records every write and flush the wrapped stream makes, and serves reads
    /// from a script.
    struct Recorder {
        writes: Vec<Vec<u8>>,
        flushes: usize,
        input: Vec<u8>,
        reads: usize,
        /// Accept at most this many bytes per write, to exercise partial writes.
        max_write: usize,
    }

    impl AsyncWrite for Recorder {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let n = buf.len().min(this.max_write);
            this.writes.push(buf[..n].to_vec());
            Poll::Ready(Ok(n))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.get_mut().flushes += 1;
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncRead for Recorder {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            dst: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.reads += 1;
            let n = dst.remaining().min(this.input.len());
            dst.put_slice(&this.input[..n]);
            this.input.drain(..n);
            Poll::Ready(Ok(()))
        }
    }

    fn recorder(input: &[u8], max_write: usize) -> Recorder {
        Recorder {
            writes: Vec::new(),
            flushes: 0,
            input: input.to_vec(),
            reads: 0,
            max_write,
        }
    }

    fn sent(r: &Recorder) -> Vec<u8> {
        r.writes.concat()
    }

    #[tokio::test]
    async fn small_writes_are_gathered_until_flush() {
        let mut s = WireStream::new(recorder(b"", usize::MAX), None);
        for i in 0..100u8 {
            s.write_all(&[i; 7]).await.unwrap();
        }
        assert!(s.inner.writes.is_empty(), "nothing leaves before a flush");
        s.flush().await.unwrap();
        assert_eq!(s.inner.writes.len(), 1, "one write for the whole batch");
        let expected: Vec<u8> = (0..100u8).flat_map(|i| [i; 7]).collect();
        assert_eq!(sent(&s.inner), expected);
    }

    #[tokio::test]
    async fn the_threshold_sends_without_a_flush_and_order_holds() {
        let mut s = WireStream::new(recorder(b"", 1000), None);
        let mut expected = Vec::new();
        // Small, then one larger than the threshold, then small again.
        for round in 0..3u8 {
            for i in 0..5000u32 {
                let piece = [(i % 251) as u8, round, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9];
                s.write_all(&piece).await.unwrap();
                expected.extend_from_slice(&piece);
            }
            let big = vec![round + 1; FLUSH_AT + 17];
            s.write_all(&big).await.unwrap();
            expected.extend_from_slice(&big);
        }
        s.write_all(b"tail").await.unwrap();
        expected.extend_from_slice(b"tail");
        s.flush().await.unwrap();
        assert_eq!(sent(&s.inner), expected, "partial writes must not reorder or drop");
        assert!(s.out.capacity() <= FLUSH_AT, "capacity is given back after a flush");
    }

    #[tokio::test]
    async fn a_read_that_needs_the_socket_flushes_first() {
        let mut s = WireStream::new(recorder(b"abcdefgh", usize::MAX), None);
        s.write_all(b"owed").await.unwrap();
        let mut one = [0u8; 2];
        s.read_exact(&mut one).await.unwrap();
        assert_eq!(sent(&s.inner), b"owed", "output is sent before reading");
        assert_eq!(&one, b"ab");
        // Served from the buffer: no further read, and no flush needed.
        s.write_all(b"more").await.unwrap();
        let mut rest = [0u8; 6];
        s.read_exact(&mut rest).await.unwrap();
        assert_eq!(&rest, b"cdefgh");
        assert_eq!(s.inner.reads, 1, "one socket read served both");
        assert_eq!(sent(&s.inner), b"owed", "a buffered read does not flush");
        // Now the buffer is empty, so the next read flushes, then sees EOF.
        let mut more = [0u8; 1];
        assert!(s.read_exact(&mut more).await.is_err());
        assert_eq!(sent(&s.inner), b"owedmore");
    }

    #[tokio::test]
    async fn a_large_read_bypasses_the_buffer_without_losing_bytes() {
        let input: Vec<u8> = (0..(READ_BUF * 3 + 5)).map(|i| (i % 253) as u8).collect();
        let mut s = WireStream::new(recorder(&input, usize::MAX), None);
        let mut head = [0u8; 3];
        s.read_exact(&mut head).await.unwrap();
        let mut body = vec![0u8; input.len() - 3];
        s.read_exact(&mut body).await.unwrap();
        assert_eq!(&head[..], &input[..3]);
        assert_eq!(&body[..], &input[3..]);
    }

    /// An idle session gives back what its last statement needed: a result
    /// of small frames grows `out` to exactly `FLUSH_AT`, which the flush-time
    /// check (`> FLUSH_AT`) never shrank, and the read buffer was never freed.
    #[tokio::test]
    async fn waiting_on_the_client_releases_buffers() {
        let (server_side, mut client) = tokio::io::duplex(1 << 20);
        let mut s = WireStream::new(server_side, None);
        // A request arrives and is read through the buffer.
        client.write_all(&[7u8; 3000]).await.unwrap();
        let mut req = [0u8; 3000];
        s.read_exact(&mut req).await.unwrap();
        // A result of many small frames.
        for _ in 0..(FLUSH_AT / 16) {
            s.write_all(&[1u8; 16]).await.unwrap();
        }
        s.flush().await.unwrap();
        assert!(s.out.capacity() >= FLUSH_AT / 2, "the result did grow the buffer");
        // Now the session waits for the next request.
        let mut next = [0u8; 5];
        let waited =
            tokio::time::timeout(std::time::Duration::from_millis(20), s.read_exact(&mut next)).await;
        assert!(waited.is_err(), "nothing was sent, so the read waits");
        assert!(s.out.capacity() <= IDLE_RETAIN, "kept {}", s.out.capacity());
        assert!(s.rbuf.is_empty(), "the read buffer is given back while idle");

        // And the session still works afterwards.
        client.write_all(b"hello").await.unwrap();
        s.read_exact(&mut next).await.unwrap();
        assert_eq!(&next, b"hello");
    }

    /// Small results never reallocate: under the hysteresis band the buffer is
    /// kept as it is across waits.
    #[tokio::test]
    async fn a_session_of_small_results_keeps_its_buffer() {
        let (server_side, _client) = tokio::io::duplex(1 << 20);
        let mut s = WireStream::new(server_side, None);
        s.write_all(&[1u8; 3000]).await.unwrap();
        s.flush().await.unwrap();
        let before = s.out.capacity();
        let mut next = [0u8; 1];
        let _ = tokio::time::timeout(std::time::Duration::from_millis(10), s.read_exact(&mut next)).await;
        assert_eq!(s.out.capacity(), before);
        assert!(before <= IDLE_SHRINK_ABOVE);
    }

    #[test]
    fn a_message_frame_counts_everything_after_its_tag() {
        let mut out = vec![0xAA];
        let start = begin_message(&mut out, b'D');
        out.extend_from_slice(b"xyz");
        finish_message(&mut out, start).unwrap();
        assert_eq!(out, [0xAA, b'D', 0, 0, 0, 7, b'x', b'y', b'z']);
    }
}

/// The in-place cell encoders against the functions they stand in for, over
/// every type and the inputs that reach each arm: every width around the
/// fixed ones, invalid UTF-8, documents, and each scale.
#[cfg(test)]
mod cell_tests {
    use super::*;

    fn all_types() -> Vec<DbType> {
        let mut types: Vec<DbType> = (1..=44u8).chain(252..=255).map(DbType::from_byte).collect();
        types.extend([DbType::NULL, DbType::START, DbType::END, DbType::UNKNOWN]);
        types.sort();
        types.dedup();
        types
    }

    fn inputs() -> Vec<Vec<u8>> {
        let mut state = 0x1234_5678_9ABC_DEF0u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut out: Vec<Vec<u8>> = Vec::new();
        for len in [0usize, 1, 2, 3, 4, 5, 8, 9, 15, 16, 17, 18, 24, 40] {
            for _ in 0..40 {
                out.push((0..len).map(|_| next() as u8).collect());
            }
            out.push(vec![0u8; len]);
            out.push(vec![0xFFu8; len]);
            out.push(vec![b'7'; len]);
        }
        for text in ["", "hello", "héllo wörld", "日本語", "a\"b\\c\nd", "\u{1F600}"] {
            out.push(text.as_bytes().to_vec());
        }
        out.push(vec![0xC3, 0x28, b'a']); // invalid UTF-8
        out.push(vec![b'a', 0xFF, 0xFE]);
        for doc in [
            serde_json::json!({"a": 1, "b": [true, null, 1.5, "x\u{1}y"], "c": {"d": -0.0}}),
            serde_json::json!([]),
            serde_json::json!("plain"),
            serde_json::json!(18446744073709551615u64),
        ] {
            out.push(crate::core::jsonb::encode(&doc));
        }
        for v in [0i128, 1, -1, 12_345_678_900, -5, i128::MAX, i128::MIN, u64::MAX as i128 + 1] {
            out.push(v.to_le_bytes().to_vec());
        }
        for us in [0i64, -1, 1_700_000_000_123_456, i64::MIN, i64::MAX] {
            out.push(us.to_le_bytes().to_vec());
        }
        out
    }

    #[test]
    fn cells_match_the_formatters_byte_for_byte() {
        let scales = [None, Some(0), Some(2), Some(4), Some(9), Some(16), Some(19), Some(20), Some(38)];
        let mut checked = 0usize;
        for ty in all_types() {
            for bytes in inputs() {
                for scale in scales {
                    let expected = super::super::format_column_text_scaled(ty.clone(), &bytes, scale)
                        .map(String::into_bytes);
                    let mut out = b"prefix".to_vec();
                    let present = encode_cell_text(&ty, &bytes, scale, &mut out);
                    let got = present.then(|| out[6..].to_vec());
                    assert_eq!(got, expected, "text {ty:?} {bytes:?} scale {scale:?}");
                    if !present {
                        assert_eq!(out, b"prefix", "a NULL appends nothing");
                    }

                    let expected = super::super::format_column_binary(ty.clone(), &bytes, scale);
                    let mut out = b"prefix".to_vec();
                    let present = encode_cell_binary(&ty, &bytes, scale, &mut out);
                    let got = present.then(|| out[6..].to_vec());
                    assert_eq!(got, expected, "binary {ty:?} {bytes:?} scale {scale:?}");
                    if !present {
                        assert_eq!(out, b"prefix", "a NULL appends nothing");
                    }
                    checked += 1;
                }
            }
        }
        assert!(checked > 10_000);
    }

    /// The binary `int2` an `I8` is sent as is sign-extended, as its text is.
    #[test]
    fn a_negative_i8_is_negative_in_binary_too() {
        let bin = super::super::format_column_binary(DbType::I8, &[0xFF], None).unwrap();
        assert_eq!(i16::from_be_bytes([bin[0], bin[1]]), -1);
        // `U8` is advertised as `int8` (`DbType::pg_oid`), so it goes out as
        // eight bytes; two bytes under an `int8` OID made a binary client
        // refuse the column.
        let bin = super::super::format_column_binary(DbType::U8, &[0xFF], None).unwrap();
        assert_eq!(bin.len(), 8);
        assert_eq!(i64::from_be_bytes(bin.try_into().unwrap()), 255);
    }
}

#[cfg(test)]
mod row_bench {
    use super::*;

    /// The per-row encoding cost alone (no socket): the old shape — a `Vec` of
    /// owned cells, a `refs` `Vec`, a payload `Vec` — against cells written
    /// into one reused frame. Both use today's value formatters, so this
    /// isolates the allocation change. Run with
    /// `cargo test --release -p rasterizeddb_core --lib -- --ignored row_encode_bench --nocapture`.
    #[test]
    #[ignore]
    fn row_encode_bench() {
        use std::hint::black_box;
        use std::time::Instant;
        let types = [DbType::I32, DbType::STRING, DbType::DECIMAL, DbType::DATETIME, DbType::BOOL];
        let scales = [None, None, Some(2u32), None, None];
        let rows: Vec<Vec<Vec<u8>>> = (0..10_000i64)
            .map(|i| {
                vec![
                    (i as i32).to_le_bytes().to_vec(),
                    format!("name-{i}-é").into_bytes(),
                    ((i * 731) as i128).to_le_bytes().to_vec(),
                    (1_700_000_000_000_000 + i * 1_234_567).to_le_bytes().to_vec(),
                    vec![(i % 2) as u8],
                ]
            })
            .collect();
        let (mut a, mut b) = (Vec::new(), Vec::new());
        for _ in 0..15 {
            let mut sink: Vec<u8> = Vec::with_capacity(1 << 20);
            let t = Instant::now();
            for row in &rows {
                let mut values = Vec::with_capacity(row.len());
                for (i, cell) in row.iter().enumerate() {
                    values.push(
                        super::super::format_column_text_scaled(types[i].clone(), cell, scales[i])
                            .map(String::into_bytes),
                    );
                }
                let refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
                let mut payload = Vec::new();
                payload.extend_from_slice(&(refs.len() as i16).to_be_bytes());
                for v in &refs {
                    match v {
                        None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
                        Some(bytes) => {
                            payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                            payload.extend_from_slice(bytes);
                        }
                    }
                }
                sink.push(b'D');
                sink.extend_from_slice(&((payload.len() + 4) as i32).to_be_bytes());
                sink.extend_from_slice(&payload);
            }
            a.push(t.elapsed().as_nanos() as f64 / rows.len() as f64);
            black_box(&sink);

            let mut sink: Vec<u8> = Vec::with_capacity(1 << 20);
            let t = Instant::now();
            let mut frame = Vec::with_capacity(256);
            for row in &rows {
                frame.clear();
                let start = begin_message(&mut frame, b'D');
                frame.extend_from_slice(&(row.len() as i16).to_be_bytes());
                for (i, cell) in row.iter().enumerate() {
                    let len_at = frame.len();
                    frame.extend_from_slice(&[0u8; 4]);
                    let len = if encode_cell_text(&types[i], cell, scales[i], &mut frame) {
                        (frame.len() - len_at - 4) as i32
                    } else {
                        -1
                    };
                    frame[len_at..len_at + 4].copy_from_slice(&len.to_be_bytes());
                }
                finish_message(&mut frame, start).unwrap();
                sink.extend_from_slice(&frame);
            }
            b.push(t.elapsed().as_nanos() as f64 / rows.len() as f64);
            black_box(&sink);
        }
        a.sort_by(f64::total_cmp);
        b.sort_by(f64::total_cmp);
        println!("row encode (5 text cells) old {:.1} ns/row  new {:.1} ns/row  ({:.2}x)", a[7], b[7], a[7] / b[7]);
    }
}
