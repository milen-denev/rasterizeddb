//! Reading and writing frames in batches.
//!
//! A frame is four bytes of little-endian length and then that many bytes. The
//! helpers in [`crate::common`] move one frame per call: a read for the prefix,
//! a read for the body, a write and a flush for every reply. That is one
//! system call in and one out per request however many requests the peer had
//! already sent, and those two calls are most of what a small request costs.
//!
//! [`FrameReader`] instead takes whatever the connection has to give and hands
//! out every complete frame in it, and [`FrameWriter`] collects replies and
//! sends them together. A peer that sends one request and waits sees no
//! difference; a peer that sends several before reading gets them answered
//! with one read and one write.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

use crate::error::RastcpError;

/// A TLS peer that hangs up without a `close_notify` surfaces as an
/// unexpected end of file. For a reader of frames that is the peer closing
/// the connection, the same as an orderly end.
fn read_error(err: std::io::Error) -> RastcpError {
    if err.kind() == std::io::ErrorKind::UnexpectedEof {
        RastcpError::ConnectionClosed
    } else {
        RastcpError::Io(err)
    }
}

/// Bytes of length before every frame.
pub(crate) const PREFIX: usize = 4;

/// How much is read ahead. One full TLS record of plaintext, so a record is
/// taken whole.
const READ_AHEAD: usize = 16 * 1024;

/// Replies up to this size are copied behind the ones already waiting, to
/// leave in one write. A larger one is written from where it is.
const COALESCE_LIMIT: usize = 16 * 1024;

/// Waiting replies are sent once they pass this, even if more requests are
/// ready, so a long pipeline is answered as it goes and the buffer stays
/// small.
const FLUSH_AT: usize = 32 * 1024;

/// A body larger than the read-ahead buffer, part-way in.
struct Partial {
    body: Vec<u8>,
    filled: usize,
}

/// Takes frames off a stream, reading ahead.
///
/// All of its state is in the struct, so a wait for a frame can be dropped and
/// started again without losing bytes — which a pair of `read_exact` calls
/// cannot offer.
pub(crate) struct FrameReader {
    buf: Box<[u8]>,
    start: usize,
    end: usize,
    partial: Option<Partial>,
}

impl FrameReader {
    pub(crate) fn new() -> Self {
        Self {
            // Initialised once. Reading into initialised memory spares the
            // TLS stream zeroing the whole spare capacity on every read.
            buf: vec![0u8; READ_AHEAD].into_boxed_slice(),
            start: 0,
            end: 0,
            partial: None,
        }
    }

    /// Whether bytes have been read that no frame handed out yet accounts
    /// for: the peer sent more than it has been answered for.
    pub(crate) fn has_buffered(&self) -> bool {
        self.partial.is_some() || self.end > self.start
    }

    /// The next frame, reading from `stream` only if it is not already here.
    ///
    /// Pending only after the stream itself returned pending, so a caller that
    /// polls once learns whether a frame can be had without waiting.
    pub(crate) fn poll_frame<S>(
        &mut self,
        stream: &mut S,
        cx: &mut Context<'_>,
        max_message_size: usize,
    ) -> Poll<Result<Vec<u8>, RastcpError>>
    where
        S: AsyncRead + Unpin + ?Sized,
    {
        loop {
            if let Some(partial) = self.partial.as_mut() {
                while partial.filled < partial.body.len() {
                    let mut unfilled = ReadBuf::new(&mut partial.body[partial.filled..]);
                    if let Err(err) = ready!(Pin::new(&mut *stream).poll_read(cx, &mut unfilled)) {
                        return Poll::Ready(Err(read_error(err)));
                    }

                    let read = unfilled.filled().len();
                    if read == 0 {
                        return Poll::Ready(Err(RastcpError::ConnectionClosed));
                    }
                    partial.filled += read;
                }

                let body = self
                    .partial
                    .take()
                    .map(|partial| partial.body)
                    .unwrap_or_default();
                return Poll::Ready(Ok(body));
            }

            let available = self.end - self.start;

            if available >= PREFIX {
                let prefix = &self.buf[self.start..self.start + PREFIX];
                let len = u32::from_le_bytes([prefix[0], prefix[1], prefix[2], prefix[3]]) as usize;

                // Checked on the prefix alone, before anything is reserved: a
                // peer cannot make this side allocate by claiming a length.
                if len > max_message_size {
                    return Poll::Ready(Err(RastcpError::MessageTooLarge {
                        len,
                        max: max_message_size,
                    }));
                }

                let buffered = (available - PREFIX).min(len);
                let from = self.start + PREFIX;

                let mut body = Vec::new();
                if let Err(err) = body.try_reserve_exact(len) {
                    return Poll::Ready(Err(RastcpError::ProtocolError(err.to_string())));
                }
                body.extend_from_slice(&self.buf[from..from + buffered]);
                self.start = from + buffered;

                if buffered == len {
                    if self.start == self.end {
                        self.start = 0;
                        self.end = 0;
                    }
                    return Poll::Ready(Ok(body));
                }

                // The rest of this body is still on its way. It goes straight
                // into the frame, not through the buffer, which is now empty.
                body.resize(len, 0);
                self.start = 0;
                self.end = 0;
                self.partial = Some(Partial {
                    body,
                    filled: buffered,
                });
                continue;
            }

            // Fewer than four bytes here: at most a split prefix. Move it to
            // the front so the read below has the whole buffer behind it.
            if self.start > 0 {
                self.buf.copy_within(self.start..self.end, 0);
                self.end = available;
                self.start = 0;
            }

            let mut unfilled = ReadBuf::new(&mut self.buf[self.end..]);
            if let Err(err) = ready!(Pin::new(&mut *stream).poll_read(cx, &mut unfilled)) {
                return Poll::Ready(Err(read_error(err)));
            }

            let read = unfilled.filled().len();
            if read == 0 {
                return Poll::Ready(Err(RastcpError::ConnectionClosed));
            }
            self.end += read;
        }
    }

    /// Waits for the next frame.
    pub(crate) async fn frame<S>(
        &mut self,
        stream: &mut S,
        max_message_size: usize,
    ) -> Result<Vec<u8>, RastcpError>
    where
        S: AsyncRead + Unpin + ?Sized,
    {
        std::future::poll_fn(|cx| self.poll_frame(stream, cx, max_message_size)).await
    }

    /// The next frame if it can be had without waiting.
    ///
    /// `None` leaves the task registered to be woken when the stream has more,
    /// exactly as a pending [`FrameReader::frame`] would.
    pub(crate) async fn frame_now<S>(
        &mut self,
        stream: &mut S,
        max_message_size: usize,
    ) -> Option<Result<Vec<u8>, RastcpError>>
    where
        S: AsyncRead + Unpin + ?Sized,
    {
        std::future::poll_fn(|cx| {
            Poll::Ready(match self.poll_frame(stream, cx, max_message_size) {
                Poll::Ready(frame) => Some(frame),
                Poll::Pending => None,
            })
        })
        .await
    }
}

/// Collects frames to send and writes them together.
pub(crate) struct FrameWriter {
    buf: Vec<u8>,
}

impl FrameWriter {
    pub(crate) fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Whether anything queued has not been sent.
    pub(crate) fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Whether enough is queued that it should be sent before more is added.
    pub(crate) fn is_full(&self) -> bool {
        self.buf.len() >= FLUSH_AT
    }

    /// What is queued, for a caller that writes it out itself.
    pub(crate) fn queued(&self) -> &[u8] {
        &self.buf
    }

    /// Forgets what is queued, once such a caller has written it.
    pub(crate) fn clear(&mut self) {
        self.buf.clear();
    }

    /// Queues the prefix of a frame of `len` bytes, whose body the caller then
    /// writes itself.
    pub(crate) fn queue_prefix(&mut self, len: usize) -> Result<(), RastcpError> {
        // The prefix is four bytes on the wire, so a longer payload cannot be
        // framed at all; truncating its length would desynchronise the stream.
        let len = u32::try_from(len).map_err(|_| RastcpError::MessageTooLarge {
            len,
            max: u32::MAX as usize,
        })?;
        self.buf.extend_from_slice(&len.to_le_bytes());
        Ok(())
    }

    /// Queues `payload` as a frame if it is small enough to be worth copying.
    /// Returns whether it was queued; if not, send it with
    /// [`FrameWriter::send_large`].
    pub(crate) fn queue(&mut self, payload: &[u8]) -> Result<bool, RastcpError> {
        if payload.len() > COALESCE_LIMIT {
            return Ok(false);
        }

        self.buf.reserve(PREFIX + payload.len());
        self.queue_prefix(payload.len())?;
        self.buf.extend_from_slice(payload);
        Ok(true)
    }

    /// Sends everything queued, then `payload` as a frame, without copying it.
    pub(crate) async fn send_large<S>(
        &mut self,
        stream: &mut S,
        payload: &[u8],
    ) -> Result<(), RastcpError>
    where
        S: AsyncWrite + Unpin + ?Sized,
    {
        // The prefix leaves with whatever was already waiting, so it does not
        // cost a record and a system call of its own.
        self.queue_prefix(payload.len())?;
        let sent = async {
            stream.write_all(&self.buf).await?;
            stream.write_all(payload).await?;
            stream.flush().await
        }
        .await;
        self.buf.clear();
        Ok(sent?)
    }

    /// Sends everything queued.
    pub(crate) async fn flush<S>(&mut self, stream: &mut S) -> Result<(), RastcpError>
    where
        S: AsyncWrite + Unpin + ?Sized,
    {
        if self.buf.is_empty() {
            return Ok(());
        }

        let sent = async {
            stream.write_all(&self.buf).await?;
            stream.flush().await
        }
        .await;
        self.buf.clear();
        Ok(sent?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    const MAX: usize = 8 * 1024 * 1024;

    /// Ends a test that is waiting for bytes that will never come, so a reader
    /// that loses some fails its test instead of holding the run.
    async fn bounded<F: std::future::Future>(test: F) -> F::Output {
        tokio::time::timeout(std::time::Duration::from_secs(30), test)
            .await
            .expect("the reader is waiting for bytes it was already given")
    }

    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut frame = (payload.len() as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(payload);
        frame
    }

    /// A payload whose every byte depends on its position and on `seed`, so a
    /// frame that came back shifted, truncated or swapped with its neighbour
    /// does not compare equal.
    fn payload(seed: usize, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| (i.wrapping_mul(31).wrapping_add(seed * 7)) as u8)
            .collect()
    }

    #[tokio::test]
    async fn frames_sent_together_come_out_one_by_one_and_in_order() {
        bounded(async {
            let (mut near, mut far) = duplex(1 << 20);
            let sizes = [0usize, 1, 3, 4, 5, 64, 1000, 4096];

            let mut wire = Vec::new();
            for (seed, len) in sizes.iter().enumerate() {
                wire.extend_from_slice(&framed(&payload(seed, *len)));
            }
            far.write_all(&wire).await.unwrap();

            let mut reader = FrameReader::new();
            for (seed, len) in sizes.iter().enumerate() {
                let frame = reader.frame(&mut near, MAX).await.unwrap();
                assert_eq!(frame, payload(seed, *len), "frame {seed}");
            }

            // Everything sent has been handed out: nothing more without waiting.
            assert!(reader.frame_now(&mut near, MAX).await.is_none());
        })
        .await
    }

    #[tokio::test]
    async fn a_frame_may_arrive_in_pieces_of_any_size() {
        bounded(async {
            // Every way of cutting the stream: a prefix split after each of its
            // bytes, bodies split anywhere, and pieces that end exactly on a frame.
            let sizes = [5usize, 0, 1, 17, 300];
            let mut wire = Vec::new();
            for (seed, len) in sizes.iter().enumerate() {
                wire.extend_from_slice(&framed(&payload(seed, *len)));
            }

            for piece in 1..=11usize {
                let (mut near, mut far) = duplex(1 << 16);
                let wire = wire.clone();

                let sender = tokio::spawn(async move {
                    for chunk in wire.chunks(piece) {
                        far.write_all(chunk).await.unwrap();
                        tokio::task::yield_now().await;
                    }
                    far
                });

                let mut reader = FrameReader::new();
                for (seed, len) in sizes.iter().enumerate() {
                    let frame = reader.frame(&mut near, MAX).await.unwrap();
                    assert_eq!(
                        frame,
                        payload(seed, *len),
                        "frame {seed} in pieces of {piece}"
                    );
                }

                drop(sender.await.unwrap());
            }
        })
        .await
    }

    #[tokio::test]
    async fn a_frame_larger_than_the_read_ahead_is_read_whole() {
        bounded(async {
            let (mut near, mut far) = duplex(1 << 16);
            let big = payload(1, READ_AHEAD * 5 + 123);
            let small = payload(2, 10);

            let mut wire = framed(&small);
            wire.extend_from_slice(&framed(&big));
            wire.extend_from_slice(&framed(&small));

            let sender = tokio::spawn(async move {
                far.write_all(&wire).await.unwrap();
                far
            });

            let mut reader = FrameReader::new();
            assert_eq!(reader.frame(&mut near, MAX).await.unwrap(), small);
            assert_eq!(reader.frame(&mut near, MAX).await.unwrap(), big);
            // The bytes right behind a large frame are the next frame's, and must
            // not have been swallowed into it or lost with it.
            assert_eq!(reader.frame(&mut near, MAX).await.unwrap(), small);

            drop(sender.await.unwrap());
        })
        .await
    }

    #[tokio::test]
    async fn a_wait_for_a_frame_can_be_abandoned_and_resumed() {
        bounded(async {
            let (mut near, mut far) = duplex(1 << 16);
            let body = payload(3, 50_000);
            let wire = framed(&body);
            let mut reader = FrameReader::new();

            // Half of a large frame, then give up on the wait, as a timeout or a
            // lost race would.
            far.write_all(&wire[..20_000]).await.unwrap();
            let abandoned = tokio::time::timeout(
                std::time::Duration::from_millis(50),
                reader.frame(&mut near, MAX),
            )
            .await;
            assert!(
                abandoned.is_err(),
                "half a frame was handed out as a whole one"
            );

            far.write_all(&wire[20_000..]).await.unwrap();
            assert_eq!(reader.frame(&mut near, MAX).await.unwrap(), body);
        })
        .await
    }

    #[tokio::test]
    async fn an_oversized_frame_is_refused_on_its_prefix_alone() {
        bounded(async {
            let (mut near, mut far) = duplex(64);

            // Only the prefix is ever sent: the refusal cannot be waiting for, or
            // have reserved room for, a body.
            far.write_all(&(1_000_001u32).to_le_bytes()).await.unwrap();

            let mut reader = FrameReader::new();
            match reader.frame(&mut near, 1_000_000).await {
                Err(RastcpError::MessageTooLarge { len, max }) => {
                    assert_eq!(len, 1_000_001);
                    assert_eq!(max, 1_000_000);
                }
                other => panic!("expected the frame to be refused, got {other:?}"),
            }
        })
        .await
    }

    #[tokio::test]
    async fn a_frame_of_exactly_the_limit_is_accepted() {
        bounded(async {
            let (mut near, mut far) = duplex(1 << 16);
            let body = payload(4, 1000);
            far.write_all(&framed(&body)).await.unwrap();

            let mut reader = FrameReader::new();
            assert_eq!(reader.frame(&mut near, 1000).await.unwrap(), body);
        })
        .await
    }

    #[tokio::test]
    async fn a_peer_that_leaves_mid_frame_is_a_closed_connection() {
        bounded(async {
            for cut in [0usize, 2, 4, 10] {
                let (mut near, mut far) = duplex(1 << 16);
                let wire = framed(&payload(5, 100));

                far.write_all(&wire[..cut]).await.unwrap();
                drop(far);

                let mut reader = FrameReader::new();
                assert!(
                    matches!(
                        reader.frame(&mut near, MAX).await,
                        Err(RastcpError::ConnectionClosed)
                    ),
                    "a stream cut after {cut} bytes"
                );
            }
        })
        .await
    }

    #[tokio::test]
    async fn queued_frames_and_a_large_one_leave_in_the_order_they_were_given() {
        bounded(async {
            let (mut near, mut far) = duplex(1 << 22);
            let small = payload(6, 100);
            let large = payload(7, COALESCE_LIMIT + 1);
            let edge = payload(8, COALESCE_LIMIT);

            let mut writer = FrameWriter::new();
            assert!(writer.queue(&small).unwrap());
            assert!(!writer.is_empty());
            assert!(
                !writer.queue(&large).unwrap(),
                "a large payload is not copied"
            );
            writer.send_large(&mut far, &large).await.unwrap();
            assert!(writer.is_empty());
            assert!(writer.queue(&edge).unwrap());
            assert!(writer.queue(&[]).unwrap());
            writer.flush(&mut far).await.unwrap();
            assert!(writer.is_empty());

            let mut reader = FrameReader::new();
            assert_eq!(reader.frame(&mut near, MAX).await.unwrap(), small);
            assert_eq!(reader.frame(&mut near, MAX).await.unwrap(), large);
            assert_eq!(reader.frame(&mut near, MAX).await.unwrap(), edge);
            assert_eq!(
                reader.frame(&mut near, MAX).await.unwrap(),
                Vec::<u8>::new()
            );
        })
        .await
    }

    #[tokio::test]
    async fn the_writer_asks_to_be_flushed_before_it_grows_without_bound() {
        bounded(async {
            let mut writer = FrameWriter::new();
            let reply = payload(9, 1000);
            let mut queued = 0;

            while !writer.is_full() {
                assert!(writer.queue(&reply).unwrap());
                queued += 1;
                assert!(queued < 1000, "the writer never reported itself full");
            }

            assert!(queued * (PREFIX + reply.len()) < FLUSH_AT + PREFIX + reply.len());
        })
        .await
    }
}
