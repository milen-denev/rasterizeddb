use crate::error::RastcpError;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

// Read a length-prefixed message - generic over any AsyncRead + AsyncWrite stream
pub async fn read_message<S>(stream: &mut S) -> Result<Vec<u8>, RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    read_message_limited(stream, DEFAULT_MAX_MESSAGE_SIZE).await
}

pub async fn read_message_limited<S>(
    stream: &mut S,
    max_message_size: usize,
) -> Result<Vec<u8>, RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    // Read the message length (4 bytes)
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            RastcpError::ConnectionClosed
        } else {
            RastcpError::Io(e)
        }
    })?;

    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > max_message_size {
        return Err(RastcpError::MessageTooLarge {
            len,
            max: max_message_size,
        });
    }

    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(len)
        .map_err(|err| RastcpError::ProtocolError(err.to_string()))?;
    buffer.resize(len, 0);
    stream.read_exact(&mut buffer).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            RastcpError::ConnectionClosed
        } else {
            RastcpError::Io(e)
        }
    })?;

    Ok(buffer)
}

/// Payloads up to this size are joined to their length prefix before they are
/// written, so the TLS stack emits one record — one seal, one system call —
/// instead of two. A larger payload is split into ~16 KiB records whatever is
/// done here, so it is written in place and spared the copy.
const COALESCE_LIMIT: usize = 16 * 1024;

// Write a length-prefixed message - generic over any AsyncRead + AsyncWrite stream
pub async fn write_message<S>(stream: &mut S, data: &[u8]) -> Result<(), RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    write_message_with(stream, &mut Vec::new(), data).await
}

/// Writes a length-prefixed message, joining prefix and payload in `scratch`.
///
/// What a connection that sends repeatedly calls, with a buffer it keeps: the
/// buffer grows to the largest small message once — never past
/// `COALESCE_LIMIT` plus the prefix — and is then reused, where
/// [`write_message`] allocates one per call. Its contents on entry are
/// discarded and on return are unspecified.
pub async fn write_message_with<S>(
    stream: &mut S,
    scratch: &mut Vec<u8>,
    data: &[u8],
) -> Result<(), RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    // The prefix is four bytes on the wire, so a longer payload cannot be
    // framed at all; truncating its length would desynchronise the stream.
    let len = u32::try_from(data.len()).map_err(|_| RastcpError::MessageTooLarge {
        len: data.len(),
        max: u32::MAX as usize,
    })?;
    let len_bytes = len.to_le_bytes();

    if data.len() <= COALESCE_LIMIT {
        scratch.clear();
        scratch.reserve(4 + data.len());
        scratch.extend_from_slice(&len_bytes);
        scratch.extend_from_slice(data);
        stream.write_all(scratch).await?;
    } else {
        stream.write_all(&len_bytes).await?;
        stream.write_all(data).await?;
    }
    stream.flush().await?;

    Ok(())
}

/// Reads a message with both a size cap and a timeout.
///
/// What an exchange that has not authenticated anyone yet gets: the default
/// cap would let an unauthenticated caller ask the server to reserve 8 MiB per
/// connection, and no timeout would let it hold the slot by staying silent.
pub async fn read_message_limited_timeout<S>(
    stream: &mut S,
    max_message_size: usize,
    timeout: std::time::Duration,
) -> Result<Vec<u8>, RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    match tokio::time::timeout(timeout, read_message_limited(stream, max_message_size)).await {
        Ok(result) => result,
        Err(_) => Err(RastcpError::ConnectionTimeout),
    }
}

// Read a message with a timeout
pub async fn read_message_timeout<S>(
    stream: &mut S,
    timeout: std::time::Duration,
) -> Result<Vec<u8>, RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(timeout, read_message(stream)).await {
        Ok(result) => result,
        Err(_) => Err(RastcpError::ConnectionTimeout),
    }
}

// Write a message with a timeout
pub async fn write_message_timeout<S>(
    stream: &mut S,
    data: &[u8],
    timeout: std::time::Duration,
) -> Result<(), RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    match tokio::time::timeout(timeout, write_message(stream, data)).await {
        Ok(result) => result,
        Err(_) => Err(RastcpError::ConnectionTimeout),
    }
}
