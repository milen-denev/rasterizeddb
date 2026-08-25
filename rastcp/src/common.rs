use crate::error::RastcpError;
use opool::{Pool, PoolAllocator};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024;

/// Allocator for pooled coalesce buffers used by `write_message`.
struct CoalesceBufAllocator;
impl PoolAllocator<Vec<u8>> for CoalesceBufAllocator {
    #[inline]
    fn allocate(&self) -> Vec<u8> {
        // Most coalesced messages are small; start with a sensible capacity.
        Vec::with_capacity(4 * 1024)
    }
    #[inline]
    fn reset(&self, obj: &mut Vec<u8>) {
        obj.clear();
    }
}

/// Process-wide pool of small write coalesce buffers. Capped so we don't
/// retain unbounded memory; pulls fall back to a fresh allocation when
/// the pool is empty.
static COALESCE_POOL: once_cell::sync::Lazy<Pool<CoalesceBufAllocator, Vec<u8>>> =
    once_cell::sync::Lazy::new(|| Pool::new(64, CoalesceBufAllocator));

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

// Write a length-prefixed message - generic over any AsyncRead + AsyncWrite stream
pub async fn write_message<S>(stream: &mut S, data: &[u8]) -> Result<(), RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let len = data.len() as u32;
    let len_bytes = len.to_le_bytes();

    // Coalesce the 4-byte length prefix and the payload into a single
    // write so the TLS stack emits a single record (one AEAD seal + one
    // syscall) instead of two. For large payloads we skip the copy and
    // fall back to two writes — TLS will chunk into ~16 KiB records
    // either way and the extra allocation/memcpy isn't worth it.
    const COALESCE_LIMIT: usize = 16 * 1024;
    if data.len() <= COALESCE_LIMIT {
        // Pool the temp buffer to avoid one allocation per RPC response on
        // the warm cache path. Guard returns the buffer on drop.
        let mut buf = COALESCE_POOL.get();
        buf.reserve(4 + data.len());
        buf.extend_from_slice(&len_bytes);
        buf.extend_from_slice(data);
        stream.write_all(&buf).await?;
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
