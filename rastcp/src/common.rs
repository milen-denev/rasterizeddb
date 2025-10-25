
use crate::error::RastcpError;
use compio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use compio::time::timeout;


// Read a length-prefixed message - generic over any AsyncRead + AsyncWrite stream
pub async fn read_message<S>(stream: &mut S) -> Result<Vec<u8>, RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Read little-endian length prefix using compio helper
    let len = stream.read_u32_le().await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            RastcpError::ConnectionClosed
        } else {
            RastcpError::Io(e)
        }
    })? as usize;
    let buffer = vec![0u8; len];
    // compio read_exact consumes and returns the buffer
    let buffer = match stream.read_exact(buffer).await {
        compio::buf::BufResult(Ok(()), buf) => buf,
        compio::buf::BufResult(Err(e), _buf) => {
            return Err(if e.kind() == std::io::ErrorKind::UnexpectedEof {
                RastcpError::ConnectionClosed
            } else {
                RastcpError::Io(e)
            });
        }
    };

    Ok(buffer)
}


// Write a length-prefixed message - generic over any AsyncRead + AsyncWrite stream
pub async fn write_message<S>(stream: &mut S, data: &[u8]) -> Result<(), RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let len = data.len() as u32;
    let len_bytes = len.to_le_bytes();

    match stream.write_all(len_bytes).await {
        compio::buf::BufResult(Ok(()), _) => {}
        compio::buf::BufResult(Err(e), _) => return Err(RastcpError::Io(e)),
    }
    // write data using owned buffer
    let owned = Vec::from(data);
    match stream.write_all(owned).await {
        compio::buf::BufResult(Ok(()), _) => {}
        compio::buf::BufResult(Err(e), _) => return Err(RastcpError::Io(e)),
    }

    Ok(())
}


// Read a message with a timeout
pub async fn read_message_timeout<S>(
    stream: &mut S,
    dur: std::time::Duration,
) -> Result<Vec<u8>, RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match timeout(dur, read_message(stream)).await {
        Ok(result) => result,
        Err(_) => Err(RastcpError::ConnectionTimeout),
    }
}

// Write a message with a timeout
pub async fn write_message_timeout<S>(
    stream: &mut S,
    data: &[u8],
    dur: std::time::Duration,
) -> Result<(), RastcpError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match timeout(dur, write_message(stream, data)).await {
        Ok(result) => result,
        Err(_) => Err(RastcpError::ConnectionTimeout),
    }
}
