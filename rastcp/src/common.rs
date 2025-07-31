use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use crate::error::RastcpError;

// Read a length-prefixed message - generic over any AsyncRead + AsyncWrite stream
pub async fn read_message<S>(stream: &mut S) -> Result<Vec<u8>, RastcpError> 
where
    S: AsyncRead + AsyncWrite + Unpin,
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
    
    let len = u32::from_be_bytes(len_bytes) as usize;
    let mut buffer = vec![0u8; len];
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
    S: AsyncRead + AsyncWrite + Unpin,
{
    let len = data.len() as u32;
    let len_bytes = len.to_le_bytes();
    
    stream.write_all(&len_bytes).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    
    Ok(())
}

// Read a message with a timeout
pub async fn read_message_timeout<S>(
    stream: &mut S, 
    timeout: std::time::Duration
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
    timeout: std::time::Duration
) -> Result<(), RastcpError> 
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(timeout, write_message(stream, data)).await {
        Ok(result) => result,
        Err(_) => Err(RastcpError::ConnectionTimeout),
    }
}
