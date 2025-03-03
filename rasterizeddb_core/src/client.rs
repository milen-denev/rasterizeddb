use std::io;
use byteorder::{ByteOrder, LittleEndian};
use rastcp::client::{TcpClient, TcpClientBuilder};

use crate::core::database::QueryExecutionResult;
use crate::core::row::Row;
use crate::SERVER_PORT;

/// Client for connecting to RasterizedDB
pub struct DbClient {
    client: TcpClient,
    connected: bool,
}

impl DbClient {
    /// Create a new database client connecting to the specified address
    pub async fn new(addr: Option<&str>) -> io::Result<Self> {
        let address = addr.unwrap_or("127.0.0.1");
        let server_address = format!("{}:{}", address, SERVER_PORT);

        // Build TCP client with default settings
        let mut client = TcpClientBuilder::new(server_address.as_str())
            .verify_certificate(false)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;

        client.connect().await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;
            
        Ok(DbClient {
            client,
            connected: true,
        })
    }

    /// Connect to the database server
    pub async fn connect(&mut self) -> io::Result<()> {
        if !self.connected {
            self.client.connect()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;
            self.connected = true;
        }
        Ok(())
    }

    /// Close the connection to the database server
    pub async fn close(&mut self) -> io::Result<()> {
        if self.connected {
            self.client.close()
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e.to_string()))?;
            self.connected = false;
        }
        Ok(())
    }

    /// Execute an RQL query and return the result
    pub async fn execute_query(&mut self, query: &str) -> io::Result<QueryExecutionResult> {
        // Make sure we're connected
        self.connect().await?;
        
        // Send the query
        let response = self.client.send(query.as_bytes().to_vec())
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e.to_string()))?;
        
        // Parse the response
        if response.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Empty response"));
        }

        // Get the result type from the first byte
        match response[0] {
            0 => Ok(QueryExecutionResult::Ok),
            1 => {
                if response.len() < 9 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid rows affected response"));
                }
                let rows_affected = LittleEndian::read_u64(&response[1..9]);
                Ok(QueryExecutionResult::RowsAffected(rows_affected))
            },
            2 => {
                // Extract the serialized rows data (everything after the marker byte)
                let rows_data = response[1..].to_vec();
                Ok(QueryExecutionResult::RowsResult(Box::new(rows_data)))
            },
            3 => {
                // Error message is a UTF-8 string after the marker byte
                let error_message = String::from_utf8(response[1..].to_vec())
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid error message"))?;
                Ok(QueryExecutionResult::Error(error_message))
            },
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid result type marker")),
        }
    }

    /// Helper method to extract rows from a QueryExecutionResult
    pub fn extract_rows(result: QueryExecutionResult) -> io::Result<Option<Vec<Row>>> {
        match result {
            QueryExecutionResult::RowsResult(data) => {
                Row::deserialize_rows(&data)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
            },
            QueryExecutionResult::Error(msg) => {
                Err(io::Error::new(io::ErrorKind::Other, msg))
            },
            _ => Ok(None)
        }
    }

    /// Check if the client is connected
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Force reconnection to the server
    pub async fn reconnect(&mut self) -> io::Result<()> {
        self.close().await?;
        self.connect().await
    }
}
