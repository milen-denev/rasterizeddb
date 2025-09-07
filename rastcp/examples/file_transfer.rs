use rastcp::{client::TcpClientBuilder, error::RastcpError, server::TcpServerBuilder};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

#[derive(Debug, Serialize, Deserialize, Clone)]
enum Command {
    ListFiles,
    DownloadFile { name: String },
    UploadFile { name: String, data: Vec<u8> },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum Response {
    FileList { files: Vec<String> },
    FileData { data: Vec<u8> },
    Error { message: String },
    Success { message: String },
}

struct FileServer {
    directory: Arc<Mutex<String>>,
}

impl FileServer {
    fn new(directory: &str) -> Self {
        Self {
            directory: Arc::new(Mutex::new(directory.to_string())),
        }
    }

    async fn handle_command(&self, data: Vec<u8>) -> Vec<u8> {
        // Try to deserialize the command
        match bincode::deserialize::<Command>(&data) {
            Ok(cmd) => match cmd {
                Command::ListFiles => self.list_files().await,
                Command::DownloadFile { name } => self.download_file(name).await,
                Command::UploadFile { name, data } => self.upload_file(name, data).await,
            },
            Err(_) => {
                let response = Response::Error {
                    message: "Failed to parse command".to_string(),
                };
                bincode::serialize(&response).unwrap()
            }
        }
    }

    async fn list_files(&self) -> Vec<u8> {
        let dir = self.directory.lock().await;

        // Read directory
        match tokio::fs::read_dir(&*dir).await {
            Ok(mut entries) => {
                let mut files = Vec::new();

                // Collect file names
                while let Ok(Some(entry)) = entries.next_entry().await {
                    if let Ok(metadata) = entry.metadata().await {
                        if metadata.is_file() {
                            if let Some(name) = entry.file_name().to_str() {
                                files.push(name.to_string());
                            }
                        }
                    }
                }

                let response = Response::FileList { files };
                bincode::serialize(&response).unwrap()
            }
            Err(e) => {
                let response = Response::Error {
                    message: format!("Failed to list files: {}", e),
                };
                bincode::serialize(&response).unwrap()
            }
        }
    }

    async fn download_file(&self, name: String) -> Vec<u8> {
        let dir = self.directory.lock().await;
        let path = Path::new(&*dir).join(&name);

        // Make sure we don't access anything outside the directory
        if !path.starts_with(&*dir) {
            let response = Response::Error {
                message: "Invalid file path".to_string(),
            };
            return bincode::serialize(&response).unwrap();
        }

        // Read the file
        match File::open(path).await {
            Ok(mut file) => {
                let mut data = Vec::new();
                if let Err(e) = file.read_to_end(&mut data).await {
                    let response = Response::Error {
                        message: format!("Failed to read file: {}", e),
                    };
                    return bincode::serialize(&response).unwrap();
                }

                let response = Response::FileData { data };
                bincode::serialize(&response).unwrap()
            }
            Err(e) => {
                let response = Response::Error {
                    message: format!("Failed to open file: {}", e),
                };
                bincode::serialize(&response).unwrap()
            }
        }
    }

    async fn upload_file(&self, name: String, data: Vec<u8>) -> Vec<u8> {
        let dir = self.directory.lock().await;
        let path = Path::new(&*dir).join(&name);

        // Make sure we don't write anything outside the directory
        if !path.starts_with(&*dir) {
            let response = Response::Error {
                message: "Invalid file path".to_string(),
            };
            return bincode::serialize(&response).unwrap();
        }

        // Write the file
        match File::create(&path).await {
            Ok(mut file) => {
                if let Err(e) = file.write_all(&data).await {
                    let response = Response::Error {
                        message: format!("Failed to write file: {}", e),
                    };
                    return bincode::serialize(&response).unwrap();
                }

                let response = Response::Success {
                    message: format!("File '{}' uploaded successfully", name),
                };
                bincode::serialize(&response).unwrap()
            }
            Err(e) => {
                let response = Response::Error {
                    message: format!("Failed to create file: {}", e),
                };
                bincode::serialize(&response).unwrap()
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), RastcpError> {
    env_logger::init();

    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        println!("Usage:");
        println!("  Server mode: {} server <directory>", args[0]);
        println!("  Client mode: {} client <command> [args...]", args[0]);
        println!("    Commands:");
        println!("      list");
        println!("      download <filename>");
        println!("      upload <filename>");
        return Ok(());
    }

    match args[1].as_str() {
        "server" => {
            if args.len() < 3 {
                println!("Error: Directory path required for server mode");
                return Ok(());
            }

            run_server(&args[2]).await?;
        }
        "client" => {
            if args.len() < 3 {
                println!("Error: Command required for client mode");
                return Ok(());
            }

            run_client(&args[2..]).await?;
        }
        _ => {
            println!("Error: Unknown mode '{}'", args[1]);
        }
    }

    Ok(())
}

async fn run_server(directory: &str) -> Result<(), RastcpError> {
    println!("Starting file server on directory: {}", directory);

    let file_server = FileServer::new(directory);

    let server = TcpServerBuilder::new("127.0.0.1", 8877).build().await?;

    println!("Server started, awaiting connections...");

    server
        .run(move |data| {
            let file_server = file_server.clone();
            async move { file_server.handle_command(data).await }
        })
        .await?;

    Ok(())
}

async fn run_client(args: &[String]) -> Result<(), RastcpError> {
    let mut client = TcpClientBuilder::new("127.0.0.1:8877")
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .await?;

    let cmd = match args[0].as_str() {
        "list" => Command::ListFiles,
        "download" => {
            if args.len() < 2 {
                println!("Error: Filename required for download command");
                return Ok(());
            }
            Command::DownloadFile {
                name: args[1].clone(),
            }
        }
        "upload" => {
            if args.len() < 2 {
                println!("Error: Filename required for upload command");
                return Ok(());
            }

            // Read the file to upload
            let path = Path::new(&args[1]);
            let file_name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(&args[1]);

            let mut file = match File::open(path).await {
                Ok(f) => f,
                Err(e) => {
                    println!("Error opening file '{}': {}", args[1], e);
                    return Ok(());
                }
            };

            let mut data = Vec::new();
            if let Err(e) = file.read_to_end(&mut data).await {
                println!("Error reading file: {}", e);
                return Ok(());
            }

            println!("Uploading {} ({} bytes)", file_name, data.len());
            Command::UploadFile {
                name: file_name.to_string(),
                data,
            }
        }
        _ => {
            println!("Error: Unknown command '{}'", args[0]);
            return Ok(());
        }
    };

    // Serialize and send the command
    let serialized_cmd = match bincode::serialize(&cmd) {
        Ok(data) => data,
        Err(e) => {
            println!("Error serializing command: {}", e);
            return Ok(());
        }
    };

    let response_data = client.send(serialized_cmd).await?;

    // Process the response
    let response: Response = match bincode::deserialize(&response_data) {
        Ok(r) => r,
        Err(e) => {
            println!("Error deserializing response: {}", e);
            return Ok(());
        }
    };

    match response {
        Response::FileList { files } => {
            println!("Available files:");
            for file in files {
                println!("  - {}", file);
            }
        }
        Response::FileData { data } => {
            match &cmd {
                Command::DownloadFile { name } => {
                    // Save the file locally
                    let path = Path::new(name);
                    let mut file = match File::create(path).await {
                        Ok(f) => f,
                        Err(e) => {
                            println!("Error creating file: {}", e);
                            return Ok(());
                        }
                    };

                    if let Err(e) = file.write_all(&data).await {
                        println!("Error writing file: {}", e);
                        return Ok(());
                    }

                    println!("Downloaded {} ({} bytes)", name, data.len());
                }
                _ => println!("Unexpected file data response"),
            }
        }
        Response::Error { message } => {
            println!("Error from server: {}", message);
        }
        Response::Success { message } => {
            println!("Success: {}", message);
        }
    }

    Ok(())
}

impl Clone for FileServer {
    fn clone(&self) -> Self {
        Self {
            directory: self.directory.clone(),
        }
    }
}
