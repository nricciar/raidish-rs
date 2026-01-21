use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::fs::FileSystem;

const NBD_INIT_MAGIC: u64 = 0x4e42444d41474943; // "NBDMAGIC"
const NBD_OPTS_MAGIC: u64 = 0x49484156454F5054; // "IHAVEOPT"
const NBD_REP_MAGIC: u64 = 0x3e889045565a9;
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_REPLY_MAGIC: u32 = 0x67446698;

const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

const NBD_OPT_EXPORT_NAME: u32 = 1;
const NBD_OPT_ABORT: u32 = 2;
const NBD_OPT_GO: u32 = 7;

const NBD_REP_ACK: u32 = 1;
//const NBD_REP_SERVER: u32 = 2;
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = 0x80000001;

const NBD_INFO_EXPORT: u16 = 0;

const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;

const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;

pub async fn serve_nbd(
    fs: Arc<Mutex<FileSystem>>,
    zvol_name: String,
    size: u64,
    port: u16,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    println!("NBD server listening on port {}", port);
    
    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Client connected from {}", addr);
        
        let fs = fs.clone();
        let zvol_name = zvol_name.clone();
        
        tokio::spawn(async move {
            if let Err(e) = handle_nbd_client(stream, fs, zvol_name, size).await {
                eprintln!("Client error: {}", e);
            }
        });
    }
}

pub async fn handle_nbd_client(
    mut stream: tokio::net::TcpStream,
    fs: Arc<Mutex<FileSystem>>,
    zvol_name: String,
    size: u64,
) -> std::io::Result<()> {
    // Initial handshake
    stream.write_u64(NBD_INIT_MAGIC).await?;
    stream.write_u64(NBD_OPTS_MAGIC).await?;
    stream.write_u16(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES).await?;
    
    // Read client flags
    let client_flags = stream.read_u32().await?;
    let no_zeroes = (client_flags & (NBD_FLAG_NO_ZEROES as u32)) != 0;
    
    println!("Client flags: 0x{:x}, no_zeroes: {}", client_flags, no_zeroes);
    
    // Option haggling phase
    loop {
        let opts_magic = stream.read_u64().await?;
        if opts_magic != NBD_OPTS_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Bad options magic: 0x{:x}", opts_magic)
            ));
        }
        
        let option = stream.read_u32().await?;
        let length = stream.read_u32().await?;
        
        let mut option_data = vec![0u8; length as usize];
        if length > 0 {
            stream.read_exact(&mut option_data).await?;
        }
        
        println!("Received option: {}, length: {}", option, length);
        
        match option {
            NBD_OPT_EXPORT_NAME => {
                // Old-style export name (goes straight to transmission)
                println!("Client requested export (old style)");
                
                // Send export size and flags
                stream.write_u64(size).await?;
                stream.write_u16(NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH).await?;
                
                if !no_zeroes {
                    stream.write_all(&[0u8; 124]).await?;
                }
                
                // Enter transmission phase
                return transmission_phase(stream, fs, zvol_name, size).await;
            }
            NBD_OPT_GO => {
                // New-style GO with info
                println!("Client requested GO");
                
                // Send NBD_REP_INFO with export info
                stream.write_u64(NBD_REP_MAGIC).await?;
                stream.write_u32(option).await?;
                stream.write_u32(NBD_REP_INFO).await?;
                
                // Info payload: type (u16) + data
                let info_data = {
                    let mut buf = Vec::new();
                    buf.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                    buf.extend_from_slice(&size.to_be_bytes());
                    buf.extend_from_slice(&(NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH).to_be_bytes());
                    buf
                };
                
                stream.write_u32(info_data.len() as u32).await?;
                stream.write_all(&info_data).await?;
                
                // Send NBD_REP_ACK
                stream.write_u64(NBD_REP_MAGIC).await?;
                stream.write_u32(option).await?;
                stream.write_u32(NBD_REP_ACK).await?;
                stream.write_u32(0).await?; // no additional data
                
                // Enter transmission phase
                return transmission_phase(stream, fs, zvol_name, size).await;
            }
            NBD_OPT_ABORT => {
                println!("Client aborted");
                return Ok(());
            }
            _ => {
                // Unsupported option
                println!("Unsupported option: {}", option);
                stream.write_u64(NBD_REP_MAGIC).await?;
                stream.write_u32(option).await?;
                stream.write_u32(NBD_REP_ERR_UNSUP).await?;
                stream.write_u32(0).await?;
            }
        }
    }
}

async fn transmission_phase(
    mut stream: tokio::net::TcpStream,
    fs: Arc<Mutex<FileSystem>>,
    zvol_name: String,
    size: u64,
) -> std::io::Result<()> {
    println!("Entering transmission phase, size: {}", size);
    
    loop {
        let magic = match stream.read_u32().await {
            Ok(m) => m,
            Err(e) => {
                println!("Error reading magic: {}", e);
                return Err(e);
            }
        };
        
        if magic != NBD_REQUEST_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Bad request magic: 0x{:x}", magic)
            ));
        }
        
        let _flags = stream.read_u16().await?;
        let cmd = stream.read_u16().await?;
        let handle = stream.read_u64().await?;
        let offset = stream.read_u64().await?;
        let length = stream.read_u32().await?;
        
        println!("CMD: {}, offset: {}, length: {}, handle: {}", cmd, offset, length, handle);
        
        match cmd {
            NBD_CMD_READ => {
                println!("  -> Handling READ");
                let mut buf = vec![0u8; length as usize];
                
                {
                    let mut fs = fs.lock().await;
                    let bytes_read = fs.block_read(&zvol_name, offset, &mut buf).await;
                    println!("  -> Read {} bytes from filesystem", bytes_read);
                    
                    if bytes_read != buf.len() {
                        println!("  -> WARNING: Short read: {} vs {}", bytes_read, buf.len());
                        // Fill rest with zeros
                        buf[bytes_read..].fill(0);
                    }
                } // Drop fs lock before writing to stream
                
                println!("  -> Sending READ reply");
                stream.write_u32(NBD_REPLY_MAGIC).await?;
                stream.write_u32(0).await?; // error = 0
                stream.write_u64(handle).await?;
                stream.write_all(&buf).await?;
                stream.flush().await?; // Important!
                println!("  -> READ reply sent");
            }
            NBD_CMD_WRITE => {
                println!("  -> Handling WRITE");
                let mut buf = vec![0u8; length as usize];
                stream.read_exact(&mut buf).await?;
                
                let error = {
                    let mut fs = fs.lock().await;
                    let bytes_written = fs.block_write(&zvol_name, offset, &buf).await;
                    println!("  -> Wrote {} bytes to filesystem", bytes_written);
                    
                    if bytes_written != buf.len() {
                        println!("  -> WARNING: Short write: {} vs {}", bytes_written, buf.len());
                        5 // EIO
                    } else {
                        0
                    }
                };
                
                println!("  -> Sending WRITE reply");
                stream.write_u32(NBD_REPLY_MAGIC).await?;
                stream.write_u32(error).await?;
                stream.write_u64(handle).await?;
                stream.flush().await?;
                println!("  -> WRITE reply sent");
            }
            NBD_CMD_FLUSH => {
                println!("  -> Handling FLUSH");
                {
                    let mut fs = fs.lock().await;
                    // Get the current file index extents to commit the txg
                    let file_index_extents = fs.current_file_index_extent.clone();
                    fs.dev.commit_txg(file_index_extents).await;
                }
                // Optionally call fs.sync() or similar
                stream.write_u32(NBD_REPLY_MAGIC).await?;
                stream.write_u32(0).await?;
                stream.write_u64(handle).await?;
                stream.flush().await?;
                println!("  -> FLUSH reply sent");
            }
            NBD_CMD_DISC => {
                println!("Client disconnected");
                return Ok(());
            }
            _ => {
                println!("  -> Unknown command: {}", cmd);
                stream.write_u32(NBD_REPLY_MAGIC).await?;
                stream.write_u32(22).await?; // EINVAL
                stream.write_u64(handle).await?;
                stream.flush().await?;
            }
        };
    }
}