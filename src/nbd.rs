//! NBD server for copy-on-write volumes.
//!
//! Requests are handled concurrently and replied to as they finish (NBD
//! matches replies by handle), so the kernel can keep its queue full. Writes
//! complete once they're in the volume's dirty records; FLUSH commits a TXG,
//! which makes every completed write durable. A background task also commits
//! every few seconds while there is dirty data.

use crate::disk::BlockDevice;
use crate::fs::FileSystem;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

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
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = 0x80000001;

const NBD_INFO_EXPORT: u16 = 0;

const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_TRIM: u16 = 4;
const NBD_CMD_WRITE_ZEROES: u16 = 6;

const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;
const TRANSMISSION_FLAGS: u16 =
    NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM | NBD_FLAG_SEND_WRITE_ZEROES;

const EIO: u32 = 5;
const EINVAL: u32 = 22;
const ENOSPC: u32 = 28;

/// NBD error for a filesystem error: out of space is reported as such.
fn errno(e: &crate::fs::FileSystemError) -> u32 {
    match e {
        crate::fs::FileSystemError::UnableToAllocateBlocks(_) => ENOSPC,
        _ => EIO,
    }
}

/// How often dirty volume data is committed without an explicit FLUSH.
pub const SYNC_INTERVAL: Duration = Duration::from_secs(5);
/// Largest READ/WRITE payload we accept (the kernel caps NBD I/O at 32 MiB).
const MAX_REQUEST_BYTES: u32 = 32 << 20;

pub async fn serve_nbd<D>(
    fs: Arc<FileSystem<D>>,
    zvol_name: String,
    port: u16,
) -> std::io::Result<()>
where
    D: BlockDevice + Send + Sync + 'static,
{
    let size = fs
        .open_volume(&zvol_name)
        .await
        .map_err(|e| std::io::Error::other(format!("volume '{}': {}", zvol_name, e)))?
        .size_bytes;

    // Background TXG sync.
    let bg = fs.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SYNC_INTERVAL);
        loop {
            tick.tick().await;
            if bg.has_dirty_volumes()
                && let Err(e) = bg.sync().await
            {
                eprintln!("nbd: background sync failed: {}", e);
            }
        }
    });

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).await?;
    println!(
        "NBD server listening on port {} (volume '{}', {} bytes)",
        port, zvol_name, size
    );

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

pub async fn handle_nbd_client<D>(
    mut stream: tokio::net::TcpStream,
    fs: Arc<FileSystem<D>>,
    zvol_name: String,
    size: u64,
) -> std::io::Result<()>
where
    D: BlockDevice + Send + Sync + 'static,
{
    stream.set_nodelay(true)?;

    // Initial handshake
    stream.write_u64(NBD_INIT_MAGIC).await?;
    stream.write_u64(NBD_OPTS_MAGIC).await?;
    stream
        .write_u16(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES)
        .await?;

    let client_flags = stream.read_u32().await?;
    let no_zeroes = (client_flags & (NBD_FLAG_NO_ZEROES as u32)) != 0;

    // Option haggling phase
    loop {
        let opts_magic = stream.read_u64().await?;
        if opts_magic != NBD_OPTS_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Bad options magic: 0x{:x}", opts_magic),
            ));
        }

        let option = stream.read_u32().await?;
        let length = stream.read_u32().await?;
        let mut option_data = vec![0u8; length as usize];
        if length > 0 {
            stream.read_exact(&mut option_data).await?;
        }

        match option {
            NBD_OPT_EXPORT_NAME => {
                stream.write_u64(size).await?;
                stream.write_u16(TRANSMISSION_FLAGS).await?;
                if !no_zeroes {
                    stream.write_all(&[0u8; 124]).await?;
                }
                return transmission_phase(stream, fs, zvol_name).await;
            }
            NBD_OPT_GO => {
                let mut info = Vec::new();
                info.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                info.extend_from_slice(&size.to_be_bytes());
                info.extend_from_slice(&TRANSMISSION_FLAGS.to_be_bytes());

                stream.write_u64(NBD_REP_MAGIC).await?;
                stream.write_u32(option).await?;
                stream.write_u32(NBD_REP_INFO).await?;
                stream.write_u32(info.len() as u32).await?;
                stream.write_all(&info).await?;

                stream.write_u64(NBD_REP_MAGIC).await?;
                stream.write_u32(option).await?;
                stream.write_u32(NBD_REP_ACK).await?;
                stream.write_u32(0).await?;

                return transmission_phase(stream, fs, zvol_name).await;
            }
            NBD_OPT_ABORT => return Ok(()),
            _ => {
                stream.write_u64(NBD_REP_MAGIC).await?;
                stream.write_u32(option).await?;
                stream.write_u32(NBD_REP_ERR_UNSUP).await?;
                stream.write_u32(0).await?;
            }
        }
    }
}

/// Send one simple reply (header plus optional read payload) atomically.
async fn reply(
    writer: &Mutex<OwnedWriteHalf>,
    handle: u64,
    error: u32,
    data: &[u8],
) -> std::io::Result<()> {
    let mut msg = Vec::with_capacity(16 + data.len());
    msg.extend_from_slice(&NBD_REPLY_MAGIC.to_be_bytes());
    msg.extend_from_slice(&error.to_be_bytes());
    msg.extend_from_slice(&handle.to_be_bytes());
    msg.extend_from_slice(data);
    writer.lock().await.write_all(&msg).await
}

async fn transmission_phase<D>(
    stream: tokio::net::TcpStream,
    fs: Arc<FileSystem<D>>,
    zvol_name: String,
) -> std::io::Result<()>
where
    D: BlockDevice + Send + Sync + 'static,
{
    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));
    let name = Arc::new(zvol_name);
    let mut tasks = JoinSet::new();

    loop {
        let magic = reader.read_u32().await?;
        if magic != NBD_REQUEST_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Bad request magic: 0x{:x}", magic),
            ));
        }
        let _flags = reader.read_u16().await?;
        let cmd = reader.read_u16().await?;
        let handle = reader.read_u64().await?;
        let offset = reader.read_u64().await?;
        let length = reader.read_u32().await?;

        // Reap finished tasks so the set doesn't grow without bound.
        while let Some(res) = tasks.try_join_next() {
            res.map_err(std::io::Error::other)??;
        }

        // Only READ/WRITE carry a payload we have to buffer; TRIM and
        // WRITE_ZEROES can legitimately cover huge ranges (the kernel issues
        // discards up to ~4 GiB).
        if length > MAX_REQUEST_BYTES && matches!(cmd, NBD_CMD_READ | NBD_CMD_WRITE) {
            eprintln!(
                "nbd: rejecting cmd {} of {} bytes (max {})",
                cmd, length, MAX_REQUEST_BYTES
            );
            if cmd == NBD_CMD_WRITE {
                // Can't skip the payload safely; drop the connection.
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("request of {} bytes too large", length),
                ));
            }
            reply(&writer, handle, EINVAL, &[]).await?;
            continue;
        }

        let (fs, writer, name) = (fs.clone(), writer.clone(), name.clone());
        match cmd {
            NBD_CMD_READ => {
                tasks.spawn(async move {
                    let mut buf = vec![0u8; length as usize];
                    match fs.block_read(&name, offset, &mut buf).await {
                        Ok(n) => {
                            buf[n..].fill(0);
                            reply(&writer, handle, 0, &buf).await
                        }
                        Err(e) => {
                            eprintln!("nbd: read {}+{}: {}", offset, length, e);
                            reply(&writer, handle, EIO, &[]).await
                        }
                    }
                });
            }
            NBD_CMD_WRITE => {
                let mut buf = vec![0u8; length as usize];
                reader.read_exact(&mut buf).await?;
                tasks.spawn(async move {
                    let error = match fs.block_write(&name, offset, &buf).await {
                        Ok(n) if n == buf.len() => 0,
                        Ok(_) => EIO,
                        Err(e) => {
                            eprintln!("nbd: write {}+{}: {}", offset, length, e);
                            errno(&e)
                        }
                    };
                    reply(&writer, handle, error, &[]).await
                });
            }
            NBD_CMD_TRIM | NBD_CMD_WRITE_ZEROES => {
                tasks.spawn(async move {
                    let error = match fs.block_zero(&name, offset, length as usize).await {
                        Ok(_) => 0,
                        Err(e) => {
                            eprintln!("nbd: zero {}+{}: {}", offset, length, e);
                            errno(&e)
                        }
                    };
                    reply(&writer, handle, error, &[]).await
                });
            }
            NBD_CMD_FLUSH => {
                tasks.spawn(async move {
                    let error = match fs.sync().await {
                        Ok(()) => 0,
                        Err(e) => {
                            eprintln!("nbd: flush: {}", e);
                            errno(&e)
                        }
                    };
                    reply(&writer, handle, error, &[]).await
                });
            }
            NBD_CMD_DISC => {
                while let Some(res) = tasks.join_next().await {
                    res.map_err(std::io::Error::other)??;
                }
                if let Err(e) = fs.sync().await {
                    eprintln!("nbd: sync on disconnect: {}", e);
                }
                println!("Client disconnected");
                return Ok(());
            }
            _ => {
                eprintln!("nbd: unsupported command {}", cmd);
                reply(&writer, handle, EINVAL, &[]).await?;
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/nbd_tests.rs"]
mod tests;
