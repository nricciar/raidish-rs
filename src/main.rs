#[macro_use]
extern crate rocket;

use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::sync::Arc;
use std::{path::Path, path::PathBuf};

mod api;
mod blkptr;
mod disk;
mod fs;
mod inode;
mod metaslab;
mod nbd;
mod nvme;
mod raidz;
mod stripe;
mod ui;
mod volume;
mod websocket;

#[cfg(test)]
#[path = "tests/util.rs"]
mod test_util;

use api::EtcdConfig;
use disk::BlockDevice;
use fs::FileSystem;
use raidz::{DATA_SHARDS, RaidZ};
use stripe::Stripe;

#[derive(Deserialize, Debug)]
struct Config {
    disks: Vec<String>,
    etcd_config: Option<EtcdConfig>,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional name to operate on
    name: Option<String>,

    #[arg(short, long, global = true, default_value = "~/.raidish.yaml")]
    config: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Map,
    Metaslab,
    Orphaned,
    /// Format the pool. Size defaults to the disks' capacity.
    Format {
        /// Override the pool size in 4 KiB blocks (e.g. for HTTP-only disks).
        #[arg(long)]
        blocks: Option<u64>,
    },
    Load,
    Nbd {
        name: String,
        /// Ignored; the size comes from the volume. Kept for compatibility.
        size: Option<u64>,
    },
    Nvme {
        name: String,
    },
    Block {
        name: String,
        size: u64,
    },
    Delete {
        path: String,
    },
    Read {
        path: String,
    },
    Inode {
        file: String,
    },
    Write {
        source: String,
        dest: String,
    },
    Mkdir {
        path: String,
    },
    Ls {
        path: String,
    },
    Listen {
        #[arg(short, long)]
        port: Option<u32>,

        id: String,
    },
}

fn load_config(path: &str) -> std::io::Result<Config> {
    // Expand tilde to home directory
    let expanded_path = if path.starts_with("~/") {
        let home = std::env::var("HOME").map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "HOME environment variable not set",
            )
        })?;
        PathBuf::from(home).join(&path[2..])
    } else {
        PathBuf::from(path)
    };

    if !expanded_path.exists() {
        eprintln!(
            "Error: Config file does not exist: {}",
            expanded_path.display()
        );
        std::process::exit(1);
    }

    let config_str = std::fs::read_to_string(&expanded_path)?;
    let config: Config = serde_yaml::from_str(&config_str).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Failed to parse config: {}", e),
        )
    })?;

    Ok(config)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let config = load_config(&cli.config)?;

    if config.disks.len() != 5 {
        eprintln!("Error: 5 disks are required");
        std::process::exit(1);
    }

    let disk_refs: Vec<&str> = config.disks.iter().map(|s| s.as_str()).collect();
    let disk_array: [&str; 5] = disk_refs
        .as_slice()
        .try_into()
        .expect("Expected exactly 5 disks");
    let stripe = Stripe::new(disk_array, DATA_SHARDS).await;
    let raid = RaidZ::new(stripe).await;

    match cli.command {
        Commands::Nbd { name, size: _ } => {
            let fs = FileSystem::load(raid).await.unwrap();
            nbd::serve_nbd(Arc::new(fs), name, 10809).await.unwrap();
        }
        Commands::Nvme { name } => {
            let fs = FileSystem::load(raid).await.unwrap();
            let server = nvme::NvmeTcpServer::new(Arc::new(fs), name, 4420)
                .await
                .unwrap();
            server.run().await.unwrap();
        }
        Commands::Block { name, size } => {
            let fs = FileSystem::load(raid).await.unwrap();
            fs.create_block(&name, size, volume::DEFAULT_VOLBLOCKSIZE)
                .await
                .unwrap();
        }
        Commands::Map => {
            let fs = FileSystem::load(raid).await.unwrap();
            fs.display_block_map().await;
        }
        Commands::Metaslab => {
            let fs = FileSystem::load(raid).await.unwrap();
            fs.display_metaslab_info().await;
        }
        Commands::Orphaned => {
            let fs = FileSystem::load(raid).await.unwrap();
            let orphans = fs.find_orphaned_blocks().await.unwrap();
            println!("orphans: {:?}", orphans);
        }
        Commands::Read { path } => {
            let fs = FileSystem::load(raid).await.unwrap();
            let data = fs.read_file(&Path::new(&path)).await.unwrap();
            std::fs::write("output.png", data)?;
        }
        Commands::Inode { file } => {
            let fs = FileSystem::load(raid).await.unwrap();
            let inode_ref = fs.file_index.read().await.files.get(&file).unwrap().clone();
            let inode = fs.read_inode(&inode_ref).await.unwrap();
            println!("inode: {:?}", inode);
        }
        Commands::Write { source, dest } => {
            let fs = FileSystem::load(raid).await.unwrap();
            let file = Path::new(&source);
            let data = std::fs::read(file).unwrap();
            fs.write_file(&Path::new(&dest), &data).await.unwrap();
            fs.sync().await.unwrap();
        }
        Commands::Mkdir { path } => {
            let fs = FileSystem::load(raid).await.unwrap();
            fs.mkdir(&Path::new(&path)).await.unwrap();
            fs.sync().await.unwrap();
        }
        Commands::Ls { path } => {
            let fs = FileSystem::load(raid).await.unwrap();
            let ls = fs.ls(&Path::new(&path)).await.unwrap();
            println!("ls: {:?}", &ls);
        }
        Commands::Delete { path } => {
            let fs = FileSystem::load(raid).await.unwrap();
            fs.delete(&Path::new(&path)).await.unwrap();
            fs.sync().await.unwrap();
        }
        Commands::Format { blocks } => {
            let blocks = match blocks {
                Some(b) => b,
                None => raid.capacity_blocks().await.unwrap_or_else(|e| {
                    eprintln!("cannot determine disk capacity ({}); pass --blocks", e);
                    std::process::exit(1);
                }),
            };
            let fs = FileSystem::format(raid, blocks).await.unwrap();
            let sb = fs.superblock().await.unwrap();
            println!(
                "formatted {} blocks ({:.1} GiB usable): {} metaslabs of {} blocks",
                blocks,
                (sb.metaslab_count * sb.metaslab_size_blocks) as f64 * 4096.0 / (1u64 << 30) as f64,
                sb.metaslab_count,
                sb.metaslab_size_blocks
            );
        }
        Commands::Load => {
            let fs = FileSystem::load(raid).await.unwrap();
            println!("index: {:?}", fs.file_index);
        }
        Commands::Listen { port, id } => {
            let port = port.unwrap_or_else(|| 8881);
            raid.listen(port, config.etcd_config, &id).await;
        }
    }
    Ok(())
}
