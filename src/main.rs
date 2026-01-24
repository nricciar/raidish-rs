#![feature(async_drop)]
#[macro_use] extern crate rocket;

use clap::{Parser, Subcommand};
use std::{io::{self}, path::PathBuf, path::Path};
use std::sync::Arc;
use tokio::sync::Mutex;
use serde::Deserialize;

mod api;
mod fs;
mod disk;
mod raidz;
mod ui;
mod nbd;
mod nvme;
mod vblock;

use fs::{FileSystem};
use raidz::{RaidZ};

#[derive(Deserialize, Debug)]
struct Config {
    disks: Vec<String>,
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
    Format,
    Load,
    Nbd {
        name: String,
        size: u64
    },
    Nvme {
        name: String
    },
    Block {
        name: String,
        size: u64
    },
    Delete {
        file: String
    },
    Read {
        file: String
    },
    Write {
        file: String
    },
    Listen {
        #[arg(short, long)]
        port: Option<u32>,
    }
}

fn load_config(path: &str) -> io::Result<Config> {
    // Expand tilde to home directory
    let expanded_path = if path.starts_with("~/") {
        let home = std::env::var("HOME")
            .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "HOME environment variable not set"))?;
        PathBuf::from(home).join(&path[2..])
    } else {
        PathBuf::from(path)
    };

    if !expanded_path.exists() {
        eprintln!("Error: Config file does not exist: {}", expanded_path.display());
        std::process::exit(1);
    }

    let config_str = std::fs::read_to_string(&expanded_path)?;
    let config: Config = serde_yaml::from_str(&config_str)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Failed to parse config: {}", e)))?;

    Ok(config)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let cli = Cli::parse();
    let config = load_config(&cli.config)?;

    if config.disks.len() != 5 {
        eprintln!("Error: 5 disks are required");
        std::process::exit(1);
    }

    let disk_refs: Vec<&str> = config.disks.iter().map(|s| s.as_str()).collect();
    let disk_array: [&str; 5] = disk_refs.as_slice().try_into()
        .expect("Expected exactly 5 disks");
    let raid = RaidZ::new(disk_array).await;

    match cli.command {
        Commands::Nbd { name, size } => {
            let fs = FileSystem::load(raid).await.unwrap();
            nbd::serve_nbd(Arc::new(Mutex::new(fs)), name, size, 10809).await.unwrap();
        },
        Commands::Nvme { name } => {
            let fs = FileSystem::load(raid).await.unwrap();
            let server = nvme::NvmeTcpServer::new(fs, name, 4420);
            server.run().await.unwrap();
        },
        Commands::Block { name, size } => {
            let mut fs = FileSystem::load(raid).await.unwrap();
            fs.create_block(&name, size, crate::disk::BLOCK_SIZE as u32).await.unwrap();
        },
        Commands::Map => {
            let fs = FileSystem::load(raid).await.unwrap();
            fs.display_block_map();
        },
        Commands::Metaslab => {
            let fs = FileSystem::load(raid).await.unwrap();
            fs.display_metaslab_info();
        },
        Commands::Orphaned => { 
            let fs = FileSystem::load(raid).await.unwrap();
            let orphans = fs.find_orphaned_blocks();
            println!("orphans: {:?}", orphans);
        },
        Commands::Read{ file } => {
            let mut fs = FileSystem::load(raid).await.unwrap();
            let data = fs.read_file(&file).await.unwrap();
            std::fs::write("output.png", data)?;
        },
        Commands::Write { file } => {
            let mut fs = FileSystem::load(raid).await.unwrap();
            let file = Path::new(&file);
            let data = std::fs::read(file).unwrap();
            fs.write_file(file.file_name().unwrap().to_str().unwrap(), &data).await.unwrap();
        },
        Commands::Delete { file } => {
            let mut fs = FileSystem::load(raid).await.unwrap();
            fs.delete(&file).await.unwrap();
        },
        Commands::Format => {
            let fs = FileSystem::format(raid, 500_000).await.unwrap();
            println!("index: {:?}", fs.file_index);
        },
        Commands::Load => {
            let fs = FileSystem::load(raid).await.unwrap();
            println!("index: {:?}", fs.file_index);
        },
        Commands::Listen { port } => {
            let port = port.unwrap_or_else(|| 8881);
            raid.listen(port).await;
        }
    }
    Ok(())
}
