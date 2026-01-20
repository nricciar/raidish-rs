#![feature(async_drop)]
#[macro_use] extern crate rocket;

use clap::{Parser, Subcommand};
use std::{io::{self}, path::Path};
use std::sync::Arc;
use tokio::sync::Mutex;

mod api;
mod fs;
mod disk;
mod raidz;
mod ui;
mod nbd;

use fs::{FileSystem};
use raidz::{RaidZ};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional name to operate on
    name: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Map,
    Orphaned,
    Format,
    Load,
    Nbd {
        name: String,
        size: u64
    },
    Block {
        name: String,
        size: u64
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

#[tokio::main]
async fn main() -> io::Result<()> {
    let raid = RaidZ::new(["disks/disk1","disks/disk2","disks/disk3","disks/disk4","disks/disk5"]).await;
    /*let raid = 
        RaidZ::new(
            ["http://127.0.0.1:8881/api/v1/disks/0",
             "http://127.0.0.1:8881/api/v1/disks/1",
             "http://127.0.0.1:8881/api/v1/disks/2",
             "http://127.0.0.1:8881/api/v1/disks/3",
             "http://127.0.0.1:8881/api/v1/disks/4"]
        ).await;*/

    let cli = Cli::parse();

    match cli.command {
        Commands::Nbd { name, size } => {
            let fs = FileSystem::load(raid).await;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:10809").await?;
            let (stream, _) = listener.accept().await?;
            nbd::handle_nbd_client(stream, Arc::new(Mutex::new(fs)), name, size).await.unwrap();
        },
        Commands::Block { name, size } => {
            let mut fs = FileSystem::load(raid).await;
            fs.create_block(&name, size, crate::disk::BLOCK_SIZE as u32).await;
        },
        Commands::Map => {
            let fs = FileSystem::load(raid).await;
            fs.display_block_map();
            //fs.display_metaslab_info();
        },
        Commands::Orphaned => { 
            let fs = FileSystem::load(raid).await;
            let orphans = fs.find_orphaned_blocks();
            println!("orphans: {:?}", orphans);
        },
        Commands::Read{ file } => {
            let mut fs = FileSystem::load(raid).await;
            let data = fs.read_file(&file).await;
            std::fs::write("output.png", data)?;
        },
        Commands::Write { file } => {
            let mut fs = FileSystem::load(raid).await;
            let file = Path::new(&file);
            let data = std::fs::read(file).unwrap();
            fs.write_file(file.file_name().unwrap().to_str().unwrap(), &data).await;
        },
        Commands::Format => {
            let fs = FileSystem::format(raid, 500_000).await;
            println!("index: {:?}", fs.file_index);
        },
        Commands::Load => {
            let fs = FileSystem::load(raid).await;
            println!("index: {:?}", fs.file_index);
        },
        Commands::Listen { port } => {
            let port = port.unwrap_or_else(|| 8881);
            raid.listen(port).await;
        }
    }
    Ok(())
}
