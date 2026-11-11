#[macro_use]
extern crate rocket;

use clap::{Parser, Subcommand};
use std::sync::Arc;
use std::{path::Path, path::PathBuf};

mod api;
mod blkptr;
mod config;
mod disk;
mod disks;
mod fs;
mod inode;
mod metaslab;
mod mirror;
mod nbd;
mod nvme;
mod raidz;
mod scan;
mod stripe;
mod ui;
mod vdev;
mod volume;
mod websocket;

#[cfg(test)]
#[path = "tests/util.rs"]
mod test_util;

use config::Config;
use disk::BlockDevice;
use disks::DiskSet;
use fs::FileSystem;
use vdev::Topology;

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
    /// Verify every block (data and parity) and the pool labels; repair
    /// what's wrong and bring degraded disks back in sync.
    Scrub {
        /// Only report; open the pool read-only and change nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Rebuild what degraded disks missed (only blocks written since).
    Resilver,
    /// Show disk health: online, label, degraded since which TXG.
    Status,
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
    Config::parse(&config_str).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// How often a serving process re-checks disk health (it also reacts to
/// reconnects immediately).
const HEALTH_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

async fn print_status<D: BlockDevice>(fs: &FileSystem<D>) {
    let degraded = fs.degraded_disks();
    if let Some(u) = fs.uberblock().await {
        println!("pool at TXG {}", u.txg);
    }
    let Some(pool) = fs.dev.pool() else {
        return;
    };
    let tolerance = pool.topology.fault_tolerance();
    println!(
        "layout: {} (survives any {} failed disk{})",
        pool.topology.summary(),
        tolerance,
        if tolerance == 1 { "" } else { "s" }
    );
    let mut lines = Vec::new();
    for d in 0..pool.disks.len() {
        let label = match fs.check_disk_label(d).await {
            fs::LabelState::Ok => "label ok",
            fs::LabelState::Offline => "OFFLINE",
            fs::LabelState::Foreign => "NOT A MEMBER (replaced?)",
        };
        let state = match degraded.get(d).copied().flatten() {
            None => "in sync".to_string(),
            Some(t) => format!("DEGRADED since TXG {} (needs resilver)", t),
        };
        lines.push(format!(
            "disk {} {:<40} {:<24} {}",
            d,
            pool.disks.disks[d].path(),
            label,
            state
        ));
    }
    print_tree(&pool.topology, &lines, 1);
}

fn print_tree(t: &Topology, disk_lines: &[String], depth: usize) {
    let pad = "  ".repeat(depth);
    match t {
        Topology::Disk(d) => println!("{}{}", pad, disk_lines[*d]),
        Topology::RaidZ { disks, .. } | Topology::Mirror { disks } => {
            println!("{}{}", pad, t.summary());
            for d in disks {
                println!("{}  {}", pad, disk_lines[*d]);
            }
        }
        Topology::Stripe { children } => {
            println!("{}stripe", pad);
            for c in children {
                print_tree(c, disk_lines, depth + 1);
            }
        }
    }
}

fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("Error: {}", msg);
    std::process::exit(1);
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let config = load_config(&cli.config)?;
    let opts = config.disk_options();
    let spec = config.pool_spec().unwrap_or_else(|e| fail(e));

    if let Commands::Listen { port, id } = &cli.command {
        let serve = config.serve_disks().unwrap_or_else(|e| fail(e));
        let paths: Vec<&str> = serve.iter().map(|s| s.as_str()).collect();
        let disks = Arc::new(DiskSet::new_with(&paths, opts).await);
        let pool = match spec {
            // Serving the pool's own disks: share them.
            Some(spec) if spec.paths == serve => Some(spec.on(disks.clone())),
            Some(spec) => Some(spec.open(opts).await),
            None => None,
        };
        let port = port.unwrap_or(8881);
        let etcd = config.etcd_config;
        api::listen(disks, pool, port, etcd, id).await;
        return Ok(());
    }

    let Some(spec) = spec else {
        fail("no pool topology configured (set `pool` in the config)");
    };
    let raid = spec.open(opts).await;

    match cli.command {
        Commands::Nbd { name, size: _ } => {
            let fs = Arc::new(FileSystem::load(raid).await.unwrap());
            scan::spawn_health_monitor(fs.clone(), HEALTH_INTERVAL);
            nbd::serve_nbd(fs, name, 10809).await.unwrap();
        }
        Commands::Nvme { name } => {
            let fs = Arc::new(FileSystem::load(raid).await.unwrap());
            scan::spawn_health_monitor(fs.clone(), HEALTH_INTERVAL);
            let server = nvme::NvmeTcpServer::new(fs, name, 4420).await.unwrap();
            server.run().await.unwrap();
        }
        Commands::Scrub { dry_run } => {
            let fs = if dry_run {
                FileSystem::load_readonly(raid).await.unwrap()
            } else {
                FileSystem::load(raid).await.unwrap()
            };
            let report = fs.scrub(!dry_run).await.unwrap();
            println!("{}", report);
            if !report.is_clean() {
                std::process::exit(2);
            }
        }
        Commands::Resilver => {
            let fs = FileSystem::load(raid).await.unwrap();
            if !fs.needs_resilver() {
                println!("no online disk needs a resilver");
                print_status(&fs).await;
                return Ok(());
            }
            let report = fs.resilver().await.unwrap();
            println!("{}", report);
            print_status(&fs).await;
            if !report.is_clean() {
                std::process::exit(2);
            }
        }
        Commands::Status => {
            let fs = FileSystem::load_readonly(raid).await.unwrap();
            print_status(&fs).await;
        }
        Commands::Block { name, size } => {
            let fs = FileSystem::load(raid).await.unwrap();
            let vbs = fs.default_volblocksize();
            fs.create_block(&name, size, vbs).await.unwrap();
        }
        Commands::Map => {
            let fs = FileSystem::load_readonly(raid).await.unwrap();
            fs.display_block_map().await;
        }
        Commands::Metaslab => {
            let fs = FileSystem::load_readonly(raid).await.unwrap();
            fs.display_metaslab_info().await;
        }
        Commands::Orphaned => {
            let fs = FileSystem::load_readonly(raid).await.unwrap();
            let orphans = fs.find_orphaned_blocks().await.unwrap();
            println!("orphans: {:?}", orphans);
        }
        Commands::Read { path } => {
            let fs = FileSystem::load_readonly(raid).await.unwrap();
            let data = fs.read_file(&Path::new(&path)).await.unwrap();
            std::fs::write("output.png", data)?;
        }
        Commands::Inode { file } => {
            let fs = FileSystem::load_readonly(raid).await.unwrap();
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
            let fs = FileSystem::load_readonly(raid).await.unwrap();
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
            let fs = FileSystem::load_readonly(raid).await.unwrap();
            println!("index: {:?}", fs.file_index);
        }
        Commands::Listen { .. } => unreachable!(),
    }
    Ok(())
}
