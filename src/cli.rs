//! The `raidish` command line (native only).

use crate::config::Config;
use crate::disk::BlockDevice;
use crate::disks::DiskSet;
use crate::fs::FileSystem;
use crate::vdev::Topology;
use crate::{api, cluster, disk, fs, nbd, nvme, scan};
use clap::{Parser, Subcommand};
use std::sync::Arc;
use std::{path::Path, path::PathBuf};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Optional name to operate on
    name: Option<String>,

    #[arg(short, long, global = true, default_value = "~/.raidish.yaml")]
    config: String,

    /// Open the newest committed state whose whole tree verifies, discarding
    /// newer (damaged) TXGs. Writable commands commit the rewind.
    #[arg(long, global = true, conflicts_with = "txg")]
    rewind: bool,

    /// Open the committed state of exactly this TXG (see `status`).
    #[arg(long, global = true)]
    txg: Option<u64>,

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
    /// Check the space maps against the tree: leaked blocks, blocks in use
    /// that the maps call free, cross-links, unreadable metadata.
    Fsck {
        /// Free leaked blocks and re-allocate in-use blocks marked free.
        #[arg(long)]
        repair: bool,
        /// Discard the space maps and rebuild them from the tree.
        #[arg(long)]
        rebuild_spacemaps: bool,
        /// Rebuild even if part of the tree is unreadable (frees whatever
        /// only the unreadable part referenced).
        #[arg(long, requires = "rebuild_spacemaps")]
        force: bool,
    },
    /// Copy everything still readable to a local directory (volumes as
    /// sparse .img files); damaged records are zero-filled and reported.
    Salvage {
        dest: String,
    },
    /// Rewrite a volume so its records are contiguous on disk.
    Defrag {
        name: String,
        /// Records rewritten per commit.
        #[arg(long, default_value_t = 2048)]
        batch: usize,
        /// Only report how fragmented the volume is.
        #[arg(long)]
        check: bool,
    },
    /// Show disk health: online, label, degraded since which TXG.
    Status,
    Listen {
        #[arg(short, long)]
        port: Option<u32>,

        /// A name for this node (logs, data directory); defaults to
        /// `node-<id>`, the id coming from the pool config.
        id: Option<String>,
    },
    /// The control plane: members, leader, leases.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommands,
    },
}

#[derive(Subcommand)]
enum ClusterCommands {
    /// Show members, the leader and who holds which lease.
    Status,
}

impl Commands {
    /// Does this command write to the pool (and so need the pool lease)?
    fn writes(&self) -> bool {
        match self {
            Commands::Format { .. }
            | Commands::Nbd { .. }
            | Commands::Nvme { .. }
            | Commands::Block { .. }
            | Commands::Delete { .. }
            | Commands::Write { .. }
            | Commands::Mkdir { .. }
            | Commands::Resilver => true,
            Commands::Scrub { dry_run } => !dry_run,
            Commands::Fsck {
                repair,
                rebuild_spacemaps,
                ..
            } => *repair || *rebuild_spacemaps,
            Commands::Defrag { check, .. } => !check,
            _ => false,
        }
    }

    /// What the lease says this writer is doing.
    fn describe(&self) -> String {
        match self {
            Commands::Nbd { name, .. } => format!("nbd {}", name),
            Commands::Nvme { name } => format!("nvme {}", name),
            Commands::Block { name, .. } => format!("block {}", name),
            Commands::Delete { path } => format!("delete {}", path),
            Commands::Write { dest, .. } => format!("write {}", dest),
            Commands::Mkdir { path } => format!("mkdir {}", path),
            Commands::Defrag { name, .. } => format!("defrag {}", name),
            Commands::Format { .. } => "format".into(),
            Commands::Resilver => "resilver".into(),
            Commands::Scrub { .. } => "scrub".into(),
            Commands::Fsck { .. } => "fsck".into(),
            _ => "writer".into(),
        }
    }
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
    let ring: Vec<String> = fs
        .uberblock_ring()
        .await
        .iter()
        .map(|u| {
            let when = chrono::DateTime::from_timestamp_millis(u.timestamp as i64)
                .map_or("?".to_string(), |t| {
                    t.format("%Y-%m-%d %H:%M:%S").to_string()
                });
            format!("TXG {} ({})", u.txg, when)
        })
        .collect();
    println!("rewind targets: {}", ring.join(", "));
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

async fn open_fs<D: BlockDevice>(dev: D, read_only: bool, rewind: fs::Rewind) -> FileSystem<D> {
    let opened = match (rewind, read_only) {
        (fs::Rewind::None, false) => FileSystem::load(dev).await,
        (fs::Rewind::None, true) => FileSystem::load_readonly(dev).await,
        _ => FileSystem::load_with(dev, fs::LoadOptions { read_only, rewind }).await,
    };
    opened.unwrap_or_else(|e| fail(format!("cannot open pool: {}", e)))
}

#[tokio::main]
pub async fn main() -> std::io::Result<()> {
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
        let members = config
            .cluster_members(port as u16)
            .unwrap_or_else(|e| fail(format!("cluster: {}", e)));
        let name = id.clone().unwrap_or_else(|| format!("node-{}", members.me));
        let data_dir = match config.cluster.as_ref().and_then(|c| c.data_dir.as_deref()) {
            Some(d) => expand_tilde(d).join(&name),
            None => cluster::default_data_dir(&name),
        };
        println!(
            "cluster: node {} ({}), {} member{}",
            members.me,
            name,
            members.disks.len(),
            if members.disks.len() == 1 { "" } else { "s" }
        );
        let floor = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let node = cluster::Cluster::start(&name, members, data_dir, floor.clone())
            .await
            .unwrap_or_else(|e| fail(format!("cluster: {}", e)));
        let pool_map = config
            .pool_map(port as u16)
            .unwrap_or_else(|e| fail(format!("dashboard: {}", e)));
        println!("dashboard: http://localhost:{}/", port);
        api::listen(disks, pool, port, node, floor, pool_map).await;
        return Ok(());
    }

    if let Commands::Cluster { command } = &cli.command {
        let endpoints = config.lease_endpoints().unwrap_or_else(|e| fail(e));
        if endpoints.is_empty() {
            fail("no cluster to ask: the pool has no remote disks");
        }
        match command {
            ClusterCommands::Status => {
                let status = cluster::lease::LeaseClient::new(endpoints)
                    .status()
                    .await
                    .unwrap_or_else(|e| fail(e));
                print_cluster_status(&status);
            }
        }
        return Ok(());
    }

    // The pool's one writer: take the lease before touching any disk, so
    // our first requests already carry the new epoch (that is what fences
    // out a previous writer).
    let lease = if cli.command.writes() {
        let endpoints = config.lease_endpoints().unwrap_or_else(|e| fail(e));
        if endpoints.is_empty() {
            None
        } else {
            let client = cluster::lease::LeaseClient::new(endpoints);
            let held = cluster::lease::HeldLease::acquire(
                client,
                cluster::state::POOL,
                &cli.command.describe(),
            )
            .await
            .unwrap_or_else(|e| fail(format!("cannot take the pool lease: {}", e)));
            disk::set_lease_epoch(held.epoch);
            disk::on_stale_epoch(|| {
                eprintln!("Error: another writer has taken over the pool; stopping");
                std::process::exit(3);
            });
            let held = Arc::new(held);
            held.hold(|msg| {
                eprintln!("Error: {}; stopping", msg);
                std::process::exit(3);
            });
            Some(held)
        }
    } else {
        None
    };

    let Some(spec) = spec else {
        fail("no pool topology configured (set `pool` in the config)");
    };
    let rewind = match (cli.rewind, cli.txg) {
        (_, Some(t)) => fs::Rewind::Txg(t),
        (true, None) => fs::Rewind::Auto,
        (false, None) => fs::Rewind::None,
    };
    let raid = spec.open(opts).await;

    match cli.command {
        Commands::Nbd { name, size: _ } => {
            let fs = Arc::new(open_fs(raid, false, rewind).await);
            scan::spawn_health_monitor(fs.clone(), HEALTH_INTERVAL);
            nbd::serve_nbd(fs, name, 10809).await.unwrap();
        }
        Commands::Nvme { name } => {
            let fs = Arc::new(open_fs(raid, false, rewind).await);
            scan::spawn_health_monitor(fs.clone(), HEALTH_INTERVAL);
            let server = nvme::NvmeTcpServer::new(fs, name, 4420).await.unwrap();
            server.run().await.unwrap();
        }
        Commands::Scrub { dry_run } => {
            let fs = if dry_run {
                open_fs(raid, true, rewind).await
            } else {
                open_fs(raid, false, rewind).await
            };
            let report = fs.scrub(!dry_run).await.unwrap();
            println!("{}", report);
            if !report.is_clean() {
                std::process::exit(2);
            }
        }
        Commands::Resilver => {
            let fs = open_fs(raid, false, rewind).await;
            if !fs.needs_resilver() {
                println!("no online disk needs a resilver");
                print_status(&fs).await;
            } else {
                let report = fs.resilver().await.unwrap();
                println!("{}", report);
                print_status(&fs).await;
                if !report.is_clean() {
                    std::process::exit(2);
                }
            }
        }
        Commands::Fsck {
            repair,
            rebuild_spacemaps,
            force,
        } => {
            let fs = open_fs(raid, !(repair || rebuild_spacemaps), rewind).await;
            if rebuild_spacemaps {
                let report = fs
                    .rebuild_spacemaps(force)
                    .await
                    .unwrap_or_else(|e| fail(e));
                println!("{}", report);
            }
            let report = fs.fsck(repair).await.unwrap_or_else(|e| fail(e));
            println!("{}", report);
            if !report.is_clean() {
                std::process::exit(2);
            }
        }
        Commands::Salvage { dest } => {
            // A damaged root shouldn't stop a salvage: fall back to the
            // newest TXG whose root directory still reads. (--rewind too:
            // salvage wants what's readable, not a fully intact tree.)
            let rewind = match rewind {
                fs::Rewind::None | fs::Rewind::Auto => fs::Rewind::Readable,
                r => r,
            };
            let fs = open_fs(raid, true, rewind).await;
            let report = fs
                .salvage(Path::new(&dest))
                .await
                .unwrap_or_else(|e| fail(e));
            println!("{}", report);
            if !report.is_complete() {
                std::process::exit(2);
            }
        }
        Commands::Defrag { name, batch, check } => {
            let fs = open_fs(raid, check, rewind).await;
            if check {
                let frag = fs.fragmentation(&name).await.unwrap_or_else(|e| fail(e));
                println!("{}", frag);
                return Ok(());
            }
            let (before, after) = fs.defrag(&name, batch).await.unwrap_or_else(|e| fail(e));
            println!("before: {}", before);
            println!("after:  {}", after);
        }
        Commands::Status => {
            let fs = open_fs(raid, true, rewind).await;
            print_status(&fs).await;
        }
        Commands::Block { name, size } => {
            let fs = open_fs(raid, false, rewind).await;
            let vbs = fs.default_volblocksize();
            fs.create_block(&name, size, vbs).await.unwrap();
        }
        Commands::Map => {
            let fs = open_fs(raid, true, rewind).await;
            fs.display_block_map().await;
        }
        Commands::Metaslab => {
            let fs = open_fs(raid, true, rewind).await;
            fs.display_metaslab_info().await;
        }
        Commands::Orphaned => {
            let fs = open_fs(raid, true, rewind).await;
            let orphans = fs.find_orphaned_blocks().await.unwrap();
            println!("orphans: {:?}", orphans);
        }
        Commands::Read { path } => {
            let fs = open_fs(raid, true, rewind).await;
            let data = fs.read_file(&Path::new(&path)).await.unwrap();
            std::fs::write("output.png", data)?;
        }
        Commands::Inode { file } => {
            let fs = open_fs(raid, true, rewind).await;
            let inode_ref = fs.file_index.read().await.files.get(&file).unwrap().clone();
            let inode = fs.read_inode(&inode_ref).await.unwrap();
            println!("inode: {:?}", inode);
        }
        Commands::Write { source, dest } => {
            let fs = open_fs(raid, false, rewind).await;
            let file = Path::new(&source);
            let data = std::fs::read(file).unwrap();
            fs.write_file(&Path::new(&dest), &data).await.unwrap();
            fs.sync().await.unwrap();
        }
        Commands::Mkdir { path } => {
            let fs = open_fs(raid, false, rewind).await;
            fs.mkdir(&Path::new(&path)).await.unwrap();
            fs.sync().await.unwrap();
        }
        Commands::Ls { path } => {
            let fs = open_fs(raid, true, rewind).await;
            let ls = fs.ls(&Path::new(&path)).await.unwrap();
            println!("ls: {:?}", &ls);
        }
        Commands::Delete { path } => {
            let fs = open_fs(raid, false, rewind).await;
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
            let fs = open_fs(raid, true, rewind).await;
            println!("index: {:?}", fs.file_index);
        }
        Commands::Listen { .. } | Commands::Cluster { .. } => unreachable!(),
    }
    if let Some(lease) = lease {
        lease.release().await;
    }
    Ok(())
}

fn expand_tilde(path: &str) -> PathBuf {
    match (path.strip_prefix("~/"), std::env::var("HOME")) {
        (Some(rest), Ok(home)) => PathBuf::from(home).join(rest),
        _ => PathBuf::from(path),
    }
}

fn print_cluster_status(s: &cluster::Status) {
    match s.leader {
        Some(l) => println!("leader: node {} (term {})", l, s.term),
        None => println!("leader: none (no quorum?) (term {})", s.term),
    }
    println!("members (as seen by node {}, {}):", s.id, s.node);
    for m in &s.members {
        let mark = if s.leader == Some(m.id) { " *" } else { "" };
        let addr = m.addr.as_deref().unwrap_or("(this node)");
        let disks: Vec<String> = m.disks.iter().map(|d| d.to_string()).collect();
        println!(
            "  node {:<3} {:<28} disks {}{}",
            m.id,
            addr,
            disks.join(","),
            mark
        );
    }
    println!("layout: {}", s.fingerprint);
    println!("epoch: {}", s.state.epoch);
    if s.state.leases.is_empty() {
        println!("leases: none");
    }
    for (resource, l) in &s.state.leases {
        let state = if l.released {
            "released".to_string()
        } else if l.expires_ms > s.now_ms {
            format!(
                "expires in {:.1}s",
                (l.expires_ms - s.now_ms) as f64 / 1000.0
            )
        } else {
            "expired".to_string()
        };
        println!(
            "lease {:<8} epoch {:<4} {} ({}) {}",
            resource, l.epoch, l.desc, l.owner, state
        );
    }
}
