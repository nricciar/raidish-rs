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

        /// Never become the pool's committer (leaves the `pool` lease free
        /// for maintenance: fsck --repair, format, defrag).
        #[arg(long)]
        no_committer: bool,
    },
    /// The control plane: members, leader, leases.
    Cluster {
        #[command(subcommand)]
        command: ClusterCommands,
    },
    /// Volumes and where they run (through the committer, if one runs).
    Volume {
        #[command(subcommand)]
        command: VolumeCommands,
    },
}

#[derive(Subcommand)]
enum VolumeCommands {
    /// Create a volume of `size` bytes.
    Create { name: String, size: u64 },
    /// Delete a volume (it must not be running).
    Delete { name: String },
    /// List volumes with their placement settings.
    List,
    /// Change where and how a volume runs.
    Set {
        name: String,
        /// Nodes that may run it, most preferred first (comma separated;
        /// empty: any node).
        #[arg(long, value_delimiter = ',')]
        nodes: Option<Vec<String>>,
        /// Export and attach it wherever it runs: nbd, nvme or none.
        #[arg(long)]
        automount: Option<String>,
        #[arg(long, conflicts_with = "disable")]
        enable: bool,
        #[arg(long)]
        disable: bool,
    },
    /// Move a running volume to `node` (and keep it there).
    Move { name: String, node: String },
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
            Commands::Volume { command } => !matches!(command, VolumeCommands::List),
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
            Commands::Volume { .. } => "volume".into(),
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

    if let Commands::Listen { port, no_committer } = &cli.command {
        let serve = config.serve_disks().unwrap_or_else(|e| fail(e));
        let paths: Vec<&str> = serve.iter().map(|s| s.as_str()).collect();
        let disks = Arc::new(DiskSet::new_with(&paths, opts).await);
        let pool = match &spec {
            // Serving the pool's own disks: share them.
            Some(spec) if spec.paths == serve => Some(spec.on(disks.clone())),
            Some(spec) => Some(spec.open(opts).await),
            None => None,
        };
        let port = port.unwrap_or(8881);
        let members = config
            .cluster_members(port as u16)
            .unwrap_or_else(|e| fail(format!("cluster: {}", e)));
        // The node's name comes from its id: the pool position of its first disk.
        let name = format!("node-{}", members.me);
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
        let pool_map = config
            .pool_map(port as u16)
            .unwrap_or_else(|e| fail(format!("dashboard: {}", e)));
        let floor = Arc::new(crate::fence::ServerFence::default());
        if let Some(map) = &pool_map {
            // Requests name the served disk; fencing works in pool disks.
            floor.set_served(
                map.disks
                    .iter()
                    .enumerate()
                    .filter_map(|(pool_idx, d)| match d {
                        crate::inspect::DiskAddr::This(i) => Some((*i as usize, pool_idx)),
                        _ => None,
                    })
                    .collect(),
            );
        }
        let node = cluster::Cluster::start(&name, members, data_dir, floor.clone())
            .await
            .unwrap_or_else(|e| fail(format!("cluster: {}", e)));
        let slot: crate::committer::CommitterSlot = Default::default();
        let agent_state: crate::agent::AgentState = Default::default();
        let me = format!("http://127.0.0.1:{}", port);
        if let (Some(spec), Some(map)) = (&spec, &pool_map) {
            // This node's own disks through its own disk server, so the
            // committer's writes are fenced like everyone else's.
            let access = crate::committer::PoolAccess {
                spec: crate::config::PoolSpec {
                    paths: map
                        .disks
                        .iter()
                        .map(|d| match d {
                            crate::inspect::DiskAddr::This(i) => {
                                format!("{}/api/v1/disks/{}", me, i)
                            }
                            crate::inspect::DiskAddr::Url(u) => u.clone(),
                        })
                        .collect(),
                    topology: spec.topology.clone(),
                },
                opts,
            };
            if !*no_committer {
                tokio::spawn(crate::committer::run(
                    slot.clone(),
                    node.clone(),
                    access.clone(),
                    cluster::lease::LeaseClient::new(vec![me.clone()]),
                    name.clone(),
                ));
            }
            // The committer is wherever the `pool` lease is: ask every node.
            let mut endpoints = vec![me.clone()];
            for e in config.lease_endpoints().unwrap_or_default() {
                if !endpoints.contains(&e) && !cluster::members::is_local_base(&e, port as u16) {
                    endpoints.push(e);
                }
            }
            let agent = crate::agent::Agent::new(
                name.clone(),
                node.clone(),
                endpoints,
                access,
                config.automount.clone(),
                agent_state.clone(),
            );
            tokio::spawn(agent.run());
        }
        println!("dashboard: http://localhost:{}/", port);
        api::listen(disks, pool, port, node, floor, pool_map, slot, agent_state).await;
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

    // A committer runs the pool (see `crate::writer`): volumes are managed
    // through it, and `nbd` / `nvme` run as volume writers.
    let endpoints = config.lease_endpoints().unwrap_or_else(|e| fail(e));
    if !endpoints.is_empty()
        && matches!(
            cli.command,
            Commands::Volume { .. }
                | Commands::Block { .. }
                | Commands::Delete { .. }
                | Commands::Nbd { .. }
                | Commands::Nvme { .. }
        )
    {
        let client = crate::committer::CommitClient::new(endpoints.clone());
        if let Some(info) = client.committer().await {
            let Some(spec) = spec else {
                fail("no pool topology configured (set `pool` in the config)");
            };
            via_committer(&cli.command, client, info, &endpoints, spec, opts).await;
            return Ok(());
        }
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
        Commands::Volume { command } => {
            let read_only = matches!(command, VolumeCommands::List);
            let fs = open_fs(raid, read_only, rewind).await;
            match command {
                VolumeCommands::Create { name, size } => {
                    let vbs = fs.default_volblocksize();
                    fs.create_block(&name, size, vbs)
                        .await
                        .unwrap_or_else(|e| fail(e));
                }
                VolumeCommands::Delete { name } => {
                    fs.delete(Path::new(&name))
                        .await
                        .unwrap_or_else(|e| fail(e));
                    fs.sync().await.unwrap_or_else(|e| fail(e));
                }
                VolumeCommands::List => {
                    print_volumes(&fs.list_volumes().await.unwrap_or_else(|e| fail(e)), None)
                }
                cmd @ (VolumeCommands::Set { .. } | VolumeCommands::Move { .. }) => {
                    let vols = fs.list_volumes().await.unwrap_or_else(|e| fail(e));
                    let (name, props) = change_props(&cmd, &vols);
                    fs.set_volume_props(&name, props)
                        .await
                        .unwrap_or_else(|e| fail(e));
                }
            }
        }
        Commands::Listen { .. } | Commands::Cluster { .. } => unreachable!(),
    }
    if let Some(lease) = lease {
        lease.release().await;
    }
    Ok(())
}

/// The volume and its new settings for `volume set` / `volume move`.
fn change_props(
    cmd: &VolumeCommands,
    vols: &[crate::volume::VolumeInfo],
) -> (String, crate::inode::VolumeProps) {
    let name = match cmd {
        VolumeCommands::Set { name, .. } | VolumeCommands::Move { name, .. } => name,
        _ => unreachable!(),
    };
    let mut props = vols
        .iter()
        .find(|v| &v.name == name)
        .unwrap_or_else(|| fail(format!("no volume '{}'", name)))
        .props
        .clone();
    match cmd {
        VolumeCommands::Set {
            nodes,
            automount,
            enable,
            disable,
            ..
        } => {
            if let Some(nodes) = nodes {
                props.nodes = nodes.iter().filter(|n| !n.is_empty()).cloned().collect();
            }
            if let Some(a) = automount {
                props.automount = match a.as_str() {
                    "none" | "" => None,
                    t => Some(t.parse().unwrap_or_else(|e: String| fail(e))),
                };
            }
            if *enable {
                props.enabled = true;
            }
            if *disable {
                props.enabled = false;
            }
        }
        VolumeCommands::Move { node, .. } => {
            if !props.nodes.is_empty() && !props.nodes.contains(node) {
                props.nodes.insert(0, node.clone());
            }
            props.move_to = Some(node.clone());
        }
        _ => unreachable!(),
    }
    (name.clone(), props)
}

fn print_volumes(vols: &[crate::volume::VolumeInfo], status: Option<&cluster::Status>) {
    if vols.is_empty() {
        println!("no volumes");
        return;
    }
    println!(
        "{:<16} {:>10} {:<8} {:<9} {:<24} RUNNING",
        "NAME", "SIZE", "ENABLED", "AUTOMOUNT", "NODES"
    );
    for v in vols {
        let running = status
            .and_then(|s| {
                s.state
                    .leases
                    .get(&crate::committer::volume_resource(&v.name))
                    .filter(|l| !l.released && l.expires_ms > s.now_ms)
                    .map(|l| l.desc.clone())
            })
            .unwrap_or_else(|| "-".into());
        println!(
            "{:<16} {:>10} {:<8} {:<9} {:<24} {}",
            v.name,
            format!("{:.1}G", v.size_bytes as f64 / (1u64 << 30) as f64),
            if v.props.enabled { "yes" } else { "no" },
            v.props.automount.map_or("-".to_string(), |t| t.to_string()),
            if v.props.nodes.is_empty() {
                "(any)".to_string()
            } else {
                v.props.nodes.join(",")
            },
            running
        );
    }
}

/// Run a volume command against a running committer.
async fn via_committer(
    command: &Commands,
    client: crate::committer::CommitClient,
    info: crate::committer::CommitterInfo,
    endpoints: &[String],
    spec: crate::config::PoolSpec,
    opts: crate::disk::DiskOptions,
) {
    let ok = |r: Result<(), crate::writer::CommitError>| r.unwrap_or_else(|e| fail(e));
    match command {
        Commands::Block { name, size }
        | Commands::Volume {
            command: VolumeCommands::Create { name, size },
        } => ok(client.create_volume(name, *size).await),
        Commands::Delete { path: name }
        | Commands::Volume {
            command: VolumeCommands::Delete { name },
        } => ok(client.delete_volume(name).await),
        Commands::Volume {
            command: VolumeCommands::List,
        } => {
            let vols = client.volumes().await.unwrap_or_else(|e| fail(e));
            let status = cluster::lease::LeaseClient::new(endpoints.to_vec())
                .status()
                .await
                .ok();
            println!("committer: {} (epoch {})", info.node, info.epoch);
            print_volumes(&vols, status.as_ref());
        }
        Commands::Volume { command: cmd } => {
            let vols = client.volumes().await.unwrap_or_else(|e| fail(e));
            let (name, props) = change_props(cmd, &vols);
            ok(client.set_props(&name, &props).await)
        }
        Commands::Nbd { name, .. } | Commands::Nvme { name } => {
            let writer = VolumeWriter::start(endpoints, spec, opts, name, &command.describe())
                .await
                .unwrap_or_else(|e| fail(e));
            scan::spawn_health_monitor(writer.fs.clone(), HEALTH_INTERVAL);
            if let Commands::Nbd { .. } = command {
                nbd::serve_nbd(writer.fs.clone(), name.clone(), 10809)
                    .await
                    .unwrap();
            } else {
                let server = nvme::NvmeTcpServer::new(writer.fs.clone(), name.clone(), 4420)
                    .await
                    .unwrap();
                server.run().await.unwrap();
            }
        }
        _ => unreachable!(),
    }
}

/// A volume writer process (`nbd` / `nvme` with a committer running): its
/// `writer/...` and `vol/<name>` leases and the pool opened in writer role.
struct VolumeWriter {
    fs: Arc<FileSystem<crate::vdev::Pool>>,
}

impl VolumeWriter {
    async fn start(
        endpoints: &[String],
        spec: crate::config::PoolSpec,
        opts: crate::disk::DiskOptions,
        volume: &str,
        desc: &str,
    ) -> Result<Self, String> {
        let leases = cluster::lease::LeaseClient::new(endpoints.to_vec());
        let resource = format!("writer/{}", cluster::lease::owner_id());
        let writer =
            Arc::new(cluster::lease::HeldLease::acquire(leases.clone(), &resource, desc).await?);
        let vol = Arc::new(
            cluster::lease::HeldLease::acquire(
                leases,
                &crate::committer::volume_resource(volume),
                desc,
            )
            .await?,
        );
        for l in [&writer, &vol] {
            l.hold(|msg| {
                eprintln!("Error: {}; stopping", msg);
                std::process::exit(3);
            });
        }
        let sink = crate::committer::CommitClient::new(endpoints.to_vec())
            .as_writer(&resource, writer.epoch);
        let fs = FileSystem::load_writer(spec.open(opts).await, Arc::new(sink))
            .await
            .map_err(|e| format!("opening the pool: {}", e))?;
        fs.attach_volume(volume, &vol.owner, vol.epoch)
            .await
            .map_err(|e| format!("volume '{}': {}", volume, e))?;
        Ok(VolumeWriter { fs: Arc::new(fs) })
    }
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
    for (idx, g) in &s.state.metaslabs {
        if let Some(w) = &g.writer {
            println!("metaslab {:<5} lent to {} (epoch {})", idx, w, g.epoch);
        }
    }
}
