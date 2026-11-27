//! The `raidish` command line (native only).

use crate::committer::PoolAdmin;
use crate::config::Config;
use crate::disk::PoolDevice;
use crate::fs::FileSystem;
use crate::inode::DatasetProps;
use crate::send::StreamReport;
use crate::snapshot::SnapshotInfo;
use crate::types::Txg;
use crate::types::{DatasetName, SnapshotRef};
use crate::vdev::Topology;
use crate::volume::VolumeInfo;
use crate::writer::CommitError;
use crate::{cluster, fs, nbd, nvme};
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
    txg: Option<Txg>,

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
        name: DatasetName,
        /// Ignored; the size comes from the volume. Kept for compatibility.
        size: Option<u64>,
    },
    Nvme {
        name: DatasetName,
    },
    Block {
        name: DatasetName,
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
        name: DatasetName,
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
        port: Option<u16>,

        /// The address to serve the API on (all interfaces: 0.0.0.0).
        #[arg(long, env = "RAIDISH_ADDRESS", default_value = "127.0.0.1")]
        address: String,

        /// Never become the pool's committer (leaves the `pool` lease free
        /// for maintenance: fsck --repair, format, defrag).
        #[arg(long)]
        no_committer: bool,
    },
    /// Serve the Kubernetes CSI driver (`csi.raidish.io`) on a unix socket:
    /// pods get raidish volumes as persistent volumes. Runs as root on every
    /// node, next to `listen`.
    Csi {
        /// The socket (`unix:///path` or a path).
        #[arg(
            long,
            default_value = "/var/lib/kubelet/plugins/csi.raidish.io/csi.sock"
        )]
        endpoint: String,
        /// This node's name in the cluster (`node-0`), the CSI node id.
        #[arg(long)]
        node: String,
        /// This node's `listen` API, for its agent's status (default:
        /// https://127.0.0.1:8881).
        #[arg(long)]
        agent: Option<String>,
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
    /// Service datasets (etcd keyspaces, S3 buckets, SQS queues) and where
    /// they run (through the committer).
    Dataset {
        #[command(subcommand)]
        command: DatasetCommands,
    },
    /// VMs whose disks are volumes, run through libvirt.
    Vm {
        #[command(subcommand)]
        command: crate::cli_vm::VmCommands,
    },
    /// The nodes themselves: host, libvirt, what live migration needs.
    Node {
        #[command(subcommand)]
        command: crate::cli_vm::NodeCommands,
    },
    /// The cluster's certificate authority: issue node and client certs.
    Cert {
        #[command(subcommand)]
        command: crate::cli_pki::CertCommands,
    },
    /// Tokens for the dashboard and scripts (signed by your admin cert).
    Token {
        #[command(subcommand)]
        command: crate::cli_pki::TokenCommands,
    },
    /// The cluster's own CA: set it up, join nodes, issue and renew.
    Ca {
        #[command(subcommand)]
        command: crate::cli_ca::CaCommands,
    },
    /// Get this node's certificate from the cluster CA with a join token
    /// (from `ca init` or `ca token create`), before its first `listen`.
    Join {
        /// A running node (`https://node0:8881`).
        url: String,
        /// Read the token from stdin.
        #[arg(long)]
        token_stdin: bool,
        /// Read the token from a file.
        #[arg(long, conflicts_with = "token_stdin")]
        token_file: Option<PathBuf>,
        /// The port this node's `listen` serves on (to find it in the pool).
        #[arg(short, long)]
        port: Option<u16>,
        /// Replace this node's certificate files.
        #[arg(long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum VolumeCommands {
    /// Create a volume of `size` bytes.
    Create { name: DatasetName, size: u64 },
    /// Delete a volume (it must not be running), or a snapshot
    /// (`volume@snapshot`).
    Delete { name: String },
    /// Snapshot a volume as last committed: `volume@snapshot`.
    Snapshot { name: SnapshotRef },
    /// List a volume's snapshots.
    Snapshots { name: DatasetName },
    /// Make volume `name` from snapshot `snapshot` (`volume@snapshot`).
    Clone {
        snapshot: SnapshotRef,
        name: DatasetName,
    },
    /// Roll a volume back to its newest snapshot, discarding every write
    /// since (it must not be running).
    Rollback { name: DatasetName },
    /// Write snapshot `volume@snapshot` to stdout as a stream: all of it,
    /// or (`--from`) what changed since an older snapshot of it.
    Send {
        snapshot: SnapshotRef,

        /// The older snapshot (its short name) the receiver already has.
        #[arg(long)]
        from: Option<String>,
    },
    /// Receive a stream from stdin into volume `name`: a full stream makes
    /// the volume, an incremental one goes onto its newest snapshot.
    Recv {
        name: DatasetName,
        /// Roll the volume back to its newest snapshot first if it was
        /// written since.
        #[arg(long)]
        force: bool,
    },
    /// List volumes with their placement settings.
    List,
    /// Change where and how a volume runs.
    Set {
        name: DatasetName,
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
    Move { name: DatasetName, node: String },
}

#[derive(Subcommand)]
enum DatasetCommands {
    /// List the pool's datasets (all formats) and where services run.
    List,
    /// Create a service dataset: `--format etcd --port 2379`, `--format
    /// s3` (a bucket) or `--format queue`.
    Create {
        name: DatasetName,
        #[arg(long)]
        format: String,
        /// An etcd dataset's client port (every node listens on it).
        #[arg(long)]
        port: Option<u16>,
        /// Nodes that may run it, most preferred first (comma separated).
        #[arg(long, value_delimiter = ',')]
        nodes: Option<Vec<String>>,
    },
    /// Delete a service dataset and everything in it (stopped first).
    Delete { name: DatasetName },
    /// Change where and how a dataset runs.
    Set {
        name: DatasetName,
        #[arg(long, value_delimiter = ',')]
        nodes: Option<Vec<String>>,
        #[arg(long)]
        port: Option<u16>,
        #[arg(long, conflicts_with = "disable")]
        enable: bool,
        #[arg(long)]
        disable: bool,
    },
}

#[derive(Subcommand)]
enum ClusterCommands {
    /// Show members, the leader and who holds which lease.
    Status,
    /// Attach volume `volume` to these nodes at once (comma separated,
    /// preferred writer first): each exports it, one writes it, the others
    /// proxy to it; a writer taken off the list hands it to the next. No
    /// nodes: back to its own placement.
    Attach {
        volume: DatasetName,
        #[arg(long, value_delimiter = ',', default_value = "")]
        nodes: Vec<String>,
    },
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
            Commands::Volume { command } => !matches!(
                command,
                VolumeCommands::List
                    | VolumeCommands::Snapshots { .. }
                    | VolumeCommands::Send { .. }
            ),
            _ => false,
        }
    }

    /// What the lease says this writer is doing.
    fn describe(&self) -> crate::types::Holder {
        crate::types::Holder::new(crate::types::HolderRole::Tool, None)
            .with_detail(self.describe_text())
    }

    fn describe_text(&self) -> String {
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

async fn print_status<D: PoolDevice>(fs: &FileSystem<D>) {
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
    let pool = fs.dev.pool();
    let tolerance = pool.topology().fault_tolerance();
    println!(
        "layout: {} (survives any {} failed disk{})",
        pool.topology().summary(),
        tolerance,
        if tolerance == 1 { "" } else { "s" }
    );
    let mut lines = Vec::new();
    for idx in pool.disks().indexes() {
        let d = idx.as_usize();
        let label = match fs.check_disk_label(idx).await {
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
            pool.disks().disk(idx).path(),
            label,
            state
        ));
    }
    print_tree(pool.topology(), &lines, 1);
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

async fn open_fs<D: PoolDevice>(dev: D, read_only: bool, rewind: fs::Rewind) -> FileSystem<D> {
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
    // A one-shot command piped into `head` should end quietly once the
    // reader is gone, as other command line tools do (Rust ignores SIGPIPE,
    // so `println!` would panic instead). Servers keep ignoring it: a peer
    // hanging up must not kill them.
    if !matches!(
        cli.command,
        Commands::Listen { .. }
            | Commands::Csi { .. }
            | Commands::Nbd { .. }
            | Commands::Nvme { .. }
    ) {
        // SAFETY: setting a signal disposition before any other thread
        // could depend on it; SIG_DFL has no handler to be unsafe in.
        unsafe {
            libc::signal(libc::SIGPIPE, libc::SIG_DFL);
        }
    }
    // Certificates are managed without (or before there is) a config.
    if let Commands::Cert { command } = &cli.command {
        let config_path = cli.config.clone();
        crate::cli_pki::run(command, &move || {
            let config = load_config(&config_path).unwrap_or_else(|e| fail(e));
            config
                .pool_spec()
                .unwrap_or_else(|e| fail(e))
                .map(|s| s.paths)
                .unwrap_or_default()
        });
        return Ok(());
    }
    let config = load_config(&cli.config)?;
    // Made before this node has certificates to init TLS with.
    match &cli.command {
        Commands::Ca {
            command: command @ crate::cli_ca::CaCommands::Init { .. },
        } => {
            crate::cli_ca::init(command, &config);
            return Ok(());
        }
        Commands::Join {
            url,
            token_stdin,
            token_file,
            port,
            force,
        } => {
            crate::cli_ca::join(
                url,
                *token_stdin,
                token_file.as_deref(),
                *port,
                *force,
                &config,
            )
            .await;
            return Ok(());
        }
        _ => {}
    }
    crate::tls::init(config.tls.as_ref()).unwrap_or_else(|e| fail(e));
    // The storage core's remote disks speak the same TLS.
    let _ = crate::disk::ws::set_tls_client(crate::tls::client());
    if let Ok(Some(spec)) = config.pool_spec() {
        crate::tls::check_urls(&spec.paths).unwrap_or_else(|e| fail(e));
    }
    if let Commands::Ca { command } = &cli.command {
        crate::cli_ca::run(command, &config).await;
        return Ok(());
    }
    if let Commands::Token { command } = &cli.command {
        if let crate::cli_pki::TokenCommands::Revoke { id, expires, url } = command {
            let expires_ms = expires.as_ref().map(|e| {
                chrono::DateTime::parse_from_rfc3339(e)
                    .map(|t| t.timestamp_millis() as u64)
                    .unwrap_or_else(|err| fail(format!("--expires '{}': {}", e, err)))
            });
            crate::cli_ca::revoke_token(id, expires_ms, url, &config).await;
        } else if let crate::cli_pki::TokenCommands::Create {
            name,
            role,
            expires,
            offline: false,
            url,
        } = command
        {
            let ttl = expires
                .as_deref()
                .map(|e| crate::pki::parse_duration(e).unwrap_or_else(|e| fail(e)));
            crate::cli_ca::create_token(name, *role, ttl, url, &config).await;
        } else if let crate::cli_pki::TokenCommands::S3Key { .. } = command {
            crate::cli_pki::s3_key(command, &config).await;
        } else {
            crate::cli_pki::run_token(command, &config);
        }
        return Ok(());
    }
    let opts = config.disk_options();
    let spec = config.pool_spec().unwrap_or_else(|e| fail(e));

    if let Commands::Listen {
        port,
        address,
        no_committer,
    } = &cli.command
    {
        crate::server::listen(&config, spec, opts, address, *port, *no_committer)
            .await
            .unwrap_or_else(|e| fail(e));
        return Ok(());
    }

    if let Commands::Node { command } = &cli.command {
        let endpoints = config.lease_endpoints().unwrap_or_else(|e| fail(e));
        if endpoints.is_empty() {
            fail("no cluster to ask: the pool has no remote disks");
        }
        crate::cli_vm::run_node(command, endpoints)
            .await
            .unwrap_or_else(|e| fail(e));
        return Ok(());
    }

    if let Commands::Vm { command } = &cli.command {
        let endpoints = config.lease_endpoints().unwrap_or_else(|e| fail(e));
        if endpoints.is_empty() {
            fail("no cluster to ask: the pool has no remote disks");
        }
        crate::cli_vm::run(command, endpoints)
            .await
            .unwrap_or_else(|e| fail(e));
        return Ok(());
    }

    if let Commands::Csi {
        endpoint,
        node,
        agent,
    } = &cli.command
    {
        let endpoints = config.lease_endpoints().unwrap_or_else(|e| fail(e));
        crate::csi::serve(endpoint, node.clone(), agent.clone(), endpoints)
            .await
            .unwrap_or_else(|e| fail(e));
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
            ClusterCommands::Attach { volume, nodes } => {
                let nodes: Vec<String> = nodes.iter().filter(|n| !n.is_empty()).cloned().collect();
                let op = cluster::LeaseOp::SetAttached {
                    volume: volume.to_string(),
                    nodes: nodes.clone(),
                };
                cluster::lease::LeaseClient::new(endpoints)
                    .call(&op)
                    .await
                    .unwrap_or_else(|e| fail(e));
                match nodes.is_empty() {
                    true => println!("{}: placed as usual", volume),
                    false => println!("{}: attached to {}", volume, nodes.join(", ")),
                }
            }
        }
        return Ok(());
    }

    // A committer runs the pool (see `crate::writer`): volumes are managed
    // through it, and `nbd` / `nvme` run as volume writers.
    let endpoints = config.lease_endpoints().unwrap_or_else(|e| fail(e));
    if let Commands::Dataset { command } = &cli.command {
        if endpoints.is_empty() {
            fail("datasets are managed through the cluster: no lease endpoints configured");
        }
        let client = crate::committer::CommitClient::new(endpoints.clone());
        if client.committer().await.is_none() {
            fail("no committer is running (start `raidish listen` on a node)");
        }
        dataset_command(command, client, &endpoints).await;
        return Ok(());
    }
    if !endpoints.is_empty()
        && matches!(
            cli.command,
            Commands::Volume { .. }
                | Commands::Block { .. }
                | Commands::Delete { .. }
                | Commands::Nbd { .. }
                | Commands::Nvme { .. }
        )
        // Sending only reads (committed snapshots never change): any node
        // can, straight from the disks.
        && !matches!(
            cli.command,
            Commands::Volume {
                command: VolumeCommands::Send { .. }
            }
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
    let fence = Arc::new(crate::fence::ClientFence::default());
    let lease = if cli.command.writes() {
        let endpoints = config.lease_endpoints().unwrap_or_else(|e| fail(e));
        if endpoints.is_empty() {
            None
        } else {
            let client = cluster::lease::LeaseClient::new(endpoints);
            let held = cluster::lease::HeldLease::acquire(
                client,
                cluster::state::POOL,
                cli.command.describe(),
            )
            .await
            .unwrap_or_else(|e| fail(format!("cannot take the pool lease: {}", e)));
            fence.set_pool_epoch(held.epoch);
            fence.on_stale(|| {
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
    let raid = spec.open_fenced(opts, fence).await;

    match cli.command {
        Commands::Nbd { name, size: _ } => {
            let fs = Arc::new(open_fs(raid, false, rewind).await);
            crate::committer::health::spawn_health_monitor(fs.clone(), HEALTH_INTERVAL, None);
            nbd::serve_nbd(fs, name, nbd::NBD_PORT).await.unwrap();
        }
        Commands::Nvme { name } => {
            let fs = Arc::new(open_fs(raid, false, rewind).await);
            crate::committer::health::spawn_health_monitor(fs.clone(), HEALTH_INTERVAL, None);
            let server = nvme::NvmeTcpServer::new(fs, name, nvme::NVME_PORT)
                .await
                .unwrap();
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
            volume_command(&VolumeCommands::Create { name, size }, &fs).await;
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
            let data = fs.read_file(Path::new(&path)).await.unwrap();
            std::fs::write("output.png", data)?;
        }
        Commands::Inode { file } => {
            let fs = open_fs(raid, true, rewind).await;
            let (id, dnode) = fs.stat(Path::new(&file)).await.unwrap();
            println!("object {}: {:?}", id, dnode);
        }
        Commands::Write { source, dest } => {
            let fs = open_fs(raid, false, rewind).await;
            let file = Path::new(&source);
            let data = std::fs::read(file).unwrap();
            fs.write_file(Path::new(&dest), &data).await.unwrap();
            fs.sync().await.unwrap();
        }
        Commands::Mkdir { path } => {
            let fs = open_fs(raid, false, rewind).await;
            fs.mkdir(Path::new(&path)).await.unwrap();
            fs.sync().await.unwrap();
        }
        Commands::Ls { path } => {
            let fs = open_fs(raid, true, rewind).await;
            let ls = fs.ls(Path::new(&path)).await.unwrap();
            println!("ls: {:?}", &ls);
        }
        Commands::Delete { path } => {
            let fs = open_fs(raid, false, rewind).await;
            fs.delete(Path::new(&path)).await.unwrap();
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
            println!("root: {:?}", fs.ls(Path::new("")).await.unwrap());
        }
        Commands::Volume { command } => {
            let read_only = matches!(
                command,
                VolumeCommands::List
                    | VolumeCommands::Snapshots { .. }
                    | VolumeCommands::Send { .. }
            );
            let fs = open_fs(raid, read_only, rewind).await;
            match command {
                VolumeCommands::Send { snapshot, from } => {
                    send_to_stdout(&fs, &snapshot, from).await
                }
                cmd => volume_command(&cmd, &fs).await,
            }
        }
        Commands::Listen { .. }
        | Commands::Vm { .. }
        | Commands::Node { .. }
        | Commands::Cluster { .. }
        | Commands::Csi { .. }
        | Commands::Cert { .. }
        | Commands::Token { .. }
        | Commands::Ca { .. }
        | Commands::Join { .. }
        | Commands::Dataset { .. } => unreachable!(),
    }
    if let Some(lease) = lease {
        lease.release().await;
    }
    Ok(())
}

/// What the volume commands need of a pool: the disks themselves (no
/// cluster, so this process is the only writer) or the cluster's committer.
#[async_trait::async_trait]
trait VolumeAdmin: Sync {
    async fn volumes(&self) -> Result<Vec<VolumeInfo>, CommitError>;
    async fn create_volume(&self, name: &DatasetName, size: u64) -> Result<(), CommitError>;
    async fn delete_volume(&self, name: &DatasetName) -> Result<(), CommitError>;
    async fn set_props(&self, name: &DatasetName, props: &DatasetProps) -> Result<(), CommitError>;
    async fn snapshots(&self, name: &DatasetName) -> Result<Vec<SnapshotInfo>, CommitError>;
    async fn create_snapshot(&self, name: &DatasetName, snap: &str) -> Result<Txg, CommitError>;
    async fn destroy_snapshot(&self, name: &DatasetName, snap: &str) -> Result<(), CommitError>;
    async fn clone_snapshot(
        &self,
        name: &DatasetName,
        snap: &str,
        new: &DatasetName,
    ) -> Result<(), CommitError>;
    async fn rollback(&self, name: &DatasetName) -> Result<(), CommitError>;
    /// Receive the stream on stdin into `name`.
    async fn recv_stdin(
        &self,
        name: &DatasetName,
        force: bool,
    ) -> Result<StreamReport, CommitError>;
    /// For `volume list`: who commits, and the cluster (where volumes run).
    async fn cluster_view(&self) -> Option<(String, Option<cluster::Status>)> {
        None
    }
}

#[async_trait::async_trait]
impl<D: PoolDevice + 'static> VolumeAdmin for FileSystem<D> {
    async fn volumes(&self) -> Result<Vec<VolumeInfo>, CommitError> {
        Ok(self.list_volumes().await?)
    }
    async fn create_volume(&self, name: &DatasetName, size: u64) -> Result<(), CommitError> {
        let vbs = self.default_volblocksize();
        self.create_block(name, size, vbs).await?;
        Ok(())
    }
    async fn delete_volume(&self, name: &DatasetName) -> Result<(), CommitError> {
        Ok(FileSystem::delete_volume(self, name).await?)
    }
    async fn set_props(&self, name: &DatasetName, props: &DatasetProps) -> Result<(), CommitError> {
        Ok(self.set_dataset_props(name, props.clone()).await?)
    }
    async fn snapshots(&self, name: &DatasetName) -> Result<Vec<SnapshotInfo>, CommitError> {
        Ok(self.list_snapshots(name).await?)
    }
    async fn create_snapshot(&self, name: &DatasetName, snap: &str) -> Result<Txg, CommitError> {
        Ok(FileSystem::create_snapshot(self, name, snap).await?)
    }
    async fn destroy_snapshot(&self, name: &DatasetName, snap: &str) -> Result<(), CommitError> {
        Ok(FileSystem::destroy_snapshot(self, name, snap).await?)
    }
    async fn clone_snapshot(
        &self,
        name: &DatasetName,
        snap: &str,
        new: &DatasetName,
    ) -> Result<(), CommitError> {
        FileSystem::clone_snapshot(self, name, snap, new).await?;
        Ok(())
    }
    async fn rollback(&self, name: &DatasetName) -> Result<(), CommitError> {
        Ok(self.rollback_volume(name).await?)
    }
    async fn recv_stdin(
        &self,
        name: &DatasetName,
        force: bool,
    ) -> Result<StreamReport, CommitError> {
        Ok(self
            .recv_snapshot(name, &mut tokio::io::stdin(), force)
            .await?)
    }
}

/// The running committer, found through the cluster at `endpoints`.
struct Remote<'a> {
    client: crate::committer::CommitClient,
    info: crate::committer::CommitterInfo,
    endpoints: &'a [String],
}

#[async_trait::async_trait]
impl VolumeAdmin for Remote<'_> {
    async fn volumes(&self) -> Result<Vec<VolumeInfo>, CommitError> {
        self.client.volumes().await
    }
    async fn create_volume(&self, name: &DatasetName, size: u64) -> Result<(), CommitError> {
        self.client.create_volume(name, size).await
    }
    async fn delete_volume(&self, name: &DatasetName) -> Result<(), CommitError> {
        self.client.delete_volume(name).await
    }
    async fn set_props(&self, name: &DatasetName, props: &DatasetProps) -> Result<(), CommitError> {
        self.client.set_props(name, props).await
    }
    async fn snapshots(&self, name: &DatasetName) -> Result<Vec<SnapshotInfo>, CommitError> {
        self.client.snapshots(name).await
    }
    async fn create_snapshot(&self, name: &DatasetName, snap: &str) -> Result<Txg, CommitError> {
        self.client.create_snapshot(name, snap).await
    }
    async fn destroy_snapshot(&self, name: &DatasetName, snap: &str) -> Result<(), CommitError> {
        self.client.destroy_snapshot(name, snap).await
    }
    async fn clone_snapshot(
        &self,
        name: &DatasetName,
        snap: &str,
        new: &DatasetName,
    ) -> Result<(), CommitError> {
        self.client.clone_snapshot(name, snap, new).await
    }
    async fn rollback(&self, name: &DatasetName) -> Result<(), CommitError> {
        self.client.rollback(name).await
    }
    async fn recv_stdin(
        &self,
        name: &DatasetName,
        force: bool,
    ) -> Result<StreamReport, CommitError> {
        self.client.recv(name, force, tokio::io::stdin()).await
    }
    async fn cluster_view(&self) -> Option<(String, Option<cluster::Status>)> {
        let status = cluster::lease::LeaseClient::new(self.endpoints.to_vec())
            .status()
            .await
            .ok();
        let who = format!("committer: {} (epoch {})", self.info.node, self.info.epoch);
        Some((who, status))
    }
}

/// Run a volume command (all but `send`, which reads the disks directly)
/// against `pool`.
async fn volume_command(cmd: &VolumeCommands, pool: &impl VolumeAdmin) {
    let ok = |r: Result<(), CommitError>| r.unwrap_or_else(|e| fail(e));
    match cmd {
        VolumeCommands::Create { name, size } => ok(pool.create_volume(name, *size).await),
        VolumeCommands::Delete { name } if name.contains('@') => {
            let r: SnapshotRef = name.parse().unwrap_or_else(|e| fail(e));
            ok(pool.destroy_snapshot(&r.volume, r.snap.as_str()).await)
        }
        VolumeCommands::Delete { name } => {
            let name: DatasetName = name.parse().unwrap_or_else(|e| fail(e));
            ok(pool.delete_volume(&name).await)
        }
        VolumeCommands::Snapshot { name } => {
            let txg = pool
                .create_snapshot(&name.volume, name.snap.as_str())
                .await
                .unwrap_or_else(|e| fail(e));
            println!("{} at TXG {}", name, txg);
        }
        VolumeCommands::Snapshots { name } => {
            print_snapshots(&pool.snapshots(name).await.unwrap_or_else(|e| fail(e)))
        }
        VolumeCommands::Clone { snapshot, name } => ok(pool
            .clone_snapshot(&snapshot.volume, snapshot.snap.as_str(), name)
            .await),
        VolumeCommands::Rollback { name } => ok(pool.rollback(name).await),
        VolumeCommands::Recv { name, force } => {
            let r = pool
                .recv_stdin(name, *force)
                .await
                .unwrap_or_else(|e| fail(e));
            eprintln!("received {}", r);
        }
        VolumeCommands::List => {
            let vols = pool.volumes().await.unwrap_or_else(|e| fail(e));
            let status = match pool.cluster_view().await {
                Some((who, status)) => {
                    println!("{}", who);
                    status
                }
                None => None,
            };
            print_volumes(&vols, status.as_ref());
        }
        VolumeCommands::Set { .. } | VolumeCommands::Move { .. } => {
            let vols = pool.volumes().await.unwrap_or_else(|e| fail(e));
            let (name, props) = change_props(cmd, &vols);
            ok(pool.set_props(&name, &props).await)
        }
        VolumeCommands::Send { .. } => unreachable!("sent from the disks"),
    }
}

/// The volume and its new settings for `volume set` / `volume move`.
fn change_props(
    cmd: &VolumeCommands,
    vols: &[crate::volume::VolumeInfo],
) -> (DatasetName, crate::inode::DatasetProps) {
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

/// `volume send`: the stream to stdout (which mustn't be a terminal), the
/// report to stderr.
async fn send_to_stdout(
    fs: &FileSystem<crate::vdev::Pool>,
    snapshot: &SnapshotRef,
    from: Option<String>,
) {
    use std::io::IsTerminal;
    if std::io::stdout().is_terminal() {
        fail("refusing to write a send stream to a terminal (redirect or pipe it)");
    }
    let (v, s) = (&snapshot.volume, snapshot.snap.as_str());

    let from = from.map(|f| f.rsplit('@').next().unwrap_or_default().to_string());
    let mut out = tokio::io::BufWriter::with_capacity(4 << 20, tokio::io::stdout());
    let r = fs
        .send_snapshot(v, s, from.as_deref(), &mut out)
        .await
        .unwrap_or_else(|e| fail(e));
    eprintln!("sent {}", r);
}

fn print_snapshots(snaps: &[crate::snapshot::SnapshotInfo]) {
    if snaps.is_empty() {
        println!("no snapshots");
        return;
    }
    println!(
        "{:<32} {:>8} {:>10}  {:<20} CLONES",
        "NAME", "TXG", "UNIQUE", "CREATED"
    );
    for s in snaps {
        let created = chrono_like(s.created_ns);
        println!(
            "{:<32} {:>8} {:>10}  {:<20} {}",
            s.name,
            s.txg,
            human_bytes(s.unique_bytes),
            created,
            if s.clones.is_empty() {
                "-".into()
            } else {
                s.clones.join(",")
            }
        );
    }
}

/// `bytes` as K/M/G.
fn human_bytes(bytes: u64) -> String {
    let b = bytes as f64;
    if b >= (1u64 << 30) as f64 {
        format!("{:.1}G", b / (1u64 << 30) as f64)
    } else if b >= (1u64 << 20) as f64 {
        format!("{:.1}M", b / (1u64 << 20) as f64)
    } else {
        format!("{:.0}K", b / 1024.0)
    }
}

/// ns since the epoch as `YYYY-MM-DD HH:MM:SS` (UTC).
fn chrono_like(ns: u64) -> String {
    let secs = ns / 1_000_000_000;
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        y,
        m,
        d,
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
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
                    .live_lease(&crate::committer::dataset_resource(&v.name), s.now_ms)
                    .map(|l| l.holder.to_string())
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

fn parse_format(f: &str) -> crate::dataset::DatasetFormat {
    use crate::dataset::DatasetFormat as F;
    match f {
        "etcd" => F::Etcd,
        "s3" | "bucket" => F::S3,
        "queue" | "sqs" => F::Queue,
        f => fail(format!(
            "unknown service format '{}' (etcd, s3 or queue)",
            f
        )),
    }
}

/// Run a dataset command against the running committer.
async fn dataset_command(
    command: &DatasetCommands,
    client: crate::committer::CommitClient,
    endpoints: &[String],
) {
    let ok = |r: Result<(), crate::writer::CommitError>| r.unwrap_or_else(|e| fail(e));
    match command {
        DatasetCommands::List => {
            let ds = client.datasets().await.unwrap_or_else(|e| fail(e));
            let status = cluster::lease::LeaseClient::new(endpoints.to_vec())
                .status()
                .await
                .ok();
            if ds.is_empty() {
                println!("no datasets");
                return;
            }
            println!(
                "{:<20} {:<6} {:<6} {:<8} {:<20} RUNNING",
                "NAME", "FORMAT", "PORT", "ENABLED", "NODES"
            );
            for d in ds {
                let running = status
                    .as_ref()
                    .and_then(|s| s.state.dataset_node(&d.name, s.now_ms).map(str::to_string))
                    .unwrap_or_else(|| "-".into());
                println!(
                    "{:<20} {:<6} {:<6} {:<8} {:<20} {}",
                    d.name,
                    d.format.name(),
                    d.props.port.map_or("-".to_string(), |p| p.to_string()),
                    if d.props.enabled { "yes" } else { "no" },
                    if d.props.nodes.is_empty() {
                        "(any)".to_string()
                    } else {
                        d.props.nodes.join(",")
                    },
                    running
                );
            }
        }
        DatasetCommands::Create {
            name,
            format,
            port,
            nodes,
        } => {
            let req = crate::committer::CreateDataset {
                name: crate::types::DatasetName::user(name).unwrap_or_else(|e| fail(e)),

                format: parse_format(format),
                props: crate::inode::DatasetProps {
                    port: *port,
                    nodes: nodes.clone().unwrap_or_default(),
                    ..Default::default()
                },
            };
            ok(client.create_dataset(&req).await)
        }
        DatasetCommands::Delete { name } => {
            let ds = client.datasets().await.unwrap_or_else(|e| fail(e));
            let Some(d) = ds.into_iter().find(|d| &d.name == name) else {
                fail(format!("no dataset '{}'", name));
            };
            if !d.format.is_service() {
                fail(format!(
                    "'{}' is a {} dataset: delete it with `volume delete`",
                    name,
                    d.format.name()
                ));
            }
            // Stopped everywhere first (the committer refuses a dataset in
            // use).
            if d.props.enabled {
                let props = crate::inode::DatasetProps {
                    enabled: false,
                    ..d.props.clone()
                };
                ok(client.set_props(name, &props).await);
            }
            let mut last = None;
            for _ in 0..60 {
                match client.delete_dataset(name).await {
                    Ok(()) => return,
                    Err(e) => last = Some(e),
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            fail(last.unwrap())
        }
        DatasetCommands::Set {
            name,
            nodes,
            port,
            enable,
            disable,
        } => {
            let ds = client.datasets().await.unwrap_or_else(|e| fail(e));
            let Some(d) = ds.into_iter().find(|d| &d.name == name) else {
                fail(format!("no dataset '{}'", name));
            };
            let mut props = d.props;
            if let Some(n) = nodes {
                props.nodes = n.iter().filter(|n| !n.is_empty()).cloned().collect();
            }
            if port.is_some() {
                props.port = *port;
            }
            if *enable {
                props.enabled = true;
            }
            if *disable {
                props.enabled = false;
            }
            ok(client.set_props(name, &props).await)
        }
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
    let remote = Remote {
        client,
        info,
        endpoints,
    };
    match command {
        Commands::Block { name, size } => {
            let cmd = VolumeCommands::Create {
                name: name.clone(),
                size: *size,
            };
            volume_command(&cmd, &remote).await
        }
        Commands::Delete { path } => {
            let cmd = VolumeCommands::Delete { name: path.clone() };
            volume_command(&cmd, &remote).await
        }
        Commands::Volume { command } => volume_command(command, &remote).await,
        Commands::Nbd { name, .. } | Commands::Nvme { name } => {
            let writer = VolumeWriter::start(endpoints, spec, opts, name, command.describe())
                .await
                .unwrap_or_else(|e| fail(e));
            crate::committer::health::spawn_health_monitor(
                writer.fs.clone(),
                HEALTH_INTERVAL,
                None,
            );
            if let Commands::Nbd { .. } = command {
                nbd::serve_nbd(writer.fs.clone(), name.clone(), nbd::NBD_PORT)
                    .await
                    .unwrap();
            } else {
                let server =
                    nvme::NvmeTcpServer::new(writer.fs.clone(), name.clone(), nvme::NVME_PORT)
                        .await
                        .unwrap();
                server.run().await.unwrap();
            }
        }
        _ => unreachable!(),
    }
}

/// A volume writer process (`nbd` / `nvme` with a committer running): its
/// `writer/...` and `ds/<name>` leases and the pool opened in writer role.
struct VolumeWriter {
    fs: Arc<FileSystem<crate::vdev::Pool>>,
    /// Held (renewed) for as long as this writer runs.
    _leases: [Arc<cluster::lease::HeldLease>; 2],
}

impl VolumeWriter {
    async fn start(
        endpoints: &[String],
        spec: crate::config::PoolSpec,
        opts: crate::disk::DiskOptions,
        volume: &DatasetName,
        desc: crate::types::Holder,
    ) -> Result<Self, String> {
        let leases = cluster::lease::LeaseClient::new(endpoints.to_vec());
        let resource = crate::types::LeaseResource::writer(cluster::lease::owner_id());
        let writer = Arc::new(
            cluster::lease::HeldLease::acquire(leases.clone(), resource, desc.clone()).await?,
        );
        let vol = Arc::new(
            cluster::lease::HeldLease::acquire(
                leases,
                crate::committer::dataset_resource(volume),
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
        let sink =
            crate::committer::CommitClient::new(endpoints.to_vec()).as_writer(writer.lease_ref());
        let fs = FileSystem::load_writer(spec.open(opts).await, Arc::new(sink))
            .await
            .map_err(|e| format!("opening the pool: {}", e))?;
        // Flushes are acknowledged only while the lease is provably held
        // (audit R3b; as the agent's writers do).
        let fresh = {
            let vol = vol.clone();
            Arc::new(move || vol.fresh()) as crate::writer::LeaseCheck
        };
        fs.attach_dataset_leased(volume, vol.holding(), None, Some(fresh))
            .await
            .map_err(|e| format!("volume '{}': {}", volume, e))?;
        Ok(VolumeWriter {
            fs: Arc::new(fs),
            _leases: [writer, vol],
        })
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
            resource.to_string(),
            l.epoch,
            l.holder,
            l.owner,
            state
        );
    }
    for (idx, g) in &s.state.metaslabs {
        if let Some(w) = g.writer() {
            println!(
                "metaslab {:<5} lent to {} (epoch {})",
                idx,
                w.resource,
                g.epoch()
            );
        }
    }
    for (volume, nodes) in &s.state.attached {
        println!("attached {} to {}", volume, nodes.join(", "));
    }
    for (vm, m) in &s.state.migrations {
        let vols: Vec<&str> = m.volumes.iter().map(|v| v.as_str()).collect();
        println!(
            "migrating VM {} from {} to {} ({}) for {:.1}s",
            vm,
            m.from,
            m.to,
            vols.join(", "),
            s.now_ms.saturating_sub(m.since_ms) as f64 / 1000.0
        );
    }
}
