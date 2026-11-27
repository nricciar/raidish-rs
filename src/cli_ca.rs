//! `raidish ca …` and `raidish join`: the cluster CA from the command line
//! (see `raidish_ca` and `docs/ca-service-plan.md`).
//!
//! `ca init` and `join` run before this node has certificates (so before
//! `tls::init`); the rest talk to running nodes with the config's
//! identity: an admin certificate for everything that grants something.

use crate::config::Config;
use crate::net::{Answer, try_each};
use clap::Subcommand;
use raidish_ca::api::{
    EnrollmentCreated, EnrollmentRequest, Issued, RenewRequest, SignRequest, UnsealRequest,
};
use raidish_types::pki::{PkiStatus, ProfileKind, TokenId};
use raidish_types::types::NodeId;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;
use zeroize::Zeroizing;

#[derive(Subcommand)]
pub enum CaCommands {
    /// Make the cluster CA, on node 0 with its config, before its first
    /// `listen` (or with every node stopped, when moving to it). Writes
    /// node 0's certificate to its `tls:` paths, an admin certificate,
    /// `root.enc`, and stages the CA in node 0's data dir; prints the
    /// recovery keys and a `raidish join` command for every other node.
    Init {
        /// A name node 0 is reached by (repeatable; the first is its CN).
        /// Default: `services.advertise`, else the host name.
        #[arg(long = "san")]
        sans: Vec<String>,
        /// Names for a peer's certificate on top of its host in the pool
        /// config: `node-1=10.0.0.2` (repeatable; commas for several).
        #[arg(long = "peer-san")]
        peer_sans: Vec<String>,
        /// Node certificates' lifetime in days (default 30; until CA-4
        /// renews them live, `ca renew` and a restart).
        #[arg(long)]
        node_days: Option<u32>,
        /// Don't leave the one-shot unseal file: run `ca unseal` after
        /// node 0 starts.
        #[arg(long)]
        no_unseal_once: bool,
        /// Where to write the sealed root key (keep it off the cluster).
        #[arg(long, default_value = "root.enc")]
        root_out: PathBuf,
        /// Where to write the operator's admin certificate bundle.
        #[arg(long, default_value = "pki-admin")]
        admin_out: PathBuf,
        #[arg(long, default_value = "admin")]
        admin_name: String,
        /// How long the peers' join tokens last, in hours.
        #[arg(long, default_value_t = 72)]
        token_hours: u64,
        /// Also write each peer's join token to `<dir>/<node>.token`.
        #[arg(long)]
        tokens_out: Option<PathBuf>,
        /// The port `listen` serves on (to find this node in the pool).
        #[arg(short, long)]
        port: Option<u16>,
        /// Replace a staged CA and node 0's certificate files.
        #[arg(long)]
        force: bool,
    },
    /// The CA as a node sees it, and whether that node can sign.
    Status {
        /// Ask this node (default: the config's nodes, first that answers).
        #[arg(long)]
        url: Vec<String>,
    },
    /// Give nodes the key to sign with (after a restart of the cluster):
    /// the recovery key `ca init` showed, typed or on stdin.
    Unseal {
        /// Unseal this node only (default: every node in the config).
        #[arg(long)]
        url: Vec<String>,
        /// Read the recovery key from stdin.
        #[arg(long)]
        stdin: bool,
    },
    /// Join tokens for nodes.
    Token {
        #[command(subcommand)]
        command: CaTokenCommands,
    },
    /// Issue a certificate (a key made here, signed by the cluster).
    Issue {
        /// Its name (CN): who an admin or readonly cert is; the host of a
        /// libvirt or server cert.
        cn: String,
        /// admin, readonly, client, server or libvirt.
        #[arg(long)]
        profile: ProfileKind,
        /// The directory to write it to (`ca.pem`, `cert.pem`, `key.pem`;
        /// for libvirt, the `etc/pki/…` tree to copy to `/` on that host).
        #[arg(long)]
        out: PathBuf,
        /// More host names or addresses (server and libvirt).
        #[arg(long = "san")]
        sans: Vec<String>,
        #[arg(long)]
        days: Option<u32>,
        /// For libvirt: the node it's for (`node-1`).
        #[arg(long)]
        member: Option<String>,
        #[arg(long)]
        url: Vec<String>,
        #[arg(long)]
        force: bool,
    },
    /// Renew this config's own certificate (a node's, or an admin's) in
    /// place; restart `listen` afterwards.
    Renew {
        #[arg(long)]
        url: Vec<String>,
    },
}

#[derive(Subcommand)]
pub enum CaTokenCommands {
    /// A single-use join token for a node.
    Create {
        /// Which node (`node-1`, or `1`).
        #[arg(long)]
        member: String,
        /// Names for its certificate on top of its host in the pool config.
        #[arg(long = "san")]
        sans: Vec<String>,
        /// It replaces a node that has a certificate (a reinstalled host):
        /// that node's certificates are revoked when this one joins.
        #[arg(long)]
        replace: bool,
        /// …revoked as compromised.
        #[arg(long)]
        compromised: bool,
        /// How long it lasts, in hours (default 24).
        #[arg(long)]
        hours: Option<u64>,
        #[arg(long)]
        url: Vec<String>,
    },
    /// Withdraw a join token by its id.
    Cancel {
        id: String,
        #[arg(long)]
        url: Vec<String>,
    },
}

fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("Error: {}", msg);
    std::process::exit(1);
}

/// `node-1` or `1`.
fn parse_member(s: &str) -> Result<NodeId, String> {
    let s = s.trim();
    s.parse::<NodeId>()
        .ok()
        .or_else(|| s.parse::<u64>().ok().map(NodeId::from_raw))
        .ok_or_else(|| format!("'{}': name a node as node-<n>", s))
}

fn is_loopback(host: &str) -> bool {
    host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn hostname() -> Option<String> {
    let mut buf = [0u8; 256];
    // SAFETY: gethostname writes at most buf.len() bytes into buf.
    let r = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    if r != 0 {
        return None;
    }
    let end = buf.iter().position(|b| *b == 0).unwrap_or(buf.len());
    let h = String::from_utf8_lossy(&buf[..end]).trim().to_string();
    (!h.is_empty()).then_some(h)
}

/// `ca init` (before `tls::init`: node 0 has no certificate yet).
pub fn init(command: &CaCommands, config: &Config) {
    let CaCommands::Init {
        sans,
        peer_sans,
        node_days,
        no_unseal_once,
        root_out,
        admin_out,
        admin_name,
        token_hours,
        tokens_out,
        port,
        force,
    } = command
    else {
        unreachable!()
    };
    let tls = config
        .tls
        .as_ref()
        .unwrap_or_else(|| fail("the config has no `tls:` section to put node 0's certificate in"));
    let (Some(ca_path), Some(cert_path), Some(key_path)) =
        (tls.ca_path(), tls.cert_path(), tls.key_path())
    else {
        fail("tls: set `dir` (or `ca`, `cert` and `key`)")
    };
    if cert_path.exists() && !force {
        fail(format!(
            "{} exists: move the old certificates aside first (or --force)",
            cert_path.display()
        ));
    }
    let port = port.unwrap_or(8881);
    let members = config
        .cluster_members(port)
        .unwrap_or_else(|e| fail(format!("cluster: {}", e)));
    if members.me != NodeId::from_raw(0) {
        eprintln!(
            "note: this config is {}'s, not node-0's: it will stage the CA and join the others",
            members.me
        );
    }
    let mut names: Vec<String> = sans.clone();
    if names.is_empty() {
        if let Some(a) = config.services.as_ref().and_then(|s| s.advertise.clone()) {
            names.push(a);
        }
        if let Some(h) = hostname() {
            names.push(h);
        }
    }
    let names: Vec<String> = names
        .into_iter()
        .map(|n| n.trim().to_ascii_lowercase())
        .filter(|n| !n.is_empty())
        .collect();
    if !names.iter().any(|n| !is_loopback(n)) {
        fail(
            "node 0 needs a name its peers reach it by: pass --san <host> \
             (as in the other nodes' pool configs)",
        );
    }
    let mut peers: BTreeMap<NodeId, Vec<String>> = BTreeMap::new();
    for p in peer_sans {
        let (m, list) = p
            .split_once('=')
            .unwrap_or_else(|| fail(format!("--peer-san '{}': node-1=10.0.0.2", p)));
        let m = parse_member(m).unwrap_or_else(|e| fail(e));
        peers.entry(m).or_default().extend(
            list.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
        );
    }
    let data_dir = config.data_dir(&members.me.name());
    let out = raidish_ca::init::init(&raidish_ca::init::InitOptions {
        members: &members,
        data_dir: &data_dir,
        sans: names,
        peer_sans: peers,
        node_days: *node_days,
        admin_name: admin_name.clone(),
        token_ttl_ms: token_hours * 3600 * 1000,
        unseal_once: !no_unseal_once,
        force: *force,
        kdf: raidish_ca::init::KDF,
    })
    .unwrap_or_else(|e| fail(e));
    out.node0
        .write(&ca_path, &cert_path, &key_path)
        .unwrap_or_else(|e| fail(e));
    out.admin.write_dir(admin_out).unwrap_or_else(|e| fail(e));
    let env = serde_json::to_vec_pretty(&out.root_envelope).unwrap();
    raidish_ca::write_file(root_out, &env, 0o600).unwrap_or_else(|e| fail(e));
    let node0_url = format!("https://{}:{}", out.node0_sans[0], port);

    println!("Cluster CA created (root {}).", out.root_fingerprint);
    println!(
        "  {} certificate: {} ({})",
        members.me,
        cert_path.display(),
        out.node0_sans.join(", ")
    );
    println!(
        "  admin certificate '{}': {}/",
        admin_name,
        admin_out.display()
    );
    println!(
        "  root key, sealed: {}  (keep it off the cluster)",
        root_out.display()
    );
    println!(
        "  staged in {}",
        raidish_ca::bootstrap_path(&data_dir).display()
    );
    println!();
    println!("RECOVERY KEY (unseals the CA after a full restart: `raidish ca unseal`):");
    println!("  {}", *out.recovery_key);
    println!(
        "ROOT KEY (opens {}, to make a new issuer in two years):",
        root_out.display()
    );
    println!("  {}", *out.root_key);
    println!("Write both down now; they are not stored anywhere.");
    println!();
    if *no_unseal_once {
        println!(
            "After starting {}, run `raidish ca unseal` against it.",
            members.me
        );
    } else {
        println!(
            "{} unseals itself on its first start (one-shot key file).",
            members.me
        );
    }
    if !out.tokens.is_empty() {
        println!(
            "Start {} (`raidish listen`), then on each other node, before its first start:",
            members.me
        );
    }
    if let Some(dir) = tokens_out {
        std::fs::create_dir_all(dir).unwrap_or_else(|e| fail(format!("{}: {}", dir.display(), e)));
    }
    for t in &out.tokens {
        println!(
            "  {} ({}):\n    echo '{}' | raidish join {} --token-stdin -c <its config>",
            t.member, t.base, *t.token, node0_url
        );
        if let Some(dir) = tokens_out {
            let p = dir.join(format!("{}.token", t.member));
            raidish_ca::write_file(&p, t.token.as_bytes(), 0o600).unwrap_or_else(|e| fail(e));
        }
    }
    if !out.tokens.is_empty() {
        println!(
            "The tokens are single-use and last {} hours (`raidish ca token create` makes more).",
            token_hours
        );
    }
}

/// `raidish join` (before `tls::init`: this node has no certificate yet).
pub async fn join(
    url: &str,
    token_stdin: bool,
    token_file: Option<&Path>,
    port: Option<u16>,
    force: bool,
    config: &Config,
) {
    let text = match (token_stdin, token_file) {
        (_, Some(p)) => Zeroizing::new(
            std::fs::read_to_string(p).unwrap_or_else(|e| fail(format!("{}: {}", p.display(), e))),
        ),
        (true, None) => {
            let mut s = Zeroizing::new(String::new());
            std::io::stdin()
                .read_to_string(&mut s)
                .unwrap_or_else(|e| fail(e));
            s
        }
        (false, None) => fail("pass the token with --token-stdin (or --token-file)"),
    };
    let token = raidish_ca::token::JoinToken::parse(&text).unwrap_or_else(|e| fail(e));
    let tls = config
        .tls
        .as_ref()
        .unwrap_or_else(|| fail("the config has no `tls:` section to put the certificate in"));
    let (Some(ca_path), Some(cert_path), Some(key_path)) =
        (tls.ca_path(), tls.cert_path(), tls.key_path())
    else {
        fail("tls: set `dir` (or `ca`, `cert` and `key`)")
    };
    if cert_path.exists() && !force {
        fail(format!(
            "{} exists: this node has a certificate already (--force replaces it)",
            cert_path.display()
        ));
    }
    let members = config
        .cluster_members(port.unwrap_or(8881))
        .unwrap_or_else(|e| fail(format!("cluster: {}", e)));
    let joined = raidish_ca::join::join(url, &token, members.me, &members.fingerprint)
        .await
        .unwrap_or_else(|e| fail(e));
    joined
        .bundle
        .write(&ca_path, &cert_path, &key_path)
        .unwrap_or_else(|e| fail(e));
    println!(
        "joined as {}: certificate {} in {}, until {}",
        members.me,
        joined.serial,
        cert_path.display(),
        when(joined.not_after_ms)
    );
    println!("now start it: raidish listen");
}

fn when(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or("?".into())
}

fn endpoints(urls: &[String], config: &Config) -> Vec<String> {
    if !urls.is_empty() {
        return urls
            .iter()
            .map(|u| u.trim_end_matches('/').to_string())
            .collect();
    }
    let mut eps = config.lease_endpoints().unwrap_or_else(|e| fail(e));
    if eps.is_empty() {
        eps.push(format!("{}://127.0.0.1:8881", crate::tls::scheme()));
    }
    eps
}

fn http() -> reqwest::Client {
    crate::tls::http_client(Some(Duration::from_secs(60))).unwrap_or_else(|e| fail(e))
}

/// Send what `req` builds to each endpoint until one answers: a `503`
/// (sealed, no CA yet) or no answer means the next.
async fn ask<T: serde::de::DeserializeOwned>(
    eps: &[String],
    req: impl Fn(&reqwest::Client, &str) -> reqwest::RequestBuilder,
) -> Result<T, String> {
    let client = http();
    let r = try_each(eps.iter().map(String::as_str), |ep| {
        let sent = req(&client, ep).send();
        async move {
            match sent.await {
                Err(e) => Answer::Unreachable(e.to_string()),
                Ok(r) if r.status().as_u16() == 503 => {
                    Answer::Unreachable(r.text().await.unwrap_or_default().trim().to_string())
                }
                Ok(r) if r.status().is_success() => match r.json::<T>().await {
                    Ok(v) => Answer::Got(v),
                    Err(e) => Answer::Unreachable(e.to_string()),
                },
                Ok(r) => {
                    let status = r.status();
                    Answer::Refused(format!(
                        "{}: {}",
                        status,
                        r.text().await.unwrap_or_default().trim()
                    ))
                }
            }
        }
    })
    .await;
    r.map_err(|e| e.to_string())
}

fn read_secret(prompt: &str, stdin: bool) -> Zeroizing<String> {
    let mut s = Zeroizing::new(String::new());
    // SAFETY: isatty on a valid fd.
    let tty = unsafe { libc::isatty(0) } == 1;
    if stdin || !tty {
        std::io::stdin()
            .read_line(&mut s)
            .unwrap_or_else(|e| fail(e));
        return s;
    }
    eprint!("{}", prompt);
    // No echo while it's typed.
    // SAFETY: termios on stdin, restored below.
    let mut old: libc::termios = unsafe { std::mem::zeroed() };
    let have = unsafe { libc::tcgetattr(0, &mut old) } == 0;
    if have {
        let mut quiet = old;
        quiet.c_lflag &= !libc::ECHO;
        unsafe { libc::tcsetattr(0, libc::TCSANOW, &quiet) };
    }
    let r = std::io::stdin().read_line(&mut s);
    if have {
        unsafe { libc::tcsetattr(0, libc::TCSANOW, &old) };
    }
    eprintln!();
    r.unwrap_or_else(|e| fail(e));
    s
}

fn print_status(s: &PkiStatus) {
    println!(
        "{}: {}{}",
        s.node,
        if s.unsealed {
            "unsealed (can sign)"
        } else {
            "SEALED (can't sign)"
        },
        if s.staged {
            ", CA staged (no quorum yet)"
        } else {
            ""
        }
    );
    match &s.pki {
        None => println!("  no CA"),
        Some(p) => {
            println!("  root {}", p.anchor_fingerprint);
            for i in &p.issuers {
                println!(
                    "  {} ({:?}), serial {}, until {}",
                    i.id,
                    i.status,
                    i.serial,
                    when(i.not_after_ms)
                );
            }
            for slot in &p.slots {
                println!("  key slot {}: {} ({})", slot.id, slot.label, slot.kind);
            }
            println!(
                "  {} certificates on record, {} revoked; {} join tokens outstanding",
                p.certs, p.revoked_certs, p.enrollments
            );
        }
    }
    for w in &s.warnings {
        println!("  WARNING: {}", w);
    }
}

/// The `ca` commands that talk to running nodes.
pub async fn run(command: &CaCommands, config: &Config) {
    match command {
        CaCommands::Init { .. } => unreachable!("handled before TLS"),
        CaCommands::Status { url } => {
            let s: PkiStatus = ask(&endpoints(url, config), |c, ep| {
                c.get(format!("{}/api/v1/pki/status", ep))
            })
            .await
            .unwrap_or_else(|e| fail(e));
            print_status(&s);
        }
        CaCommands::Unseal { url, stdin } => {
            let key = read_secret("recovery key: ", *stdin);
            let body = UnsealRequest {
                recovery_key: key.trim().to_string(),
            };
            let client = http();
            let mut ok = 0;
            for ep in endpoints(url, config) {
                let r = client
                    .post(format!("{}/api/v1/pki/unseal", ep))
                    .json(&body)
                    .send()
                    .await;
                match r {
                    Ok(r) if r.status().is_success() => match r.json::<PkiStatus>().await {
                        Ok(s) => {
                            ok += 1;
                            println!("{}: unsealed", s.node);
                        }
                        Err(e) => eprintln!("{}: {}", ep, e),
                    },
                    Ok(r) => {
                        let st = r.status();
                        eprintln!(
                            "{}: {}: {}",
                            ep,
                            st,
                            r.text().await.unwrap_or_default().trim()
                        )
                    }
                    Err(e) => eprintln!("{}: {}", ep, e),
                }
            }
            if ok == 0 {
                fail("no node unsealed");
            }
        }
        CaCommands::Token { command } => match command {
            CaTokenCommands::Create {
                member,
                sans,
                replace,
                compromised,
                hours,
                url,
            } => {
                let req = EnrollmentRequest {
                    member: parse_member(member).unwrap_or_else(|e| fail(e)),
                    extra_sans: sans.clone(),
                    replace: *replace,
                    compromised: *compromised,
                    ttl_secs: hours.map(|h| h * 3600),
                };
                let eps = endpoints(url, config);
                let e: EnrollmentCreated = ask(&eps, |c, ep| {
                    c.post(format!("{}/api/v1/pki/enrollments", ep)).json(&req)
                })
                .await
                .unwrap_or_else(|e| fail(e));
                eprintln!(
                    "join token {} for {}, single-use, until {}{}",
                    e.id,
                    e.member,
                    when(e.expires_ms),
                    if *replace {
                        " (its old certificates are revoked when it joins)"
                    } else {
                        ""
                    }
                );
                eprintln!(
                    "on {}: raidish join <a node's https URL> --token-stdin",
                    e.member
                );
                println!("{}", e.token);
            }
            CaTokenCommands::Cancel { id, url } => {
                let id = TokenId::parse(id).unwrap_or_else(|e| fail(e));
                let client = http();
                let eps = endpoints(url, config);
                let r = try_each(eps.iter().map(String::as_str), |ep| {
                    let sent = client
                        .delete(format!("{}/api/v1/pki/enrollments/{}", ep, id))
                        .send();
                    async move {
                        match sent.await {
                            Err(e) => Answer::Unreachable(e.to_string()),
                            Ok(r) if r.status().as_u16() == 503 => {
                                Answer::Unreachable(r.text().await.unwrap_or_default())
                            }
                            Ok(r) if r.status().is_success() => Answer::Got(()),
                            Ok(r) => Answer::Refused(r.text().await.unwrap_or_default()),
                        }
                    }
                })
                .await;
                r.unwrap_or_else(|e| fail(e));
                println!("join token {} withdrawn", id);
            }
        },
        CaCommands::Issue {
            cn,
            profile,
            out,
            sans,
            days,
            member,
            url,
            force,
        } => {
            let libvirt = *profile == ProfileKind::Libvirt;
            let first = if libvirt {
                out.join(crate::pki::LIBVIRT_FILES[1].0)
            } else {
                out.join("cert.pem")
            };
            if first.exists() && !force {
                fail(format!("{} exists (--force replaces it)", first.display()));
            }
            let (key_pem, csr_pem) = raidish_auth::ca::generate_csr(cn).unwrap_or_else(|e| fail(e));
            let req = SignRequest {
                csr_pem,
                profile: *profile,
                cn: cn.clone(),
                sans: sans.clone(),
                days: *days,
                member: member
                    .as_deref()
                    .map(|m| parse_member(m).unwrap_or_else(|e| fail(e))),
            };
            let issued: Issued = ask(&endpoints(url, config), |c, ep| {
                c.post(format!("{}/api/v1/pki/sign", ep)).json(&req)
            })
            .await
            .unwrap_or_else(|e| fail(e));
            if libvirt {
                write_libvirt(out, &issued, &key_pem).unwrap_or_else(|e| fail(e));
            } else {
                raidish_ca::Bundle {
                    ca_pem: issued.ca_pem.clone(),
                    cert_pem: issued.cert_pem.clone(),
                    key_pem,
                }
                .write_dir(out)
                .unwrap_or_else(|e| fail(e));
            }
            println!(
                "{} certificate for '{}' ({}), until {}: {}/",
                profile,
                cn,
                issued.serial,
                when(issued.not_after_ms),
                out.display()
            );
            if libvirt {
                println!("copy its etc/ to / on that host, then restart libvirtd");
            }
        }
        CaCommands::Renew { url } => {
            let tls = config
                .tls
                .as_ref()
                .unwrap_or_else(|| fail("the config has no `tls:` section"));
            let (Some(cert_path), Some(key_path)) = (tls.cert_path(), tls.key_path()) else {
                fail("tls: no certificate configured");
            };
            let current = crate::pki::read_pem_certs(
                &std::fs::read_to_string(&cert_path)
                    .unwrap_or_else(|e| fail(format!("{}: {}", cert_path.display(), e))),
            )
            .unwrap_or_else(|e| fail(e));
            let name = current
                .first()
                .and_then(|d| crate::pki::inspect_der(d).ok())
                .map(|i| i.name)
                .unwrap_or_default();
            let (key_pem, csr_pem) =
                raidish_auth::ca::generate_csr(&name).unwrap_or_else(|e| fail(e));
            let issued: Issued = ask(&endpoints(url, config), |c, ep| {
                c.post(format!("{}/api/v1/pki/renew", ep))
                    .json(&RenewRequest {
                        csr_pem: csr_pem.clone(),
                    })
            })
            .await
            .unwrap_or_else(|e| fail(e));
            // Each file whole (renamed into place); `listen` reads them
            // only when it starts.
            raidish_ca::write_file(&key_path, key_pem.as_bytes(), 0o600)
                .unwrap_or_else(|e| fail(e));
            raidish_ca::write_file(&cert_path, issued.cert_pem.as_bytes(), 0o644)
                .unwrap_or_else(|e| fail(e));
            println!(
                "renewed '{}': certificate {} until {} in {}",
                name,
                issued.serial,
                when(issued.not_after_ms),
                cert_path.display()
            );
            println!("restart `raidish listen` to serve it");
        }
    }
}

/// A libvirt certificate laid out as libvirt and qemu read theirs.
///
/// GnuTLS (libvirt, qemu) checks a certificate against the CA file alone,
/// so that file has the issuer as well as the root: the certificate files
/// hold the leaf and its issuer, and the issuer is what signed the leaf.
fn write_libvirt(out: &Path, issued: &Issued, key_pem: &str) -> Result<(), String> {
    // The chain's PEM after the leaf: its issuer.
    const END: &str = "-----END CERTIFICATE-----";
    let issuers = issued
        .cert_pem
        .find(END)
        .map(|i| issued.cert_pem[i + END.len()..].trim_start())
        .unwrap_or("");
    let ca = format!("{}\n{}", issued.ca_pem.trim_end(), issuers);
    for (path, secret) in crate::pki::LIBVIRT_FILES {
        let is_ca = path.ends_with("/cacert.pem") || path.ends_with("/ca-cert.pem");
        let (body, mode) = match (*secret, is_ca) {
            (true, _) => (key_pem, 0o600),
            (false, true) => (ca.as_str(), 0o644),
            (false, false) => (issued.cert_pem.as_str(), 0o644),
        };
        raidish_ca::write_file(&out.join(path), body.as_bytes(), mode)?;
    }
    use std::os::unix::fs::PermissionsExt;
    for (path, _) in crate::pki::LIBVIRT_FILES {
        let mut d = out.join(path);
        while d.pop() && d != out {
            std::fs::set_permissions(&d, std::fs::Permissions::from_mode(0o755))
                .map_err(|e| format!("{}: {}", d.display(), e))?;
        }
    }
    Ok(())
}
