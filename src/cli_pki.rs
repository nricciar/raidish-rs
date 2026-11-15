//! `raidish cert …`: the cluster CA from the command line (see `crate::pki`).

use crate::pki::{self, Ca, CertInfo, Role};
use clap::Subcommand;
use std::path::{Path, PathBuf};

#[derive(Subcommand)]
pub enum CertCommands {
    /// Create a new CA: `<dir>/ca.key` (keep it safe; only `cert` commands
    /// need it) and the trust bundle `<dir>/ca.pem`.
    Init {
        #[arg(long, default_value = "pki")]
        dir: PathBuf,
        /// The CA's name (its certificate's CN).
        #[arg(long, default_value = "raidish CA")]
        name: String,
        /// Lifetime in days (default: never expires).
        #[arg(long)]
        days: Option<u32>,
        /// Replace an existing CA.
        #[arg(long)]
        force: bool,
    },
    /// Issue a certificate into `<dir>/<name>/` (ca.pem, cert.pem, key.pem),
    /// ready to copy to the node or client as its `tls.dir`.
    Issue {
        name: String,
        /// node (a `listen` server), admin (the command line) or readonly.
        #[arg(long)]
        role: Role,
        /// Extra host names or IP addresses the node is reached by (its
        /// name, localhost and 127.0.0.1 are always included).
        #[arg(long = "san")]
        sans: Vec<String>,
        #[arg(long, default_value = "pki")]
        dir: PathBuf,
        /// Lifetime in days (default: never expires).
        #[arg(long)]
        days: Option<u32>,
        /// Replace an existing certificate (rotation).
        #[arg(long)]
        force: bool,
    },
    /// Issue a node certificate for every host in the config's disk URLs
    /// (give it a config that lists every disk by URL, like a client's).
    IssueNodes {
        #[arg(long, default_value = "pki")]
        dir: PathBuf,
        #[arg(long)]
        days: Option<u32>,
        /// Reissue certificates that already exist.
        #[arg(long)]
        force: bool,
    },
    /// List the trusted CAs and every issued certificate, or show PEM files.
    Status {
        #[arg(long, default_value = "pki")]
        dir: PathBuf,
        /// Show these certificate files instead.
        files: Vec<PathBuf>,
    },
    /// Trust another CA too (CA rotation): append it to `<dir>/ca.pem` and
    /// every issued bundle's copy.
    Trust {
        /// PEM file with the CA certificate(s).
        ca: PathBuf,
        #[arg(long, default_value = "pki")]
        dir: PathBuf,
    },
    /// Stop trusting a CA (by name or fingerprint), in `<dir>/ca.pem` and
    /// every issued bundle's copy.
    Untrust {
        which: String,
        #[arg(long, default_value = "pki")]
        dir: PathBuf,
    },
}

fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("Error: {}", msg);
    std::process::exit(1);
}

fn print_cert(c: &CertInfo) {
    let role = if c.is_ca {
        "CA".to_string()
    } else {
        c.role.map(|r| r.to_string()).unwrap_or_else(|| "?".into())
    };
    let sans = if c.sans.is_empty() {
        String::new()
    } else {
        format!("  [{}]", c.sans.join(", "))
    };
    println!(
        "  {:<20} {:<9} expires {:<11} {}{}",
        c.name,
        role,
        c.expiry(),
        c.fingerprint,
        sans
    );
}

/// The paths of a config's pool disks (for `issue-nodes`).
pub type PoolPaths = dyn Fn() -> Vec<String>;

pub fn run(command: &CertCommands, pool_paths: &PoolPaths) {
    match command {
        CertCommands::Init {
            dir,
            name,
            days,
            force,
        } => {
            let ca = Ca::init(dir, name, *days, *force).unwrap_or_else(|e| fail(e));
            println!("created CA in {}:", dir.display());
            print_cert(&ca);
            println!(
                "\nnext: raidish cert issue-nodes --dir {d}   (or: cert issue <host> --role node)\n      raidish cert issue admin --role admin --dir {d}",
                d = dir.display()
            );
        }
        CertCommands::Issue {
            name,
            role,
            sans,
            dir,
            days,
            force,
        } => {
            let ca = Ca::load(dir).unwrap_or_else(|e| fail(e));
            if *role != Role::Node && !sans.is_empty() {
                fail("--san only applies to node certificates");
            }
            let c = ca
                .issue(name, *role, sans, *days, *force)
                .unwrap_or_else(|e| fail(e));
            print_cert(&c);
            print_next(dir, name, *role);
        }
        CertCommands::IssueNodes { dir, days, force } => {
            let ca = Ca::load(dir).unwrap_or_else(|e| fail(e));
            let hosts = pki::hosts_in(&pool_paths());
            if hosts.is_empty() {
                fail("the config's pool has no disk URLs to take host names from");
            }
            for h in &hosts {
                if dir.join(h).join("cert.pem").exists() && !force {
                    println!("  {:<20} exists (--force to reissue)", h);
                    continue;
                }
                let c = ca
                    .issue(h, Role::Node, &[], *days, true)
                    .unwrap_or_else(|e| fail(e));
                print_cert(&c);
            }
            println!(
                "\ncopy each bundle to its node, e.g.  scp -r {}/{} {}:/etc/raidish/tls",
                dir.display(),
                hosts[0],
                hosts[0]
            );
        }
        CertCommands::Status { dir, files } => {
            if !files.is_empty() {
                for f in files {
                    println!("{}:", f.display());
                    let pem = std::fs::read_to_string(f)
                        .unwrap_or_else(|e| fail(format!("{}: {}", f.display(), e)));
                    for der in pki::read_pem_certs(&pem).unwrap_or_else(|e| fail(e)) {
                        print_cert(&pki::inspect_der(&der).unwrap_or_else(|e| fail(e)));
                    }
                }
                return;
            }
            let (cas, issued) = pki::status(dir).unwrap_or_else(|e| fail(e));
            println!("trusted CAs ({}/ca.pem):", dir.display());
            cas.iter().for_each(print_cert);
            println!("issued:");
            if issued.is_empty() {
                println!("  (none)");
            }
            for c in &issued {
                print_cert(c);
                if !cas.iter().any(|ca| ca.name == c.issuer) {
                    println!("    ^ issued by '{}', which is no longer trusted", c.issuer);
                }
            }
        }
        CertCommands::Trust { ca, dir } => {
            let pem = std::fs::read_to_string(ca)
                .unwrap_or_else(|e| fail(format!("{}: {}", ca.display(), e)));
            let added = pki::trust(dir, &pem).unwrap_or_else(|e| fail(e));
            if added.is_empty() {
                println!("already trusted");
            }
            for c in &added {
                print!("trusting");
                print_cert(c);
            }
            println!(
                "copy the updated ca.pem to every node and client, then restart them one at a time"
            );
        }
        CertCommands::Untrust { which, dir } => {
            let removed = pki::untrust(dir, which).unwrap_or_else(|e| fail(e));
            for c in &removed {
                print!("untrusted");
                print_cert(c);
            }
            println!(
                "copy the updated ca.pem to every node and client, then restart them one at a time"
            );
        }
    }
}

fn print_next(dir: &Path, name: &str, role: Role) {
    let bundle = dir.join(name);
    match role {
        Role::Node => println!(
            "\ncopy it to the node:  scp -r {} {}:/etc/raidish/tls\nand set in its config:  tls: {{ dir: /etc/raidish/tls }}",
            bundle.display(),
            name
        ),
        _ => println!(
            "\nuse it from the command line with, in ~/.raidish.yaml:  tls: {{ dir: {} }}",
            std::fs::canonicalize(&bundle).unwrap_or(bundle).display()
        ),
    }
}

#[derive(Subcommand)]
pub enum TokenCommands {
    /// Mint a token (for the dashboard, or `Authorization: Bearer`), signed
    /// with the admin certificate in this config's `tls:` section.
    Create {
        /// Who it's for (shown in logs and `whoami`).
        name: String,
        /// readonly (the default) or admin.
        #[arg(long, default_value = "readonly")]
        role: Role,
        /// Lifetime, e.g. 90d, 12h, 30m (default: never expires).
        #[arg(long)]
        expires: Option<String>,
    },
    /// Decode a token, and check it against this config's CA bundle.
    Inspect { token: String },
}

/// `90d`, `12h`, `30m`, `45s` or plain seconds.
pub fn parse_duration(s: &str) -> Result<i64, String> {
    let (num, mult) = match s.chars().last() {
        Some('d') => (&s[..s.len() - 1], 86400),
        Some('h') => (&s[..s.len() - 1], 3600),
        Some('m') => (&s[..s.len() - 1], 60),
        Some('s') => (&s[..s.len() - 1], 1),
        _ => (s, 1),
    };
    num.parse::<i64>()
        .ok()
        .filter(|n| *n > 0)
        .map(|n| n * mult)
        .ok_or_else(|| format!("bad duration '{}' (try 90d, 12h, 30m)", s))
}

fn when(s: i64) -> String {
    chrono::DateTime::from_timestamp(s, 0)
        .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or("?".into())
}

pub fn run_token(command: &TokenCommands, config: &crate::config::Config) {
    use rustls::pki_types::PrivateKeyDer;
    match command {
        TokenCommands::Create {
            name,
            role,
            expires,
        } => {
            let tls = config.tls.as_ref().unwrap_or_else(|| {
                fail("tokens are signed with an admin certificate: set `tls:` in the config")
            });
            let Some((chain, key)) = tls.identity().unwrap_or_else(|e| fail(e)) else {
                fail("the config's `tls:` has no cert.pem/key.pem to sign with");
            };
            let PrivateKeyDer::Pkcs8(key) = key else {
                fail("the signing key must be PKCS#8 (as `raidish cert issue` writes it)");
            };
            let ttl = expires
                .as_deref()
                .map(|e| parse_duration(e).unwrap_or_else(|e| fail(e)));
            let (token, claims) =
                crate::auth::create_token(name, *role, ttl, &chain[0], key.secret_pkcs8_der())
                    .unwrap_or_else(|e| fail(e));
            eprintln!(
                "token {} for '{}' ({}), expires {}",
                claims.id,
                claims.sub,
                claims.role,
                claims.exp.map(when).unwrap_or("never".into())
            );
            eprintln!("revoke it by adding its id to `auth: {{ revoked: [...] }}` on every node");
            println!("{}", token);
        }
        TokenCommands::Inspect { token } => {
            let t = crate::auth::parse_token(token).unwrap_or_else(|e| fail(e));
            println!("id:      {}", t.claims.id);
            println!("for:     {}", t.claims.sub);
            println!("role:    {}", t.claims.role);
            println!("issued:  {}", when(t.claims.iat));
            println!(
                "expires: {}",
                t.claims.exp.map(when).unwrap_or("never".into())
            );
            println!(
                "signer:  {} ({}, cert {})",
                t.signer.name,
                t.signer.role.map(|r| r.to_string()).unwrap_or("?".into()),
                t.signer.fingerprint
            );
            match &config.tls {
                None => println!("valid:   (not checked: the config has no `tls:`)"),
                Some(tls) => {
                    let v = crate::auth::Verifier::from_config(tls, &config.auth)
                        .unwrap_or_else(|e| fail(e));
                    match v.verify_token(token) {
                        Ok(_) => println!("valid:   yes"),
                        Err(e) => {
                            println!("valid:   NO: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
            }
        }
    }
}
