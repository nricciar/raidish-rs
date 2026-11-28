//! `raidish token …`: tokens for the dashboard and scripts, minted by the
//! cluster CA's token signer (`rdt2`). `rdt1` tokens, from before the CA,
//! are still verified (`token inspect`, and every node) but no longer made.

use crate::pki::Role;
use clap::Subcommand;

fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("Error: {}", msg);
    std::process::exit(1);
}

#[derive(Subcommand)]
pub enum TokenCommands {
    /// Mint a token (for the dashboard, or `Authorization: Bearer`): asks
    /// the cluster's CA (by this config's admin certificate), which signs it
    /// with its token signer (an `rdt2` token).
    Create {
        /// Who it's for (shown in logs and `whoami`).
        name: String,
        /// readonly (the default) or admin.
        #[arg(long, default_value = "readonly")]
        role: Role,
        /// Lifetime, e.g. 90d, 12h, 30m. Default: as long as the CA's
        /// token signer lasts (up to two years).
        #[arg(long)]
        expires: Option<String>,
        #[arg(long)]
        url: Vec<String>,
    },
    /// Decode a token, and check it against this config's CA bundle.
    Inspect { token: String },
    /// Revoke a token (and the S3/SQS keys made from it) by its id
    /// (`token inspect` shows it): every node refuses it at once.
    Revoke {
        id: String,
        /// When it expires anyway (RFC 3339, `token inspect` shows it):
        /// the revocation is kept until then. Default: kept for good.
        #[arg(long)]
        expires: Option<String>,
        #[arg(long)]
        url: Vec<String>,
    },
    /// Make an S3/SQS access key (for AWS clients) from a token, or from
    /// this config's admin certificate. A token's key carries the token's
    /// id, role and expiry: revoking the token revokes the key.
    S3Key {
        /// The token to make it from (default: this config's certificate).
        #[arg(long)]
        token: Option<String>,
        /// readonly or admin (default: the token's role, or admin).
        #[arg(long)]
        role: Option<Role>,
        /// Lifetime, e.g. 90d (at most the token's).
        #[arg(long)]
        expires: Option<String>,
    },
}

/// `token s3-key`: ask a node for an access key.
pub async fn s3_key(command: &TokenCommands, config: &crate::config::Config) {
    let TokenCommands::S3Key {
        token,
        role,
        expires,
    } = command
    else {
        unreachable!()
    };
    if config.tls.is_none() {
        fail("S3 and SQS need TLS: set `tls:` in the config");
    }
    let endpoints = config.lease_endpoints().unwrap_or_else(|e| fail(e));
    if endpoints.is_empty() {
        fail("no cluster endpoints configured");
    }
    let req = serde_json::json!({
        "token": token,
        "role": role.map(|r| r.to_string()),
        "expires": expires,
    });
    let http = crate::tls::http_client(Some(std::time::Duration::from_secs(15)))
        .unwrap_or_else(|e| fail(e));
    use crate::net::{Answer, try_each};
    let r = try_each(endpoints.iter().map(String::as_str), |ep| {
        let sent = http
            .post(format!("{}/api/v1/services/keys", ep))
            .json(&req)
            .send();
        async move {
            match sent.await {
                Ok(r) if r.status().is_success() => Answer::Got(r),
                Ok(r) => {
                    let status = r.status();
                    Answer::Refused(format!(
                        "{}: {}",
                        status,
                        r.text().await.unwrap_or_default()
                    ))
                }
                Err(e) => Answer::Unreachable(e.to_string()),
            }
        }
    })
    .await
    .unwrap_or_else(|e| fail(e));
    let k: crate::frontend::routes::KeyResponse = r.json().await.unwrap_or_else(|e| fail(e));
    eprintln!(
        "access key for {} ({}), expires {}; revoked with id {}",
        if token.is_some() {
            "the token"
        } else {
            "this certificate"
        },
        k.role,
        k.expires.map(when).unwrap_or("never".into()),
        k.id
    );
    println!("AWS_ACCESS_KEY_ID={}", k.access_key_id);
    println!("AWS_SECRET_ACCESS_KEY={}", k.secret_access_key);
}

fn when(s: i64) -> String {
    chrono::DateTime::from_timestamp(s, 0)
        .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or("?".into())
}

pub fn run_token(command: &TokenCommands, config: &crate::config::Config) {
    match command {
        TokenCommands::Create { .. }
        | TokenCommands::S3Key { .. }
        | TokenCommands::Revoke { .. } => unreachable!("async"),
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
                if t.is_rdt2() {
                    "the CA's token signer".into()
                } else {
                    t.signer.role.map(|r| r.to_string()).unwrap_or("?".into())
                },
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
