//! A throwaway cluster PKI for tests: a CA, a node certificate (`n1`) and
//! an admin one, the node's TLS config and a token verifier.

use crate::api_types::Role;
use std::sync::Arc;

pub struct Pki {
    _dir: tempfile::TempDir,
    /// The CA's directory (each certificate in a subdirectory by name).
    pub dir: std::path::PathBuf,
    /// Node `n1`'s TLS config.
    pub tls: crate::tls::TlsConfig,
    pub verifier: Arc<crate::auth::Verifier>,
}

impl Pki {
    /// The PKI, with the token ids in `revoked` refused by the verifier.
    pub fn new(revoked: Vec<String>) -> Self {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().to_path_buf();
        crate::pki::Ca::init(&dir, "ca", None, false).unwrap();
        let ca = crate::pki::Ca::load(&dir).unwrap();
        ca.issue("n1", Role::Node, &[], None, false).unwrap();
        ca.issue("admin", Role::Admin, &[], None, false).unwrap();
        let tls = crate::tls::TlsConfig {
            dir: Some(dir.join("n1").to_string_lossy().into()),
            ..Default::default()
        };
        let verifier = Arc::new(crate::auth::Verifier::new(tls.roots().unwrap(), revoked).unwrap());
        Pki {
            _dir: d,
            dir,
            tls,
            verifier,
        }
    }

    /// An HTTPS client trusting the CA.
    pub fn http(&self) -> reqwest::Client {
        let pem = std::fs::read(self.dir.join("n1").join("ca.pem")).unwrap();
        reqwest::Client::builder()
            .use_rustls_tls()
            .add_root_certificate(reqwest::Certificate::from_pem(&pem).unwrap())
            .build()
            .unwrap()
    }
}
