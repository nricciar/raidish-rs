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

/// Requests to an axum app in process (no socket), for route tests.
#[derive(Clone)]
pub struct Client {
    app: axum::Router,
}

/// One request being built (see [`Client`]).
pub struct Req {
    app: axum::Router,
    req: axum::http::request::Builder,
    body: axum::body::Body,
}

/// What came back.
pub struct Resp {
    pub status: axum::http::StatusCode,
    pub headers: axum::http::HeaderMap,
    pub body: axum::body::Bytes,
}

impl Client {
    /// `app` as served, minus TLS: `auth` as the server would have it.
    pub fn new(app: axum::Router, auth: crate::auth::AuthState) -> Client {
        Client {
            app: crate::http::finish(app, auth),
        }
    }

    pub fn req(&self, method: &str, uri: impl AsRef<str>) -> Req {
        Req {
            app: self.app.clone(),
            req: axum::http::Request::builder()
                .method(method)
                .uri(uri.as_ref()),
            body: axum::body::Body::empty(),
        }
    }
    pub fn get(&self, uri: impl AsRef<str>) -> Req {
        self.req("GET", uri)
    }
    pub fn post(&self, uri: impl AsRef<str>) -> Req {
        self.req("POST", uri)
    }
    pub fn put(&self, uri: impl AsRef<str>) -> Req {
        self.req("PUT", uri)
    }
    pub fn patch(&self, uri: impl AsRef<str>) -> Req {
        self.req("PATCH", uri)
    }
    pub fn delete(&self, uri: impl AsRef<str>) -> Req {
        self.req("DELETE", uri)
    }
}

impl Req {
    pub fn header(mut self, k: &str, v: impl AsRef<str>) -> Req {
        self.req = self.req.header(k, v.as_ref());
        self
    }
    pub fn body(mut self, b: impl AsRef<[u8]>) -> Req {
        self.body = axum::body::Body::from(b.as_ref().to_vec());
        self
    }
    pub fn json<T: serde::Serialize>(self, v: &T) -> Req {
        self.header("Content-Type", "application/json")
            .body(serde_json::to_vec(v).unwrap())
    }
    /// As [`Req::send`].
    pub async fn dispatch(self) -> Resp {
        self.send().await
    }

    pub async fn send(self) -> Resp {
        use tower::ServiceExt;
        let r = self
            .app
            .oneshot(self.req.body(self.body).unwrap())
            .await
            .unwrap();
        let status = r.status();
        let headers = r.headers().clone();
        let body = axum::body::to_bytes(r.into_body(), usize::MAX)
            .await
            .unwrap_or_default();
        Resp {
            status,
            headers,
            body,
        }
    }
}

impl Resp {
    pub fn status(&self) -> axum::http::StatusCode {
        self.status
    }
    pub async fn into_bytes(self) -> Option<Vec<u8>> {
        Some(self.body.to_vec())
    }
    pub async fn into_string(self) -> Option<String> {
        String::from_utf8(self.body.to_vec()).ok()
    }
    pub async fn into_json<T: serde::de::DeserializeOwned>(self) -> Option<T> {
        serde_json::from_slice(&self.body).ok()
    }
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("{}: not JSON ({}): {}", self.status, e, self.text()))
    }
    pub fn header(&self, k: &str) -> Option<&str> {
        self.headers.get(k).and_then(|v| v.to_str().ok())
    }
}
