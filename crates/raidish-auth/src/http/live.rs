//! A server certificate and a client verifier that can be replaced while
//! the server runs. rustls asks for them on every handshake, so a swap
//! applies to the next connection; connections already open are unaffected.

use rustls::client::danger::HandshakeSignatureValid;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};
use std::sync::{Arc, RwLock};

/// This server's certificate chain and key.
#[derive(Debug)]
pub struct LiveCert(RwLock<Arc<CertifiedKey>>);

fn certified(
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<CertifiedKey>, String> {
    let key = rustls::crypto::ring::sign::any_supported_type(&key)
        .map_err(|e| format!("tls: key: {}", e))?;
    Ok(Arc::new(CertifiedKey::new(chain, key)))
}

impl LiveCert {
    pub fn new(
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Arc<LiveCert>, String> {
        Ok(Arc::new(LiveCert(RwLock::new(certified(chain, key)?))))
    }

    /// Serve `chain` from the next handshake on.
    pub fn set(
        &self,
        chain: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<(), String> {
        let ck = certified(chain, key)?;
        *self.0.write().unwrap() = ck;
        Ok(())
    }

    /// The leaf being served now.
    pub fn leaf(&self) -> CertificateDer<'static> {
        self.0.read().unwrap().cert[0].clone()
    }
}

impl ResolvesServerCert for LiveCert {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.read().unwrap().clone())
    }
}

/// Which client certificates are accepted: those chaining to the current
/// roots. Clients without one are let through (tokens), as today.
#[derive(Debug)]
pub struct LiveTrust {
    inner: RwLock<Arc<dyn ClientCertVerifier>>,
    mandatory: bool,
}

fn verifier(
    roots: Vec<CertificateDer<'static>>,
    mandatory: bool,
) -> Result<Arc<dyn ClientCertVerifier>, String> {
    crate::tls::install_provider();
    let mut store = rustls::RootCertStore::empty();
    for c in roots {
        store.add(c).map_err(|e| format!("tls: bad CA: {}", e))?;
    }
    let b = WebPkiClientVerifier::builder(Arc::new(store));
    let b = if mandatory {
        b
    } else {
        b.allow_unauthenticated()
    };
    b.build().map_err(|e| format!("tls: {}", e))
}

impl LiveTrust {
    /// `mandatory`: refuse clients without a certificate.
    pub fn new(
        roots: Vec<CertificateDer<'static>>,
        mandatory: bool,
    ) -> Result<Arc<LiveTrust>, String> {
        Ok(Arc::new(LiveTrust {
            inner: RwLock::new(verifier(roots, mandatory)?),
            mandatory,
        }))
    }

    /// Trust `roots` from the next handshake on.
    pub fn set(&self, roots: Vec<CertificateDer<'static>>) -> Result<(), String> {
        let v = verifier(roots, self.mandatory)?;
        *self.inner.write().unwrap() = v;
        Ok(())
    }

    fn current(&self) -> Arc<dyn ClientCertVerifier> {
        self.inner.read().unwrap().clone()
    }
}

impl ClientCertVerifier for LiveTrust {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        self.mandatory
    }

    /// No CA names in the CertificateRequest. They're only a hint (they'd
    /// have to borrow from behind the lock), and our clients have one
    /// identity each and send it regardless.
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.current()
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.current().supported_verify_schemes()
    }
}
