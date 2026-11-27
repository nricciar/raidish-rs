//! Open connections by peer serial, so a revocation can end them.
//!
//! A connection registers when its handshake completes and every request
//! on it carries the registration as a [`Peer`]. The entry lives as long
//! as any clone of it does — not just as long as the HTTP connection: a
//! WebSocket upgrade hands the socket out of hyper, which then considers
//! the connection finished, so the stream's task has to hold the `Peer`
//! and watch [`Peer::closed`] itself.

use raidish_types::pki::{KeyId, Serial};
use rustls::pki_types::CertificateDer;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

/// Per serial: each registration's id, its issuer's key id, and its
/// close signal.
type Entries = HashMap<Serial, HashMap<u64, (Option<KeyId>, watch::Sender<bool>)>>;

#[derive(Default)]
pub struct Connections {
    next: AtomicU64,
    open: Arc<Mutex<Entries>>,
}

/// One connection's client certificate, and its registration.
#[derive(Clone)]
pub struct Peer(Arc<Registration>);

struct Registration {
    der: CertificateDer<'static>,
    serial: Serial,
    id: u64,
    closed: watch::Receiver<bool>,
    open: Arc<Mutex<Entries>>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut open = self.open.lock().unwrap();
        if let Some(m) = open.get_mut(&self.serial) {
            m.remove(&self.id);
            if m.is_empty() {
                open.remove(&self.serial);
            }
        }
    }
}

impl Peer {
    /// The leaf the client presented.
    pub fn cert(&self) -> &CertificateDer<'static> {
        &self.0.der
    }

    pub fn serial(&self) -> &Serial {
        &self.0.serial
    }

    /// Resolves once the connection is to end ([`Connections::close`]).
    pub async fn closed(&self) {
        let mut rx = self.0.closed.clone();
        let _ = rx.wait_for(|c| *c).await;
    }
}

impl Connections {
    pub fn new() -> Arc<Connections> {
        Arc::new(Connections::default())
    }

    /// Register a connection whose client presented `der`. None: not a
    /// certificate we can read (the verifier passed it, so not expected).
    pub fn register(&self, der: CertificateDer<'static>) -> Option<Peer> {
        let serial = crate::ca::facts(&der).ok()?.serial;
        let aki = crate::ca::authority_key_id(&der);
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = watch::channel(false);
        self.open
            .lock()
            .unwrap()
            .entry(serial.clone())
            .or_default()
            .insert(id, (aki, tx));
        Some(Peer(Arc::new(Registration {
            der,
            serial,
            id,
            closed: rx,
            open: self.open.clone(),
        })))
    }

    /// End every connection and stream of `serial`. How many there were.
    pub fn close(&self, serial: &Serial) -> usize {
        let open = self.open.lock().unwrap();
        let Some(m) = open.get(serial) else {
            return 0;
        };
        for (_, tx) in m.values() {
            let _ = tx.send(true);
        }
        m.len()
    }

    /// End every connection and stream whose certificate the issuer with
    /// key id `ski` signed. How many there were.
    pub fn close_issued_by(&self, ski: &KeyId) -> usize {
        let open = self.open.lock().unwrap();
        let mut n = 0;
        for (aki, tx) in open.values().flat_map(|m| m.values()) {
            if aki.as_ref() == Some(ski) {
                let _ = tx.send(true);
                n += 1;
            }
        }
        n
    }

    /// How many are open for `serial`.
    pub fn count(&self, serial: &Serial) -> usize {
        self.open.lock().unwrap().get(serial).map_or(0, |m| m.len())
    }
}
