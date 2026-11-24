//! What each node is, for running and moving VMs (NODE-1): its host, its
//! libvirt and qemu, and whether the other nodes' libvirt can be reached
//! the way a live migration would reach it.
//!
//! Every node collects its own (`Collector`) at startup and every few
//! minutes, and stores them, when they changed, in the pool's `.vms` tree
//! next to the VM definitions (`crate::vm_store`, key `N<node>`), through
//! the committer. `crate::vm_check` turns two nodes' facts and a VM into a
//! verdict: live, cold, or not at all.
//!
//! libvirt's TLS is checked by connecting, not by reading files: what
//! matters is what answers on `<host>:16514`, and file layouts and
//! permissions differ between hosts. Each node probes every peer (its own
//! TLS is what the others say about it):
//!
//! 1. a TCP connect;
//! 2. a TLS handshake, keeping the certificate the server sends (checked
//!    against the cluster's CA, for the name the migration uses, OU
//!    `libvirt`, not expired) whether or not the server then wants a client
//!    certificate we don't send;
//! 3. `virsh -c qemu+tls://<host>/system version`, end to end: this node's
//!    libvirt client certificate, the peer's `tls_allowed_dn_list`.
//!
//! qemu's migration stream (`migrate_tls_x509_cert_dir`) has nothing to
//! connect to outside a migration: a migration's TLS failure is noted
//! against its target (`FactsHandle::note_migration`) until one succeeds.

use crate::committer::PoolAdmin;
use crate::types::Bytes;
use crate::vm::{El, Virsh, VmError, VmRuntime, parse_root};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// libvirt's TLS port.
pub const LIBVIRT_TLS_PORT: u16 = 16514;
/// How long a migration's TLS failure counts against its target (with no
/// live migration tried meanwhile, nothing else would clear it).
const TLS_NOTE_MS: u64 = 10 * 60 * 1000;
/// How often a node collects its facts again.
pub const REFRESH: Duration = Duration::from_secs(300);
/// Stored facts are rewritten at least this often, changed or not.
const REWRITE_MS: u64 = 30 * 60 * 1000;
/// A probe step's limit.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// A virsh command's limit.
const VIRSH_TIMEOUT: Duration = Duration::from_secs(15);
/// A certificate expiring sooner than this is flagged.
const EXPIRES_SOON_S: i64 = 14 * 24 * 3600;

pub use crate::api_types::{
    CertSummary, CpuFacts, HostFacts, LibvirtFacts, NodeFacts, PeerFacts, Probe,
};

/// A probe that worked.
fn probe_ok(cert: Option<CertSummary>) -> Probe {
    Probe {
        ok: true,
        checked_at: crate::cluster::now_ms(),
        cert,
        ..Default::default()
    }
}
/// A probe that failed, and what to do about it.
fn probe_fail(error: impl Into<String>, hint: impl Into<String>) -> Probe {
    Probe {
        ok: false,
        checked_at: crate::cluster::now_ms(),
        error: Some(error.into()),
        hint: Some(hint.into()).filter(|h: &String| !h.is_empty()),
        cert: None,
    }
}

// ---- parsing (what the host and virsh say) ------------------------------

/// `/proc/cpuinfo`: vendor, model name, threads.
pub fn parse_cpuinfo(s: &str) -> (String, String, u32) {
    let field = |k: &str| {
        s.lines()
            .find_map(|l| {
                let (a, b) = l.split_once(':')?;
                (a.trim() == k).then(|| b.trim().to_string())
            })
            .unwrap_or_default()
    };
    let threads = s
        .lines()
        .filter(|l| l.split(':').next().map(str::trim) == Some("processor"))
        .count() as u32;
    (field("vendor_id"), field("model name"), threads)
}

/// `/proc/meminfo`'s `key` (`MemTotal`, `MemAvailable`; given in kB,
/// which the kernel means as KiB). Zero if it isn't there.
pub fn parse_meminfo(s: &str, key: &str) -> Bytes {
    s.lines()
        .find_map(|l| {
            let rest = l.strip_prefix(key)?.strip_prefix(':')?;
            rest.split_whitespace().next()?.parse::<u64>().ok()
        })
        .map(Bytes::from_kib)
        .unwrap_or_default()
}

/// `virsh version`: (libvirt, qemu).
pub fn parse_virsh_version(s: &str) -> (String, String) {
    let after = |prefix: &str| {
        s.lines()
            .find_map(|l| l.trim().strip_prefix(prefix).map(|v| v.trim().to_string()))
            .unwrap_or_default()
    };
    (
        after("Using library: libvirt"),
        after("Running hypervisor: QEMU"),
    )
}

/// `virsh domcapabilities`: virt type, host-model CPU, usable models.
pub fn parse_domcaps(xml: &str) -> Result<(String, CpuFacts, Vec<String>), String> {
    let root = parse_root(xml)?;
    if root.name != "domainCapabilities" {
        return Err(format!(
            "expected <domainCapabilities>, got <{}>",
            root.name
        ));
    }
    let virt = root.child("domain").map(El::text).unwrap_or_default();
    let mut cpu = CpuFacts::default();
    let mut models = Vec::new();
    if let Some(c) = root.child("cpu") {
        for mode in c.elements().filter(|e| e.name == "mode") {
            if mode.attr("supported") == Some("no") {
                continue;
            }
            match mode.attr("name") {
                Some("host-model") => {
                    cpu.model = mode.child("model").map(El::text).unwrap_or_default();
                    cpu.vendor = mode.child("vendor").map(El::text).unwrap_or_default();
                    cpu.features = mode
                        .elements()
                        .filter(|e| e.name == "feature" && e.attr("policy") != Some("disable"))
                        .filter_map(|e| e.attr("name").map(str::to_string))
                        .collect();
                }
                Some("custom") => {
                    models = mode
                        .elements()
                        .filter(|e| e.name == "model" && e.attr("usable") != Some("no"))
                        .map(El::text)
                        .collect();
                }
                _ => {}
            }
        }
    }
    Ok((virt, cpu, models))
}

/// `virsh capabilities`: the machine types for `arch` (aliases too).
pub fn parse_machines(xml: &str, arch: &str) -> Result<Vec<String>, String> {
    let root = parse_root(xml)?;
    let mut out: Vec<String> = Vec::new();
    let mut add = |m: &El| {
        for n in [Some(m.text()), m.attr("canonical").map(str::to_string)]
            .into_iter()
            .flatten()
        {
            if !n.is_empty() && !out.contains(&n) {
                out.push(n);
            }
        }
    };
    for g in root.elements().filter(|e| e.name == "guest") {
        let Some(a) = g.child("arch").filter(|a| a.attr("name") == Some(arch)) else {
            continue;
        };
        for m in a.elements().filter(|e| e.name == "machine") {
            add(m);
        }
        for d in a.elements().filter(|e| e.name == "domain") {
            for m in d.elements().filter(|e| e.name == "machine") {
                add(m);
            }
        }
    }
    out.sort();
    Ok(out)
}

// ---- collecting ---------------------------------------------------------

fn read(p: &str) -> String {
    std::fs::read_to_string(p).unwrap_or_default()
}

pub fn host_facts() -> HostFacts {
    let (cpu_vendor, cpu_model, cpu_threads) = parse_cpuinfo(&read("/proc/cpuinfo"));
    HostFacts {
        hostname: read("/proc/sys/kernel/hostname").trim().to_string(),
        kernel: read("/proc/sys/kernel/osrelease").trim().to_string(),
        arch: std::env::consts::ARCH.to_string(),
        cpu_vendor,
        cpu_model,
        cpu_threads,
        memory: parse_meminfo(&read("/proc/meminfo"), "MemTotal"),
        kvm: std::path::Path::new("/dev/kvm").exists(),
    }
}

/// Memory available on this host now.
pub fn available_memory() -> Bytes {
    parse_meminfo(&read("/proc/meminfo"), "MemAvailable")
}

fn bridges() -> Vec<String> {
    let mut out: Vec<String> = std::fs::read_dir("/sys/class/net")
        .map(|d| {
            d.flatten()
                .filter(|e| e.path().join("bridge").exists())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    out.sort();
    out
}

async fn timed<T>(f: impl std::future::Future<Output = Result<T, VmError>>) -> Result<T, String> {
    match tokio::time::timeout(VIRSH_TIMEOUT, f).await {
        Ok(r) => r.map_err(String::from),
        Err(_) => Err(format!("no answer in {:?}", VIRSH_TIMEOUT)),
    }
}

pub async fn libvirt_facts(virsh: &Virsh, arch: &str) -> LibvirtFacts {
    let mut f = LibvirtFacts {
        uri: virsh.uri.clone(),
        ..Default::default()
    };
    match timed(virsh.version()).await {
        Ok(v) => (f.libvirt_version, f.qemu_version) = parse_virsh_version(&v),
        Err(e) => {
            f.error = Some(e);
            return f;
        }
    }
    match timed(virsh.domcapabilities()).await {
        Ok(xml) => {
            match parse_domcaps(&xml) {
                Ok((virt, cpu, models)) => (f.virt_type, f.cpu, f.cpu_models) = (virt, cpu, models),
                Err(e) => f.error = Some(format!("domcapabilities: {}", e)),
            }
            f.domcapabilities = xml;
        }
        Err(e) => f.error = Some(e),
    }
    if let Ok(xml) = timed(virsh.capabilities()).await {
        f.machines = parse_machines(&xml, arch).unwrap_or_default();
    }
    f.networks = timed(virsh.networks()).await.unwrap_or_default();
    f.bridges = bridges();
    f
}

// ---- probing libvirt's TLS ---------------------------------------------

/// Keeps the server's certificate, and what checking it said, and lets
/// the handshake go on (to see whether it'd stop there for another reason).
#[derive(Debug)]
struct Capture {
    inner: Arc<rustls::client::WebPkiServerVerifier>,
    seen: Mutex<Option<(Vec<u8>, Result<(), String>)>>,
}

impl rustls::client::danger::ServerCertVerifier for Capture {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let r = self
            .inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
            .map(|_| ())
            .map_err(|e| e.to_string());
        *self.seen.lock().unwrap() = Some((end_entity.to_vec(), r));
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn summary(der: &[u8]) -> Option<CertSummary> {
    let c = crate::pki::inspect_der(der).ok()?;
    Some(CertSummary {
        subject: c.name,
        ou: c.ou,
        issuer: c.issuer,
        sans: c.sans,
        not_after: c.not_after.unix_timestamp(),
    })
}

/// Steps 1 and 2 (see the module docs): `node`'s libvirt at `host:port`,
/// its certificate checked against `roots`.
pub async fn probe_tls(
    node: &str,
    host: &str,
    port: u16,
    roots: &[CertificateDer<'static>],
) -> Probe {
    let addr = format!("{}:{}", host, port);
    let tcp = match tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(&addr)).await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
            return probe_fail(
                format!("nothing listens on {}", addr),
                format!(
                    "on {}: enable libvirt's TLS socket (systemctl stop libvirtd.service; \
                     systemctl enable --now libvirtd-tls.socket; systemctl start libvirtd \
                     — virtproxyd-tls.socket with modular daemons)",
                    node
                ),
            );
        }
        Ok(Err(e)) => {
            return probe_fail(
                format!("{}: {}", addr, e),
                "check the network or firewall between the nodes",
            );
        }
        Err(_) => {
            return probe_fail(
                format!("{} doesn't answer", addr),
                "a firewall dropping it? libvirt's TLS port must be open between the nodes",
            );
        }
    };
    crate::tls::install_provider();
    let mut store = rustls::RootCertStore::empty();
    for r in roots {
        let _ = store.add(r.clone());
    }
    let inner = match rustls::client::WebPkiServerVerifier::builder(Arc::new(store)).build() {
        Ok(v) => v,
        Err(e) => return probe_fail(format!("the cluster CA: {}", e), ""),
    };
    let capture = Arc::new(Capture {
        inner,
        seen: Mutex::new(None),
    });
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(capture.clone())
        .with_no_client_auth();
    let name = match ServerName::try_from(host.to_string()) {
        Ok(n) => n,
        Err(e) => return probe_fail(format!("'{}': {}", host, e), ""),
    };
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let handshake = tokio::time::timeout(PROBE_TIMEOUT, connector.connect(name, tcp)).await;
    let seen = capture.seen.lock().unwrap().take();
    let Some((der, verified)) = seen else {
        let why = match handshake {
            Ok(Err(e)) => e.to_string(),
            Err(_) => "timed out".into(),
            Ok(Ok(_)) => "no certificate".into(),
        };
        return probe_fail(
            format!("no TLS from {}: {}", addr, why),
            format!("is libvirt's TLS socket what listens on {}?", addr),
        );
    };
    let cert = summary(&der);
    // Its name as its certificate has it (what `cert libvirt` was given).
    let cn = cert
        .as_ref()
        .map(|c| c.subject.clone())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| host.to_string());
    let san = if cn == host {
        String::new()
    } else {
        format!(" --san {}", host)
    };
    let reissue = format!(
        "reissue it: raidish cert libvirt {}{} --force, and install it on {}",
        cn, san, node
    );
    let mut p = match verified {
        Ok(()) => probe_ok(cert.clone()),
        Err(e) => {
            let hint = if e.contains("UnknownIssuer") || e.contains("BadSignature") {
                format!("not signed by this cluster's CA: {}", reissue)
            } else if e.contains("NotValidForName")
                || e.to_lowercase().contains("not valid for name")
            {
                format!("it doesn't name '{}': {}", host, reissue)
            } else if e.contains("Expired") || e.contains("NotValidYet") {
                format!("out of date: {}", reissue)
            } else {
                reissue.clone()
            };
            probe_fail(format!("{}'s libvirt certificate: {}", node, e), hint)
        }
    };
    if p.ok {
        if let Some(c) = &cert {
            if c.ou.as_deref() != Some("libvirt") {
                p = probe_fail(
                    format!(
                        "{}'s libvirt certificate isn't a raidish libvirt certificate (OU {:?})",
                        node,
                        c.ou.as_deref().unwrap_or("none")
                    ),
                    reissue.clone(),
                );
            } else if c.not_after - ((crate::cluster::now_ms() / 1000) as i64) < EXPIRES_SOON_S {
                p.hint = Some(format!("expires soon: {}", reissue));
            }
        }
    }
    p.cert = cert;
    p
}

/// Step 3: libvirt end to end, from this node's libvirt client.
pub async fn probe_virsh(node: &str, host: &str, bin: &str) -> Result<(), Probe> {
    let h = if host.contains(':') {
        format!("[{}]", host)
    } else {
        host.to_string()
    };
    let v = Virsh {
        bin: bin.to_string(),
        uri: format!("qemu+tls://{}/system", h),
    };
    match timed(v.version()).await {
        Ok(_) => Ok(()),
        Err(e) => {
            let hint = if e.contains("Distinguished Name") || e.contains("not on the list") {
                format!(
                    "{}'s libvirtd doesn't let this node in: tls_allowed_dn_list = [\"*OU=libvirt*\"] in its libvirtd.conf (virtproxyd.conf with modular daemons)",
                    node
                )
            } else if e.contains("clientcert")
                || e.contains("clientkey")
                || e.contains("client certificate")
            {
                "this node has no libvirt client certificate: raidish cert libvirt <this node> and install it here".to_string()
            } else if e.contains("cacert") {
                "this node has no /etc/pki/CA/cacert.pem: install its raidish cert libvirt tree"
                    .to_string()
            } else {
                String::new()
            };
            Err(probe_fail(format!("virsh -c {}: {}", v.uri, e), hint))
        }
    }
}

/// All three steps against `node` at `host`.
pub async fn probe_libvirt(
    node: &str,
    host: &str,
    roots: &[CertificateDer<'static>],
    virsh_bin: &str,
) -> Probe {
    let p = probe_tls(node, host, LIBVIRT_TLS_PORT, roots).await;
    if !p.ok {
        return p;
    }
    match probe_virsh(node, host, virsh_bin).await {
        Ok(()) => p,
        Err(mut e) => {
            e.cert = p.cert;
            e
        }
    }
}

/// A TCP connect to `host:port`.
pub async fn probe_port(host: &str, port: u16, what: &str) -> Probe {
    let addr = format!("{}:{}", host, port);
    match tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(&addr)).await {
        Ok(Ok(_)) => probe_ok(None),
        Ok(Err(e)) => probe_fail(format!("{} ({}): {}", what, addr, e), ""),
        Err(_) => probe_fail(
            format!("{} ({}) doesn't answer", what, addr),
            "a firewall between the nodes?",
        ),
    }
}

// ---- the collector ------------------------------------------------------

/// What the API and the agent share with the collector.
#[derive(Default)]
pub struct FactsState {
    /// The latest collected (stored or not).
    pub latest: Mutex<Option<NodeFacts>>,
    /// TLS failures of migrations from here: target node → error.
    /// (error, when), kept for `TLS_NOTE_MS`.
    migrate_tls: Mutex<BTreeMap<String, (String, u64)>>,
    /// Collect again now.
    pub poke: tokio::sync::Notify,
    /// The cluster's CAs and the virsh binary, for probing a peer's
    /// libvirt now (`probe_now`); set by the collector.
    probe: Mutex<Option<(Vec<CertificateDer<'static>>, String)>>,
}

pub type FactsHandle = Arc<FactsState>;

impl FactsState {
    /// A live migration from here to `to` ended: a TLS failure is kept
    /// against it (and collected into the facts) for `TLS_NOTE_MS`, or
    /// until one succeeds.
    pub fn note_migration(&self, to: &str, result: &Result<(), String>) {
        let mut m = self.migrate_tls.lock().unwrap();
        let changed = match result {
            Ok(()) => m.remove(to).is_some(),
            Err(e) if is_tls_error(e) => {
                let old = m.insert(to.to_string(), (e.clone(), crate::cluster::now_ms()));
                old.map(|(o, _)| o).as_ref() != Some(e)
            }
            Err(_) => false,
        };
        if changed {
            self.poke.notify_one();
        }
    }

    pub fn latest(&self) -> Option<NodeFacts> {
        self.latest.lock().unwrap().clone()
    }

    /// This node's facts with `node`'s libvirt probed again now, rather
    /// than as last collected (a migration is about to rely on it).
    pub async fn with_fresh_probe(&self, node: &str) -> Option<NodeFacts> {
        let mut f = self.latest()?;
        let conf = self.probe.lock().unwrap().clone();
        let host = f.peers.get(node).map(|p| p.host.clone());
        if let (Some((roots, bin)), Some(host)) = (conf, host) {
            if !roots.is_empty() && f.libvirt.is_some() {
                let p = probe_libvirt(node, &host, &roots, &bin).await;
                if let Some(peer) = f.peers.get_mut(node) {
                    let changed = peer.libvirt.as_ref().map(|x| x.ok) != Some(p.ok);
                    peer.libvirt = Some(p);
                    if changed {
                        self.poke.notify_one();
                    }
                }
            }
        }
        Some(f)
    }
}

/// Whether a migration error is qemu's TLS (certificates, the cert dir).
pub fn is_tls_error(e: &str) -> bool {
    let e = e.to_ascii_lowercase();
    [
        "tls",
        "x509",
        "certificate",
        "gnutls",
        "credentials",
        "/pki/",
    ]
    .iter()
    .any(|k| e.contains(k))
}

pub struct Collector {
    pub node: String,
    /// The other nodes: id → base URL (`Members::peers`).
    pub peers: BTreeMap<u64, String>,
    pub vm: Option<Arc<VmRuntime>>,
    /// The cluster's CAs (empty without `tls:`).
    pub roots: Vec<CertificateDer<'static>>,
    pub peer_port: u16,
    pub committer: crate::committer::Admin,
    pub state: FactsHandle,
}

fn url_host(url: &str) -> Option<String> {
    let u = reqwest::Url::parse(url).ok()?;
    Some(u.host_str()?.trim_matches(['[', ']']).to_string())
}

impl Collector {
    pub async fn collect(&self) -> NodeFacts {
        let host = host_facts();
        let libvirt = match &self.vm {
            Some(vm) => Some(libvirt_facts(&vm.virsh, &host.arch).await),
            None => None,
        };
        let peer_io = !self.roots.is_empty();
        let tls_notes = {
            let mut m = self.state.migrate_tls.lock().unwrap();
            let now = crate::cluster::now_ms();
            m.retain(|_, (_, at)| now.saturating_sub(*at) < TLS_NOTE_MS);
            m.clone()
        };
        let mut peers = BTreeMap::new();
        let probes = self.peers.iter().filter_map(|(id, url)| {
            let name = crate::cluster::members::node_name(*id);
            let h = url_host(url)?;
            Some(async move {
                let libvirt = match &self.vm {
                    Some(vm) => Some(if self.roots.is_empty() {
                        probe_fail(
                            "no tls: here (no cluster CA to check against)",
                            "add `tls:` to this node's config",
                        )
                    } else {
                        probe_libvirt(&name, &h, &self.roots, &vm.virsh.bin).await
                    }),
                    None => None,
                };
                let pio = peer_io.then_some(());
                let peer_io = match pio {
                    Some(()) => Some(probe_port(&h, self.peer_port, "peer I/O").await),
                    None => None,
                };
                (name, h, libvirt, peer_io)
            })
        });
        for (name, h, libvirt, peer_io) in futures::future::join_all(probes).await {
            let migrate_tls_error = tls_notes.get(&name).map(|(e, _)| e.clone());
            peers.insert(
                name,
                PeerFacts {
                    host: h,
                    libvirt,
                    peer_io,
                    migrate_tls_error,
                },
            );
        }
        NodeFacts {
            node: self.node.clone(),
            collected_at: crate::cluster::now_ms(),
            raidish: env!("CARGO_PKG_VERSION").to_string(),
            host,
            peer_io,
            libvirt,
            peers,
        }
    }

    /// Collect at start, every `REFRESH` and when poked; store what changed.
    pub async fn run(self) {
        *self.state.probe.lock().unwrap() = self
            .vm
            .as_ref()
            .map(|vm| (self.roots.clone(), vm.virsh.bin.clone()));
        let mut stored: Option<NodeFacts> = None;
        let mut said: Option<Option<String>> = None;
        loop {
            let f = self.collect().await;
            // Say once when libvirt stops (or starts) answering.
            let err = f.libvirt.as_ref().and_then(|l| l.error.clone());
            if f.libvirt.is_some() && said.as_ref() != Some(&err) {
                match &err {
                    Some(e) => eprintln!(
                        "vm: libvirt doesn't answer, no VMs here until it does: {}",
                        e
                    ),
                    None if said.is_some() => println!("vm: libvirt answers again"),
                    None => {}
                }
                said = Some(err);
            }
            *self.state.latest.lock().unwrap() = Some(f.clone());
            let due = match &stored {
                Some(s) => {
                    !s.same_as(&f) || f.collected_at.saturating_sub(s.collected_at) >= REWRITE_MS
                }
                None => true,
            };
            if due {
                match self.committer.put_node_facts(&f).await {
                    Ok(()) => stored = Some(f.clone()),
                    Err(e) => eprintln!("node facts: storing them: {}", e),
                }
            }
            // Not stored (no committer yet), or a peer didn't answer (it may
            // be starting): look again sooner.
            let failing = f.peers.values().any(|p| {
                [&p.libvirt, &p.peer_io]
                    .into_iter()
                    .flatten()
                    .any(|pr| !pr.ok)
            });
            let wait = if stored.is_none() {
                Duration::from_secs(15)
            } else if failing {
                Duration::from_secs(60)
            } else {
                REFRESH
            };
            tokio::select! {
                _ = tokio::time::sleep(wait) => {}
                _ = self.state.poke.notified() => {}
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/node_facts_tests.rs"]
mod tests;
