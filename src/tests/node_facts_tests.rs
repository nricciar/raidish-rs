//! Node facts: reading what the host and virsh say, and the libvirt TLS
//! probe against a server like libvirtd's (wants a client certificate).

use super::*;
use crate::pki::{Ca, Role};
use rustls::pki_types::{PrivateKeyDer, pem::PemObject};

const CPUINFO: &str = "processor\t: 0\nvendor_id\t: AuthenticAMD\nmodel name\t: AMD Ryzen 9 5900X 12-Core Processor\n\n\
processor\t: 1\nvendor_id\t: AuthenticAMD\nmodel name\t: AMD Ryzen 9 5900X 12-Core Processor\n";

#[test]
fn reads_the_host() {
    let (v, m, n) = parse_cpuinfo(CPUINFO);
    assert_eq!(v, "AuthenticAMD");
    assert_eq!(m, "AMD Ryzen 9 5900X 12-Core Processor");
    assert_eq!(n, 2);
    let mem = "MemTotal:        2048000 kB\nMemFree: 1 kB\nMemAvailable:    1024000 kB\n";
    assert_eq!(parse_meminfo(mem, "MemTotal"), Bytes::from_kib(2048000));
    assert_eq!(parse_meminfo(mem, "MemAvailable"), Bytes::from_kib(1024000));
    assert_eq!(
        parse_meminfo(mem, "Mem"),
        Bytes::default(),
        "whole keys only"
    );
    let ver = "Compiled against library: libvirt 12.0.0\nUsing library: libvirt 12.0.0\n\
               Using API: QEMU 12.0.0\nRunning hypervisor: QEMU 10.2.1\n";
    assert_eq!(
        parse_virsh_version(ver),
        ("12.0.0".to_string(), "10.2.1".to_string())
    );
    assert_eq!(version_nums("10.2.1"), Some(vec![10, 2, 1]));
    assert!(version_nums("10.1").unwrap() < version_nums("10.2.1").unwrap());
}

pub(crate) const DOMCAPS: &str = "<domainCapabilities>
  <path>/usr/bin/qemu-system-x86_64</path>
  <domain>kvm</domain>
  <machine>pc-i440fx-10.2</machine>
  <arch>x86_64</arch>
  <cpu>
    <mode name='host-passthrough' supported='yes'/>
    <mode name='host-model' supported='yes'>
      <model fallback='forbid'>EPYC-Milan</model>
      <vendor>AMD</vendor>
      <feature policy='require' name='x2apic'/>
      <feature policy='disable' name='svm'/>
      <feature policy='require' name='pku'/>
    </mode>
    <mode name='custom' supported='yes'>
      <model usable='yes' vendor='AMD'>EPYC</model>
      <model usable='yes' vendor='AMD'>EPYC-Milan</model>
      <model usable='no' vendor='AMD'>EPYC-Genoa</model>
      <model usable='yes' vendor='unknown'>qemu64</model>
    </mode>
  </cpu>
</domainCapabilities>";

#[test]
fn reads_domcapabilities_and_machines() {
    let (virt, cpu, models) = parse_domcaps(DOMCAPS).unwrap();
    assert_eq!(virt, "kvm");
    assert_eq!(cpu.model, "EPYC-Milan");
    assert_eq!(cpu.vendor, "AMD");
    assert_eq!(cpu.features, vec!["x2apic", "pku"]);
    assert_eq!(models, vec!["EPYC", "EPYC-Milan", "qemu64"]);
    assert!(parse_domcaps("<capabilities/>").is_err());

    let caps = "<capabilities><host/><guest><os_type>hvm</os_type>
      <arch name='x86_64'><machine maxCpus='255'>pc-i440fx-10.2</machine>
        <machine canonical='pc-i440fx-10.2' maxCpus='255'>pc</machine>
        <domain type='kvm'><machine maxCpus='288'>pc-q35-10.2</machine></domain></arch></guest>
      <guest><arch name='i686'><machine>pc-i440fx-9.0</machine></arch></guest></capabilities>";
    assert_eq!(
        parse_machines(caps, "x86_64").unwrap(),
        vec!["pc", "pc-i440fx-10.2", "pc-q35-10.2"]
    );
}

#[test]
fn facts_compare_without_their_timestamps() {
    let mut a = NodeFacts {
        node: "node-0".into(),
        collected_at: 1,
        ..Default::default()
    };
    a.peers.insert(
        "node-1".into(),
        PeerFacts {
            peer_io: Some(Probe::ok(None)),
            ..Default::default()
        },
    );
    let mut b = a.clone();
    b.collected_at = 99;
    b.peers
        .get_mut("node-1")
        .unwrap()
        .peer_io
        .as_mut()
        .unwrap()
        .checked_at += 5;
    assert!(a.same_as(&b));
    b.host.kvm = true;
    assert!(!a.same_as(&b));
}

#[test]
fn a_migrations_tls_failure_is_kept_until_one_works() {
    let st = FactsState::default();
    st.note_migration(
        "node-1",
        &Err("operation failed: job 'migration out' failed: no route".into()),
    );
    assert!(st.migrate_tls.lock().unwrap().is_empty());
    st.note_migration(
        "node-1",
        &Err("internal error: unable to execute QEMU command 'object-add': Unable to access credentials /etc/pki/qemu/ca-cert.pem".into()),
    );
    assert!(st.migrate_tls.lock().unwrap().contains_key("node-1"));
    let tls = "Cannot load x509 certificate";
    st.note_migration("node-2", &Err(tls.into()));
    assert_eq!(
        st.migrate_tls
            .lock()
            .unwrap()
            .get("node-2")
            .map(|s| s.0.as_str()),
        Some(tls)
    );
    st.note_migration("node-2", &Ok(()));
    assert!(!st.migrate_tls.lock().unwrap().contains_key("node-2"));
}

// ---- the TLS probe -------------------------------------------------------

fn roots(dir: &std::path::Path) -> Vec<CertificateDer<'static>> {
    CertificateDer::pem_file_iter(dir.join("ca.pem"))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Serve TLS once on 127.0.0.1 with `cert`/`key`, wanting a client cert
/// chaining to `ca_dir`'s CA (as libvirtd does). Returns the port.
async fn serve_once(
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
    ca_dir: &std::path::Path,
) -> u16 {
    crate::tls::install_provider();
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_file(&key).unwrap();
    let mut store = rustls::RootCertStore::empty();
    for r in roots(ca_dir) {
        store.add(r).unwrap();
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(store))
        .build()
        .unwrap();
    let cfg = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        if let Ok((s, _)) = l.accept().await {
            let _ = acceptor.accept(s).await;
        }
    });
    port
}

fn ca(dir: &std::path::Path) -> Ca {
    Ca::init(dir, "test CA", None, false).unwrap();
    Ca::load(dir).unwrap()
}

fn libvirt_files(dir: &std::path::Path, name: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let d = dir.join("libvirt").join(name).join("etc/pki/libvirt");
    (d.join("servercert.pem"), d.join("private/serverkey.pem"))
}

#[tokio::test]
async fn a_good_libvirt_certificate_passes() {
    let dir = tempfile::tempdir().unwrap();
    ca(dir.path())
        .issue_libvirt("node1", &["127.0.0.1".into()], None, false)
        .unwrap();
    let (c, k) = libvirt_files(dir.path(), "node1");
    let port = serve_once(c, k, dir.path()).await;
    let p = probe_tls("node-1", "127.0.0.1", port, &roots(dir.path())).await;
    assert!(p.ok, "{:?}", p);
    let cert = p.cert.unwrap();
    assert_eq!(cert.subject, "node1");
    assert_eq!(cert.ou.as_deref(), Some("libvirt"));
}

#[tokio::test]
async fn another_cas_certificate_is_named() {
    let (mine, theirs) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    ca(mine.path());
    ca(theirs.path())
        .issue_libvirt("node1", &["127.0.0.1".into()], None, false)
        .unwrap();
    let (c, k) = libvirt_files(theirs.path(), "node1");
    let port = serve_once(c, k, theirs.path()).await;
    let p = probe_tls("node-1", "127.0.0.1", port, &roots(mine.path())).await;
    assert!(!p.ok);
    assert!(
        p.hint
            .as_deref()
            .unwrap()
            .contains("not signed by this cluster's CA"),
        "{:?}",
        p
    );
    // Still says what it got.
    assert_eq!(p.cert.unwrap().subject, "node1");
}

#[tokio::test]
async fn a_certificate_for_another_name_is_named() {
    let dir = tempfile::tempdir().unwrap();
    ca(dir.path())
        .issue_libvirt("node1", &[], None, false)
        .unwrap();
    let (c, k) = libvirt_files(dir.path(), "node1");
    let port = serve_once(c, k, dir.path()).await;
    let p = probe_tls("node-1", "127.0.0.1", port, &roots(dir.path())).await;
    assert!(!p.ok);
    let hint = p.hint.unwrap();
    assert!(hint.contains("doesn't name '127.0.0.1'"), "{}", hint);
    assert!(hint.contains("--san 127.0.0.1"), "{}", hint);
}

#[tokio::test]
async fn a_raidish_node_certificate_is_not_a_libvirt_one() {
    let dir = tempfile::tempdir().unwrap();
    ca(dir.path())
        .issue("node1", Role::Node, &["127.0.0.1".into()], None, false)
        .unwrap();
    let d = dir.path().join("node1");
    let port = serve_once(d.join("cert.pem"), d.join("key.pem"), dir.path()).await;
    let p = probe_tls("node-1", "127.0.0.1", port, &roots(dir.path())).await;
    assert!(!p.ok);
    assert!(
        p.error
            .unwrap()
            .contains("isn't a raidish libvirt certificate")
    );
}

#[tokio::test]
async fn nothing_listening_says_to_enable_the_socket() {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let p = probe_tls("node-1", "127.0.0.1", port, &[]).await;
    assert!(!p.ok);
    assert!(p.error.unwrap().contains("nothing listens"));
    assert!(p.hint.unwrap().contains("libvirtd-tls.socket"));
    let p = probe_port("127.0.0.1", port, "peer I/O").await;
    assert!(!p.ok);
}
