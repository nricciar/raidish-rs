//! Readiness verdicts: what rules a move out, or makes it cold.

use super::*;
use crate::node_facts::{CpuFacts, HostFacts, LibvirtFacts, PeerFacts, Probe};
use crate::vm::judged_domain;

const VM: &str = "<domain type='kvm'><name>v</name>
  <memory unit='MiB'>512</memory>
  <os><type arch='x86_64' machine='pc-q35-10.2'>hvm</type></os>
  <cpu mode='custom' match='exact'><model fallback='forbid'>EPYC-Milan</model>
    <feature policy='require' name='pku'/><feature policy='disable' name='svm'/></cpu>
  <devices>
    <disk type='file' device='disk'><source file='raidish:v1'/></disk>
    <interface type='network'><source network='lan'/></interface>
    <interface type='bridge'><source bridge='br0'/></interface>
  </devices></domain>";

fn node(name: &str, cpu: &str, qemu: &str) -> NodeFacts {
    NodeFacts {
        node: name.into(),
        peer_io: true,
        host: HostFacts {
            arch: "x86_64".into(),
            cpu_model: cpu.into(),
            memory: Bytes::from_mib(4096),
            kvm: true,
            ..Default::default()
        },
        libvirt: Some(LibvirtFacts {
            qemu_version: qemu.into(),
            virt_type: "kvm".into(),
            cpu: CpuFacts {
                model: "EPYC-Milan".into(),
                vendor: "AMD".into(),
                features: vec!["pku".into()],
            },
            cpu_models: vec!["EPYC".into(), "EPYC-Milan".into()],
            machines: vec!["pc-q35-10.2".into(), "q35".into()],
            networks: vec!["lan".into()],
            bridges: vec!["br0".into()],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// `from` reaches `to`'s libvirt fine.
fn reaches(from: &mut NodeFacts, to: &str) {
    from.peers.insert(
        to.into(),
        PeerFacts {
            host: to.into(),
            libvirt: Some(Probe {
                ok: true,
                ..Default::default()
            }),
            peer_io: Some(Probe {
                ok: true,
                ..Default::default()
            }),
            migrate_tls_error: None,
        },
    );
}

fn pair() -> (NodeFacts, NodeFacts) {
    let mut a = node("node-0", "AMD Ryzen 9 5900X", "10.2.1");
    reaches(&mut a, "node-1");
    (a, node("node-1", "AMD Ryzen 9 7900X3D", "10.2.1"))
}

#[test]
fn reads_what_a_domain_needs() {
    let n = needs(VM).unwrap();
    assert_eq!(n.virt_type, "kvm");
    assert_eq!(n.machine.as_deref(), Some("pc-q35-10.2"));
    assert_eq!(n.memory, Bytes::from_mib(512));
    assert_eq!(n.cpu_mode.as_deref(), Some("custom"));
    assert_eq!(n.cpu_model.as_deref(), Some("EPYC-Milan"));
    assert_eq!(n.cpu_features, vec!["pku"]);
    assert_eq!(n.networks, vec!["lan"]);
    assert_eq!(n.bridges, vec!["br0"]);
    assert!(n.cpu_xml.unwrap().starts_with("<cpu"));
    assert_eq!(
        needs("<domain><memory>1048576</memory></domain>")
            .unwrap()
            .memory,
        Bytes::from_mib(1024)
    );
}

#[test]
fn a_matching_pair_moves_live() {
    let (a, b) = pair();
    let v = check(&needs(VM).unwrap(), "node-1", Some(&a), Some(&b), None);
    assert_eq!(v.mode, Mode::Live, "{:?}", v);
    // Not running: a move is where it starts; nothing live is checked.
    let v = check(&needs(VM).unwrap(), "node-1", None, Some(&b), None);
    assert_eq!(v.mode, Mode::Live);
}

#[test]
fn what_it_cant_run_without_rules_the_node_out() {
    let (a, mut b) = pair();
    let lv = b.libvirt.as_mut().unwrap();
    lv.networks.clear();
    lv.machines = vec!["pc-i440fx-9.0".into()];
    let v = check(&needs(VM).unwrap(), "node-1", Some(&a), Some(&b), None);
    assert_eq!(v.mode, Mode::No);
    let why = v.why();
    assert!(why.contains("network 'lan'"), "{}", why);
    assert!(why.contains("machine type 'pc-q35-10.2'"), "{}", why);

    let mut c = node("node-2", "x", "10.2.1");
    c.libvirt = None;
    assert_eq!(
        check(&needs(VM).unwrap(), "node-2", Some(&a), Some(&c), None).mode,
        Mode::No
    );
    let mut d = node("node-2", "x", "10.2.1");
    d.host.kvm = false;
    let v = check(&needs(VM).unwrap(), "node-2", Some(&a), Some(&d), None);
    assert!(v.why().contains("no KVM"), "{:?}", v);
}

#[test]
fn what_only_rules_out_live_makes_it_cold() {
    let n = needs(VM).unwrap();
    // libvirt TLS not reachable.
    let (mut a, b) = pair();
    let p = a.peers.get_mut("node-1").unwrap();
    p.libvirt = Some(Probe {
        ok: false,
        error: Some("nothing listens on node1:16514".into()),
        ..Default::default()
    });
    let v = check(&n, "node-1", Some(&a), Some(&b), None);
    assert_eq!(v.mode, Mode::Cold);
    assert!(v.why().contains("nothing listens"));

    // Older qemu there.
    let (a, mut b) = pair();
    b.libvirt.as_mut().unwrap().qemu_version = "9.2.0".into();
    let v = check(&n, "node-1", Some(&a), Some(&b), None);
    assert_eq!(v.mode, Mode::Cold);
    assert!(v.why().contains("older"), "{}", v.why());

    // No peer I/O on one.
    let (a, mut b) = pair();
    b.peer_io = false;
    assert_eq!(
        check(&n, "node-1", Some(&a), Some(&b), None).mode,
        Mode::Cold
    );

    // A migration's TLS failed before.
    let (mut a, b) = pair();
    a.peers.get_mut("node-1").unwrap().migrate_tls_error = Some("x509".into());
    let v = check(&n, "node-1", Some(&a), Some(&b), None);
    assert!(v.why().contains("migrate_tls_x509_cert_dir"), "{}", v.why());

    // The target says it can't give it that CPU.
    let (a, b) = pair();
    let live = Live {
        cpu: Some(Err("host CPU lacks avx512f".into())),
        available: Some(Bytes::from_mib(2048)),
    };
    let v = check(&n, "node-1", Some(&a), Some(&b), Some(&live));
    assert_eq!(v.mode, Mode::Cold);
    assert!(v.why().contains("avx512f"));
}

#[test]
fn cpu_modes() {
    let (a, b) = pair();
    let pass = needs("<domain type='kvm'><cpu mode='host-passthrough'/></domain>").unwrap();
    let v = check(&pass, "node-1", Some(&a), Some(&b), None);
    assert_eq!(v.mode, Mode::Cold);
    assert!(v.why().contains("cpu-baseline"), "{}", v.why());
    // The same CPU both sides: fine.
    let mut same = b.clone();
    same.host.cpu_model = a.host.cpu_model.clone();
    assert_eq!(
        check(&pass, "node-1", Some(&a), Some(&same), None).mode,
        Mode::Live
    );

    let tsc = needs("<domain type='kvm'><cpu mode='custom'><model>EPYC</model><feature policy='require' name='invtsc'/></cpu></domain>").unwrap();
    assert!(
        check(&tsc, "node-1", Some(&a), Some(&b), None)
            .why()
            .contains("invtsc")
    );

    // host-model: asked about as what it is on the source.
    let hm = needs("<domain type='kvm'><cpu mode='host-model'/></domain>").unwrap();
    let x = cpu_to_compare(&hm, Some(&a)).unwrap();
    assert!(
        x.contains("<model fallback='forbid'>EPYC-Milan</model>"),
        "{}",
        x
    );
    assert!(x.contains("name='pku'"), "{}", x);
    // Without a live answer: the target lacks the source's model.
    let mut old = b.clone();
    old.libvirt.as_mut().unwrap().cpu_models = vec!["EPYC".into()];
    assert_eq!(
        check(&hm, "node-1", Some(&a), Some(&old), None).mode,
        Mode::Cold
    );
    // No <cpu>: nothing to ask, fine anywhere.
    let none = needs("<domain type='kvm'/>").unwrap();
    assert!(cpu_to_compare(&none, Some(&a)).is_none());
}

#[test]
fn not_enough_memory_is_a_warning() {
    let (a, b) = pair();
    let live = Live {
        cpu: Some(Ok("identical".into())),
        available: Some(Bytes::from_mib(100)),
    };
    let v = check(
        &needs(VM).unwrap(),
        "node-1",
        Some(&a),
        Some(&b),
        Some(&live),
    );
    assert_eq!(v.mode, Mode::Live);
    assert!(
        v.reasons
            .iter()
            .any(|r| r.level == ReasonLevel::Warn && r.text.contains("100 MiB"))
    );
}

#[test]
fn a_node_that_never_reported_is_cold_at_best() {
    let (a, _) = pair();
    let v = check(&needs(VM).unwrap(), "node-1", Some(&a), None, None);
    assert_eq!(v.mode, Mode::Cold);
}

/// What `virsh dumpxml` of a running EPYC-Rome guest adds (qemu's model
/// turns on `topoext`, libvirt's doesn't name it) is judged away; what
/// qemu couldn't give the model (`disable`) stays.
#[test]
fn judged_domain_drops_requires_only_the_live_cpu_adds() {
    let live = "<domain type='kvm'><name>v</name>
      <cpu mode='custom' match='exact' check='full'><model fallback='forbid'>EPYC-Rome</model>
        <feature policy='require' name='x2apic'/>
        <feature policy='disable' name='rdseed'/>
        <feature policy='disable' name='svm'/>
        <feature policy='require' name='topoext'/></cpu></domain>";
    let mig = "<domain type='kvm'><name>v</name>
      <cpu mode='custom' match='exact' check='partial'><model fallback='forbid'>EPYC-Rome</model>
        <feature policy='require' name='x2apic'/>
        <feature policy='disable' name='rdseed'/></cpu></domain>";
    let n = needs(&judged_domain(live, mig)).unwrap();
    assert_eq!(n.cpu_features, vec!["x2apic".to_string()]);
    let x = n.cpu_xml.unwrap();
    assert!(!x.contains("topoext"), "{}", x);
    assert!(x.contains("'svm'") || x.contains("\"svm\""), "{}", x);
    assert!(x.contains("rdseed"), "{}", x);
}

/// A CPU-less domain: `--migratable` has no `<cpu>`, the live one is the
/// bare model qemu gave it with what it couldn't (`svm` on Intel).
#[test]
fn judged_domain_keeps_disables_of_a_cpu_less_domain() {
    let live = "<domain type='kvm'><name>v</name>
      <cpu mode='custom' match='exact' check='full'><model fallback='forbid'>qemu64</model>
        <feature policy='disable' name='svm'/>
        <feature policy='require' name='x2apic'/></cpu></domain>";
    let mig = "<domain type='kvm'><name>v</name></domain>";
    let n = needs(&judged_domain(live, mig)).unwrap();
    assert_eq!(n.cpu_model.as_deref(), Some("qemu64"));
    assert!(n.cpu_features.is_empty());
    assert!(n.cpu_xml.unwrap().contains("svm"));
}

#[test]
fn judged_domain_unparsable_is_the_live_xml() {
    assert_eq!(judged_domain("<domain/>", "<nope"), "<domain/>");
    assert_eq!(
        judged_domain("<domain><name>v</name></domain>", "<domain/>"),
        "<domain><name>v</name></domain>"
    );
}
