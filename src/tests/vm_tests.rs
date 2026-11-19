use super::*;

const DUMPXML: &str = r#"<domain type='kvm'>
  <name>old-name</name>
  <uuid>7c3b1d0a-5f43-4d8e-9a54-3d3c1e0f2a11</uuid>
  <metadata>
    <libosinfo:libosinfo xmlns:libosinfo="http://libosinfo.org/xmlns/libvirt/domain/1.0">
      <libosinfo:os id="http://debian.org/debian/12"/>
    </libosinfo:libosinfo>
  </metadata>
  <memory unit='KiB'>2097152</memory>
  <vcpu placement='static'>2</vcpu>
  <os><type arch='x86_64' machine='q35'>hvm</type></os>
  <devices>
    <emulator>/usr/bin/qemu-system-x86_64</emulator>
    <disk type='file' device='cdrom'>
      <driver name='qemu' type='raw'/>
      <source file='raidish:iso'/>
      <target dev='sda' bus='sata'/>
      <readonly/>
    </disk>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2' discard='unmap'/>
      <source file='raidish:vm1'/>
      <target dev='vda' bus='virtio'/>
      <boot order='1'/>
    </disk>
    <disk type='network' device='disk'>
      <source protocol='nbd' name='data'>
        <host name='10.0.0.9' port='10809'/>
      </source>
      <target dev='vdb' bus='virtio'/>
    </disk>
    <disk type='file' device='cdrom'>
      <target dev='sdb' bus='sata'/>
      <readonly/>
    </disk>
    <serial type='pty'><source path='/dev/pts/3'/><target port='0'/></serial>
    <interface type='network'><source network='default'/><model type='virtio'/></interface>
    <graphics type='spice' autoport='yes'><listen type='address'/></graphics>
    <video><model type='virtio'/></video>
  </devices>
</domain>"#;

fn ident() -> VmIdent {
    VmIdent {
        pool: "00ff".into(),
        node: "node-0".into(),
    }
}

#[test]
fn a_domain_is_its_pools_and_nodes() {
    let s = VmSpec::parse("web", DUMPXML, 7);
    let xml = render(&s, ("127.0.0.1", 1), "127.0.0.1", "o", &ident()).unwrap();
    let root = parse_domain(&xml).unwrap();
    let meta = root.child("metadata").unwrap();
    let m = parse_meta(&to_string(meta.elements().next().unwrap())).unwrap();
    assert!(ident().owns(&m));
    for other in [
        VmIdent {
            pool: "00ff".into(),
            node: "node-1".into(),
        },
        VmIdent {
            pool: "1234".into(),
            node: "node-0".into(),
        },
    ] {
        assert!(!other.owns(&m));
    }
    // One started before domains said whose they are: nobody's.
    let old = parse_meta("<raidish:vm xmlns:raidish='x' anchor='a' owner='o'/>").unwrap();
    assert!(!ident().owns(&old));
}

#[test]
fn finds_raidish_disks_anchor_first() {
    let s = VmSpec::parse("web", DUMPXML, 7);
    assert_eq!(s.error, None);
    assert_eq!(s.anchor, "vm1");
    assert_eq!(s.volumes, vec!["vm1", "data", "iso"]);
}

#[test]
fn no_raidish_disk_is_an_error() {
    let s = VmSpec::parse("x", "<domain><name>x</name><devices/></domain>", 1);
    assert!(s.error.is_some());
    assert!(!s.usable());
    let s = VmSpec::parse("x", "<notadomain/>", 1);
    assert!(s.error.unwrap().contains("domain"));
    let s = VmSpec::parse("x", "<domain><devices>", 1);
    assert!(s.error.is_some());
}

#[test]
fn renders_disks_console_name_and_metadata() {
    let s = VmSpec::parse("web", DUMPXML, 7);
    let xml = render(&s, ("127.0.0.1", 10809), "127.0.0.1", "host:1:2", &ident()).unwrap();
    let root = parse_domain(&xml).unwrap();
    assert_eq!(
        to_string(root.child("name").unwrap()),
        "<name>web</name>".to_string()
    );
    // Only our metadata.
    let meta = root.child("metadata").unwrap();
    assert_eq!(meta.elements().count(), 1);
    let m = parse_meta(&to_string(meta.elements().next().unwrap())).unwrap();
    assert_eq!(
        m,
        DomMeta {
            anchor: "vm1".into(),
            volumes: vec!["vm1".into(), "data".into(), "iso".into()],
            change: 7,
            owner: "host:1:2".into(),
            pool: "00ff".into(),
            node: "node-0".into(),
            migrating: false,
        }
    );
    let devices = root.child("devices").unwrap();
    let disks: Vec<&El> = devices.elements().filter(|e| e.name == "disk").collect();
    assert_eq!(disks.len(), 4);
    for (d, vol) in disks.iter().take(3).zip(["iso", "vm1", "data"]) {
        assert_eq!(d.attr("type"), Some("network"));
        assert_eq!(d.child("driver").unwrap().attr("type"), Some("raw"));
        let src = d.child("source").unwrap();
        assert_eq!(src.attr("protocol"), Some("nbd"));
        assert_eq!(src.attr("name"), Some(vol));
        let host = src.child("host").unwrap();
        assert_eq!(host.attr("name"), Some("127.0.0.1"));
        assert_eq!(host.attr("port"), Some("10809"));
        assert!(d.child("target").is_some());
    }
    assert_eq!(disks[0].attr("device"), Some("cdrom"));
    assert!(disks[0].child("readonly").is_some());
    assert!(disks[1].child("boot").is_some());
    // An empty drive: untouched.
    assert_eq!(disks[3].attr("type"), Some("file"));
    assert!(disks[3].child("source").is_none());
    // libvirt's choices: no emulator, no pty path.
    assert!(devices.child("emulator").is_none());
    assert!(devices.child("serial").unwrap().child("source").is_none());
    // One console, VNC on localhost.
    let g: Vec<&El> = devices
        .elements()
        .filter(|e| e.name == "graphics")
        .collect();
    assert_eq!(g.len(), 1);
    assert_eq!(g[0].attr("type"), Some("vnc"));
    assert_eq!(g[0].attr("listen"), Some("127.0.0.1"));
    // Everything else kept.
    assert!(devices.child("interface").is_some());
    assert!(devices.child("video").is_some());
    assert!(root.child("uuid").is_some());
    // And it parses back to the same VM.
    let again = VmSpec::parse("web", &xml, 7);
    assert_eq!(again.volumes, s.volumes);
}

#[test]
fn escapes_survive_a_round_trip() {
    let xml = "<domain><name>a</name><description>x &amp; y &lt;z&gt;</description>\
               <devices><disk device='disk'><source file='raidish:v'/></disk></devices></domain>";
    let s = VmSpec::parse("a", xml, 1);
    let out = render(&s, ("127.0.0.1", 1), "127.0.0.1", "o", &ident()).unwrap();
    assert!(out.contains("x &amp; y &lt;z&gt;"), "{}", out);
}

#[test]
fn names() {
    assert!(valid_name("web-1.prod_x"));
    assert!(!valid_name(""));
    assert!(!valid_name(".hidden"));
    assert!(!valid_name("a/b"));
    assert!(!valid_name("a b"));
}

#[test]
fn parses_virsh_list() {
    let out = " 3    web    running\n -    old    shut off\n 12   db     in shutdown\n\n";
    let l = parse_list(out);
    assert_eq!(l.len(), 3);
    assert_eq!(l[0].id, Some(3));
    assert_eq!(l[1].id, None);
    assert_eq!(l[1].state, "shut off");
    assert_eq!(l[2].state, "in shutdown");
}

#[test]
fn parses_metadata_whatever_the_prefix() {
    let m = parse_meta(&format!(
        "<rd:vm xmlns:rd=\"{}\" anchor=\"a\" volumes=\"a,b\" change=\"3\"/>",
        META_NS
    ))
    .unwrap();
    assert_eq!(m.volumes, vec!["a", "b"]);
    assert_eq!(parse_meta("<other/>"), None);
}

#[test]
fn reads_the_hardware() {
    let s = VmSpec::parse("web", DUMPXML, 7);
    let hw = &s.hw;
    assert_eq!(hw.vcpus, Some(2));
    assert_eq!(hw.memory_kib, Some(2 * 1024 * 1024));
    assert_eq!(hw.arch.as_deref(), Some("x86_64"));
    assert_eq!(hw.machine.as_deref(), Some("q35"));
    assert_eq!(hw.disks.len(), 4);
    assert_eq!(hw.disks[0].device, "cdrom");
    assert_eq!(hw.disks[0].volume.as_deref(), Some("iso"));
    assert_eq!(hw.disks[1].target.as_deref(), Some("vda"));
    assert_eq!(hw.disks[1].bus.as_deref(), Some("virtio"));
    assert_eq!(hw.disks[2].volume.as_deref(), Some("data"));
    assert_eq!(hw.disks[3].volume, None);
    assert_eq!(hw.disks[3].source, None);
    assert_eq!(hw.nics.len(), 1);
    assert_eq!(hw.nics[0].kind, "network");
    assert_eq!(hw.nics[0].source.as_deref(), Some("default"));
    assert_eq!(hw.nics[0].model.as_deref(), Some("virtio"));
}

#[test]
fn memory_units() {
    let mem = |x: &str| {
        VmSpec::parse("m", &format!("<domain>{}<devices/></domain>", x), 1)
            .hw
            .memory_kib
    };
    assert_eq!(mem("<memory>1024</memory>"), Some(1024));
    assert_eq!(mem("<memory unit='MiB'>2</memory>"), Some(2048));
    assert_eq!(mem("<memory unit='G'>1</memory>"), Some(1024 * 1024));
    assert_eq!(mem("<memory unit='bytes'>4096</memory>"), Some(4));
    assert_eq!(
        mem("<memory unit='GiB'>4</memory><currentMemory unit='GiB'>2</currentMemory>"),
        Some(2 * 1024 * 1024)
    );
    assert_eq!(mem("<memory unit='parsecs'>1</memory>"), None);
}

#[test]
fn a_new_vms_template_is_a_usable_vm() {
    use crate::vm_def::{NewVm, disk_volume, template};
    let new = NewVm {
        name: "web1".into(),
        vcpus: 4,
        memory_mib: 4096,
        disk: disk_volume("web1"),
        iso: Some("debian-iso".into()),
        network: Some("default".into()),
    };
    let s = VmSpec::parse("web1", &template(&new), 1);
    assert_eq!(s.error, None);
    assert_eq!(s.anchor, "web1-disk0");
    assert_eq!(s.volumes, vec!["web1-disk0", "debian-iso"]);
    assert_eq!(s.hw.vcpus, Some(4));
    assert_eq!(s.hw.memory_kib, Some(4096 * 1024));
    assert_eq!(s.hw.boot, vec!["hd", "cdrom"]);
    assert_eq!(s.hw.disks[1].device, "cdrom");
    assert_eq!(s.hw.nics[0].source.as_deref(), Some("default"));
    // It renders (gets its console etc.) like any other.
    let xml = render(&s, ("127.0.0.1", 10809), "127.0.0.1", "o", &ident()).unwrap();
    assert!(xml.contains("<graphics type=\"vnc\""));

    let bare = NewVm {
        iso: None,
        network: None,
        ..new
    };
    let s = VmSpec::parse("web1", &template(&bare), 1);
    assert_eq!(s.error, None);
    assert_eq!(s.volumes, vec!["web1-disk0"]);
    assert!(s.hw.nics.is_empty());
}

/// A domain with `devices` (and `extra` at the top), around a raidish disk.
fn with(extra: &str, devices: &str) -> VmSpec {
    VmSpec::parse(
        "x",
        &format!(
            "<domain type='kvm'><name>x</name>{}<devices>\
             <disk type='file' device='disk'><source file='raidish:v'/>\
             <target dev='vda' bus='virtio'/></disk>{}</devices></domain>",
            extra, devices
        ),
        1,
    )
}

#[test]
fn refuses_what_reaches_the_host() {
    for (extra, devices) in [
        (
            "",
            "<disk type='block' device='disk'><source dev='/dev/sda'/><target dev='vdb'/></disk>",
        ),
        (
            "",
            "<disk type='file' device='cdrom'><source file='/etc/shadow'/><target dev='sda'/></disk>",
        ),
        ("", "<hostdev mode='subsystem' type='pci'/>"),
        (
            "",
            "<filesystem type='mount'><source dir='/'/><target dir='host'/></filesystem>",
        ),
        (
            "",
            "<serial type='file'><source path='/etc/cron.d/x'/></serial>",
        ),
        (
            "",
            "<console type='pty'><log file='/root/.bashrc'/></console>",
        ),
        (
            "",
            "<interface type='ethernet'><script path='/tmp/x'/></interface>",
        ),
        (
            "",
            "<interface type='network'><source network='default'/><script path='/tmp/x'/></interface>",
        ),
        (
            "",
            "<interface type='network'><rom file='/etc/shadow'/></interface>",
        ),
        (
            "",
            "<input type='passthrough'><source evdev='/dev/input/event0'/></input>",
        ),
        (
            "",
            "<rng model='virtio'><backend model='random'>/dev/sda</backend></rng>",
        ),
        (
            "",
            "<tpm model='tpm-tis'><backend type='passthrough'/></tpm>",
        ),
        ("", "<smartcard mode='host'/>"),
        ("", "<audio id='1' type='file' path='/root/x'/>"),
        (
            "",
            "<disk type='file' device='disk'><source file='raidish:w'/><backingStore type='file'><source file='/etc/shadow'/></backingStore></disk>",
        ),
        (
            "<qemu:commandline xmlns:qemu='http://libvirt.org/schemas/domain/qemu/1.0'><qemu:arg value='-x'/></qemu:commandline>",
            "",
        ),
        (
            "<commandline xmlns='http://libvirt.org/schemas/domain/qemu/1.0'/>",
            "",
        ),
        (
            "<os><type>hvm</type><kernel>/boot/vmlinuz</kernel><initrd>/etc/shadow</initrd></os>",
            "",
        ),
        ("<os><type>hvm</type><loader>/etc/shadow</loader></os>", ""),
        (
            "<os><type>hvm</type><loader>/usr/share/../../etc/shadow</loader></os>",
            "",
        ),
        ("<os><type>hvm</type><nvram>/etc/passwd</nvram></os>", ""),
        (
            "<sysinfo type='fwcfg'><entry name='opt/x' file='/etc/shadow'/></sysinfo>",
            "",
        ),
    ] {
        let s = with(extra, devices);
        assert!(!s.usable(), "allowed: {} {}", extra, devices);
        assert!(render(&s, ("127.0.0.1", 1), "127.0.0.1", "o", &ident()).is_err());
    }
    let s = VmSpec::parse(
        "x",
        "<domain type='lxc'><devices><disk device='disk'><source file='raidish:v'/></disk></devices></domain>",
        1,
    );
    assert!(!s.usable());
}

#[test]
fn allows_what_stays_in_the_guest() {
    let s = with(
        "<os firmware='efi'><type>hvm</type><loader readonly='yes' type='pflash'>\
         /usr/share/OVMF/OVMF_CODE.fd</loader><nvram template='/usr/share/OVMF/OVMF_VARS.fd'/></os>\
         <seclabel type='none'/><resource><partition>/machine</partition></resource>",
        "<emulator>/usr/bin/evil</emulator>\
         <interface type='bridge'><source bridge='br0'/><model type='virtio'/></interface>\
         <channel type='unix'><source mode='bind' path='/etc/x.sock'/><target type='virtio' name='org.qemu.guest_agent.0'/></channel>\
         <rng model='virtio'><backend model='random'>/dev/urandom</backend></rng>\
         <tpm model='tpm-crb'><backend type='emulator' version='2.0'/></tpm>\
         <audio id='1' type='none'/><controller type='usb'/><memballoon model='virtio'/>",
    );
    assert_eq!(s.error, None);
    let xml = render(&s, ("127.0.0.1", 1), "127.0.0.1", "o", &ident()).unwrap();
    for gone in ["seclabel", "resource", "/usr/bin/evil", "/etc/x.sock"] {
        assert!(!xml.contains(gone), "{} in {}", gone, xml);
    }
    assert!(xml.contains("OVMF_CODE.fd"));
}

#[test]
fn a_volume_belongs_to_one_vm() {
    let mut specs = BTreeMap::new();
    for (name, vols) in [("a", "v"), ("b", "v"), ("c", "w")] {
        let xml = format!(
            "<domain><devices><disk device='disk'><source file='raidish:{}'/></disk></devices></domain>",
            vols
        );
        specs.insert(name.to_string(), VmSpec::parse(name, &xml, 1));
    }
    mark_conflicts(&mut specs);
    assert!(specs["a"].usable());
    assert!(specs["b"].error.as_ref().unwrap().contains("VM 'a'"));
    assert!(specs["c"].usable());
}

#[test]
fn consoles_listen_where_configured() {
    let s = VmSpec::parse("web", DUMPXML, 7);
    let xml = render(&s, ("127.0.0.1", 10809), "172.31.250.1", "o", &ident()).unwrap();
    let root = parse_domain(&xml).unwrap();
    let g = root.child("devices").unwrap().child("graphics").unwrap();
    assert_eq!(g.attr("listen"), Some("172.31.250.1"));
    assert_eq!(
        g.child("listen").unwrap().attr("address"),
        Some("172.31.250.1")
    );
}

/// What `virsh dumpxml --migratable` of a domain raidish started on node-0
/// looks like (trimmed).
const RUNNING: &str = r#"<domain type='kvm'>
  <name>web</name>
  <uuid>4dea22b3-1d52-d8f3-2516-782e98ab3fa0</uuid>
  <metadata>
    <raidish:vm xmlns:raidish="https://raidish.dev/xmlns/vm/1" anchor="vm1" volumes="vm1,data" change="7" owner="host0:1:2" pool="00ff" node="node-0"/>
  </metadata>
  <memory unit='KiB'>65536</memory>
  <devices>
    <disk type='network' device='disk'>
      <driver name='qemu' type='raw' cache='none' discard='unmap'/>
      <source protocol='nbd' name='vm1'>
        <host name='127.0.0.1' port='10809'/>
      </source>
      <target dev='vda' bus='virtio'/>
      <address type='pci' domain='0x0000' bus='0x04' slot='0x00' function='0x0'/>
    </disk>
    <disk type='network' device='disk'>
      <driver name='qemu' type='raw' cache='none' discard='unmap'/>
      <source protocol='nbd' name='data'>
        <host name='127.0.0.1' port='10809'/>
      </source>
      <target dev='vdb' bus='virtio'/>
      <address type='pci' domain='0x0000' bus='0x05' slot='0x00' function='0x0'/>
    </disk>
    <graphics type='vnc' port='-1' autoport='yes' listen='127.0.0.1'>
      <listen type='address' address='127.0.0.1'/>
    </graphics>
  </devices>
</domain>"#;

fn incoming() -> Incoming {
    Incoming {
        uri: Some("qemu+tls://node1/system".into()),
        migrate_host: None,
        nbd_host: "10.0.0.2".into(),
        nbd_port: 10900,
        console: "10.0.0.2".into(),
        node: "node-1".into(),
    }
}

#[test]
fn a_running_domain_is_retargeted_for_another_node() {
    let root = parse_domain(RUNNING).unwrap();
    let m = parse_meta(&to_string(
        root.child("metadata").unwrap().elements().next().unwrap(),
    ))
    .unwrap();
    let meta = DomMeta {
        owner: String::new(),
        node: "node-1".into(),
        migrating: true,
        ..m
    };
    let out = parse_domain(&retarget(RUNNING, &incoming(), &meta).unwrap()).unwrap();
    // What libvirt's ABI check compares is untouched.
    assert_eq!(
        to_string(out.child("uuid").unwrap()),
        "<uuid>4dea22b3-1d52-d8f3-2516-782e98ab3fa0</uuid>"
    );
    let devices = out.child("devices").unwrap();
    for (d, bus) in devices
        .elements()
        .filter(|e| e.name == "disk")
        .zip(["0x04", "0x05"])
    {
        assert_eq!(d.child("address").unwrap().attr("bus"), Some(bus));
        let host = d.child("source").unwrap().child("host").unwrap();
        assert_eq!(host.attr("name"), Some("10.0.0.2"));
        assert_eq!(host.attr("port"), Some("10900"));
    }
    let g = devices.child("graphics").unwrap();
    assert_eq!(g.attr("listen"), Some("10.0.0.2"));
    assert_eq!(g.child("listen").unwrap().attr("address"), Some("10.0.0.2"));
    // One raidish metadata element, the new one.
    let md = out.child("metadata").unwrap();
    assert_eq!(md.elements().count(), 1);
    let got = parse_meta(&to_string(md.elements().next().unwrap())).unwrap();
    assert_eq!(got, meta);
    assert!(got.migrating && got.owner.is_empty());
}

#[test]
fn retargeting_refuses_a_domain_whose_disks_dont_match() {
    let root = parse_domain(RUNNING).unwrap();
    let m = parse_meta(&to_string(
        root.child("metadata").unwrap().elements().next().unwrap(),
    ))
    .unwrap();
    let meta = DomMeta {
        volumes: vec!["vm1".into(), "data".into(), "iso".into()],
        ..m
    };
    assert!(retarget(RUNNING, &incoming(), &meta).is_err());
}

#[test]
fn metadata_round_trips_with_the_migrating_mark() {
    let m = DomMeta {
        anchor: "vm1".into(),
        volumes: vec!["vm1".into(), "data".into()],
        change: 3,
        owner: "h:1:2".into(),
        pool: "00ff".into(),
        node: "node-2".into(),
        migrating: true,
    };
    assert_eq!(parse_meta(&meta_xml(&m)).unwrap(), m);
    let settled = DomMeta {
        migrating: false,
        ..m
    };
    let xml = meta_xml(&settled);
    assert!(!xml.contains("migrating"), "{}", xml);
    assert_eq!(parse_meta(&xml).unwrap(), settled);
}

/// `vm: {}` is a whole VM node; migrations are TLS only.
#[test]
fn an_empty_vm_section_is_all_defaults() {
    let c: crate::vm::VmConfig = serde_yaml::from_str("{}").unwrap();
    assert!(c.check().is_ok());
    assert_eq!(c.virsh().uri, "qemu:///system");
    assert_eq!(c.migrate_timeout(), std::time::Duration::from_secs(300));
    let tcp: crate::vm::VmConfig =
        serde_yaml::from_str("migrate_uri: qemu+tcp://node1/system").unwrap();
    assert!(tcp.check().unwrap_err().contains("TLS only"));
    let tls: crate::vm::VmConfig =
        serde_yaml::from_str("migrate_uri: qemu+tls://n1.example/system").unwrap();
    assert!(tls.check().is_ok());
}

#[test]
fn the_migration_target_defaults_to_its_host() {
    let to = Incoming {
        uri: None,
        migrate_host: None,
        nbd_host: "127.0.0.1".into(),
        nbd_port: 10809,
        console: "127.0.0.1".into(),
        node: "node-1".into(),
    }
    .resolved("node1");
    assert_eq!(to.uri.as_deref(), Some("qemu+tls://node1/system"));
    assert_eq!(to.migrate_host.as_deref(), Some("node1"));
    let v6 = Incoming {
        uri: None,
        migrate_host: None,
        ..to.clone()
    }
    .resolved("fd00::1");
    assert_eq!(v6.uri.as_deref(), Some("qemu+tls://[fd00::1]/system"));
    // Overrides stay.
    let o = Incoming {
        uri: Some("qemu+tls://other/system".into()),
        migrate_host: Some("10.0.0.9".into()),
        ..to
    }
    .resolved("node1");
    assert_eq!(o.uri.as_deref(), Some("qemu+tls://other/system"));
    assert_eq!(o.migrate_host.as_deref(), Some("10.0.0.9"));
}
