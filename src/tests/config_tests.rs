use super::*;

fn spec(yaml: &str) -> Result<Option<PoolSpec>, String> {
    Config::parse(yaml)?.pool_spec()
}

#[test]
fn legacy_five_disks_are_raidz2() {
    let s = spec("disks: [a, b, c, d, e]\n").unwrap().unwrap();
    assert_eq!(s.paths, ["a", "b", "c", "d", "e"]);
    assert_eq!(
        s.topology,
        Topology::RaidZ {
            parity: 2,
            disks: vec![0, 1, 2, 3, 4]
        }
    );
}

#[test]
fn disks_alone_are_only_served() {
    let c = Config::parse("disks: [disks/disk1]\n").unwrap();
    assert_eq!(c.pool_spec().unwrap(), None);
    assert_eq!(c.serve_disks().unwrap(), ["disks/disk1"]);
}

#[test]
fn raidz1_three_disks() {
    let s = spec("pool:\n  raidz1: [a, b, c]\n").unwrap().unwrap();
    assert_eq!(
        s.topology,
        Topology::RaidZ {
            parity: 1,
            disks: vec![0, 1, 2]
        }
    );
    assert_eq!(s.topology.width(), 2);
}

#[test]
fn raidz_with_explicit_parity() {
    let s = spec("pool:\n  raidz:\n    parity: 3\n    disks: [a, b, c, d, e, f]\n")
        .unwrap()
        .unwrap();
    assert_eq!(s.topology.fault_tolerance(), 3);
    assert_eq!(s.topology.width(), 3);
}

#[test]
fn striped_mirrors() {
    let yaml = "
pool:
  stripe:
    - mirror: [a, b]
    - mirror: [c, d]
";
    let c = Config::parse(yaml).unwrap();
    let s = c.pool_spec().unwrap().unwrap();
    assert_eq!(s.paths, ["a", "b", "c", "d"]);
    assert_eq!(
        s.topology,
        Topology::Stripe {
            children: vec![
                Topology::Mirror { disks: vec![0, 1] },
                Topology::Mirror { disks: vec![2, 3] },
            ]
        }
    );
    assert_eq!(s.topology.fault_tolerance(), 1);
    // `listen` with only a pool serves the pool's disks.
    assert_eq!(c.serve_disks().unwrap(), s.paths);
}

#[test]
fn stripe_of_raidz_and_plain_disks() {
    let yaml = "
pool:
  stripe:
    - raidz1: [a, b, c]
    - raidz: { parity: 1, disks: [d, e, f, g] }
    - h
";
    let s = spec(yaml).unwrap().unwrap();
    assert_eq!(s.paths.len(), 8);
    // lcm(2, 3, 1)
    assert_eq!(s.topology.width(), 6);
    assert_eq!(s.topology.fault_tolerance(), 0);
}

#[test]
fn invalid_layouts_are_rejected() {
    for yaml in [
        "pool:\n  mirror: [a]\n",
        "pool:\n  raidz1: [a]\n",
        "pool:\n  raidz: { parity: 0, disks: [a, b, c] }\n",
        "pool:\n  mirror: [a, a]\n",
        "pool:\n  stripe:\n    - stripe: [a, b]\n",
        "pool:\n  stripe: []\n",
        // 31 data shards: wider than a record
        &format!(
            "pool:\n  raidz1: [{}]\n",
            (0..32)
                .map(|i| format!("d{}", i))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        // lcm(5, 7) = 35
        "pool:\n  stripe:\n    - raidz1: [a, b, c, d, e, f]\n    - raidz1: [g, h, i, j, k, l, m, n]\n",
    ] {
        assert!(spec(yaml).is_err(), "accepted: {}", yaml);
    }
}

#[test]
fn record_sizes_follow_alignment() {
    use crate::vdev::max_record_blocks;
    assert_eq!(max_record_blocks(1), 30);
    assert_eq!(max_record_blocks(2), 30);
    assert_eq!(max_record_blocks(3), 30);
    assert_eq!(max_record_blocks(4), 28);
    assert_eq!(max_record_blocks(30), 30);
}
