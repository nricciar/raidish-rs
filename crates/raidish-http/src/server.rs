//! `raidish listen`: one node of the cluster, put together. Its disk
//! server, Raft member, committer (when it holds the `pool` lease), agent,
//! services, NFS and the HTTP API, wired to each other from the config.

use crate::committer::PoolAdmin;
use crate::config::{Config, PoolSpec};
use crate::disk::DiskOptions;
use crate::disks::DiskSet;
use crate::{api, cluster};
use std::sync::Arc;

/// Serve this node until it is asked to stop. Err: the config can't be
/// served (said once, before anything starts).
pub async fn listen(
    config: &Config,
    spec: Option<PoolSpec>,
    opts: DiskOptions,
    address: &str,
    port: Option<u16>,
    no_committer: bool,
) -> Result<(), String> {
    let serve = config.serve_disks()?;
    let paths: Vec<&str> = serve.iter().map(|s| s.as_str()).collect();
    let disks = Arc::new(DiskSet::new_with(&paths, opts).await);
    let pool = match &spec {
        // Serving the pool's own disks: share them. Otherwise share the
        // local ones still: each local file is locked by its one open.
        Some(spec) if spec.paths == serve => Some(spec.on(disks.clone())),
        Some(spec) => Some(spec.open_sharing(opts, &disks).await),
        None => None,
    };
    let port = port.unwrap_or(8881);
    let members = config
        .cluster_members(port)
        .map_err(|e| format!("cluster: {}", e))?;
    // The node's name comes from its id: the pool position of its first disk.
    let name = members.me.name();
    let data_dir = config.data_dir(&name);
    println!(
        "cluster: node {} ({}), {} member{}",
        members.me,
        name,
        members.disks.len(),
        if members.disks.len() == 1 { "" } else { "s" }
    );
    let pool_map = config
        .pool_map(port)
        .map_err(|e| format!("dashboard: {}", e))?;
    let floor = Arc::new(crate::fence::ServerFence::default());
    if let Some(map) = &pool_map {
        // Requests name the served disk; fencing works in pool disks.
        floor.set_served(
            map.disks
                .iter()
                .enumerate()
                .filter_map(|(pool_idx, d)| match d {
                    crate::inspect::DiskAddr::This(i) => Some((
                        crate::coords::ServedDiskIdx::new(*i),
                        crate::coords::PoolDiskIdx::new(pool_idx)?,
                    )),
                    _ => None,
                })
                .collect(),
        );
    }
    // What the config gets wrong, said before this node joins anything.
    if let Some(c) = &config.vm {
        c.check()?;
    }
    // What this node refuses, live: every verifier made from `tls` checks
    // it, and the cluster feeds it as revocations are applied (CA-2a).
    let trust = crate::trust::Trust::new(config.auth.revoked.clone());
    let node_tls = config.tls.clone().map(|mut t| {
        t.trust = Some(trust.clone());
        t
    });
    if node_tls.is_some() {
        crate::tls::check_servers_against(trust.clone());
    }
    let verifier = match &node_tls {
        Some(t) => Some(
            crate::auth::Verifier::from_config(t, &config.auth)
                .map_err(|e| format!("tls: {}", e))?,
        ),
        None => None,
    };
    let mut service_tls = match (&config.services, &node_tls) {
        (None, _) => None,
        // Access keys come from tokens (see `crate::frontend::keys`),
        // checked against the CA bundle: no TLS, no services.
        (Some(_), None) => {
            return Err("services (S3, SQS, etcd) need TLS: set `tls:` in the config".into());
        }
        (Some(_), Some(tls)) => {
            Some(crate::frontend::ServiceTls::new(tls).map_err(|e| format!("services: {}", e))?)
        }
    };
    let node = cluster::Cluster::start(&name, members, data_dir, floor.clone())
        .await
        .map_err(|e| format!("cluster: {}", e))?;
    node.observe_pki(trust.clone());
    let ca = raidish_ca::CaService::start(
        node.clone(),
        raidish_ca::CaOptions {
            unseal_once: config.ca.unseal_once,
        },
    )
    .map_err(|e| format!("ca: {}", e))?;
    let slot: crate::committer::CommitterSlot = Default::default();
    let agent_state: crate::agent::AgentState = Default::default();
    let files: crate::files_api::WriterSlot = Default::default();
    let mut nfs_backend = None;
    let vms = config.vm.as_ref().map(|c| {
        println!("vm: running VMs through libvirt ({})", c.virsh().uri);
        Arc::new(crate::vm::VmRuntime::new(
            c,
            crate::agent::Agent::nbd_target(&config.automount),
            name.clone(),
        ))
    });
    let me = format!("{}://127.0.0.1:{}", crate::tls::scheme(), port);
    // What happens on this node goes to the activity log (through
    // whichever node is the committer).
    let events = crate::events::EventSink::new(&name);
    // The committer is wherever the `pool` lease is: here in process,
    // else ask every node.
    let endpoints: Vec<String> = {
        let mut endpoints = vec![me.clone()];
        for e in config.lease_endpoints().unwrap_or_default() {
            if !endpoints.contains(&e) && !cluster::members::is_local_base(&e, port) {
                endpoints.push(e);
            }
        }
        endpoints
    };
    let admin = crate::committer::Admin::new(
        slot.clone(),
        crate::committer::CommitClient::new(endpoints.clone()),
    );
    {
        let admin = admin.clone();
        tokio::spawn(events.clone().run(move |batch| {
            let admin = admin.clone();
            async move {
                admin
                    .report_events(&batch)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }
        }));
    }
    // What this node is (host, libvirt, reaching the others), stored
    // with the VM definitions (see `crate::node_facts`).
    let facts: crate::node_facts::FactsHandle = Default::default();
    {
        let collector = crate::node_facts::Collector {
            node: name.clone(),
            peers: node.members.peers.clone(),
            vm: vms.clone(),
            roots: match &node_tls {
                Some(t) => t.roots()?,
                None => Vec::new(),
            },
            peer_port: config.automount.peer_port(),
            committer: admin.clone(),
            state: facts.clone(),
        };
        tokio::spawn(collector.run());
    }
    if let (Some(spec), Some(map)) = (&spec, &pool_map) {
        // This node's own disks through its own disk server, so the
        // committer's writes are fenced like everyone else's.
        let access = crate::committer::PoolAccess {
            spec: crate::config::PoolSpec {
                paths: map
                    .disks
                    .iter()
                    .map(|d| match d {
                        crate::inspect::DiskAddr::This(i) => {
                            format!("{}/api/v1/disks/{}", me, i)
                        }
                        crate::inspect::DiskAddr::Url(u) => u.clone(),
                    })
                    .collect(),
                topology: spec.topology.clone(),
            },
            opts,
        };
        if !no_committer {
            tokio::spawn(crate::committer::run(
                slot.clone(),
                node.clone(),
                access.clone(),
                cluster::lease::LeaseClient::local(node.clone()),
                name.clone(),
                events.clone(),
            ));
        }
        let mut agent = crate::agent::Agent::new(
            name.clone(),
            node.clone(),
            admin.clone(),
            access,
            config.automount.clone(),
            agent_state.clone(),
            files.clone(),
        );
        agent = agent.with_events(events.clone());
        if let Some(vms) = &vms {
            agent = agent.with_vms(vms.clone());
        }
        agent = agent.with_facts(facts.clone());
        // Peer I/O (proxied exports, handoffs) is node-to-node TLS.
        if let Some(tls) = &node_tls {
            agent = agent
                .with_peer_io(tls)
                .map_err(|e| format!("peer I/O: {}", e))?;
        }
        if let Some(services) = &config.services {
            let (Some(tls), Some(service_tls)) = (&node_tls, service_tls.take()) else {
                unreachable!("checked before starting");
            };
            let verifier = crate::auth::Verifier::from_config(tls, &config.auth)
                .map_err(|e| format!("services: {}", e))?;
            let keys_from = node.clone();
            let auth = crate::frontend::keys::ServiceAuth::new(
                services.region(),
                Arc::new(verifier),
                Arc::new(move || keys_from.service_key()),
            );
            // The cluster's service key, made now if no node has yet
            // (nodes sign requests to each other with it).
            let ensure = node.clone();
            tokio::spawn(async move {
                while let Err(e) = ensure.ensure_service_key().await {
                    eprintln!("services: making the service key: {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            });
            let fe = crate::frontend::Frontend::new(
                name.clone(),
                services.clone(),
                auth,
                service_tls,
                admin.clone(),
            );
            if let Err(e) = fe.start().await {
                return Err(format!("services: {}", e));
            }
            agent = agent.with_frontend(fe);
        }
        tokio::spawn(agent.run());
        if let Some(nfs) = &config.nfs {
            let slot = files.clone();
            let leases = cluster::lease::LeaseClient::local(node.clone());
            let mut peers = crate::nfs::peers::ClusterPeers::new(node.clone(), leases.clone());
            if nfs.reachable() {
                peers = peers.with_locations(nfs.advertised(), nfs.port.unwrap_or(2049));
            }
            let peers = std::sync::Arc::new(peers);
            let backend = std::sync::Arc::new(
                crate::nfs::backend::Backend::new(
                    std::sync::Arc::new(move || slot.read().unwrap().clone()),
                    Some(std::sync::Arc::new(leases)),
                    name.clone(),
                )
                .with_peers(peers.clone()),
            );
            match crate::nfs::start(&nfs.addr(), backend.clone(), Some(peers.clone())).await {
                Ok((addr, v4)) => {
                    println!("nfs: serving NFSv3 and NFSv4.1 on {}", addr);
                    tokio::spawn(crate::nfs::peers::own_v4_state(
                        v4.clone(),
                        peers,
                        name.clone(),
                    ));
                    nfs_backend = Some((backend, v4));
                }
                Err(e) => return Err(format!("nfs: {}: {}", nfs.addr(), e)),
            }
        }
    }
    let auth = crate::auth::AuthState(verifier);
    if let Some(t) = &node_tls {
        warn_expiry(t);
    }
    println!("dashboard: {}://localhost:{}/", crate::tls::scheme(), port);
    api::listen(
        disks,
        pool,
        address,
        port,
        node,
        floor,
        pool_map,
        slot,
        agent_state,
        files,
        node_tls.as_ref(),
        auth,
        nfs_backend,
        vms,
        facts,
        events,
        ca,
    )
    .await;
    Ok(())
}

/// Warn if this node's certificate has less than a third of its life, at
/// most 30 days, left (or has run out).
fn warn_expiry(tls: &crate::tls::TlsConfig) {
    let Ok(Some((chain, _))) = tls.identity() else {
        return;
    };
    let Some(info) = chain.first().and_then(|c| crate::pki::inspect_der(c).ok()) else {
        return;
    };
    let left = info.not_after - ::time::OffsetDateTime::now_utc();
    let life = chain
        .first()
        .and_then(|c| raidish_auth::ca::facts(c).ok())
        .map(|f| ::time::Duration::milliseconds((f.not_after_ms - f.not_before_ms) as i64))
        .unwrap_or(::time::Duration::days(90));
    println!(
        "tls: serving as '{}' ({}), certificate expires {}",
        info.name,
        info.role.map(|r| r.to_string()).unwrap_or("?".into()),
        info.expiry()
    );
    let soon: ::time::Duration = std::cmp::min(life / 3, ::time::Duration::days(30));
    if left < soon {
        eprintln!(
            "WARNING: this node's certificate {} on {}: renew it with `raidish ca renew` \
             (then restart), or, from a `cert` CA, `raidish cert issue {} --role node --force`",
            if left.is_negative() {
                "expired"
            } else {
                "expires"
            },
            info.not_after.date(),
            info.name
        );
    }
}
