//! Dashboard panels.

use crate::app::Sample;
use crate::cluster::Status;
use crate::fmt;
use crate::session::{LayoutData, Shared, Snapshot, hostname};
use raidish::fs::LabelState;
use raidish::inspect::{DiskReport, Health, ObjectKind};
use raidish::vdev::Topology;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use yew::prelude::*;

fn health_class(h: Health) -> &'static str {
    match h {
        Health::Online => "ok",
        Health::Degraded => "warn",
        Health::Faulted => "bad",
    }
}

fn health_name(h: Health) -> &'static str {
    match h {
        Health::Online => "ONLINE",
        Health::Degraded => "DEGRADED",
        Health::Faulted => "FAULTED",
    }
}

// ---------------------------------------------------------------- hero ----

#[derive(Properties, PartialEq)]
pub struct HeroProps {
    pub snap: Shared<Snapshot>,
    pub now: f64,
}

#[function_component]
pub fn Hero(p: &HeroProps) -> Html {
    let r = &p.snap.report;
    let total = r.used_blocks + r.free_blocks + r.deferred_blocks;
    let usable = r.total_blocks;
    let overhead = if r.raw_blocks > 0 {
        100.0 - fmt::pct(usable, r.raw_blocks)
    } else {
        0.0
    };
    // Only say "more" once redundancy has actually been consumed.
    let more = if r.failures_left < r.fault_tolerance {
        " more"
    } else {
        ""
    };
    let survives = match (r.health, r.failures_left) {
        (Health::Faulted, _) => "Data is unavailable: too many disks are missing".to_string(),
        (_, 0) if r.fault_tolerance == 0 => {
            "No redundancy: any disk failure loses data".to_string()
        }
        (_, 0) => "No redundancy left: one more failure loses data".to_string(),
        (_, 1) => format!("Can survive 1{} disk failure", more),
        (_, n) => format!("Can survive {}{} disk failures", n, more),
    };
    let offline = r.disks.iter().filter(|d| d.unavailable()).count();
    let resilver = r
        .disks
        .iter()
        .filter(|d| d.degraded_since.is_some())
        .count();

    html! {
        <section class="hero card">
            <div class="hero-main">
                <span class={classes!("pill", health_class(r.health))}>{ health_name(r.health) }</span>
                <h1>{ survives }</h1>
                <p class="muted">
                    { format!("{} · tolerates {} failure{} when healthy", r.layout, r.fault_tolerance,
                        if r.fault_tolerance == 1 { "" } else { "s" }) }
                </p>
                <div class="facts">
                    <Fact label="TXG" value={fmt::count(r.txg)} sub={r.committed_ms.map(|t| format!("committed {}", fmt::ago(t as f64, p.now)))} />
                    <Fact label="Disks" value={format!("{}/{}", r.disks.len() - offline, r.disks.len())}
                        sub={Some(if offline > 0 { format!("{} unavailable", offline) } else { String::from("all online") })} />
                    <Fact label="Resilver" value={if resilver > 0 { format!("{} disk{}", resilver, if resilver == 1 { "" } else { "s" }) } else { "none".into() }}
                        sub={Some(String::from(if resilver > 0 { "run `raidish resilver`" } else { "all in sync" }))} />
                    <Fact label="Metaslabs" value={fmt::count(r.metaslab_count)} sub={Some(format!("{} each", fmt::blocks(total.checked_div(r.metaslab_count).unwrap_or(0))))} />
                </div>
            </div>
            <div class="hero-cap">
                <Donut used={r.used_blocks} deferred={r.deferred_blocks} free={r.free_blocks} />
                <dl class="cap">
                    <dt><i class="sw used"></i>{ "Used" }</dt><dd>{ fmt::blocks(r.used_blocks) }</dd>
                    <dt><i class="sw deferred"></i>{ "Deferred free" }</dt><dd>{ fmt::blocks(r.deferred_blocks) }</dd>
                    <dt><i class="sw free"></i>{ "Free" }</dt><dd>{ fmt::blocks(r.free_blocks) }</dd>
                    <dt class="sep">{ "Usable" }</dt><dd class="sep">{ fmt::blocks(usable) }</dd>
                    <dt>{ "Raw" }</dt><dd>{ fmt::blocks(r.raw_blocks) }</dd>
                    <dt>{ "Redundancy cost" }</dt><dd>{ format!("{:.0}%", overhead) }</dd>
                </dl>
            </div>
        </section>
    }
}

#[derive(Properties, PartialEq)]
struct FactProps {
    label: AttrValue,
    value: String,
    sub: Option<String>,
}

#[function_component]
fn Fact(p: &FactProps) -> Html {
    html! {
        <div class="fact">
            <div class="fact-label">{ p.label.clone() }</div>
            <div class="fact-value">{ p.value.clone() }</div>
            if let Some(s) = &p.sub { <div class="fact-sub">{ s }</div> }
        </div>
    }
}

#[derive(Properties, PartialEq)]
struct DonutProps {
    used: u64,
    deferred: u64,
    free: u64,
}

#[function_component]
fn Donut(p: &DonutProps) -> Html {
    let total = (p.used + p.deferred + p.free).max(1) as f64;
    let c = 2.0 * std::f64::consts::PI * 42.0;
    let used = p.used as f64 / total * c;
    let deferred = p.deferred as f64 / total * c;
    html! {
        <svg class="donut" viewBox="0 0 100 100" role="img" aria-label="pool capacity">
            <circle class="track" cx="50" cy="50" r="42" />
            <circle class="seg used" cx="50" cy="50" r="42"
                stroke-dasharray={format!("{:.2} {:.2}", used, c)} />
            <circle class="seg deferred" cx="50" cy="50" r="42"
                stroke-dasharray={format!("{:.2} {:.2}", deferred, c)}
                stroke-dashoffset={format!("{:.2}", -used)} />
            <text x="50" y="49" class="donut-pct">{ format!("{:.0}%", fmt::pct(p.used + p.deferred, total as u64)) }</text>
            <text x="50" y="62" class="donut-sub">{ "used" }</text>
        </svg>
    }
}

// ------------------------------------------------------------ topology ----

#[derive(Properties, PartialEq)]
pub struct TopologyProps {
    pub snap: Shared<Snapshot>,
    pub history: Rc<Vec<VecDeque<Sample>>>,
    /// Live connection state (newer than the snapshot's).
    pub online: Rc<Vec<bool>>,
    pub cluster: Option<Status>,
}

#[function_component]
pub fn TopologyPanel(p: &TopologyProps) -> Html {
    let r = &p.snap.report;
    let body = match &r.topology {
        Some(t) => vdev(t, p, true),
        None => html! { <p class="muted">{ "A single device (no pool layout)." }</p> },
    };
    html! {
        <section class="card">
            <h2>{ "Topology" }</h2>
            <p class="muted small">{ "Latency is measured from this browser to each disk's node." }</p>
            { body }
        </section>
    }
}

fn vdev(t: &Topology, p: &TopologyProps, top: bool) -> Html {
    let r = &p.snap.report;
    match t {
        Topology::Disk(d) => disk_card(&r.disks[*d], p),
        Topology::Stripe { children } => html! {
            <div class={classes!("vdev", top.then_some("top"))}>
                <div class="vdev-head"><span>{ "stripe" }</span></div>
                <div class="vdev-kids">{ for children.iter().map(|c| vdev(c, p, false)) }</div>
            </div>
        },
        Topology::RaidZ { disks, .. } | Topology::Mirror { disks } => {
            let bad = disks
                .iter()
                .filter(|d| {
                    r.disks[**d].unavailable() || !p.online.get(**d).copied().unwrap_or(true)
                })
                .count();
            let tol = t.fault_tolerance();
            let (cls, state) = if bad > tol {
                ("bad", "FAULTED".to_string())
            } else if bad > 0 || disks.iter().any(|d| r.disks[*d].degraded_since.is_some()) {
                ("warn", format!("DEGRADED · {} more", tol - bad))
            } else {
                ("ok", "ONLINE".to_string())
            };
            html! {
                <div class="vdev">
                    <div class="vdev-head">
                        <span>{ t.summary() }</span>
                        <span class={classes!("tag", cls)}>{ state }</span>
                    </div>
                    <div class="disks">{ for disks.iter().map(|d| disk_card(&r.disks[*d], p)) }</div>
                </div>
            }
        }
    }
}

fn disk_card(d: &DiskReport, p: &TopologyProps) -> Html {
    let online = d.online && p.online.get(d.index).copied().unwrap_or(true);
    let (cls, state) = if !online {
        ("bad", "OFFLINE")
    } else if d.label == LabelState::Foreign {
        ("bad", "NOT A MEMBER")
    } else if d.degraded_since.is_some() {
        ("warn", "RESILVER")
    } else if d.fenced {
        ("warn", "FENCED")
    } else {
        ("ok", "ONLINE")
    };
    let node = p
        .cluster
        .as_ref()
        .and_then(|c| c.owner_of(d.index))
        .map(|m| match &m.addr {
            Some(a) => format!("node {} · {}", m.id, a.trim_start_matches("http://")),
            None => format!("node {} · this node", m.id),
        });
    let history = p.history.get(d.index);
    // Mean round trip over each refresh interval.
    let rtts: Vec<f64> = history
        .map(|h| {
            h.iter()
                .zip(h.iter().skip(1))
                .map(|(a, b)| {
                    let ops = (b.io.reads + b.io.writes + b.io.errors)
                        .saturating_sub(a.io.reads + a.io.writes + a.io.errors);
                    if ops == 0 || !b.online {
                        f64::NAN
                    } else {
                        (b.io.rtt_us_total - a.io.rtt_us_total) as f64 / ops as f64
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let last = rtts.iter().rev().find(|v| v.is_finite()).copied();
    let read = history
        .and_then(|h| h.back())
        .map(|s| s.io.read_bytes)
        .unwrap_or(0);
    html! {
        <div class={classes!("disk", cls)}>
            <div class="disk-top">
                <span class="disk-name">{ format!("disk {}", d.index) }</span>
                <span class={classes!("tag", cls)}>{ state }</span>
            </div>
            <div class="disk-path" title={d.path.clone()}>{ short_path(&d.path) }</div>
            if let Some(n) = node { <div class="disk-meta">{ n }</div> }
            <div class="disk-meta">
                { d.capacity_blocks.map(fmt::blocks).unwrap_or_else(|| "capacity ?".into()) }
                if let Some(t) = d.degraded_since { { format!(" · missed writes since TXG {}", t) } }
                if d.missed_writes > 0 { { format!(" · {} failed writes", d.missed_writes) } }
            </div>
            <div class="disk-io">
                <Sparkline values={rtts} />
                <span class="small muted">
                    { last.map(fmt::ms).unwrap_or_else(|| "—".into()) }
                    { format!(" · {} read", fmt::bytes(read)) }
                </span>
            </div>
        </div>
    }
}

fn short_path(p: &str) -> String {
    p.trim_start_matches("http://")
        .trim_start_matches("https://")
        .replace("/api/v1/disks/", " #")
}

#[derive(Properties, PartialEq)]
pub struct SparkProps {
    pub values: Vec<f64>,
}

#[function_component]
pub fn Sparkline(p: &SparkProps) -> Html {
    let (w, h) = (120.0, 24.0);
    let finite: Vec<f64> = p.values.iter().copied().filter(|v| v.is_finite()).collect();
    if finite.len() < 2 {
        return html! { <svg class="spark" viewBox={format!("0 0 {} {}", w, h)}></svg> };
    }
    let max = finite.iter().copied().fold(f64::MIN, f64::max).max(1.0);
    let n = p.values.len().max(2) - 1;
    let mut d = String::new();
    let mut pen = false;
    for (i, v) in p.values.iter().enumerate() {
        if !v.is_finite() {
            pen = false;
            continue;
        }
        let x = i as f64 / n as f64 * w;
        let y = h - 2.0 - v / max * (h - 4.0);
        d.push_str(&format!(
            "{}{:.1},{:.1} ",
            if pen { "L" } else { "M" },
            x,
            y
        ));
        pen = true;
    }
    html! {
        <svg class="spark" viewBox={format!("0 0 {} {}", w, h)} preserveAspectRatio="none">
            <path d={d} />
        </svg>
    }
}

// ------------------------------------------------------------- cluster ----

#[derive(Properties, PartialEq)]
pub struct ClusterProps {
    pub status: Option<Status>,
    pub error: Option<String>,
    pub now: f64,
}

#[function_component]
pub fn ClusterPanel(p: &ClusterProps) -> Html {
    let Some(s) = &p.status else {
        return html! {
            <section class="card">
                <h2>{ "Cluster" }</h2>
                <p class="muted">{ p.error.clone().unwrap_or_else(|| "Asking the node…".into()) }</p>
            </section>
        };
    };
    let quorum = s.leader.is_some();
    html! {
        <section class="card">
            <h2>{ "Cluster" }</h2>
            if let Some(e) = &p.error { <p class="banner small">{ format!("status refresh failed: {}", e) }</p> }
            <div class="kv">
                <span>{ "Leader" }</span>
                <span>
                    if let Some(l) = s.leader { { format!("node {}", l) } }
                    else { <span class="tag bad">{ "no quorum" }</span> }
                </span>
                <span>{ "Raft term" }</span><span>{ s.term }</span>
                <span>{ "Lease epoch" }</span><span>{ s.state.epoch }</span>
                <span>{ "Layout" }</span><span class="mono">{ s.fingerprint.clone() }</span>
            </div>
            <h3>{ "Members" }</h3>
            <ul class="members">
                { for s.members.iter().map(|m| {
                    let leader = s.leader == Some(m.id);
                    html! {
                        <li>
                            <span class={classes!("dot", if quorum { "ok" } else { "warn" })}></span>
                            <span class="mono">{ format!("node {}", m.id) }</span>
                            if leader { <span class="crown" title="leader">{ "♛" }</span> }
                            <span class="muted">{ m.addr.clone().unwrap_or_else(|| format!("{} (this node)", s.node)) }</span>
                            <span class="muted small">{ format!("disks {}", m.disks.iter().map(|d| d.to_string()).collect::<Vec<_>>().join(",")) }</span>
                        </li>
                    }
                }) }
            </ul>
            <h3>{ "Leases" }</h3>
            if s.state.leases.is_empty() {
                <p class="muted">{ "Nobody has taken a lease yet." }</p>
            }
            <ul class="leases">
                { for s.state.leases.iter().map(|(res, l)| {
                    let left = s.remaining_ms(l, p.now);
                    let (cls, state) = if l.released {
                        ("idle", "released".to_string())
                    } else if left > 0.0 {
                        ("ok", format!("{:.1} s left", left / 1000.0))
                    } else {
                        ("idle", "expired".to_string())
                    };
                    html! {
                        <li class={cls}>
                            <span class="mono">{ res.clone() }</span>
                            <span>{ l.desc.clone() }</span>
                            <span class="muted small">{ format!("{} · epoch {}", l.owner, l.epoch) }</span>
                            <span class={classes!("tag", cls)}>{ state }</span>
                        </li>
                    }
                }) }
            </ul>
        </section>
    }
}

// ----------------------------------------------------------- metaslabs ----

#[derive(Properties, PartialEq)]
pub struct MetaslabProps {
    pub snap: Shared<Snapshot>,
}

#[function_component]
pub fn Metaslabs(p: &MetaslabProps) -> Html {
    let list = &p.snap.metaslabs;
    let most = list
        .iter()
        .map(|m| m.free_extents)
        .max()
        .unwrap_or(1)
        .max(1);
    html! {
        <section class="card">
            <h2>{ "Metaslabs" }</h2>
            <p class="muted small">{ "Fill shows used space; the corner dot gets brighter the more free extents a metaslab is split into." }</p>
            <div class="ms-grid">
                { for list.iter().map(|m| {
                    let used = fmt::pct(m.used, m.blocks);
                    let deferred = fmt::pct(m.deferred, m.blocks);
                    let frag = m.free_extents as f64 / most as f64;
                    let title = format!(
                        "metaslab {}\nblocks {}–{}\nused {} ({:.1}%)\nfree {} in {} extents\ndeferred {}\nspace map: {}{}",
                        m.index, m.start, m.start + m.blocks - 1,
                        fmt::blocks(m.used), used, fmt::blocks(m.free), m.free_extents,
                        fmt::blocks(m.deferred),
                        m.active_slot.map(|s| format!("slot {} at TXG {}", s, m.synced_txg)).unwrap_or_else(|| "never synced".into()),
                        if m.dirty { " (dirty)" } else { "" },
                    );
                    html! {
                        <div class="ms" title={title}>
                            <div class="ms-used" style={format!("height:{:.1}%", used)}></div>
                            <div class="ms-def" style={format!("height:{:.1}%;bottom:{:.1}%", deferred, used)}></div>
                            if m.free_extents > 1 {
                                <i class="ms-frag" style={format!("opacity:{:.2}", 0.25 + 0.75 * frag)}></i>
                            }
                            <span>{ m.index }</span>
                        </div>
                    }
                }) }
            </div>
        </section>
    }
}

// ------------------------------------------------------------ timeline ----

#[derive(Properties, PartialEq)]
pub struct TimelineProps {
    pub snap: Shared<Snapshot>,
    pub now: f64,
}

#[function_component]
pub fn Timeline(p: &TimelineProps) -> Html {
    let r = &p.snap.report;
    let mut ring = r.ring.clone();
    ring.sort();
    html! {
        <section class="card">
            <h2>{ "Rewind points" }</h2>
            <p class="muted small">{ "Committed TXGs still intact on disk. Open one with --txg N, or the newest fully readable one with --rewind." }</p>
            <ol class="timeline">
                { for ring.iter().map(|(txg, ts)| {
                    let current = *txg == r.txg;
                    html! {
                        <li class={classes!(current.then_some("current"))}>
                            <span class="tl-dot"></span>
                            <span class="mono">{ format!("TXG {}", txg) }</span>
                            <span class="muted small">{ format!("{} · {}", fmt::clock(*ts), fmt::ago(*ts as f64, p.now)) }</span>
                            if current { <span class="tag ok">{ "current" }</span> }
                        </li>
                    }
                }) }
            </ol>
        </section>
    }
}

// ------------------------------------------------------------- objects ----

/// Volume name → (records, discontiguities), or why not (yet).
pub type FragResults = HashMap<String, Result<(u64, u64), String>>;

#[derive(Properties, PartialEq)]
pub struct ObjectsProps {
    pub layout: Shared<LayoutData>,
    pub frag: Rc<FragResults>,
    pub on_frag: Callback<String>,
}

#[function_component]
pub fn Objects(p: &ObjectsProps) -> Html {
    let sort_by_size = use_state(|| true);
    let mut objects: Vec<_> = p.layout.objects.iter().collect();
    if *sort_by_size {
        objects.sort_by_key(|o| std::cmp::Reverse(o.allocated_blocks));
    }
    let host = hostname();
    let toggle = {
        let s = sort_by_size.clone();
        Callback::from(move |_| s.set(!*s))
    };
    let volumes = objects
        .iter()
        .filter(|o| o.kind == ObjectKind::Volume)
        .count();
    html! {
        <section class="card">
            <div class="card-head">
                <h2>{ "Files & volumes" }</h2>
                <button class="btn ghost" onclick={toggle}>
                    { if *sort_by_size { "sorted by size" } else { "sorted by path" } }
                </button>
            </div>
            if objects.is_empty() {
                <p class="muted">{ "The pool is empty. Create a volume with `raidish block <name> <bytes>`." }</p>
            } else {
                <div class="table-wrap">
                <table class="objects">
                    <thead><tr>
                        <th>{ "Path" }</th><th>{ "Kind" }</th>
                        <th class="num">{ "Size" }</th><th class="num">{ "On disk" }</th><th>{ "" }</th>
                    </tr></thead>
                    <tbody>
                    { for objects.iter().map(|o| {
                        let name = o.path.trim_start_matches('/').to_string();
                        let kind = match o.kind {
                            ObjectKind::File => "file",
                            ObjectKind::Directory => "dir",
                            ObjectKind::Volume => "volume",
                        };
                        let action = if o.kind == ObjectKind::Volume {
                            let on_frag = p.on_frag.clone();
                            let n = name.clone();
                            match p.frag.get(&name) {
                                Some(Ok((records, breaks))) => html! {
                                    <span class="small">{ format!("{} records, {:.1}% fragmented", records,
                                        if *records > 1 { *breaks as f64 * 100.0 / (*records - 1) as f64 } else { 0.0 }) }</span>
                                },
                                Some(Err(e)) => html! { <span class="small muted">{ e.clone() }</span> },
                                None => html! {
                                    <button class="btn ghost small" onclick={Callback::from(move |_| on_frag.emit(n.clone()))}>
                                        { "check fragmentation" }
                                    </button>
                                },
                            }
                        } else {
                            Html::default()
                        };
                        html! {
                            <>
                            <tr>
                                <td class="mono">{ o.path.clone() }</td>
                                <td><span class={classes!("kind", kind)}>{ kind }</span></td>
                                <td class="num">{ if o.kind == ObjectKind::Directory { "—".to_string() } else { fmt::bytes(o.size_bytes) } }</td>
                                <td class="num">{ fmt::blocks(o.allocated_blocks) }</td>
                                <td>{ action }</td>
                            </tr>
                            if o.kind == ObjectKind::Volume {
                                <tr class="cmds"><td colspan="5">
                                    <code>{ format!("raidish nbd {}  →  sudo nbd-client {} 10809 /dev/nbd0", name, host) }</code>
                                    <code>{ format!("raidish nvme {}  →  sudo nvme connect -t tcp -a {} -s 4420 -n nqn.2024-01.com.raidish:nvme:{}", name, host, name) }</code>
                                </td></tr>
                            }
                            </>
                        }
                    }) }
                    </tbody>
                </table>
                </div>
                <p class="muted small">{ format!("{} objects, {} volumes. Commands assume the server runs on this host.", objects.len(), volumes) }</p>
            }
        </section>
    }
}
