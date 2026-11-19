//! The nodes as VM hosts (`GET /api/v1/nodes/facts`): libvirt, CPU, and
//! whether the other nodes reach each one's libvirt over TLS, which is
//! what live migration needs. What's wrong says what to do.

use crate::api::{self, FactsView, NodeFacts};
use gloo_timers::callback::Interval;
use yew::prelude::*;

const REFRESH_MS: u32 = 30_000;

/// What's wrong with `f` as a place to migrate VMs to, as the others see it.
fn problems(f: &NodeFacts, view: &FactsView) -> Vec<String> {
    let mut out = Vec::new();
    if !f.peer_io {
        out.push("peer I/O is off (no `tls:` in its config): VMs move to it cold only".into());
    }
    for (from, o) in view.nodes.iter().filter(|(n, _)| **n != f.node) {
        let Some(p) = o.peers.get(&f.node) else {
            continue;
        };
        if let Some(pr) = p.libvirt.as_ref().filter(|p| !p.ok) {
            let mut s = format!(
                "from {}: {}",
                from,
                pr.error
                    .clone()
                    .unwrap_or_else(|| "libvirt TLS failed".into())
            );
            if let Some(h) = &pr.hint {
                s.push_str(&format!(" ({})", h));
            }
            out.push(s);
        }
        if let Some(e) = &p.migrate_tls_error {
            out.push(format!(
                "from {}: the last live migration failed on TLS: {}",
                from, e
            ));
        }
        if let Some(pr) = p.peer_io.as_ref().filter(|p| !p.ok) {
            out.push(format!(
                "from {}: {}",
                from,
                pr.error
                    .clone()
                    .unwrap_or_else(|| "peer I/O unreachable".into())
            ));
        }
    }
    out
}

#[function_component]
pub fn VmHosts() -> Html {
    let view = use_state(|| None::<Result<FactsView, String>>);
    {
        let view = view.clone();
        use_effect_with((), move |_| {
            let load = {
                let view = view.clone();
                move || {
                    let view = view.clone();
                    wasm_bindgen_futures::spawn_local(async move {
                        view.set(Some(api::node_facts().await));
                    });
                }
            };
            load();
            let timer = Interval::new(REFRESH_MS, load);
            move || drop(timer)
        });
    }
    let body = match &*view {
        None => html! { <p class="muted">{ "Asking the nodes…" }</p> },
        Some(Err(e)) => html! { <p class="muted">{ format!("Node facts: {}", e) }</p> },
        Some(Ok(v)) if v.nodes.is_empty() => {
            html! { <p class="muted">{ "No node has reported yet." }</p> }
        }
        Some(Ok(v)) => html! {
            <ul class="hosts">
                { for v.nodes.values().map(|f| host(f, v)) }
            </ul>
        },
    };
    html! {
        <section class="card">
            <h2>{ "VM hosts" }</h2>
            { body }
        </section>
    }
}

fn host(f: &NodeFacts, v: &FactsView) -> Html {
    let up = v.live.contains(&f.node);
    let probs = problems(f, v);
    let (cls, tag, sub) = match &f.libvirt {
        None => ("idle", "NO VMS", "no `vm:` in its config".to_string()),
        Some(l) if l.error.is_some() => {
            ("bad", "LIBVIRT DOWN", l.error.clone().unwrap_or_default())
        }
        Some(l) => (
            if !up {
                "warn"
            } else if probs.is_empty() {
                "ok"
            } else {
                "warn"
            },
            if !up {
                "DOWN"
            } else if probs.is_empty() {
                "LIVE OK"
            } else {
                "COLD ONLY"
            },
            format!(
                "libvirt {} · qemu {} · {} · CPU {}",
                l.libvirt_version,
                l.qemu_version,
                l.virt_type,
                if l.cpu.model.is_empty() {
                    "?"
                } else {
                    &l.cpu.model
                }
            ),
        ),
    };
    html! {
        <li>
            <div class="host-head">
                <span class="mono">{ f.node.clone() }</span>
                <span class="muted">{ f.host.hostname.clone() }</span>
                <span class={classes!("tag", cls)}>{ tag }</span>
            </div>
            <div class="muted small">{ format!(
                "{} ({} threads, {} MiB{})",
                if f.host.cpu_model.is_empty() { "?" } else { &f.host.cpu_model },
                f.host.cpu_threads,
                f.host.memory_mb,
                if f.host.kvm { ", KVM" } else { ", no KVM" }
            ) }</div>
            <div class="muted small">{ sub }</div>
            if f.libvirt.as_ref().is_some_and(|l| l.error.is_none()) && !probs.is_empty() {
                <ul class="host-problems">
                    { for probs.iter().map(|p| html! { <li class="small">{ p.clone() }</li> }) }
                </ul>
            }
        </li>
    }
}
