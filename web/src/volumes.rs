//! Volumes and their snapshots: create, configure, delete; snapshot, clone,
//! roll back, destroy. Every change goes to the committer (any node passes
//! it on); the list is re-read after each and every few seconds.

use crate::api::{self, DatasetProps, VolumeView};
use crate::cluster::Status;
use crate::dialog::{Dialog, Field, Spec, Values};
use crate::fmt;
use crate::session::{Session, Shared};
use crate::session::{hostname, now_ms, tab_hidden};
use gloo_timers::callback::Interval;
use std::collections::{BTreeSet, HashMap};
use yew::prelude::*;

const REFRESH_MS: u32 = 10_000;

#[derive(Properties, PartialEq)]
pub struct Props {
    pub active: bool,
    pub can_write: bool,
    pub cluster: Option<Status>,
    /// The pool, opened by the browser (for the fragmentation check).
    pub session: Option<Shared<Session>>,
}

#[derive(Clone)]
pub enum Action {
    Create,
    /// The volume, and its props as they were (what the form doesn't show
    /// is kept).
    Settings(String, DatasetProps),
    Delete(String),
    Snapshot(String),
    Clone(String, String),
    Rollback(String),
    Destroy(String, String),
}

pub enum Msg {
    Load,
    Loaded(Result<Vec<VolumeView>, String>),
    Expand(String),
    Ask(Spec, Action),
    Submit(Values),
    Close,
    Finished(Result<String, String>),
    Frag(String),
    FragDone(String, Result<(u64, u64), String>),
}

pub struct Volumes {
    volumes: Option<Vec<VolumeView>>,
    error: Option<String>,
    loading: bool,
    expanded: BTreeSet<String>,
    dialog: Option<(Spec, Action)>,
    busy: bool,
    dialog_error: Option<String>,
    notice: Option<String>,
    /// Fragmentation checks: (records, breaks), or why not (or "checking…").
    frag: HashMap<String, Result<(u64, u64), String>>,
    _timer: Interval,
}

/// The server's rules (`raidish_types::types`), checked before asking.
fn check_name(name: &str, what: &str) -> Result<String, String> {
    let n = name.trim();
    if n.is_empty() {
        return Err(format!("a {} name is needed", what));
    }
    let checked = match what {
        "snapshot" => raidish_types::types::SnapshotName::try_from(n).map(drop),
        _ => raidish_types::types::DatasetName::user(n).map(drop),
    };
    checked.map_err(|e| e.to_string())?;
    Ok(n.to_string())
}

/// A default snapshot name: the local time.
fn stamp() -> String {
    let d = js_sys::Date::new_0();
    format!(
        "{}{:02}{:02}-{:02}{:02}{:02}",
        d.get_full_year(),
        d.get_month() + 1,
        d.get_date(),
        d.get_hours(),
        d.get_minutes(),
        d.get_seconds()
    )
}

impl Component for Volumes {
    type Message = Msg;
    type Properties = Props;

    fn create(ctx: &Context<Self>) -> Self {
        ctx.link().send_message(Msg::Load);
        let link = ctx.link().clone();
        let timer = Interval::new(REFRESH_MS, move || link.send_message(Msg::Load));
        Volumes {
            volumes: None,
            error: None,
            loading: false,
            expanded: BTreeSet::new(),
            dialog: None,
            busy: false,
            dialog_error: None,
            notice: None,
            frag: HashMap::new(),
            _timer: timer,
        }
    }

    fn changed(&mut self, ctx: &Context<Self>, old: &Props) -> bool {
        if ctx.props().active && !old.active {
            ctx.link().send_message(Msg::Load);
        }
        true
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Msg) -> bool {
        match msg {
            Msg::Load => {
                if self.loading || !ctx.props().active || tab_hidden() {
                    return false;
                }
                self.loading = true;
                ctx.link()
                    .send_future(async { Msg::Loaded(api::volumes().await) });
            }
            Msg::Loaded(r) => {
                self.loading = false;
                match r {
                    Ok(mut v) => {
                        v.sort_by(|a, b| a.info.name.cmp(&b.info.name));
                        self.volumes = Some(v);
                        self.error = None;
                    }
                    Err(e) => self.error = Some(e),
                }
            }
            Msg::Expand(name) => {
                if !self.expanded.remove(&name) {
                    self.expanded.insert(name);
                }
            }
            Msg::Ask(spec, action) => {
                self.dialog = Some((spec, action));
                self.busy = false;
                self.dialog_error = None;
            }
            Msg::Close => {
                if !self.busy {
                    self.dialog = None;
                }
            }
            Msg::Submit(values) => {
                let Some((_, action)) = &self.dialog else {
                    return false;
                };
                let action = action.clone();
                let get = |k: &str| values.get(k).cloned().unwrap_or_default();
                let result: Result<(), String> = (|| {
                    match &action {
                        Action::Create => {
                            let name = check_name(&get("name"), "volume")?;
                            let size = api::parse_size(&get("size"))
                                .ok_or("size: a number with an optional unit, like 10G or 512M")?;
                            self.run(ctx, async move {
                                api::create_volume(&name, size).await?;
                                Ok(format!("Created {} ({})", name, fmt::bytes(size)))
                            });
                        }
                        Action::Settings(name, was) => {
                            let nodes: Vec<String> = get("nodes")
                                .split([',', ' '])
                                .map(str::trim)
                                .filter(|s| !s.is_empty())
                                .map(String::from)
                                .collect();
                            let automount = match get("automount").as_str() {
                                "" => None,
                                t => Some(t.parse::<raidish_core::inode::Transport>()?),
                            };
                            let move_to =
                                Some(get("move_to").trim().to_string()).filter(|s| !s.is_empty());
                            let props = DatasetProps {
                                nodes,
                                automount,
                                enabled: !get("enabled").is_empty(),
                                move_to,
                                ..was.clone()
                            };
                            let name = name.clone();
                            self.run(ctx, async move {
                                api::set_props(&name, &props).await?;
                                Ok(format!("Updated {}", name))
                            });
                        }
                        Action::Delete(name) => {
                            if get("confirm").trim() != name {
                                return Err(format!("type {} to confirm", name));
                            }
                            let name = name.clone();
                            self.run(ctx, async move {
                                api::delete_volume(&name).await?;
                                Ok(format!("Deleted {}", name))
                            });
                        }
                        Action::Snapshot(vol) => {
                            let snap = check_name(&get("name"), "snapshot")?;
                            let vol = vol.clone();
                            self.expanded.insert(vol.clone());
                            self.run(ctx, async move {
                                api::snapshot(&vol, &snap).await?;
                                Ok(format!("Took {}@{}", vol, snap))
                            });
                        }
                        Action::Clone(vol, snap) => {
                            let new = check_name(&get("name"), "volume")?;
                            let (vol, snap) = (vol.clone(), snap.clone());
                            self.run(ctx, async move {
                                api::clone_snapshot(&vol, &snap, &new).await?;
                                Ok(format!("Cloned {}@{} to {}", vol, snap, new))
                            });
                        }
                        Action::Rollback(vol) => {
                            let vol = vol.clone();
                            self.run(ctx, async move {
                                api::rollback(&vol).await?;
                                Ok(format!("Rolled {} back", vol))
                            });
                        }
                        Action::Destroy(vol, snap) => {
                            let (vol, snap) = (vol.clone(), snap.clone());
                            self.run(ctx, async move {
                                api::destroy_snapshot(&vol, &snap).await?;
                                Ok(format!("Destroyed {}@{}", vol, snap))
                            });
                        }
                    }
                    Ok(())
                })();
                if let Err(e) = result {
                    self.dialog_error = Some(e);
                }
            }
            Msg::Frag(name) => {
                let Some(session) = ctx.props().session.clone() else {
                    self.frag
                        .insert(name, Err("the pool isn't open in this browser yet".into()));
                    return true;
                };
                self.frag.insert(name.clone(), Err("checking…".into()));
                ctx.link().send_future(async move {
                    // The newest committed state, read here (not by the node).
                    let r = match session.snapshot().await {
                        Ok(snap) => {
                            match raidish_types::types::DatasetName::try_from(name.as_str()) {
                                Ok(n) => snap.fs.volume_fragmentation(&n).await,
                                Err(e) => Err(e.to_string()),
                            }
                        }
                        Err(e) => Err(e),
                    };
                    Msg::FragDone(name, r)
                });
            }
            Msg::FragDone(name, r) => {
                self.frag.insert(name, r);
            }
            Msg::Finished(r) => {
                self.busy = false;
                match r {
                    Ok(note) => {
                        self.dialog = None;
                        self.notice = Some(note);
                    }
                    Err(e) => self.dialog_error = Some(e),
                }
                ctx.link().send_message(Msg::Load);
            }
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let link = ctx.link();
        let can_write = ctx.props().can_write;
        let create = link.callback(|_| {
            Msg::Ask(
                Spec::new("New volume", "Create")
                    .message(html! { <p class="muted">{ "A block device, thin provisioned: space is only used as it's written." }</p> })
                    .field(Field::text("name", "Name", "").placeholder("vm-disk"))
                    .field(Field::text("size", "Size", "").placeholder("20G").hint("K, M, G, T (binary units)")),
                Action::Create,
            )
        });
        html! {
            <section class="card" id="volumes">
                <div class="card-head">
                    <h2>{ "Volumes" }</h2>
                    <div class="toolbar">
                        if can_write {
                            <button class="btn primary" onclick={create}>{ "New volume" }</button>
                        }
                        <button class="btn ghost" title="Refresh" onclick={link.callback(|_| Msg::Load)}>
                            { if self.loading { "…" } else { "⟳" } }
                        </button>
                    </div>
                </div>
                if let Some(n) = &self.notice {
                    <div class="note">{ n }</div>
                }
                if let Some(e) = &self.error {
                    <div class="banner">{ format!("Can't list the volumes: {}", e) }</div>
                }
                {
                    match &self.volumes {
                        None if self.error.is_none() => html! { <p class="muted">{ "Loading…" }</p> },
                        None => Html::default(),
                        Some(v) if v.is_empty() => html! {
                            <p class="muted">{ if can_write { "No volumes yet. Create one to export it over NBD or NVMe/TCP." } else { "No volumes." } }</p>
                        },
                        Some(v) => html! {
                            <div class="table-wrap">
                            <table class="objects volumes">
                                <thead><tr>
                                    <th></th>
                                    <th>{ "Name" }</th>
                                    <th class="num">{ "Size" }</th>
                                    <th>{ "Export" }</th>
                                    <th>{ "Status" }</th>
                                    <th class="num">{ "Snapshots" }</th>
                                    <th></th>
                                </tr></thead>
                                <tbody>
                                    { for v.iter().map(|vol| self.volume_rows(ctx, vol)) }
                                </tbody>
                            </table>
                            </div>
                        },
                    }
                }
                if let Some((spec, _)) = &self.dialog {
                    <Dialog spec={spec.clone()} busy={self.busy} error={self.dialog_error.clone()}
                        on_submit={link.callback(Msg::Submit)} on_cancel={link.callback(|_| Msg::Close)} />
                }
            </section>
        }
    }
}

impl Volumes {
    fn frag_view(&self, ctx: &Context<Self>, name: &str) -> Html {
        let n = name.to_string();
        let check = ctx.link().callback(move |_| Msg::Frag(n.clone()));
        match self.frag.get(name) {
            Some(Ok((records, breaks))) => {
                let pct = if *records > 1 {
                    *breaks as f64 * 100.0 / (*records - 1) as f64
                } else {
                    0.0
                };
                html! {
                    <>
                        { format!("{:.1}% ({} records, {} breaks) ", pct, fmt::count(*records), fmt::count(*breaks)) }
                        <button class="btn ghost small" onclick={check}>{ "Check again" }</button>
                    </>
                }
            }
            Some(Err(e)) if e == "checking…" => html! { <span class="muted">{ e }</span> },
            Some(Err(e)) => html! {
                <>
                    <span class="bad-text">{ e }</span>{ " " }
                    <button class="btn ghost small" onclick={check}>{ "Retry" }</button>
                </>
            },
            None => html! { <button class="btn ghost small" onclick={check}>{ "Check" }</button> },
        }
    }

    /// Who runs `name` now, from the cluster's `ds/<name>` lease.
    fn running(&self, ctx: &Context<Self>, name: &str) -> Option<String> {
        let c = ctx.props().cluster.as_ref()?;
        let lease = c
            .state
            .leases
            .get(&raidish_types::types::LeaseResource::dataset(name))?;
        (!lease.released && c.remaining_ms(lease, now_ms()) > 0.0).then(|| {
            lease
                .holder
                .node
                .clone()
                .unwrap_or_else(|| lease.holder.to_string())
        })
    }

    fn volume_rows(&self, ctx: &Context<Self>, vol: &VolumeView) -> Html {
        let link = ctx.link();
        let can_write = ctx.props().can_write;
        let name = vol.info.name.to_string();
        let open = self.expanded.contains(&name);
        let running = self.running(ctx, &name);
        let status = match (&running, vol.info.props.enabled) {
            (Some(by), _) => html! { <span class="tag ok" title={by.clone()}>{ "RUNNING" }</span> },
            (None, false) => html! { <span class="tag idle">{ "DISABLED" }</span> },
            (None, true) => html! { <span class="tag idle">{ "STOPPED" }</span> },
        };
        let export = match &vol.info.props.automount {
            Some(t) => html! { <span class="kind volume">{ t.to_string().to_uppercase() }</span> },
            None => html! { <span class="muted">{ "manual" }</span> },
        };
        let toggle = {
            let n = name.clone();
            link.callback(move |_| Msg::Expand(n.clone()))
        };
        let snapshot = {
            let n = name.clone();
            link.callback(move |_| {
                Msg::Ask(
                    Spec::new(&format!("Snapshot {}", n), "Take snapshot")
                        .message(html! { <p class="muted">{ "A read-only copy of the volume as it is now. It costs nothing until the volume changes." }</p> })
                        .field(Field::text("name", "Name", &stamp())),
                    Action::Snapshot(n.clone()),
                )
            })
        };
        let settings = {
            let (n, p) = (name.clone(), vol.info.props.clone());
            link.callback(move |_| {
                Msg::Ask(
                    Spec::new(&format!("{} settings", n), "Save")
                        .field(
                            Field::check("enabled", "Enabled", p.enabled)
                                .hint("a disabled volume isn't run anywhere"),
                        )
                        .field(Field::select(
                            "automount",
                            "Export",
                            &p.automount.map(|t| t.to_string()).unwrap_or_default(),
                            &[
                                ("", "Manual (raidish nbd / nvme)"),
                                ("nbd", "NBD, automatically"),
                                ("nvme", "NVMe/TCP, automatically"),
                            ],
                        ))
                        .field(
                            Field::text("nodes", "Nodes", &p.nodes.join(", "))
                                .placeholder("any node")
                                .hint("node names that may run it, most preferred first"),
                        )
                        .field(
                            Field::text("move_to", "Move to", p.move_to.as_deref().unwrap_or(""))
                                .placeholder("stay where it is")
                                .hint("a node to move it to while that node is up"),
                        ),
                    Action::Settings(n.clone(), p.clone()),
                )
            })
        };
        let delete = {
            let n = name.clone();
            link.callback(move |_| {
                Msg::Ask(
                    Spec::new(&format!("Delete {}", n), "Delete volume")
                        .message(html! { <p>{ "This destroys the volume and all its data. It can't be undone." }</p> })
                        .field(Field::text("confirm", &format!("Type {} to confirm", n), "").placeholder(&n))
                        .danger(),
                    Action::Delete(n.clone()),
                )
            })
        };
        let host = hostname();
        let has_snaps = !vol.snapshots.is_empty();
        html! {
            <>
            <tr class={classes!("vol", open.then_some("open"))}>
                <td class="sel"><button class="expander" onclick={toggle.clone()} title="Snapshots and details">
                    { if open { "▾" } else { "▸" } }</button></td>
                <td><button class="name-link" onclick={toggle}><span class="ico">{ "🖴" }</span>{ &name }</button></td>
                <td class="num">{ fmt::bytes(vol.info.size_bytes) }</td>
                <td>{ export }</td>
                <td>{ status }
                    if let Some(by) = &running { <span class="small muted run-by">{ by }</span> }
                </td>
                <td class="num">{ vol.snapshots.len() }</td>
                <td class="acts">
                    if can_write {
                        <span class="row-actions">
                            <button class="btn ghost small" onclick={snapshot}>{ "Snapshot" }</button>
                            <button class="btn ghost small" onclick={settings}>{ "Settings" }</button>
                            <button class="btn ghost small danger" onclick={delete} disabled={has_snaps}
                                title={if has_snaps { "it has snapshots: destroy them first" } else { "" }}>{ "Delete" }</button>
                        </span>
                    }
                </td>
            </tr>
            if open {
                <tr class="vol-detail"><td></td><td colspan="6">
                    <div class="kv small">
                        <span>{ "Block size" }</span><span>{ fmt::bytes(vol.info.volblocksize as u64) }</span>
                        <span>{ "Placement" }</span><span>{
                            if vol.info.props.nodes.is_empty() { "any node".to_string() } else { vol.info.props.nodes.join(" → ") }
                        }</span>
                        if let Some(m) = &vol.info.props.move_to {
                            <span>{ "Moving to" }</span><span>{ m }</span>
                        }
                        <span>{ "Fragmentation" }</span>
                        <span>{ self.frag_view(ctx, &name) }</span>
                        <span>{ "Attach" }</span>
                        <span class="cmds">
                            <code>{ format!("raidish nbd {}  →  sudo nbd-client {} 10809 /dev/nbd0", name, host) }</code>
                            <code>{ format!("raidish nvme {}  →  sudo nvme connect -t tcp -a {} -s 4420 -n nqn.2024-01.com.raidish:nvme:{}", name, host, name) }</code>
                        </span>
                    </div>
                    { self.snapshots(ctx, vol, running.is_some()) }
                </td></tr>
            }
            </>
        }
    }

    fn snapshots(&self, ctx: &Context<Self>, vol: &VolumeView, running: bool) -> Html {
        let link = ctx.link();
        let can_write = ctx.props().can_write;
        if vol.snapshots.is_empty() {
            return html! { <p class="muted small snap-none">{ "No snapshots." }</p> };
        }
        let last = vol.snapshots.len() - 1;
        html! {
            <table class="objects snaps">
                <thead><tr>
                    <th>{ "Snapshot" }</th><th class="when">{ "Taken" }</th>
                    <th class="num">{ "TXG" }</th><th class="num">{ "Unique" }</th>
                    <th>{ "Clones" }</th><th></th>
                </tr></thead>
                <tbody>
                { for vol.snapshots.iter().enumerate().map(|(i, s)| {
                    let (v, snap) = (vol.info.name.to_string(), s.short().to_string());
                    let clone = {
                        let (v, snap) = (v.clone(), snap.clone());
                        link.callback(move |_| Msg::Ask(
                            Spec::new(&format!("Clone {}@{}", v, snap), "Clone")
                                .message(html! { <p class="muted">{ "A new, writable volume that starts with this snapshot's contents. It shares their space until either is changed." }</p> })
                                .field(Field::text("name", "New volume", &format!("{}-{}", v, snap))),
                            Action::Clone(v.clone(), snap.clone()),
                        ))
                    };
                    let rollback = {
                        let (v, snap) = (v.clone(), snap.clone());
                        link.callback(move |_| Msg::Ask(
                            Spec::new(&format!("Roll {} back", v), "Roll back")
                                .message(html! { <p>{ format!("Everything written to {} since {} was taken is lost. The volume must not be running.", v, snap) }</p> })
                                .danger(),
                            Action::Rollback(v.clone()),
                        ))
                    };
                    let destroy = {
                        let (v, snap) = (v.clone(), snap.clone());
                        let bytes = s.unique_bytes;
                        link.callback(move |_| Msg::Ask(
                            Spec::new(&format!("Destroy {}@{}", v, snap), "Destroy")
                                .message(html! { <p>{ format!("This frees {} that only this snapshot holds. It can't be undone.", fmt::bytes(bytes)) }</p> })
                                .danger(),
                            Action::Destroy(v.clone(), snap.clone()),
                        ))
                    };
                    let has_clones = !s.clones.is_empty();
                    html! {
                        <tr>
                            <td class="mono">{ format!("@{}", snap) }</td>
                            <td class="when muted">{ fmt::date_ns(s.created_ns) }</td>
                            <td class="num">{ s.txg.to_string() }</td>
                            <td class="num">{ fmt::bytes(s.unique_bytes) }</td>
                            <td class="small">{ if has_clones { s.clones.join(", ") } else { "—".into() } }</td>
                            <td class="acts">
                                if can_write {
                                    <span class="row-actions">
                                        <button class="btn ghost small" onclick={clone}>{ "Clone" }</button>
                                        if i == last {
                                            <button class="btn ghost small" onclick={rollback} disabled={running}
                                                title={if running { "stop the volume first" } else { "discard changes since this snapshot" }}>
                                                { "Roll back" }
                                            </button>
                                        }
                                        <button class="btn ghost small danger" onclick={destroy} disabled={has_clones}
                                            title={if has_clones { "it has clones: delete them first" } else { "" }}>
                                            { "Destroy" }
                                        </button>
                                    </span>
                                }
                            </td>
                        </tr>
                    }
                }) }
                </tbody>
            </table>
        }
    }

    fn run(
        &mut self,
        ctx: &Context<Self>,
        f: impl std::future::Future<Output = Result<String, String>> + 'static,
    ) {
        self.busy = true;
        self.dialog_error = None;
        ctx.link()
            .send_future(async move { Msg::Finished(f.await) });
    }
}
