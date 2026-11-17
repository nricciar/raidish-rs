//! One VM (`#vms/<name>`). A header says what it is and what it's doing,
//! with one power control and a menu for the rest; under it the console
//! and its live numbers side by side; then its hardware and its XML.
//! Re-read every few seconds, like the list.

use crate::api::{self, Vm, Volume};
use crate::app::Route;
use crate::cluster::Status;
use crate::dialog::{Dialog, Field, Spec, Values};
use crate::fmt;
use crate::session::{now_ms, tab_hidden};
use crate::vms::{Console, ConsolePreview, REFRESH_MS, confirm, state, state_tag};
use gloo_timers::callback::Interval;
use std::collections::VecDeque;
use web_sys::HtmlTextAreaElement;
use yew::prelude::*;

/// CPU samples kept (one per refresh: a few minutes).
const CPU_SAMPLES: usize = 45;

#[derive(Properties, PartialEq)]
pub struct Props {
    pub name: String,
    pub can_write: bool,
    pub cluster: Option<Status>,
}

/// What a confirmed dialog does.
#[derive(Clone)]
pub enum Action {
    /// `stop`, `reboot`, `restart`, `reset`, `kill`.
    Power(&'static str),
    Move,
    Delete,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Tab {
    Hardware,
    Definition,
}

pub enum Msg {
    Load,
    Loaded(Result<Vec<Vm>, String>),
    Volumes(Vec<Volume>),
    /// A power action: asks first unless it's harmless.
    Power(&'static str),
    Ask(Spec, Action),
    Submit(Values),
    Close,
    Finished(Result<String, String>),
    Deleted(Result<(), String>),
    Console(bool),
    Menu(bool),
    Tab(Tab),
    Xml(u64, Result<String, String>),
    Edit(bool),
    XmlInput(String),
    SaveXml,
    Saved(Result<(), String>),
}

pub struct VmPage {
    vm: Option<Vm>,
    /// Loaded, and it isn't there.
    missing: bool,
    error: Option<String>,
    loading: bool,
    volumes: Vec<Volume>,
    notice: Option<String>,
    dialog: Option<(Spec, Action)>,
    busy: bool,
    dialog_error: Option<String>,
    console: bool,
    menu: bool,
    tab: Tab,
    /// (when, CPU seconds) while it runs.
    cpu: VecDeque<(f64, f64)>,
    /// The XML, as of change counter `.0`.
    xml: Option<(u64, String)>,
    xml_error: Option<String>,
    /// Being edited: the text.
    draft: Option<String>,
    saving: bool,
    _timer: Interval,
}

impl Component for VmPage {
    type Message = Msg;
    type Properties = Props;

    fn create(ctx: &Context<Self>) -> Self {
        ctx.link().send_message(Msg::Load);
        let link = ctx.link().clone();
        let timer = Interval::new(REFRESH_MS, move || link.send_message(Msg::Load));
        VmPage {
            vm: None,
            missing: false,
            error: None,
            loading: false,
            volumes: Vec::new(),
            notice: None,
            dialog: None,
            busy: false,
            dialog_error: None,
            console: false,
            menu: false,
            tab: Tab::Hardware,
            cpu: VecDeque::new(),
            xml: None,
            xml_error: None,
            draft: None,
            saving: false,
            _timer: timer,
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Msg) -> bool {
        let name = ctx.props().name.clone();
        match msg {
            Msg::Load => {
                if self.loading || tab_hidden() {
                    return false;
                }
                self.loading = true;
                ctx.link()
                    .send_future(async { Msg::Loaded(api::vms().await) });
                // Only for disk sizes: once, and again after an action.
                if self.volumes.is_empty() {
                    ctx.link().send_future(async {
                        Msg::Volumes(api::volumes().await.unwrap_or_default())
                    });
                }
                return false;
            }
            Msg::Loaded(r) => {
                self.loading = false;
                match r {
                    Ok(vms) => {
                        self.error = None;
                        self.vm = vms.into_iter().find(|v| v.name == name);
                        self.missing = self.vm.is_none();
                        let vm = self.vm.as_ref();
                        match vm.and_then(|v| v.cpu_s) {
                            Some(c) => {
                                // A restart starts the counter again.
                                if self.cpu.back().is_some_and(|(_, last)| c < *last) {
                                    self.cpu.clear();
                                }
                                self.cpu.push_back((now_ms(), c));
                                while self.cpu.len() > CPU_SAMPLES + 1 {
                                    self.cpu.pop_front();
                                }
                            }
                            None => self.cpu.clear(),
                        }
                        // (Re)read the XML when it changed.
                        if let Some(vm) = vm
                            && self.xml.as_ref().is_none_or(|(c, _)| *c != vm.change)
                        {
                            let (change, path) = (vm.change, vm.xml_path());
                            ctx.link().send_future(async move {
                                Msg::Xml(change, api::get_text(&path).await)
                            });
                        }
                    }
                    Err(e) => self.error = Some(e),
                }
            }
            Msg::Volumes(v) => self.volumes = v,
            Msg::Power(action) => {
                self.menu = false;
                match confirm(action, &name) {
                    Some(spec) => ctx
                        .link()
                        .send_message(Msg::Ask(spec, Action::Power(action))),
                    None => self.power(ctx, action),
                }
            }
            Msg::Ask(spec, action) => {
                self.menu = false;
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
                let Some((_, action)) = self.dialog.clone() else {
                    return false;
                };
                let get = |k: &str| values.get(k).cloned().unwrap_or_default();
                self.busy = true;
                self.dialog_error = None;
                match action {
                    Action::Power(a) => self.power(ctx, a),
                    Action::Move => {
                        let node = get("node");
                        ctx.link().send_future(async move {
                            Msg::Finished(
                                api::vm_move(&name, &node)
                                    .await
                                    .map(|_| format!("Moving to {}", node)),
                            )
                        });
                    }
                    Action::Delete => {
                        let disks = if get("disks").is_empty() {
                            Vec::new()
                        } else {
                            self.own_disks()
                        };
                        ctx.link().send_future(async move {
                            Msg::Deleted(api::vm_delete(&name, &disks).await)
                        });
                    }
                }
            }
            Msg::Finished(r) => {
                self.busy = false;
                self.volumes.clear();
                match r {
                    Ok(note) => {
                        self.dialog = None;
                        self.notice = Some(note);
                        self.error = None;
                    }
                    Err(e) if self.dialog.is_some() => self.dialog_error = Some(e),
                    Err(e) => self.error = Some(e),
                }
                ctx.link().send_message(Msg::Load);
            }
            Msg::Deleted(r) => {
                self.busy = false;
                match r {
                    Ok(()) => Route::Vms(None).go(),
                    Err(e) => self.dialog_error = Some(e),
                }
            }
            Msg::Console(open) => {
                self.menu = false;
                self.console = open;
            }
            Msg::Menu(open) => self.menu = open,
            Msg::Tab(t) => self.tab = t,
            Msg::Xml(change, r) => match r {
                Ok(x) => {
                    self.xml = Some((change, x));
                    self.xml_error = None;
                }
                Err(e) => self.xml_error = Some(e),
            },
            Msg::Edit(on) => {
                self.draft = on.then(|| self.xml.as_ref().map(|x| x.1.clone()).unwrap_or_default());
                self.xml_error = None;
            }
            Msg::XmlInput(s) => {
                self.draft = Some(s);
                return false;
            }
            Msg::SaveXml => {
                let Some(text) = self.draft.clone() else {
                    return false;
                };
                self.saving = true;
                let path = api::vm_xml_path(&name);
                ctx.link()
                    .send_future(async move { Msg::Saved(api::put_text(&path, &text).await) });
            }
            Msg::Saved(r) => {
                self.saving = false;
                match r {
                    Ok(()) => {
                        self.draft = None;
                        self.xml = None;
                        self.notice = Some(if self.vm.as_ref().is_some_and(|v| v.running()) {
                            "Definition saved. Restart the VM to apply it.".into()
                        } else {
                            "Definition saved. It applies on the next start.".into()
                        });
                        ctx.link().send_message(Msg::Load);
                    }
                    Err(e) => self.xml_error = Some(e),
                }
            }
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let link = ctx.link();
        let name = &ctx.props().name;
        let back = html! {
            <a class="crumb" href={Route::Vms(None).href()}>{ "← Virtual machines" }</a>
        };
        let Some(vm) = &self.vm else {
            return html! {
                <section class="card">
                    { back }
                    if self.missing {
                        <h2>{ format!("No VM '{}'", name) }</h2>
                        <p class="muted">{ format!("There's no /{}.", api::vm_xml_path(name)) }</p>
                    } else if let Some(e) = &self.error {
                        <div class="banner">{ e }</div>
                    } else {
                        <p class="muted">{ "Loading…" }</p>
                    }
                </section>
            };
        };
        html! {
            <div class="vm-page">
                { back }
                { self.header(ctx, vm) }
                if let Some(n) = &self.notice {
                    <div class="note">{ n }</div>
                }
                if let Some(e) = &self.error {
                    <div class="banner">{ e }</div>
                }
                { problems(vm) }
                <div class="vm-hero">
                    { self.screen(ctx, vm) }
                    { self.side(ctx, vm) }
                </div>
                <section class="card vm-tabs">
                    <nav class="seg">
                        { self.tab_btn(ctx, Tab::Hardware, "Hardware") }
                        { self.tab_btn(ctx, Tab::Definition, "Definition") }
                    </nav>
                    {
                        match self.tab {
                            Tab::Hardware => self.hardware(vm),
                            Tab::Definition => self.definition(ctx, vm),
                        }
                    }
                </section>
                if let Some((spec, _)) = &self.dialog {
                    <Dialog spec={spec.clone()} busy={self.busy} error={self.dialog_error.clone()}
                        on_submit={link.callback(Msg::Submit)} on_cancel={link.callback(|_| Msg::Close)} />
                }
                if self.console {
                    <Console name={name.clone()} on_close={link.callback(|_| Msg::Console(false))} />
                }
            </div>
        }
    }
}

/// Why it isn't doing what it should, if anything.
fn problems(vm: &Vm) -> Html {
    let mut out = Vec::new();
    if let Some(e) = &vm.error {
        out.push(format!("Its definition can't be used: {}", e));
    }
    if let Some(e) = &vm.start_error {
        out.push(format!("Not starting: {}", e));
    }
    if let Some(e) = &vm.unreachable {
        out.push(format!("Its node can't be asked: {}", e));
    }
    html! { <>{ for out.into_iter().map(|m| html! { <div class="banner">{ m }</div> }) }</> }
}

/// A small line icon, drawn in the text colour.
fn icon(kind: &str) -> Html {
    let paths: &[&str] = match kind {
        "cpu" => &[
            "M7 7h10v10H7z",
            "M10 10h4v4h-4z",
            "M9 3v4M15 3v4M9 17v4M15 17v4M3 9h4M3 15h4M17 9h4M17 15h4",
        ],
        "memory" => &["M3 7h18v9H3z", "M7 10v3M11 10v3M15 10v3", "M6 16v3M18 16v3"],
        "disk" => &[
            "M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3v12c0 1.7-3.6 3-8 3s-8-1.3-8-3z",
            "M4 6c0 1.7 3.6 3 8 3s8-1.3 8-3",
        ],
        "cdrom" => &[
            "M12 3a9 9 0 1 0 0 18a9 9 0 1 0 0-18z",
            "M12 10a2 2 0 1 0 0 4a2 2 0 1 0 0-4z",
        ],
        "net" => &["M8 3h8v6H8z", "M12 9v5", "M5 21v-4h14v4", "M12 14v3"],
        "board" => &["M4 4h16v16H4z", "M8 8h5v5H8z", "M16 8v2M16 13v3M8 16h4"],
        "boot" => &["M12 3v9", "M7 6.5a7 7 0 1 0 10 0"],
        _ => &["M5 5h14v14H5z"],
    };
    html! {
        <svg class="ico" viewBox="0 0 24 24" aria-hidden="true">
            { for paths.iter().map(|d| html! { <path d={*d} /> }) }
        </svg>
    }
}

impl VmPage {
    fn power(&mut self, ctx: &Context<Self>, action: &'static str) {
        self.notice = None;
        let name = ctx.props().name.clone();
        let done = match action {
            "start" => "Starting",
            "stop" => "Shutting down",
            "reboot" => "Rebooting",
            "restart" => "Restarting",
            "reset" => "Reset",
            "kill" => "Powered off",
            a => a,
        };
        ctx.link().send_future(async move {
            Msg::Finished(
                api::vm_action(&name, action)
                    .await
                    .map(|_| done.to_string()),
            )
        });
    }

    fn nodes(&self, ctx: &Context<Self>) -> Vec<String> {
        let mut nodes: Vec<String> = ctx
            .props()
            .cluster
            .as_ref()
            .map(|c| c.members.iter().map(|m| format!("node-{}", m.id)).collect())
            .unwrap_or_default();
        nodes.sort_by_key(|n| (n.len(), n.clone()));
        nodes
    }

    /// Its disk volumes, not counting cdroms (an ISO may be shared).
    fn own_disks(&self) -> Vec<String> {
        self.vm
            .iter()
            .flat_map(|v| v.hw.disks.iter())
            .filter(|d| d.device != "cdrom")
            .filter_map(|d| d.volume.clone())
            .collect()
    }

    fn volume_size(&self, v: &str) -> Option<u64> {
        self.volumes
            .iter()
            .find(|x| x.name == v)
            .map(|x| x.size_bytes)
    }

    /// CPU use over the samples, as % of its vCPUs.
    fn cpu_pct(&self, vcpus: u32) -> Vec<f64> {
        let n = vcpus.max(1) as f64;
        self.cpu
            .iter()
            .zip(self.cpu.iter().skip(1))
            .map(|((t0, c0), (t1, c1))| {
                let dt = (t1 - t0) / 1000.0;
                if dt <= 0.0 {
                    0.0
                } else {
                    ((c1 - c0) / dt / n * 100.0).clamp(0.0, 100.0)
                }
            })
            .collect()
    }

    fn move_dialog(&self, ctx: &Context<Self>, vm: &Vm) -> Option<Callback<MouseEvent>> {
        let here = vm.node.clone().unwrap_or_default();
        let others: Vec<String> = self.nodes(ctx).into_iter().filter(|n| *n != here).collect();
        if others.is_empty() || vm.error.is_some() {
            return None;
        }
        let opts: Vec<(&str, &str)> = others.iter().map(|n| (n.as_str(), n.as_str())).collect();
        let spec = Spec::new(&format!("Move {}?", vm.name), "Move")
            .message(html! { <p>{ if vm.running() {
                "Its disks move to that node. It's shut down here first, then started there."
            } else {
                "Its disks move to that node, and it runs there from now on."
            } }</p> })
            .field(Field::select("node", "To", opts[0].0, &opts));
        Some(
            ctx.link()
                .callback(move |_| Msg::Ask(spec.clone(), Action::Move)),
        )
    }

    fn header(&self, ctx: &Context<Self>, vm: &Vm) -> Html {
        let link = ctx.link();
        let (class, _, why) = state(vm);
        let mut facts = Vec::new();
        if let Some(n) = &vm.node {
            facts.push(if vm.running() {
                format!("on {}", n)
            } else {
                format!("placed on {}", n)
            });
        }
        if let Some(c) = vm.vcpus() {
            facts.push(format!("{} vCPU", c));
        }
        if let Some(k) = vm.memory_kib() {
            facts.push(fmt::bytes(k * 1024));
        }
        if let Some(m) = &vm.hw.machine {
            facts.push(m.clone());
        }
        html! {
            <header class={classes!("vm-header", class)}>
                <div class="vm-ident">
                    <span class={classes!("dot", "big", class)}></span>
                    <div>
                        <div class="vm-title">
                            <h1>{ &vm.name }</h1>
                            { state_tag(vm) }
                            if vm.restart_pending {
                                <span class="tag warn" title="its definition changed since it started">{ "RESTART PENDING" }</span>
                            }
                        </div>
                        <div class="vm-facts muted">
                            { facts.join(" · ") }
                            if let Some(w) = why { <span class="small">{ format!(" — {}", w) }</span> }
                        </div>
                    </div>
                </div>
                if ctx.props().can_write {
                    { self.controls(ctx, vm) }
                } else if vm.console {
                    <div class="vm-controls">
                        <button class="btn" onclick={link.callback(|_| Msg::Console(true))}>{ "Console" }</button>
                    </div>
                }
            </header>
        }
    }

    fn controls(&self, ctx: &Context<Self>, vm: &Vm) -> Html {
        let link = ctx.link();
        let p = |a: &'static str| link.callback(move |_| Msg::Power(a));
        let running = vm.running();
        let usable = vm.error.is_none();
        // One power button: what it does depends on where it is.
        let power = if !usable {
            Html::default()
        } else if !vm.want_running && !running {
            html! { <button class="btn primary power" onclick={p("start")}>{ "▶  Start" }</button> }
        } else if vm.want_running && running {
            html! { <button class="btn power" onclick={p("stop")}>{ "⏻  Shut down" }</button> }
        } else if vm.want_running {
            html! { <button class="btn power" onclick={p("stop")} title="it should run, but isn't yet: stop trying">{ "Cancel start" }</button> }
        } else {
            html! { <button class="btn power danger" onclick={p("kill")} title="it's shutting down: stop it now instead">{ "Power off now" }</button> }
        };
        let deletable = !vm.want_running && !running && vm.unreachable.is_none();
        let disks = self.own_disks();
        let delete = {
            let spec = Spec::new(&format!("Delete {}?", vm.name), "Delete")
                .message(html! { <p>{ format!("Deletes its definition, /{}. This can't be undone.", vm.xml_path()) }</p> })
                .field(
                    Field::check("disks", "Also delete its disk volumes", false).hint(&if disks.is_empty() {
                        "(it has none)".to_string()
                    } else {
                        format!("{}: gone for good, snapshots too", disks.join(", "))
                    }),
                )
                .danger();
            link.callback(move |_| Msg::Ask(spec.clone(), Action::Delete))
        };
        let mv = self.move_dialog(ctx, vm);
        let item = |label: &str, hint: &str, cb: Option<Callback<MouseEvent>>, danger: bool| {
            let disabled = cb.is_none();
            html! {
                <button class={classes!("menu-item", danger.then_some("danger"))} disabled={disabled}
                    onclick={cb.unwrap_or_default()}>
                    <span>{ label }</span>
                    <span class="muted small">{ hint }</span>
                </button>
            }
        };
        html! {
            <div class="vm-controls">
                if vm.console {
                    <button class="btn" onclick={link.callback(|_| Msg::Console(true))}>{ "Console" }</button>
                }
                if running && usable {
                    <button class="btn" onclick={p("reboot")}>{ "Reboot" }</button>
                }
                { power }
                <div class="menu-wrap" onkeydown={link.batch_callback(|e: KeyboardEvent| (e.key() == "Escape").then_some(Msg::Menu(false)))}>
                    <button class="btn" title="More actions" onclick={link.callback(|_| Msg::Menu(true))}>{ "⋯" }</button>
                    if self.menu {
                        <div class="menu-back" onclick={link.callback(|_| Msg::Menu(false))}></div>
                        <div class="menu">
                            { item("Restart", "apply its current definition", (running && usable).then(|| p("restart")), false) }
                            { item("Move…", "to another node", mv, false) }
                            <hr />
                            { item("Reset", "hard reset", running.then(|| p("reset")), true) }
                            { item("Power off", "pull the plug", running.then(|| p("kill")), true) }
                            { item("Delete…", if deletable { "remove this VM" } else { "shut it down first" }, deletable.then_some(delete), true) }
                        </div>
                    }
                </div>
            </div>
        }
    }

    fn screen(&self, ctx: &Context<Self>, vm: &Vm) -> Html {
        let link = ctx.link();
        let off = if vm.console && self.console {
            Some("Showing in the console window")
        } else if vm.console {
            None
        } else if vm.running() {
            Some("No console")
        } else if vm.want_running {
            Some("Starting…")
        } else {
            Some("Powered off")
        };
        html! {
            <section class="vm-screen">
                if let Some(text) = off {
                    <div class="preview off">
                        <span class="off-text">{ text }</span>
                        if text == "Powered off" && ctx.props().can_write && vm.error.is_none() {
                            <button class="btn primary" onclick={link.callback(|_| Msg::Power("start"))}>{ "▶  Start" }</button>
                        }
                    </div>
                } else {
                    <ConsolePreview name={vm.name.clone()} on_open={link.callback(|_| Msg::Console(true))} />
                }
            </section>
        }
    }

    fn side(&self, ctx: &Context<Self>, vm: &Vm) -> Html {
        let vcpus = vm.vcpus().unwrap_or(1);
        let pct = self.cpu_pct(vcpus);
        let disk_bytes: u64 = vm
            .hw
            .disks
            .iter()
            .filter(|d| d.device != "cdrom")
            .filter_map(|d| d.volume.as_deref().and_then(|v| self.volume_size(v)))
            .sum();
        let n_disks = vm.hw.disks.iter().filter(|d| d.device != "cdrom").count();
        let mv = self.move_dialog(ctx, vm).filter(|_| ctx.props().can_write);
        html! {
            <aside class="vm-side">
                <div class="fact cpu-fact">
                    <div class="fact-label">{ "CPU" }</div>
                    <div class="fact-value">{
                        pct.last().map_or("—".to_string(), |p| format!("{:.0}%", p))
                    }</div>
                    <div class="fact-sub">{
                        match vm.cpu_s {
                            Some(c) => format!("of {} vCPU · {} used", vcpus, fmt::duration(c)),
                            None => format!("{} vCPU", vcpus),
                        }
                    }</div>
                    <CpuChart values={pct} empty={if vm.running() { "Measuring…" } else { "Not running" }} />
                </div>
                <div class="facts two">
                    <div class="fact">
                        <div class="fact-label">{ "Memory" }</div>
                        <div class="fact-value">{ vm.memory_kib().map_or("—".into(), |k| fmt::bytes(k * 1024)) }</div>
                        <div class="fact-sub">{ if vm.running() { "assigned" } else { "configured" } }</div>
                    </div>
                    <div class="fact">
                        <div class="fact-label">{ "Storage" }</div>
                        <div class="fact-value">{ if disk_bytes > 0 { fmt::bytes(disk_bytes) } else { "—".into() } }</div>
                        <div class="fact-sub">{ match n_disks { 1 => "1 disk".to_string(), n => format!("{} disks", n) } }</div>
                    </div>
                </div>
                <div class="placement">
                    <div class="fact-label">{ "Placement" }</div>
                    <div class="kv small">
                        <span>{ "Node" }</span><span>{ vm.node.clone().unwrap_or_else(|| "none (its disks aren't served)".into()) }</span>
                        <span>{ "Follows" }</span><span>{ if vm.anchor.is_empty() { "—".into() } else { format!("volume {}", vm.anchor) } }</span>
                        <span>{ "Keep running" }</span><span>{ if vm.want_running { "yes, restarted if it stops" } else { "no" } }</span>
                    </div>
                    if let Some(mv) = mv {
                        <button class="btn small" onclick={mv}>{ "Move…" }</button>
                    }
                </div>
            </aside>
        }
    }

    fn tab_btn(&self, ctx: &Context<Self>, t: Tab, label: &str) -> Html {
        html! {
            <button type="button" class={classes!("seg-btn", (self.tab == t).then_some("on"))}
                onclick={ctx.link().callback(move |_| Msg::Tab(t))}>{ label }</button>
        }
    }

    fn hardware(&self, vm: &Vm) -> Html {
        let hw = &vm.hw;
        let row = |ico: &str, label: String, value: Html, meta: String| {
            html! {
                <li class="hw-row">
                    { icon(ico) }
                    <span class="hw-label">{ label }</span>
                    <span class="hw-value">{ value }</span>
                    <span class="hw-meta muted small">{ meta }</span>
                </li>
            }
        };
        let mut rows = vec![
            row(
                "cpu",
                "Processors".into(),
                html! { { vm.vcpus().map_or("—".into(), |c| format!("{} vCPU", c)) } },
                String::new(),
            ),
            row(
                "memory",
                "Memory".into(),
                html! { { vm.memory_kib().map_or("—".into(), |k| fmt::bytes(k * 1024)) } },
                String::new(),
            ),
            row(
                "board",
                "Machine".into(),
                html! { { hw.machine.clone().unwrap_or_else(|| "—".into()) } },
                hw.arch.clone().unwrap_or_default(),
            ),
        ];
        if !hw.boot.is_empty() {
            rows.push(row(
                "boot",
                "Boot order".into(),
                html! { { hw.boot.iter().map(|b| boot_name(b)).collect::<Vec<_>>().join(" → ") } },
                String::new(),
            ));
        }
        for d in &hw.disks {
            let cd = d.device == "cdrom";
            let label = format!(
                "{}{}",
                if cd { "CD/DVD" } else { "Hard disk" },
                d.target
                    .as_ref()
                    .map(|t| format!(" ({})", t))
                    .unwrap_or_default()
            );
            let value = match (&d.volume, &d.source) {
                (Some(v), _) => html! { <>{ "volume " }<strong>{ v }</strong></> },
                (None, Some(s)) => html! {
                    <code title="not a raidish volume: it must exist on whichever node runs the VM">{ s }</code>
                },
                (None, None) => html! { "empty" },
            };
            let meta = [
                d.volume
                    .as_deref()
                    .and_then(|v| self.volume_size(v))
                    .map(fmt::bytes),
                d.bus.clone(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" · ");
            rows.push(row(if cd { "cdrom" } else { "disk" }, label, value, meta));
        }
        for (i, n) in hw.nics.iter().enumerate() {
            let value = html! { <>{ format!("{} ", n.kind) }<strong>{ n.source.clone().unwrap_or_default() }</strong></> };
            let meta = [
                n.model.clone(),
                Some(
                    n.mac
                        .clone()
                        .unwrap_or_else(|| "MAC assigned at start".into()),
                ),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" · ");
            rows.push(row("net", format!("Network (net{})", i), value, meta));
        }
        if hw.nics.is_empty() {
            rows.push(row(
                "net",
                "Network".into(),
                html! { <span class="muted">{ "none" }</span> },
                String::new(),
            ));
        }
        html! { <ul class="hw">{ for rows }</ul> }
    }

    fn definition(&self, ctx: &Context<Self>, vm: &Vm) -> Html {
        let link = ctx.link();
        let can_write = ctx.props().can_write;
        let oninput = link.callback(|e: InputEvent| {
            Msg::XmlInput(e.target_unchecked_into::<HtmlTextAreaElement>().value())
        });
        html! {
            <div class="definition">
                <div class="def-head">
                    <span class="muted small">
                        <code>{ format!("/{}", vm.xml_path()) }</code>
                        { " — libvirt domain XML. raidish fills in the disks' NBD sources, the VNC console and its metadata when it starts the VM." }
                    </span>
                    if can_write && self.draft.is_none() && self.xml.is_some() {
                        <button class="btn" onclick={link.callback(|_| Msg::Edit(true))}>{ "Edit" }</button>
                    }
                </div>
                if let Some(e) = &self.xml_error {
                    <div class="banner">{ e }</div>
                }
                if let Some(draft) = &self.draft {
                    <textarea class="xml" spellcheck="false" value={draft.clone()} {oninput} />
                    <div class="modal-actions">
                        <span class="muted small">{ if vm.running() { "Applies when it's restarted." } else { "Applies on the next start." } }</span>
                        <button class="btn" onclick={link.callback(|_| Msg::Edit(false))} disabled={self.saving}>{ "Cancel" }</button>
                        <button class="btn primary" onclick={link.callback(|_| Msg::SaveXml)} disabled={self.saving}>
                            { if self.saving { "Saving…" } else { "Save" } }
                        </button>
                    </div>
                } else if let Some((_, x)) = &self.xml {
                    <pre class="xml">{ x }</pre>
                } else {
                    <p class="muted">{ "Loading…" }</p>
                }
            </div>
        }
    }
}

fn boot_name(dev: &str) -> &str {
    match dev {
        "hd" => "disk",
        "cdrom" => "CD/DVD",
        "network" => "network",
        "fd" => "floppy",
        d => d,
    }
}

#[derive(Properties, PartialEq)]
pub struct CpuProps {
    /// Percentages, oldest first.
    pub values: Vec<f64>,
    /// Shown while there's nothing to draw.
    pub empty: AttrValue,
}

/// CPU use, 0–100%, filled, the newest at the right.
#[function_component]
pub fn CpuChart(p: &CpuProps) -> Html {
    let (w, h) = (CPU_SAMPLES as f64, 40.0);
    let n = p.values.len();
    if n < 2 {
        return html! {
            <div class="cpu-chart empty"><span class="muted small">{ p.empty.clone() }</span></div>
        };
    }
    // Right-aligned: a short history fills from the right.
    let x0 = w - (n - 1) as f64;
    let pts: Vec<(f64, f64)> = p
        .values
        .iter()
        .enumerate()
        .map(|(i, v)| (x0 + i as f64, h - v / 100.0 * (h - 2.0) - 1.0))
        .collect();
    let line: String = pts
        .iter()
        .enumerate()
        .map(|(i, (x, y))| format!("{}{:.2},{:.2}", if i == 0 { "M" } else { " L" }, x, y))
        .collect();
    let area = format!(
        "{} L{:.2},{} L{:.2},{} Z",
        line,
        pts[n - 1].0,
        h,
        pts[0].0,
        h
    );
    let mid = (h / 2.0).to_string();
    html! {
        <svg class="cpu-chart" viewBox={format!("0 0 {} {}", w, h)} preserveAspectRatio="none">
            <line class="grid" x1="0" x2={w.to_string()} y1={mid.clone()} y2={mid} />
            <path class="area" d={area} />
            <path class="line" d={line} />
        </svg>
    }
}
