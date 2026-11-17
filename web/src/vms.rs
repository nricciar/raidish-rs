//! VMs (see `raidish::vm`): the list (where each runs, start / stop, a
//! console), the New VM dialog, and the VNC console used by the list and
//! the VM page (`crate::vm_page`). The list is libvirt's live answer from
//! the node holding each VM's disks, re-read every few seconds.

use crate::api::{self, Vm};
use crate::app::Route;
use crate::dialog::{Dialog, Spec};
use crate::fmt;
use crate::session::tab_hidden;
use crate::vm_new::NewVmDialog;
use gloo_timers::callback::Interval;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use yew::prelude::*;

pub const REFRESH_MS: u32 = 4_000;

#[derive(Properties, PartialEq)]
pub struct Props {
    pub active: bool,
    pub can_write: bool,
}

pub enum Msg {
    Load,
    Loaded(Result<Vec<Vm>, String>),
    Do(String, &'static str),
    /// Confirmed (or not needing it): do it.
    Confirmed,
    Cancel,
    Finished(Result<String, String>),
    Console(Option<String>),
    New(bool),
}

pub struct Vms {
    vms: Option<Vec<Vm>>,
    error: Option<String>,
    loading: bool,
    notice: Option<String>,
    console: Option<String>,
    new: bool,
    /// Waiting for a yes: (spec, VM, action).
    confirm: Option<(Spec, String, &'static str)>,
    _timer: Interval,
}

impl Component for Vms {
    type Message = Msg;
    type Properties = Props;

    fn create(ctx: &Context<Self>) -> Self {
        ctx.link().send_message(Msg::Load);
        let link = ctx.link().clone();
        let timer = Interval::new(REFRESH_MS, move || link.send_message(Msg::Load));
        Vms {
            vms: None,
            error: None,
            loading: false,
            notice: None,
            console: None,
            new: false,
            confirm: None,
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
                    .send_future(async { Msg::Loaded(api::vms().await) });
            }
            Msg::Loaded(r) => {
                self.loading = false;
                match r {
                    Ok(v) => {
                        self.vms = Some(v);
                        self.error = None;
                    }
                    Err(e) => self.error = Some(e),
                }
            }
            Msg::Do(name, action) => {
                if let Some(spec) = confirm(action, &name) {
                    self.confirm = Some((spec, name, action));
                    return true;
                }
                self.run(ctx, name, action);
            }
            Msg::Cancel => self.confirm = None,
            Msg::Confirmed => {
                if let Some((_, name, action)) = self.confirm.take() {
                    self.run(ctx, name, action);
                }
            }
            Msg::Finished(r) => {
                match r {
                    Ok(note) => {
                        self.notice = Some(note);
                        self.error = None;
                    }
                    Err(e) => self.error = Some(e),
                }
                ctx.link().send_message(Msg::Load);
            }
            Msg::Console(c) => self.console = c,
            Msg::New(open) => self.new = open,
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let link = ctx.link();
        let can_write = ctx.props().can_write;
        let new_btn = html! {
            <button class="btn primary" onclick={link.callback(|_| Msg::New(true))}>{ "+ New VM" }</button>
        };
        html! {
            <section class="card" id="vms">
                <div class="card-head">
                    <h2>{ "Virtual machines" }</h2>
                    <div class="toolbar">
                        if can_write { { new_btn.clone() } }
                        <button class="btn ghost" title="Refresh" onclick={link.callback(|_| Msg::Load)}>
                            { if self.loading { "…" } else { "⟳" } }
                        </button>
                    </div>
                </div>
                if let Some(n) = &self.notice {
                    <div class="note">{ n }</div>
                }
                if let Some(e) = &self.error {
                    <div class="banner">{ e }</div>
                }
                {
                    match &self.vms {
                        None if self.error.is_none() => html! { <p class="muted">{ "Loading…" }</p> },
                        None => Html::default(),
                        Some(v) if v.is_empty() => html! {
                            <div class="empty-state">
                                <p>{ "No virtual machines yet." }</p>
                                <p class="muted small">{ "Create one from a few settings, or import a libvirt domain XML (" }
                                    <code>{ "virsh dumpxml" }</code>{ ") whose disks name raidish volumes." }</p>
                                if can_write { { new_btn } }
                            </div>
                        },
                        Some(v) => html! {
                            <div class="table-wrap">
                            <table class="objects vms">
                                <thead><tr>
                                    <th>{ "Name" }</th>
                                    <th>{ "State" }</th>
                                    <th>{ "Node" }</th>
                                    <th class="num">{ "vCPU" }</th>
                                    <th class="num">{ "Memory" }</th>
                                    <th>{ "Disks" }</th>
                                    <th></th>
                                </tr></thead>
                                <tbody>
                                    { for v.iter().map(|vm| self.row(ctx, vm)) }
                                </tbody>
                            </table>
                            </div>
                        },
                    }
                }
                if let Some((spec, _, _)) = &self.confirm {
                    <Dialog spec={spec.clone()} busy={false} error={None::<String>}
                        on_submit={link.callback(|_| Msg::Confirmed)} on_cancel={link.callback(|_| Msg::Cancel)} />
                }
                if self.new {
                    <NewVmDialog on_close={link.callback(|_| Msg::New(false))} />
                }
                if let Some(name) = &self.console {
                    <Console name={name.clone()} on_close={link.callback(|_| Msg::Console(None))} />
                }
            </section>
        }
    }
}

/// The confirmation for a power action on `name`; None: none needed
/// (`start`).
pub fn confirm(action: &str, name: &str) -> Option<Spec> {
    let (title, ok, msg, danger) = match action {
        "stop" => (
            "Shut down",
            "Shut down",
            "Asks the guest to shut down (ACPI power button). If it hasn't stopped in time, it's destroyed. It stays off until started again.",
            false,
        ),
        "reboot" => (
            "Reboot",
            "Reboot",
            "Asks the guest to reboot. It keeps running from the XML it was started with.",
            false,
        ),
        "restart" => (
            "Restart",
            "Restart",
            "Shuts the guest down and starts it again from its current XML, which applies any changes to it.",
            false,
        ),
        "reset" => (
            "Reset",
            "Reset",
            "A hard reset, like pressing the reset button. Unsaved data in the guest is lost.",
            true,
        ),
        "kill" => (
            "Power off",
            "Power off",
            "Stops it at once, like pulling the plug. Unsaved data in the guest is lost, and it stays off.",
            true,
        ),
        _ => return None,
    };
    let spec = Spec::new(&format!("{} {}?", title, name), ok).message(html! { <p>{ msg }</p> });
    Some(if danger { spec.danger() } else { spec })
}

/// A VM's state as a tag (with why, on hover, when it's not running).
pub fn state_tag(vm: &Vm) -> Html {
    let (class, text, title) = state(vm);
    html! { <span class={classes!("tag", class)} title={title.unwrap_or_default()}>{ text }</span> }
}

/// (tag class, label, explanation).
pub fn state(vm: &Vm) -> (&'static str, String, Option<String>) {
    if vm.error.is_some() {
        ("bad", "INVALID".to_string(), vm.error.clone())
    } else if let Some(s) = &vm.state {
        if !vm.want_running && s == "running" {
            return (
                "warn",
                "STOPPING".into(),
                Some("shutting down (destroyed if it doesn't stop in time)".into()),
            );
        }
        let class = if s == "running" { "ok" } else { "warn" };
        (class, s.to_uppercase(), None)
    } else if vm.unreachable.is_some() {
        ("warn", "UNKNOWN".into(), vm.unreachable.clone())
    } else if vm.start_error.is_some() {
        ("bad", "FAILED".into(), vm.start_error.clone())
    } else if vm.want_running {
        ("warn", "STARTING".into(), None)
    } else {
        ("idle", "STOPPED".into(), None)
    }
}

impl Vms {
    fn run(&mut self, ctx: &Context<Self>, name: String, action: &'static str) {
        self.notice = None;
        ctx.link().send_future(async move {
            Msg::Finished(
                api::vm_action(&name, action)
                    .await
                    .map(|_| format!("{}: {} requested", name, action))
                    .map_err(|e| format!("{}: {}", name, e)),
            )
        });
    }

    fn row(&self, ctx: &Context<Self>, vm: &Vm) -> Html {
        let link = ctx.link();
        let can_write = ctx.props().can_write;
        let name = vm.name.clone();
        let href = Route::Vms(Some(name.clone())).href();
        let act = |a: &'static str| {
            let n = name.clone();
            link.callback(move |e: MouseEvent| {
                e.stop_propagation();
                Msg::Do(n.clone(), a)
            })
        };
        let console = {
            let n = name.clone();
            link.callback(move |e: MouseEvent| {
                e.stop_propagation();
                Msg::Console(Some(n.clone()))
            })
        };
        let open = {
            let r = Route::Vms(Some(name.clone()));
            Callback::from(move |_: MouseEvent| r.go())
        };
        let disks = vm.hw.disks.iter().filter(|d| d.device != "cdrom").count();
        html! {
            <tr class="vm-row" onclick={open}>
                <td>
                    <a class="name-link" href={href} onclick={Callback::from(|e: MouseEvent| e.stop_propagation())}>
                        <span class={classes!("dot", state(vm).0)}></span>{ &name }
                    </a>
                    if vm.restart_pending { <span class="tag warn" title="its XML changed since it started: restart to apply">{ "RESTART PENDING" }</span> }
                </td>
                <td>{ state_tag(vm) }</td>
                <td>{ vm.node.clone().unwrap_or_else(|| "—".into()) }</td>
                <td class="num">{ vm.vcpus().map_or("—".into(), |v| v.to_string()) }</td>
                <td class="num">{ vm.memory_kib().map_or("—".into(), |k| fmt::bytes(k * 1024)) }</td>
                <td class="small muted">{ match disks { 0 => "—".to_string(), 1 => "1 disk".into(), n => format!("{} disks", n) } }</td>
                <td class="acts">
                    <span class="row-actions">
                        if vm.console {
                            <button class="btn small" onclick={console}>{ "Console" }</button>
                        }
                        if can_write && vm.error.is_none() {
                            if vm.want_running {
                                <button class="btn ghost small" onclick={act("stop")} title="shut the guest down">{ "Stop" }</button>
                            } else {
                                <button class="btn ghost small" onclick={act("start")}>{ "Start" }</button>
                            }
                        }
                    </span>
                </td>
            </tr>
        }
    }
}

// ---- the console ----------------------------------------------------------

fn call(obj: &JsValue, method: &str) {
    if let Ok(f) = js_sys::Reflect::get(obj, &method.into())
        && let Some(f) = f.dyn_ref::<js_sys::Function>()
    {
        let _ = f.call0(obj);
    }
}

/// A noVNC connection to a VM's console, drawn into an element.
struct Rfb {
    rfb: JsValue,
    _listeners: Vec<Closure<dyn FnMut(web_sys::Event)>>,
}

impl Rfb {
    /// Connect `name`'s console into `el`. `status` gets "" once connected,
    /// and why when it ends. `view_only`: a preview (no input, lower
    /// quality).
    fn connect(
        el: &web_sys::Element,
        name: &str,
        view_only: bool,
        status: Callback<String>,
    ) -> Result<Rfb, String> {
        let window = web_sys::window().expect("window");
        let ctor = js_sys::Reflect::get(&window, &"RFB".into()).map_err(|e| format!("{:?}", e))?;
        let Some(ctor) = ctor.dyn_ref::<js_sys::Function>() else {
            return Err("the VNC client (noVNC) didn't load".into());
        };
        let options = js_sys::Object::new();
        let _ = js_sys::Reflect::set(&options, &"shared".into(), &true.into());
        let args = js_sys::Array::of3(el, &api::console_url(name).into(), &options);
        let rfb = js_sys::Reflect::construct(ctor, &args).map_err(|e| format!("{:?}", e))?;
        let set = |k: &str, v: JsValue| {
            let _ = js_sys::Reflect::set(&rfb, &k.into(), &v);
        };
        set("scaleViewport", true.into());
        if view_only {
            set("viewOnly", true.into());
            set("qualityLevel", 3.into());
            set("compressionLevel", 6.into());
        } else {
            set("focusOnClick", true.into());
        }
        let target: &web_sys::EventTarget = rfb.unchecked_ref();
        let on = |event: &str, f: Box<dyn FnMut(web_sys::Event)>| {
            let c = Closure::wrap(f);
            let _ = target.add_event_listener_with_callback(event, c.as_ref().unchecked_ref());
            c
        };
        let s = status.clone();
        let connected = on("connect", Box::new(move |_| s.emit(String::new())));
        let disconnected = on(
            "disconnect",
            Box::new(move |e| {
                let clean = js_sys::Reflect::get(&e, &"detail".into())
                    .ok()
                    .and_then(|d| js_sys::Reflect::get(&d, &"clean".into()).ok())
                    .and_then(|c| c.as_bool())
                    .unwrap_or(false);
                status.emit(if clean {
                    "Disconnected.".into()
                } else {
                    "Connection lost (is the VM still running?)".into()
                })
            }),
        );
        if !view_only {
            call(&rfb, "focus");
        }
        Ok(Rfb {
            rfb,
            _listeners: vec![connected, disconnected],
        })
    }

    fn ctrl_alt_del(&self) {
        call(&self.rfb, "sendCtrlAltDel");
    }
}

impl Drop for Rfb {
    fn drop(&mut self) {
        call(&self.rfb, "disconnect");
    }
}

#[derive(Properties, PartialEq)]
pub struct ConsoleProps {
    pub name: String,
    pub on_close: Callback<()>,
}

pub enum ConsoleMsg {
    Status(String),
    CtrlAltDel,
    Close,
}

/// The full console, in a modal.
pub struct Console {
    screen: NodeRef,
    rfb: Option<Rfb>,
    status: String,
}

impl Component for Console {
    type Message = ConsoleMsg;
    type Properties = ConsoleProps;

    fn create(_ctx: &Context<Self>) -> Self {
        Console {
            screen: NodeRef::default(),
            rfb: None,
            status: "Connecting…".into(),
        }
    }

    fn rendered(&mut self, ctx: &Context<Self>, first: bool) {
        if !first {
            return;
        }
        let Some(el) = self.screen.cast::<web_sys::Element>() else {
            return;
        };
        let status = ctx.link().callback(ConsoleMsg::Status);
        match Rfb::connect(&el, &ctx.props().name, false, status) {
            Ok(r) => self.rfb = Some(r),
            Err(e) => ctx.link().send_message(ConsoleMsg::Status(e)),
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: ConsoleMsg) -> bool {
        match msg {
            ConsoleMsg::Status(s) => self.status = s,
            ConsoleMsg::CtrlAltDel => {
                if let Some(r) = &self.rfb {
                    r.ctrl_alt_del();
                }
                return false;
            }
            ConsoleMsg::Close => {
                self.rfb = None;
                ctx.props().on_close.emit(());
                return false;
            }
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let link = ctx.link();
        html! {
            <div class="console-back">
                <div class="console">
                    <div class="console-head">
                        <strong>{ &ctx.props().name }</strong>
                        <span class="muted small">{ &self.status }</span>
                        <span class="row-actions">
                            <button class="btn small" onclick={link.callback(|_| ConsoleMsg::CtrlAltDel)}>{ "Ctrl+Alt+Del" }</button>
                            <button class="btn small" onclick={link.callback(|_| ConsoleMsg::Close)}>{ "Close" }</button>
                        </span>
                    </div>
                    <div class="console-screen" ref={self.screen.clone()}></div>
                </div>
            </div>
        }
    }
}

#[derive(Properties, PartialEq)]
pub struct PreviewProps {
    pub name: String,
    /// Clicked: open the full console.
    pub on_open: Callback<()>,
}

/// A live, view-only thumbnail of a VM's console.
pub struct ConsolePreview {
    screen: NodeRef,
    rfb: Option<Rfb>,
    status: String,
}

pub enum PreviewMsg {
    Status(String),
    Reconnect,
}

impl ConsolePreview {
    fn connect(&mut self, ctx: &Context<Self>) {
        self.rfb = None;
        let Some(el) = self.screen.cast::<web_sys::Element>() else {
            return;
        };
        // noVNC leaves its old canvas behind.
        el.set_inner_html("");
        let status = ctx.link().callback(PreviewMsg::Status);
        match Rfb::connect(&el, &ctx.props().name, true, status) {
            Ok(r) => self.rfb = Some(r),
            Err(e) => ctx.link().send_message(PreviewMsg::Status(e)),
        }
    }
}

impl Component for ConsolePreview {
    type Message = PreviewMsg;
    type Properties = PreviewProps;

    fn create(_ctx: &Context<Self>) -> Self {
        ConsolePreview {
            screen: NodeRef::default(),
            rfb: None,
            status: "Connecting…".into(),
        }
    }

    fn rendered(&mut self, ctx: &Context<Self>, first: bool) {
        if first {
            self.connect(ctx);
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: PreviewMsg) -> bool {
        match msg {
            PreviewMsg::Status(s) => self.status = s,
            PreviewMsg::Reconnect => {
                self.status = "Connecting…".into();
                self.connect(ctx);
            }
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let open = ctx.props().on_open.reform(|_: MouseEvent| ());
        let lost = !self.status.is_empty() && self.status != "Connecting…";
        let reconnect = ctx.link().callback(|e: MouseEvent| {
            e.stop_propagation();
            PreviewMsg::Reconnect
        });
        html! {
            <div class="preview" onclick={open} title="Open the console">
                <div class="preview-screen" ref={self.screen.clone()}></div>
                if !self.status.is_empty() {
                    <div class="preview-note">
                        <span>{ &self.status }</span>
                        if lost { <button class="btn small" onclick={reconnect}>{ "Reconnect" }</button> }
                    </div>
                }
                <div class="preview-hint">{ "Open console ⤢" }</div>
            </div>
        }
    }
}
