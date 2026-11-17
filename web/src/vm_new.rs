//! The New VM dialog: a VM from a few settings (its disk volume and XML are
//! made here), or an imported libvirt domain XML. Either way it's stored
//! through the VM API, which checks it.

use crate::api::{self, NewVm, Volume};
use crate::app::Route;
use wasm_bindgen_futures::JsFuture;
use web_sys::{HtmlInputElement, HtmlSelectElement, HtmlTextAreaElement};
use yew::prelude::*;

#[derive(Properties, PartialEq)]
pub struct Props {
    pub on_close: Callback<()>,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Mode {
    Create,
    Import,
}

pub enum Msg {
    Mode(Mode),
    Set(&'static str, String),
    Volumes(Vec<Volume>),
    Picked(web_sys::File),
    Read(Result<String, String>),
    Submit,
    Done(Result<String, String>),
    Cancel,
}

pub struct NewVmDialog {
    mode: Mode,
    name: String,
    vcpus: String,
    memory: String,
    disk: String,
    iso: String,
    network: String,
    xml: String,
    volumes: Vec<Volume>,
    busy: bool,
    error: Option<String>,
}

impl Component for NewVmDialog {
    type Message = Msg;
    type Properties = Props;

    fn create(ctx: &Context<Self>) -> Self {
        ctx.link()
            .send_future(async { Msg::Volumes(api::volumes().await.unwrap_or_default()) });
        NewVmDialog {
            mode: Mode::Create,
            name: String::new(),
            vcpus: "2".into(),
            memory: "2G".into(),
            disk: "20G".into(),
            iso: String::new(),
            network: "default".into(),
            xml: String::new(),
            volumes: Vec::new(),
            busy: false,
            error: None,
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Msg) -> bool {
        match msg {
            Msg::Mode(m) => {
                self.mode = m;
                self.error = None;
            }
            Msg::Set(k, v) => {
                match k {
                    "name" => self.name = v,
                    "vcpus" => self.vcpus = v,
                    "memory" => self.memory = v,
                    "disk" => self.disk = v,
                    "iso" => self.iso = v,
                    "network" => self.network = v,
                    "xml" => self.xml = v,
                    _ => {}
                }
                return k == "iso" || k == "name";
            }
            Msg::Volumes(v) => self.volumes = v,
            Msg::Picked(file) => {
                // A file named like a VM names it, unless one's been typed.
                if self.name.is_empty() {
                    let stem = file.name();
                    let stem = stem.strip_suffix(".xml").unwrap_or(&stem);
                    if api::valid_vm_name(stem) {
                        self.name = stem.to_string();
                    }
                }
                ctx.link().send_future(async move {
                    Msg::Read(
                        JsFuture::from(file.text())
                            .await
                            .map(|t| t.as_string().unwrap_or_default())
                            .map_err(|e| format!("{:?}", e)),
                    )
                });
            }
            Msg::Read(r) => match r {
                Ok(t) => self.xml = t,
                Err(e) => self.error = Some(e),
            },
            Msg::Submit => {
                if self.busy {
                    return false;
                }
                let name = self.name.trim().to_string();
                let job = match self.mode {
                    Mode::Create => match self.new_vm(&name) {
                        Ok((new, bytes)) => Some((new, bytes)),
                        Err(e) => {
                            self.error = Some(e);
                            return true;
                        }
                    },
                    Mode::Import => {
                        if self.xml.trim().is_empty() {
                            self.error = Some("paste the domain XML, or pick a file".into());
                            return true;
                        }
                        None
                    }
                };
                self.busy = true;
                self.error = None;
                let xml = self.xml.clone();
                ctx.link().send_future(async move {
                    let r = match job {
                        Some((new, bytes)) => api::vm_create_simple(new, bytes).await,
                        None => api::vm_create(&name, &xml).await,
                    };
                    Msg::Done(r.map(|_| name))
                });
            }
            Msg::Done(r) => {
                self.busy = false;
                match r {
                    Ok(name) => {
                        Route::Vms(Some(name)).go();
                        ctx.props().on_close.emit(());
                    }
                    Err(e) => self.error = Some(e),
                }
            }
            Msg::Cancel => {
                if !self.busy {
                    ctx.props().on_close.emit(());
                }
                return false;
            }
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let link = ctx.link();
        let onsubmit = link.callback(|e: SubmitEvent| {
            e.prevent_default();
            Msg::Submit
        });
        // Only a click on the backdrop itself, not one inside the form.
        let backdrop = link.batch_callback(|e: MouseEvent| {
            (e.target() == e.current_target()).then_some(Msg::Cancel)
        });
        let onkeydown =
            link.batch_callback(|e: KeyboardEvent| (e.key() == "Escape").then_some(Msg::Cancel));
        let seg = |m: Mode, label: &str| {
            html! {
                <button type="button" class={classes!("seg-btn", (self.mode == m).then_some("on"))}
                    onclick={link.callback(move |_| Msg::Mode(m))}>{ label }</button>
            }
        };
        html! {
            <div class="modal-back" onclick={backdrop} {onkeydown}>
                <form class="modal card wide" {onsubmit}>
                    <h2>{ "New virtual machine" }</h2>
                    <div class="seg">
                        { seg(Mode::Create, "Create") }
                        { seg(Mode::Import, "Import XML") }
                    </div>
                    { self.text(ctx, "name", "Name", &self.name, "web1", "Letters, digits, '-', '_', '.'") }
                    if self.mode == Mode::Create { { self.create_fields(ctx) } } else { { self.import_fields(ctx) } }
                    if let Some(e) = &self.error {
                        <p class="bad">{ e }</p>
                    }
                    <div class="modal-actions">
                        <button type="button" class="btn" onclick={link.callback(|_| Msg::Cancel)} disabled={self.busy}>{ "Cancel" }</button>
                        <button type="submit" class="btn primary" disabled={self.busy}>
                            { if self.busy { "Working…" } else if self.mode == Mode::Create { "Create" } else { "Import" } }
                        </button>
                    </div>
                </form>
            </div>
        }
    }
}

impl NewVmDialog {
    /// The form's VM, and its disk's size.
    fn new_vm(&self, name: &str) -> Result<(NewVm, u64), String> {
        if !api::valid_vm_name(name) {
            return Err("name it: letters, digits, '-', '_', '.'".into());
        }
        let vcpus: u32 = self
            .vcpus
            .trim()
            .parse()
            .ok()
            .filter(|n| (1..=512).contains(n))
            .ok_or("vCPUs: a number from 1")?;
        let memory = api::parse_size(&self.memory)
            .filter(|b| *b >= 64 << 20)
            .ok_or("memory: a size like 2G or 512M (at least 64M)")?;
        let disk = api::parse_size(&self.disk)
            .filter(|b| *b >= 1 << 20)
            .ok_or("disk: a size like 20G")?;
        let network = self.network.trim();
        Ok((
            NewVm {
                name: name.to_string(),
                vcpus,
                memory_mib: memory >> 20,
                disk: raidish::vm_def::disk_volume(name),
                iso: (!self.iso.is_empty()).then(|| self.iso.clone()),
                network: (!network.is_empty()).then(|| network.to_string()),
            },
            disk,
        ))
    }

    fn text(
        &self,
        ctx: &Context<Self>,
        key: &'static str,
        label: &str,
        value: &str,
        placeholder: &str,
        hint: &str,
    ) -> Html {
        let oninput = ctx.link().callback(move |e: InputEvent| {
            Msg::Set(key, e.target_unchecked_into::<HtmlInputElement>().value())
        });
        html! {
            <label class="field">
                <span>{ label }</span>
                <input type="text" value={value.to_string()} {oninput} placeholder={placeholder.to_string()}
                    autocomplete="off" spellcheck="false" />
                if !hint.is_empty() { <span class="muted small">{ hint }</span> }
            </label>
        }
    }

    fn create_fields(&self, ctx: &Context<Self>) -> Html {
        let onchange = ctx.link().callback(|e: Event| {
            Msg::Set(
                "iso",
                e.target_unchecked_into::<HtmlSelectElement>().value(),
            )
        });
        let name = self.name.trim();
        let disk = if name.is_empty() {
            "<name>-disk0".to_string()
        } else {
            raidish::vm_def::disk_volume(name)
        };
        html! {
            <>
                <div class="field-row">
                    { self.text(ctx, "vcpus", "vCPUs", &self.vcpus, "2", "") }
                    { self.text(ctx, "memory", "Memory", &self.memory, "2G", "") }
                    { self.text(ctx, "disk", "Disk", &self.disk, "20G", "") }
                </div>
                <label class="field">
                    <span>{ "Install ISO" }</span>
                    <select {onchange}>
                        <option value="" selected={self.iso.is_empty()}>{ "None" }</option>
                        { for self.volumes.iter().map(|v| html! {
                            <option value={v.name.clone()} selected={v.name == self.iso}>
                                { format!("{} ({})", v.name, crate::fmt::bytes(v.size_bytes)) }
                            </option>
                        }) }
                    </select>
                    <span class="muted small">{ "A volume holding the ISO, attached as a CD-ROM. It boots from the disk, then the CD-ROM." }</span>
                </label>
                { self.text(ctx, "network", "Network", &self.network, "default",
                    "A libvirt network on every node that may run it. Leave it empty for no network.") }
                <p class="muted small">{ format!("Creates volume {} and VM {}. Start it from its page.", disk,
                    if name.is_empty() { "<name>" } else { name }) }</p>
            </>
        }
    }

    fn import_fields(&self, ctx: &Context<Self>) -> Html {
        let oninput = ctx.link().callback(|e: InputEvent| {
            Msg::Set(
                "xml",
                e.target_unchecked_into::<HtmlTextAreaElement>().value(),
            )
        });
        let onchange = ctx.link().batch_callback(|e: Event| {
            let input = e.target_unchecked_into::<HtmlInputElement>();
            input.files().and_then(|f| f.get(0)).map(Msg::Picked)
        });
        html! {
            <>
                <label class="field">
                    <span>{ "Domain XML" }</span>
                    <textarea class="xml short" spellcheck="false" value={self.xml.clone()} {oninput}
                        placeholder="<domain type='kvm'>…</domain>" />
                </label>
                <label class="field">
                    <span>{ "…or from a file" }</span>
                    <input type="file" accept=".xml,text/xml,application/xml" {onchange} />
                </label>
                <p class="muted small">
                    { "From " }<code>{ "virsh dumpxml" }</code>{ ". Disks that are raidish volumes say so: " }
                    <code>{ "<source file='raidish:VOLUME'/>" }</code>
                    { "; every disk must be one (no other VM's), and nothing may reach the host. If libvirt still has the domain defined on some host, undefine it there (raidish runs VMs as transient domains)." }
                </p>
            </>
        }
    }
}
