//! The file manager's explorer: browse the pool's namespace, make folders,
//! rename, delete, download and upload (with progress, one file at a time,
//! streamed from disk by the browser).

use crate::api::{self, DirEntry};
use crate::dialog::{Dialog, Field, Spec, Values};
use crate::fmt;
use crate::session::{now_ms, tab_hidden};
use crate::upload::{self, Upload};
use gloo_timers::callback::Interval;
use raidish_core::inode::ObjType;
use std::cell::Cell;
use std::rc::Rc;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use web_sys::{DragEvent, File, FileList, HtmlInputElement};
use yew::prelude::*;

/// The listing is re-read this often while the page is shown.
const LIST_REFRESH_MS: u32 = 10_000;

#[derive(Properties, PartialEq)]
pub struct Props {
    /// The directory shown ("" for the root), from the URL.
    pub path: AttrValue,
    /// The page is shown (otherwise it only keeps uploading).
    pub active: bool,
    pub can_write: bool,
    pub on_navigate: Callback<String>,
}

#[derive(Clone, Copy, PartialEq)]
pub enum SortKey {
    Name,
    Size,
    Modified,
}

pub enum UpState {
    Queued,
    Sending,
    Committing,
    Done,
    Failed(String),
}

struct UpItem {
    id: u64,
    path: String,
    size: f64,
    loaded: f64,
    state: UpState,
    file: Option<File>,
    handle: Option<Upload>,
    started_ms: f64,
    finished_ms: f64,
    /// For the rate: the last sample and a smoothed bytes/ms.
    sample: (f64, f64),
    rate: f64,
}

impl UpItem {
    fn active(&self) -> bool {
        matches!(
            self.state,
            UpState::Queued | UpState::Sending | UpState::Committing
        )
    }
}

#[derive(Clone)]
pub enum Action {
    Mkdir,
    Rename(String),
    Delete(Vec<(String, bool)>),
    Replace(Vec<File>),
}

pub enum Msg {
    Load,
    Loaded(String, Result<Vec<DirEntry>, String>),
    Open(String),
    Sort(SortKey),
    Pick,
    Picked(Vec<File>),
    DragOver(bool),
    Up(u64, upload::Event),
    Cancel(u64),
    ClearDone,
    Retry(u64),
    Ask(Spec, Action),
    Submit(Values),
    Close,
    Finished(Result<String, String>),
    Toggle(String),
    SelectAll(bool),
}

pub struct FileManager {
    entries: Option<Vec<DirEntry>>,
    /// Which path `entries` is for.
    listed: String,
    error: Option<String>,
    loading: bool,
    sort: (SortKey, bool),
    selected: std::collections::BTreeSet<String>,
    uploads: Vec<UpItem>,
    next_id: u64,
    drag: bool,
    dialog: Option<(Spec, Action)>,
    busy: bool,
    dialog_error: Option<String>,
    notice: Option<String>,
    input: NodeRef,
    active_uploads: Rc<Cell<usize>>,
    _timer: Interval,
    _unload: Closure<dyn FnMut(web_sys::Event)>,
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", dir, name)
    }
}

fn parent(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(p, _)| p.to_string())
        .unwrap_or_default()
}

fn file_list(list: Option<FileList>) -> Vec<File> {
    let Some(list) = list else { return Vec::new() };
    (0..list.length()).filter_map(|i| list.get(i)).collect()
}

fn check_name(name: &str) -> Result<String, String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("a name is needed".into());
    }
    if name.contains('/') || name == "." || name == ".." {
        return Err("a name can't contain '/' or be '.' or '..'".into());
    }
    Ok(name.to_string())
}

impl Component for FileManager {
    type Message = Msg;
    type Properties = Props;

    fn create(ctx: &Context<Self>) -> Self {
        ctx.link().send_message(Msg::Load);
        let link = ctx.link().clone();
        let timer = Interval::new(LIST_REFRESH_MS, move || link.send_message(Msg::Load));
        // Leaving the page would cut uploads short: ask first.
        let active_uploads = Rc::new(Cell::new(0));
        let n = active_uploads.clone();
        let unload = Closure::<dyn FnMut(web_sys::Event)>::new(move |e: web_sys::Event| {
            if n.get() > 0 {
                e.prevent_default();
                let _ = js_sys::Reflect::set(&e, &"returnValue".into(), &"".into());
            }
        });
        if let Some(w) = web_sys::window() {
            w.set_onbeforeunload(Some(unload.as_ref().unchecked_ref()));
        }
        FileManager {
            entries: None,
            listed: String::new(),
            error: None,
            loading: false,
            sort: (SortKey::Name, true),
            selected: Default::default(),
            uploads: Vec::new(),
            next_id: 0,
            drag: false,
            dialog: None,
            busy: false,
            dialog_error: None,
            notice: None,
            input: NodeRef::default(),
            active_uploads,
            _timer: timer,
            _unload: unload,
        }
    }

    fn changed(&mut self, ctx: &Context<Self>, old: &Props) -> bool {
        let p = ctx.props();
        if p.path != old.path {
            self.selected.clear();
            self.notice = None;
            if self.listed != p.path.as_str() {
                self.entries = None;
                self.error = None;
            }
        }
        if p.path != old.path || (p.active && !old.active) {
            ctx.link().send_message(Msg::Load);
        }
        true
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Msg) -> bool {
        let path = ctx.props().path.to_string();
        match msg {
            Msg::Load => {
                if self.loading || !ctx.props().active || tab_hidden() {
                    return false;
                }
                self.loading = true;
                ctx.link().send_future(async move {
                    let r = api::list_dir(&path).await;
                    Msg::Loaded(path, r)
                });
            }
            Msg::Loaded(p, r) => {
                self.loading = false;
                if p != path {
                    // Navigated meanwhile: read the new one.
                    ctx.link().send_message(Msg::Load);
                    return false;
                }
                match r {
                    Ok(mut entries) => {
                        // Volumes are managed below, not here.
                        entries.retain(|e| e.kind != ObjType::Volume);
                        self.selected
                            .retain(|n| entries.iter().any(|e| &e.name == n));
                        self.entries = Some(entries);
                        self.listed = p;
                        self.error = None;
                    }
                    Err(e) => self.error = Some(e),
                }
            }
            Msg::Open(p) => ctx.props().on_navigate.emit(p),
            Msg::Sort(k) => {
                self.sort = if self.sort.0 == k {
                    (k, !self.sort.1)
                } else {
                    (k, k == SortKey::Name)
                };
            }
            Msg::Pick => {
                if let Some(input) = self.input.cast::<HtmlInputElement>() {
                    input.set_value("");
                    input.click();
                }
                return false;
            }
            Msg::DragOver(on) => {
                if self.drag == on {
                    return false;
                }
                self.drag = on;
            }
            Msg::Picked(files) => {
                self.drag = false;
                if files.is_empty() {
                    return true;
                }
                let entries = self.entries.clone().unwrap_or_default();
                let mut clash = Vec::new();
                for f in &files {
                    if let Some(e) = entries.iter().find(|e| e.name == f.name()) {
                        if e.kind != ObjType::File {
                            self.notice = Some(format!(
                                "can't upload {}: there's a {} by that name here",
                                f.name(),
                                if e.is_dir() { "folder" } else { "volume" }
                            ));
                            return true;
                        }
                        clash.push(f.name());
                    }
                }
                if clash.is_empty() {
                    self.enqueue(ctx, &path, files);
                } else {
                    let msg = html! {
                        <p>{ if clash.len() == 1 {
                            format!("{} already exists here. Replace it?", clash[0])
                        } else {
                            format!("{} files already exist here ({}). Replace them?", clash.len(), clash.join(", "))
                        } }</p>
                    };
                    let spec = Spec::new("Replace files?", "Replace").message(msg).danger();
                    return Component::update(self, ctx, Msg::Ask(spec, Action::Replace(files)));
                }
            }
            Msg::Up(id, ev) => {
                let Some(u) = self.uploads.iter_mut().find(|u| u.id == id) else {
                    return false;
                };
                let now = now_ms();
                match ev {
                    upload::Event::Progress(loaded) => {
                        let (t0, l0) = u.sample;
                        let dt = now - t0;
                        if dt >= 250.0 {
                            let inst = (loaded - l0) / dt;
                            u.rate = if u.rate == 0.0 {
                                inst
                            } else {
                                u.rate * 0.75 + inst * 0.25
                            };
                            u.sample = (now, loaded);
                        }
                        u.loaded = loaded;
                    }
                    upload::Event::Sent => {
                        u.loaded = u.size;
                        u.state = UpState::Committing;
                    }
                    upload::Event::Done(r) => {
                        if !u.active() {
                            return false;
                        }
                        u.handle = None;
                        u.finished_ms = now;
                        match r {
                            Ok(()) => {
                                u.loaded = u.size;
                                u.state = UpState::Done;
                                u.file = None;
                            }
                            Err(e) => u.state = UpState::Failed(e),
                        }
                        self.pump(ctx);
                        ctx.link().send_message(Msg::Load);
                    }
                }
            }
            Msg::Cancel(id) => {
                if let Some(u) = self.uploads.iter_mut().find(|u| u.id == id) {
                    match &u.handle {
                        Some(h) => h.abort(),
                        None if matches!(u.state, UpState::Queued) => {
                            u.state = UpState::Failed("cancelled".into());
                            u.file = None;
                        }
                        None => {}
                    }
                }
                self.count_active();
            }
            Msg::Retry(id) => {
                if let Some(u) = self.uploads.iter_mut().find(|u| u.id == id)
                    && u.file.is_some()
                {
                    u.state = UpState::Queued;
                    u.loaded = 0.0;
                    u.rate = 0.0;
                }
                self.pump(ctx);
            }
            Msg::ClearDone => self.uploads.retain(|u| u.active()),
            Msg::Toggle(name) => {
                if !self.selected.remove(&name) {
                    self.selected.insert(name);
                }
            }
            Msg::SelectAll(on) => {
                self.selected.clear();
                if on && let Some(entries) = &self.entries {
                    self.selected.extend(
                        entries
                            .iter()
                            .filter(|e| e.kind != ObjType::Volume)
                            .map(|e| e.name.clone()),
                    );
                }
            }
            Msg::Ask(spec, action) => {
                self.dialog = Some((spec, action));
                self.busy = false;
                self.dialog_error = None;
            }
            Msg::Close => {
                if self.busy {
                    return false;
                }
                self.dialog = None;
            }
            Msg::Submit(values) => {
                let Some((_, action)) = &self.dialog else {
                    return false;
                };
                let action = action.clone();
                let get = |k: &str| values.get(k).cloned().unwrap_or_default();
                match action {
                    Action::Mkdir => match check_name(&get("name")) {
                        Ok(name) => {
                            let p = join(&path, &name);
                            self.run(ctx, async move {
                                api::mkdir(&p)
                                    .await
                                    .map(|_| format!("Made folder {}", name))
                            });
                        }
                        Err(e) => self.dialog_error = Some(e),
                    },
                    Action::Rename(ref from) => match check_name(&get("name")) {
                        Ok(name) => {
                            let from = from.clone();
                            let to = join(&parent(&from), &name);
                            self.run(ctx, async move {
                                api::rename(&from, &to)
                                    .await
                                    .map(|_| format!("Renamed to {}", name))
                            });
                        }
                        Err(e) => self.dialog_error = Some(e),
                    },
                    Action::Delete(ref items) => {
                        let items = items.clone();
                        self.run(ctx, async move {
                            let mut n = 0;
                            for (p, dir) in items {
                                n += api::delete_tree(p, dir).await?;
                            }
                            Ok(format!(
                                "Deleted {} item{}",
                                n,
                                if n == 1 { "" } else { "s" }
                            ))
                        });
                    }
                    Action::Replace(files) => {
                        self.dialog = None;
                        self.enqueue(ctx, &path, files);
                    }
                }
            }
            Msg::Finished(r) => {
                self.busy = false;
                match r {
                    Ok(note) => {
                        self.dialog = None;
                        self.selected.clear();
                        self.notice = Some(note);
                        ctx.link().send_message(Msg::Load);
                    }
                    Err(e) => {
                        self.dialog_error = Some(e);
                        // Some of it may have happened.
                        ctx.link().send_message(Msg::Load);
                    }
                }
            }
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let p = ctx.props();
        let link = ctx.link();
        let path = p.path.to_string();
        let can_write = p.can_write;

        let crumbs = {
            let mut acc = String::new();
            let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
            let mut out = vec![html! {
                <button class={classes!("crumb", parts.is_empty().then_some("on"))}
                    onclick={link.callback(|_| Msg::Open(String::new()))}>{ "pool" }</button>
            }];
            for (i, part) in parts.iter().enumerate() {
                acc = join(&acc, part);
                let target = acc.clone();
                let last = i + 1 == parts.len();
                out.push(html! { <span class="muted">{ "/" }</span> });
                out.push(html! {
                    <button class={classes!("crumb", last.then_some("on"))}
                        onclick={link.callback(move |_| Msg::Open(target.clone()))}>{ *part }</button>
                });
            }
            out
        };

        let mut entries: Vec<DirEntry> = if self.listed == path {
            self.entries.clone().unwrap_or_default()
        } else {
            Vec::new()
        };
        let (key, asc) = self.sort;
        entries.sort_by(|a, b| {
            let ord = match key {
                SortKey::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
                SortKey::Size => a.size.cmp(&b.size),
                SortKey::Modified => a.mtime_ns.cmp(&b.mtime_ns),
            };
            // Folders first, whichever way.
            b.is_dir()
                .cmp(&a.is_dir())
                .then(if asc { ord } else { ord.reverse() })
        });
        let total: u64 = entries
            .iter()
            .filter(|e| e.kind == ObjType::File)
            .map(|e| e.size)
            .sum();

        let sort_head = |label: &'static str, k: SortKey, class: &'static str| {
            let arrow = if self.sort.0 == k {
                if self.sort.1 { " ▲" } else { " ▼" }
            } else {
                ""
            };
            html! {
                <th class={class}>
                    <button class="th-sort" onclick={link.callback(move |_| Msg::Sort(k))}>
                        { label }{ arrow }
                    </button>
                </th>
            }
        };

        let new_folder = link.callback(|_| {
            Msg::Ask(
                Spec::new("New folder", "Create").field(Field::text("name", "Name", "")),
                Action::Mkdir,
            )
        });
        let delete_selected = {
            let items: Vec<(String, bool)> = entries
                .iter()
                .filter(|e| self.selected.contains(&e.name))
                .map(|e| (join(&path, &e.name), e.is_dir()))
                .collect();
            let dirs = items.iter().filter(|(_, d)| *d).count();
            link.callback(move |_| {
                let msg = html! {
                    <p>{ format!("Delete {} item{}{}? This can't be undone.",
                        items.len(), if items.len() == 1 { "" } else { "s" },
                        if dirs > 0 { " (folders with everything in them)" } else { "" }) }</p>
                };
                Msg::Ask(
                    Spec::new("Delete", "Delete").message(msg).danger(),
                    Action::Delete(items.clone()),
                )
            })
        };
        let selectable = entries.iter().filter(|e| e.kind != ObjType::Volume);
        let all_selected = selectable.clone().next().is_some()
            && selectable
                .into_iter()
                .all(|e| self.selected.contains(&e.name));

        let drop_zone = can_write;
        let ondragover = link.batch_callback(move |e: DragEvent| {
            if !drop_zone {
                return None;
            }
            e.prevent_default();
            Some(Msg::DragOver(true))
        });
        let ondragleave = link.batch_callback(|e: DragEvent| {
            // Only leaving the zone, not moving onto a child.
            let inside = e
                .related_target()
                .and_then(|t| t.dyn_into::<web_sys::Node>().ok())
                .zip(
                    e.current_target()
                        .and_then(|t| t.dyn_into::<web_sys::Node>().ok()),
                )
                .is_some_and(|(to, zone)| zone.contains(Some(&to)));
            (!inside).then_some(Msg::DragOver(false))
        });
        let ondrop = link.batch_callback(move |e: DragEvent| {
            e.prevent_default();
            if !drop_zone {
                return None;
            }
            Some(Msg::Picked(file_list(
                e.data_transfer().and_then(|d| d.files()),
            )))
        });
        let onchange = link.callback(|e: Event| {
            Msg::Picked(file_list(
                e.target_unchecked_into::<HtmlInputElement>().files(),
            ))
        });

        html! {
            <section class="card fm">
                <div class="card-head">
                    <h2>{ "Files" }</h2>
                    <div class="toolbar">
                        if can_write {
                            if !self.selected.is_empty() {
                                <button class="btn danger" onclick={delete_selected}>
                                    { format!("Delete {}", self.selected.len()) }
                                </button>
                            }
                            <button class="btn" onclick={new_folder}>{ "New folder" }</button>
                            <button class="btn primary" onclick={link.callback(|_| Msg::Pick)}>{ "Upload" }</button>
                            <input ref={self.input.clone()} type="file" multiple=true hidden=true {onchange} />
                        }
                        <button class="btn ghost" title="Refresh" onclick={link.callback(|_| Msg::Load)}>
                            { if self.loading { "…" } else { "⟳" } }
                        </button>
                    </div>
                </div>
                <nav class="crumbs">{ for crumbs }</nav>
                if let Some(n) = &self.notice {
                    <div class="note">{ n }</div>
                }
                if let Some(e) = &self.error {
                    <div class="banner">{ format!("Can't list this folder: {}", e) }</div>
                }
                <div class={classes!("dropzone", self.drag.then_some("over"))}
                    {ondragover} {ondragleave} {ondrop}>
                    if self.drag {
                        <div class="drop-hint">{ format!("Drop to upload to /{}", path) }</div>
                    }
                    <div class="table-wrap">
                    <table class="objects files">
                        <thead><tr>
                            if can_write {
                                <th class="sel"><input type="checkbox" checked={all_selected}
                                    onchange={link.callback(move |_| Msg::SelectAll(!all_selected))} /></th>
                            }
                            { sort_head("Name", SortKey::Name, "") }
                            { sort_head("Size", SortKey::Size, "num") }
                            { sort_head("Modified", SortKey::Modified, "when") }
                            <th></th>
                        </tr></thead>
                        <tbody>
                            if !path.is_empty() {
                                <tr class="up">
                                    if can_write { <td></td> }
                                    <td colspan="4">
                                        <button class="name-link" onclick={
                                            let up = parent(&path);
                                            link.callback(move |_| Msg::Open(up.clone()))
                                        }><span class="ico">{ "↰" }</span>{ ".." }</button>
                                    </td>
                                </tr>
                            }
                            { for entries.iter().map(|e| self.row(ctx, &path, e)) }
                        </tbody>
                    </table>
                    </div>
                    if self.listed == path && entries.is_empty() && self.error.is_none() {
                        <p class="muted empty-dir">{
                            if can_write { "This folder is empty. Drop files here or use Upload." } else { "This folder is empty." }
                        }</p>
                    } else if self.entries.is_none() && self.error.is_none() {
                        <p class="muted empty-dir">{ "Loading…" }</p>
                    }
                </div>
                <p class="muted small">{ format!("{} item{}, {} in files", entries.len(),
                    if entries.len() == 1 { "" } else { "s" }, fmt::bytes(total)) }</p>
                { self.uploads_view(ctx) }
                if let Some((spec, _)) = &self.dialog {
                    <Dialog spec={spec.clone()} busy={self.busy} error={self.dialog_error.clone()}
                        on_submit={link.callback(Msg::Submit)} on_cancel={link.callback(|_| Msg::Close)} />
                }
            </section>
        }
    }
}

impl FileManager {
    fn row(&self, ctx: &Context<Self>, dir: &str, e: &DirEntry) -> Html {
        let link = ctx.link();
        let can_write = ctx.props().can_write;
        let full = join(dir, &e.name);
        let ico = match e.kind {
            ObjType::Directory => "📁",
            ObjType::Volume => "🖴",
            _ => "📄",
        };
        let kind = e.kind.name();
        let name = if e.is_dir() {
            let target = full.clone();
            html! {
                <button class="name-link" onclick={link.callback(move |_| Msg::Open(target.clone()))}>
                    <span class="ico">{ ico }</span>{ &e.name }
                </button>
            }
        } else if e.kind == ObjType::File {
            html! {
                <a class="name-link" href={api::download_url(&full)} download={e.name.clone()}>
                    <span class="ico">{ ico }</span>{ &e.name }
                </a>
            }
        } else {
            html! {
                <a class="name-link" href="#volumes">
                    <span class="ico">{ ico }</span>{ &e.name }
                    <span class={classes!("kind", kind)}>{ kind }</span>
                </a>
            }
        };
        let actions = if e.kind == ObjType::Volume {
            html! { <a class="btn ghost small" href="#volumes">{ "Manage" }</a> }
        } else {
            let rename = {
                let (from, n) = (full.clone(), e.name.clone());
                link.callback(move |_| {
                    Msg::Ask(
                        Spec::new("Rename", "Rename").field(Field::text("name", "New name", &n)),
                        Action::Rename(from.clone()),
                    )
                })
            };
            let delete = {
                let (p, dir, n) = (full.clone(), e.is_dir(), e.name.clone());
                link.callback(move |_| {
                    let msg = html! { <p>{
                        if dir { format!("Delete the folder {} and everything in it? This can't be undone.", n) }
                        else { format!("Delete {}? This can't be undone.", n) }
                    }</p> };
                    Msg::Ask(
                        Spec::new("Delete", "Delete").message(msg).danger(),
                        Action::Delete(vec![(p.clone(), dir)]),
                    )
                })
            };
            html! {
                <span class="row-actions">
                    if e.kind == ObjType::File {
                        <a class="btn ghost small" href={api::download_url(&full)} download={e.name.clone()}>{ "Download" }</a>
                    }
                    if can_write {
                        <button class="btn ghost small" onclick={rename}>{ "Rename" }</button>
                        <button class="btn ghost small danger" onclick={delete}>{ "Delete" }</button>
                    }
                </span>
            }
        };
        let checked = self.selected.contains(&e.name);
        let n = e.name.clone();
        html! {
            <tr class={classes!(checked.then_some("selected"))}>
                if can_write {
                    <td class="sel">
                        if e.kind != ObjType::Volume {
                            <input type="checkbox" {checked} onchange={link.callback(move |_| Msg::Toggle(n.clone()))} />
                        }
                    </td>
                }
                <td class="name">{ name }</td>
                <td class="num">{ if e.is_dir() { "—".to_string() } else { fmt::bytes(e.size) } }</td>
                <td class="when muted">{ fmt::date_ns(e.mtime_ns) }</td>
                <td class="acts">{ actions }</td>
            </tr>
        }
    }

    fn uploads_view(&self, ctx: &Context<Self>) -> Html {
        if self.uploads.is_empty() {
            return Html::default();
        }
        let link = ctx.link();
        let finished = self.uploads.iter().filter(|u| !u.active()).count();
        html! {
            <div class="uploads">
                <div class="card-head">
                    <h3>{ "Uploads" }</h3>
                    if finished > 0 {
                        <button class="btn ghost small" onclick={link.callback(|_| Msg::ClearDone)}>{ "Clear finished" }</button>
                    }
                </div>
                <ul>
                { for self.uploads.iter().map(|u| {
                    let pct = if u.size > 0.0 { (u.loaded / u.size * 100.0).min(100.0) } else { 100.0 };
                    let id = u.id;
                    let (class, status) = match &u.state {
                        UpState::Queued => ("queued", "waiting…".to_string()),
                        UpState::Sending => {
                            let rate = u.rate * 1000.0;
                            let eta = if rate > 0.0 { format!(", {} left", fmt::duration((u.size - u.loaded) / rate)) } else { String::new() };
                            ("sending", format!("{} of {} · {}/s{}", fmt::bytes(u.loaded as u64), fmt::bytes(u.size as u64), fmt::bytes(rate as u64), eta))
                        }
                        UpState::Committing => ("committing", "sent, committing…".to_string()),
                        UpState::Done => {
                            let secs = (u.finished_ms - u.started_ms) / 1000.0;
                            let avg = if secs > 0.0 { format!(" · {}/s", fmt::bytes((u.size / secs) as u64)) } else { String::new() };
                            ("done", format!("{} in {}{}", fmt::bytes(u.size as u64), fmt::duration(secs), avg))
                        }
                        UpState::Failed(e) => ("failed", e.clone()),
                    };
                    html! {
                        <li class={classes!("upload", class)}>
                            <div class="up-top">
                                <span class="mono up-name" title={u.path.clone()}>{ format!("/{}", u.path) }</span>
                                <span class="up-actions">
                                    if u.active() {
                                        <button class="btn ghost small" onclick={link.callback(move |_| Msg::Cancel(id))}>{ "Cancel" }</button>
                                    } else if matches!(u.state, UpState::Failed(_)) && u.file.is_some() {
                                        <button class="btn ghost small" onclick={link.callback(move |_| Msg::Retry(id))}>{ "Retry" }</button>
                                    }
                                </span>
                            </div>
                            <div class="bar"><i style={format!("width: {:.2}%", pct)}></i></div>
                            <div class="small muted up-status">
                                <span>{ status }</span>
                                <span>{ format!("{:.0}%", pct) }</span>
                            </div>
                        </li>
                    }
                }) }
                </ul>
            </div>
        }
    }

    fn enqueue(&mut self, ctx: &Context<Self>, dir: &str, files: Vec<File>) {
        for f in files {
            self.next_id += 1;
            self.uploads.push(UpItem {
                id: self.next_id,
                path: join(dir, &f.name()),
                size: f.size(),
                loaded: 0.0,
                state: UpState::Queued,
                file: Some(f),
                handle: None,
                started_ms: 0.0,
                finished_ms: 0.0,
                sample: (0.0, 0.0),
                rate: 0.0,
            });
        }
        self.pump(ctx);
    }

    /// Start the next queued upload, if none is running.
    fn pump(&mut self, ctx: &Context<Self>) {
        let running = self
            .uploads
            .iter()
            .any(|u| matches!(u.state, UpState::Sending | UpState::Committing));
        if !running
            && let Some(u) = self
                .uploads
                .iter_mut()
                .find(|u| matches!(u.state, UpState::Queued))
        {
            let id = u.id;
            let link = ctx.link().clone();
            let file = u.file.clone().expect("a queued upload has its file");
            let now = now_ms();
            u.started_ms = now;
            u.sample = (now, 0.0);
            match Upload::start(&u.path, &file, move |ev| link.send_message(Msg::Up(id, ev))) {
                Ok(h) => {
                    u.handle = Some(h);
                    u.state = UpState::Sending;
                }
                Err(e) => u.state = UpState::Failed(e),
            }
        }
        self.count_active();
    }

    fn count_active(&self) {
        self.active_uploads
            .set(self.uploads.iter().filter(|u| u.active()).count());
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
