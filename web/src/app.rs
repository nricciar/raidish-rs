//! The dashboard: opens the pool, refreshes it on a timer, lays out panels.

use crate::blockmap::BlockMap;
use crate::cluster::{self, Status};
use crate::files::FileManager;
use crate::panels::{ClusterPanel, Hero, Metaslabs, Timeline, TopologyPanel};
use crate::session::{LayoutData, Session, Shared, Snapshot, now_ms, tab_hidden};
use crate::vm_page::VmPage;
use crate::vms::Vms;
use crate::volumes::Volumes;
use gloo_timers::callback::Interval;
use raidish_core::disk::IoStats;
use std::collections::VecDeque;
use std::rc::Rc;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use yew::prelude::*;

/// Pool refresh period.
const REFRESH_MS: u32 = 5_000;
/// The layout (a full tree walk) is re-read at most this often, and only
/// when the pool has committed since.
const LAYOUT_MIN_MS: f64 = 30_000.0;
/// I/O samples kept per disk (one per refresh).
const HISTORY: usize = 36;

/// Who's signed in (None on a node without TLS: no sign-in).
#[derive(Properties, PartialEq)]
pub struct Props {
    #[prop_or_default]
    pub user: Option<String>,
    #[prop_or_default]
    pub on_sign_out: Callback<()>,
    /// May change things (not a read-only token).
    #[prop_or(true)]
    pub can_write: bool,
}

/// Which page is shown, from the URL's fragment: `#files/<dir>` for the
/// file manager, anything else for the overview.
#[derive(Clone, PartialEq, Debug)]
pub enum Route {
    Overview,
    Files(String),
    /// `#vms`: the list; `#vms/<name>`: one VM.
    Vms(Option<String>),
}

impl Route {
    fn current() -> Route {
        let hash = web_sys::window()
            .and_then(|w| w.location().hash().ok())
            .unwrap_or_default();
        let hash = hash.trim_start_matches('#');
        if let Some(rest) = hash.strip_prefix("vms")
            && (rest.is_empty() || rest.starts_with('/'))
        {
            let name = rest.trim_matches('/');
            return Route::Vms((!name.is_empty()).then(|| {
                js_sys::decode_uri_component(name)
                    .map(String::from)
                    .unwrap_or_else(|_| name.to_string())
            }));
        }
        match hash.strip_prefix("files") {
            Some(rest) if rest.is_empty() || rest.starts_with('/') => Route::Files(
                rest.split('/')
                    .filter(|p| !p.is_empty())
                    .map(|p| {
                        js_sys::decode_uri_component(p)
                            .map(String::from)
                            .unwrap_or_else(|_| p.to_string())
                    })
                    .collect::<Vec<_>>()
                    .join("/"),
            ),
            _ => Route::Overview,
        }
    }

    pub fn href(&self) -> String {
        match self {
            Route::Overview => "#overview".into(),
            Route::Vms(None) => "#vms".into(),
            Route::Vms(Some(n)) => format!("#vms/{}", crate::api::encode_path(n)),
            Route::Files(p) if p.is_empty() => "#files".into(),
            Route::Files(p) => format!("#files/{}", crate::api::encode_path(p)),
        }
    }

    pub fn go(&self) {
        if let Some(w) = web_sys::window() {
            let _ = w.location().set_hash(&self.href());
        }
    }
}

pub enum Msg {
    Opened(Result<Rc<Session>, String>),
    Refresh,
    Snapshot(Result<Rc<Snapshot>, String>),
    Layout(Rc<LayoutData>),
    ReloadLayout,
    Cluster(Result<Status, String>),
    Tick,
    Route,
}

/// One I/O sample of a disk.
#[derive(Clone, Copy, PartialEq)]
pub struct Sample {
    pub at_ms: f64,
    pub io: IoStats,
    pub online: bool,
}

pub struct App {
    session: Option<Rc<Session>>,
    error: Option<String>,
    snap: Option<Rc<Snapshot>>,
    snap_error: Option<String>,
    snap_ms: f64,
    loading: bool,
    layout: Option<Rc<LayoutData>>,
    layout_loading: bool,
    layout_ms: f64,
    want_layout: bool,
    cluster: Option<Status>,
    cluster_error: Option<String>,
    history: Vec<VecDeque<Sample>>,
    now: f64,
    ticks: u64,
    route: Route,
    /// The last directory the file manager showed (the tab goes back to it).
    files_dir: String,
    _timers: Vec<Interval>,
    _hash: Closure<dyn FnMut()>,
}

impl Component for App {
    type Message = Msg;
    type Properties = Props;

    fn create(ctx: &Context<Self>) -> Self {
        ctx.link()
            .send_future(async { Msg::Opened(Session::open().await.map(Rc::new)) });
        ctx.link()
            .send_future(async { Msg::Cluster(cluster::fetch().await) });
        let link = ctx.link().clone();
        let refresh = Interval::new(REFRESH_MS, move || link.send_message(Msg::Refresh));
        let link = ctx.link().clone();
        let tick = Interval::new(1_000, move || link.send_message(Msg::Tick));
        let link = ctx.link().clone();
        let hash = Closure::<dyn FnMut()>::new(move || link.send_message(Msg::Route));
        if let Some(w) = web_sys::window() {
            w.set_onhashchange(Some(hash.as_ref().unchecked_ref()));
        }
        let route = Route::current();
        let files_dir = match &route {
            Route::Files(d) => d.clone(),
            Route::Overview | Route::Vms(_) => String::new(),
        };
        App {
            session: None,
            error: None,
            snap: None,
            snap_error: None,
            snap_ms: 0.0,
            loading: false,
            layout: None,
            layout_loading: false,
            layout_ms: 0.0,
            want_layout: true,
            cluster: None,
            cluster_error: None,
            history: Vec::new(),
            now: now_ms(),
            ticks: 0,
            route,
            files_dir,
            _timers: vec![refresh, tick],
            _hash: hash,
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Msg) -> bool {
        match msg {
            Msg::Opened(Ok(session)) => {
                self.history = vec![VecDeque::new(); session.disks.len()];
                self.session = Some(session);
                self.error = None;
                ctx.link().send_message(Msg::Refresh);
            }
            Msg::Opened(Err(e)) => {
                self.error = Some(e);
                // Try again later (the node may still be starting).
                ctx.link().send_future(async {
                    gloo_timers::future::sleep(std::time::Duration::from_secs(5)).await;
                    Msg::Opened(Session::open().await.map(Rc::new))
                });
            }
            Msg::Refresh => {
                let Some(session) = self.session.clone() else {
                    return false;
                };
                // The pool is only read for the overview.
                if self.loading || tab_hidden() || self.route != Route::Overview {
                    return false;
                }
                self.loading = true;
                ctx.link().send_future(async move {
                    Msg::Snapshot(session.snapshot().await.map(Rc::new))
                });
            }
            Msg::Snapshot(result) => {
                self.loading = false;
                self.snap_ms = now_ms();
                if let Some(session) = &self.session {
                    let online = session.online();
                    for (d, io) in session.io_stats().into_iter().enumerate() {
                        let h = &mut self.history[d];
                        h.push_back(Sample {
                            at_ms: self.snap_ms,
                            io,
                            online: online[d],
                        });
                        while h.len() > HISTORY {
                            h.pop_front();
                        }
                    }
                }
                match result {
                    Ok(snap) => {
                        self.snap_error = None;
                        let changed = self
                            .layout
                            .as_ref()
                            .is_none_or(|l| l.txg != snap.report.txg);
                        let due = now_ms() - self.layout_ms >= LAYOUT_MIN_MS;
                        if !self.layout_loading && (self.want_layout || (changed && due)) {
                            self.want_layout = false;
                            self.layout_loading = true;
                            let s = snap.clone();
                            ctx.link()
                                .send_future(async move { Msg::Layout(Rc::new(s.layout().await)) });
                        }
                        self.snap = Some(snap);
                    }
                    Err(e) => {
                        let reachable = self
                            .session
                            .as_ref()
                            .is_some_and(|s| s.online().contains(&true));
                        self.snap_error = Some(if reachable {
                            e
                        } else {
                            "no disk is reachable (is the node down?)".into()
                        });
                    }
                }
            }
            Msg::Layout(layout) => {
                self.layout_loading = false;
                self.layout_ms = now_ms();
                self.layout = Some(layout);
            }
            Msg::ReloadLayout => {
                self.want_layout = true;
                ctx.link().send_message(Msg::Refresh);
            }
            Msg::Cluster(result) => match result {
                Ok(s) => {
                    self.cluster = Some(s);
                    self.cluster_error = None;
                }
                Err(e) => self.cluster_error = Some(e),
            },
            Msg::Tick => {
                self.now = now_ms();
                self.ticks += 1;
                if self.ticks.is_multiple_of(2) && !tab_hidden() {
                    ctx.link()
                        .send_future(async { Msg::Cluster(cluster::fetch().await) });
                }
            }
            Msg::Route => {
                let route = Route::current();
                if route == self.route {
                    return false;
                }
                if let Route::Files(d) = &route {
                    self.files_dir = d.clone();
                }
                let to_overview = route == Route::Overview;
                self.route = route;
                if to_overview {
                    ctx.link().send_message(Msg::Refresh);
                }
            }
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let on_files = matches!(self.route, Route::Files(_));
        let on_vms = matches!(self.route, Route::Vms(_));
        let on_overview = self.route == Route::Overview;
        let files_href = Route::Files(self.files_dir.clone()).href();
        let header = html! {
            <header class="top">
                <div class="brand">
                    <span class="logo" aria-hidden="true"><i></i><i></i><i></i></span>
                    <span>{ "raidish" }</span>
                </div>
                <nav class="tabs">
                    <a class={classes!("tab", on_overview.then_some("on"))} href="#overview">{ "Overview" }</a>
                    <a class={classes!("tab", on_files.then_some("on"))} href={files_href}>{ "File Manager" }</a>
                    <a class={classes!("tab", on_vms.then_some("on"))} href="#vms">{ "VMs" }</a>
                </nav>
                <div class="top-meta">
                    { self.cluster.as_ref().map(|c| html! {
                        <span class="muted">{ format!("via {}", c.node) }</span>
                    }).unwrap_or_default() }
                    if on_overview { { self.refresh_note() } }
                    if let Some(user) = &ctx.props().user {
                        <span class="user">
                            <span class="muted">{ user }</span>
                            <button class="btn small" onclick={ctx.props().on_sign_out.reform(|_| ())}>
                                { "Sign out" }
                            </button>
                        </span>
                    }
                </div>
            </header>
        };
        let path: AttrValue = match &self.route {
            Route::Files(d) => d.clone().into(),
            Route::Overview | Route::Vms(_) => self.files_dir.clone().into(),
        };
        let can_write = ctx.props().can_write;
        let on_navigate = Callback::from(|dir: String| Route::Files(dir).go());
        html! {
            <>
                { header }
                <main>
                    if on_overview { { self.overview(ctx) } }
                    if let Route::Vms(None) = &self.route {
                        <div class="wrap">
                            <Vms active={true} {can_write} />
                        </div>
                    }
                    if let Route::Vms(Some(name)) = &self.route {
                        <div class="wrap">
                            <VmPage key={name.clone()} name={name.clone()} {can_write} cluster={self.cluster.clone()} />
                        </div>
                    }
                    // Kept mounted on the overview too, so uploads carry on.
                    <div class="wrap" hidden={!on_files}>
                        <FileManager {path} active={on_files} {can_write} {on_navigate} />
                        <Volumes active={on_files} {can_write} cluster={self.cluster.clone()}
                            session={self.session.clone().map(Shared)} />
                    </div>
                </main>
                <crate::activity::Activity />
            </>
        }
    }
}

impl App {
    fn overview(&self, ctx: &Context<Self>) -> Html {
        let Some(_session) = &self.session else {
            return html! {
                <>
                    <div class="wrap">
                        <div class="card empty">
                            if let Some(e) = &self.error {
                                <h2>{ "Can't open the pool" }</h2>
                                <p>{ e }</p>
                                <p class="muted">{ "Retrying every 5 s." }</p>
                            } else {
                                <h2>{ "Connecting to the disks…" }</h2>
                            }
                        </div>
                        { self.cluster_view() }
                    </div>
                </>
            };
        };

        let Some(snap) = &self.snap else {
            return html! {
                <>
                    <div class="wrap">
                        <div class="card empty">
                            if let Some(e) = &self.snap_error {
                                <h2>{ "Can't read the pool" }</h2>
                                <p>{ e }</p>
                                <p class="muted">{ "Is it formatted? Retrying every 5 s." }</p>
                            } else {
                                <h2>{ "Reading the pool…" }</h2>
                            }
                        </div>
                        { self.cluster_view() }
                    </div>
                </>
            };
        };
        let snap = Shared(snap.clone());
        let history = Rc::new(self.history.clone());
        let online = Rc::new(
            self.session
                .as_ref()
                .map(|s| s.online())
                .unwrap_or_default(),
        );
        let on_reload = ctx.link().callback(|_| Msg::ReloadLayout);

        html! {
            <>
                <div class="wrap">
                    if let Some(e) = &self.snap_error {
                        <div class="banner">{ format!("Last refresh failed: {} (showing the previous state)", e) }</div>
                    }
                    <Hero snap={snap.clone()} now={self.now} />
                    <div class="grid two">
                        <TopologyPanel snap={snap.clone()} history={history} online={online} cluster={self.cluster.clone()} />
                        { self.cluster_view() }
                    </div>
                    if let Some(layout) = &self.layout {
                        <BlockMap layout={Shared(layout.clone())} loading={self.layout_loading} on_reload={on_reload} age={fmt_age(self.layout_ms, self.now)} />
                    } else {
                        <section class="card"><h2>{ "Block map" }</h2><p class="muted">{ "Walking the tree…" }</p></section>
                    }
                    <crate::hosts::VmHosts />
                    <div class="grid two">
                        <Metaslabs snap={snap.clone()} />
                        <Timeline snap={snap.clone()} now={self.now} />
                    </div>
                </div>
            </>
        }
    }
}

fn fmt_age(then: f64, now: f64) -> AttrValue {
    crate::fmt::ago(then, now).into()
}

impl App {
    fn refresh_note(&self) -> Html {
        if self.snap_ms == 0.0 {
            return Html::default();
        }
        let live = self.snap_error.is_none() && self.now - self.snap_ms < (REFRESH_MS as f64) * 3.0;
        html! {
            <span class={classes!("live", (!live).then_some("stale"))}>
                <i></i>{ format!("updated {}", crate::fmt::ago(self.snap_ms, self.now)) }
            </span>
        }
    }

    fn cluster_view(&self) -> Html {
        html! { <ClusterPanel status={self.cluster.clone()} error={self.cluster_error.clone()} now={self.now} /> }
    }
}
