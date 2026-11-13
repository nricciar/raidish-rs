//! The dashboard: opens the pool, refreshes it on a timer, lays out panels.

use crate::blockmap::BlockMap;
use crate::cluster::{self, Status};
use crate::panels::{ClusterPanel, Hero, Metaslabs, Objects, Timeline, TopologyPanel};
use crate::session::{LayoutData, Session, Shared, Snapshot, now_ms, tab_hidden};
use gloo_timers::callback::Interval;
use raidish::disk::IoStats;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
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
}

pub enum Msg {
    Opened(Result<Rc<Session>, String>),
    Refresh,
    Snapshot(Result<Rc<Snapshot>, String>),
    Layout(Rc<LayoutData>),
    ReloadLayout,
    Cluster(Result<Status, String>),
    Tick,
    Frag(String),
    FragDone(String, Result<(u64, u64), String>),
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
    frag: HashMap<String, Result<(u64, u64), String>>,
    now: f64,
    ticks: u64,
    _timers: Vec<Interval>,
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
            frag: HashMap::new(),
            now: now_ms(),
            ticks: 0,
            _timers: vec![refresh, tick],
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
                if self.loading || tab_hidden() {
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
            Msg::Frag(name) => {
                let Some(snap) = self.snap.clone() else {
                    return false;
                };
                self.frag.insert(name.clone(), Err("checking…".into()));
                ctx.link().send_future(async move {
                    let r = snap.fs.volume_fragmentation(&name).await;
                    Msg::FragDone(name, r)
                });
            }
            Msg::FragDone(name, r) => {
                self.frag.insert(name, r);
            }
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let header = html! {
            <header class="top">
                <div class="brand">
                    <span class="logo" aria-hidden="true"><i></i><i></i><i></i></span>
                    <span>{ "raidish" }</span>
                </div>
                <div class="top-meta">
                    { self.cluster.as_ref().map(|c| html! {
                        <span class="muted">{ format!("via node {} ({})", c.id, c.node) }</span>
                    }).unwrap_or_default() }
                    { self.refresh_note() }
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

        let Some(_session) = &self.session else {
            return html! {
                <>
                    { header }
                    <main class="wrap">
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
                    </main>
                </>
            };
        };

        let Some(snap) = &self.snap else {
            return html! {
                <>
                    { header }
                    <main class="wrap">
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
                    </main>
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
        let on_frag = ctx.link().callback(Msg::Frag);
        let on_reload = ctx.link().callback(|_| Msg::ReloadLayout);

        html! {
            <>
                { header }
                <main class="wrap">
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
                    <div class="grid two">
                        <Metaslabs snap={snap.clone()} />
                        <Timeline snap={snap.clone()} now={self.now} />
                    </div>
                    if let Some(layout) = &self.layout {
                        <Objects layout={Shared(layout.clone())} frag={Rc::new(self.frag.clone())} on_frag={on_frag} />
                    }
                </main>
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
