//! The activity footer: the cluster's activity log (`GET /api/v1/events`,
//! see `raidish::events`). Collapsed, it shows the newest event; expanded,
//! a table of recent ones, newest first.

use crate::app::Route;
use crate::session::{now_ms, tab_hidden};
use gloo_timers::callback::Interval;
use std::collections::VecDeque;
use wasm_bindgen::JsCast;
use yew::prelude::*;

/// How often it asks for new events.
const POLL_MS: u32 = 3_000;
/// Events kept here (the log keeps more: "Older" reads them).
const KEEP: usize = 2_000;
/// Events read at once.
const PAGE: usize = 200;
const OPEN_KEY: &str = "raidish.activity.open";
const HEIGHT_KEY: &str = "raidish.activity.height";
/// The expanded table's height, px: at least this...
const MIN_HEIGHT: f64 = 120.0;

fn window_height() -> f64 {
    web_sys::window()
        .and_then(|w| w.inner_height().ok())
        .and_then(|h| h.as_f64())
        .unwrap_or(800.0)
}

/// ...and at most most of the window (the bar and the top bar stay).
fn max_height() -> f64 {
    (window_height() - 140.0).max(MIN_HEIGHT)
}

fn default_height() -> f64 {
    (window_height() * 0.28).clamp(MIN_HEIGHT, 280.0)
}

fn remembered_height() -> f64 {
    storage()
        .and_then(|s| s.get_item(HEIGHT_KEY).ok().flatten())
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|h| h.is_finite())
        .unwrap_or_else(default_height)
}

fn remember_height(h: Option<f64>) {
    if let Some(s) = storage() {
        let _ = match h {
            Some(h) => s.set_item(HEIGHT_KEY, &format!("{:.0}", h)),
            None => s.remove_item(HEIGHT_KEY),
        };
    }
}

pub use raidish_types::api_types::{Event, EventPage as Page, Level, Subject};

async fn fetch(query: String) -> Result<Page, String> {
    let resp = crate::session::get(&format!("/api/v1/events?{}", query))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.ok() {
        let body = resp.text().await.unwrap_or_default();
        return Err(if body.trim().is_empty() {
            format!("HTTP {}", resp.status())
        } else {
            body.trim().to_string()
        });
    }
    resp.json().await.map_err(|e| e.to_string())
}

fn storage() -> Option<web_sys::Storage> {
    web_sys::window()?.local_storage().ok()?
}

fn remembered_open() -> bool {
    storage()
        .and_then(|s| s.get_item(OPEN_KEY).ok().flatten())
        .is_some_and(|v| v == "1")
}

fn remember_open(open: bool) {
    if let Some(s) = storage() {
        let _ = s.set_item(OPEN_KEY, if open { "1" } else { "0" });
    }
}

/// Which events show.
#[derive(Clone, Copy, PartialEq)]
pub enum Filter {
    All,
    Warnings,
    Errors,
}

impl Filter {
    fn shows(&self, e: &Event) -> bool {
        match self {
            Filter::All => true,
            Filter::Warnings => e.level != Level::Info,
            Filter::Errors => e.level == Level::Error,
        }
    }
}

pub enum Msg {
    Poll,
    Got(Result<Page, String>),
    Older,
    GotOlder(Result<Page, String>),
    Toggle,
    Filter(Filter),
    Search(String),
    /// Resizing: the grip was grabbed (pointer y), moved, let go.
    DragStart(f64),
    DragMove(f64),
    DragEnd,
    /// Back to the default height.
    ResetHeight,
}

pub struct Activity {
    /// Newest first.
    events: VecDeque<Event>,
    /// The number the next event will get (None: nothing read yet).
    next: Option<u64>,
    /// The oldest number the log still has.
    first: u64,
    loading: bool,
    loading_older: bool,
    error: Option<String>,
    open: bool,
    /// Warnings and errors that came in while it was collapsed.
    unseen: usize,
    filter: Filter,
    search: String,
    /// The expanded table's height (px).
    height: f64,
    /// While resizing: where the pointer and the height started.
    drag: Option<(f64, f64)>,
    _poll: Interval,
}

impl Component for Activity {
    type Message = Msg;
    type Properties = ();

    fn create(ctx: &Context<Self>) -> Self {
        ctx.link().send_message(Msg::Poll);
        let link = ctx.link().clone();
        let poll = Interval::new(POLL_MS, move || link.send_message(Msg::Poll));
        Activity {
            events: VecDeque::new(),
            next: None,
            first: 0,
            loading: false,
            loading_older: false,
            error: None,
            open: remembered_open(),
            unseen: 0,
            filter: Filter::All,
            search: String::new(),
            height: remembered_height(),
            drag: None,
            _poll: poll,
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Msg) -> bool {
        match msg {
            Msg::Poll => {
                if self.loading || (tab_hidden() && self.next.is_some()) {
                    return false;
                }
                self.loading = true;
                let query = match self.next {
                    Some(n) => format!("after={}&limit=1000", n.saturating_sub(1)),
                    None => format!("limit={}", PAGE),
                };
                ctx.link()
                    .send_future(async move { Msg::Got(fetch(query).await) });
                false
            }
            Msg::Got(Ok(page)) => {
                self.loading = false;
                self.error = None;
                let first_read = self.next.is_none();
                // A newer log (the pool was formatted again): start over.
                if self.next.is_some_and(|n| page.next < n) {
                    self.events.clear();
                }
                let more = page.events.len() >= 1000;
                for e in page.events {
                    if !first_read && !self.open && e.level != Level::Info {
                        self.unseen += 1;
                    }
                    self.events.push_front(e);
                }
                self.events.truncate(KEEP);
                self.next = Some(page.next);
                self.first = page.first;
                if more {
                    ctx.link().send_message(Msg::Poll);
                }
                true
            }
            Msg::Got(Err(e)) => {
                self.loading = false;
                let changed = self.error.as_ref() != Some(&e);
                self.error = Some(e);
                changed
            }
            Msg::Older => {
                let Some(oldest) = self.events.back().map(|e| e.seq) else {
                    return false;
                };
                if self.loading_older {
                    return false;
                }
                self.loading_older = true;
                let query = format!("before={}&limit={}", oldest, PAGE);
                ctx.link()
                    .send_future(async move { Msg::GotOlder(fetch(query).await) });
                true
            }
            Msg::GotOlder(r) => {
                self.loading_older = false;
                match r {
                    Ok(page) => {
                        let oldest = self.events.back().map_or(u64::MAX, |e| e.seq);
                        for e in page.events.into_iter().rev() {
                            if e.seq < oldest {
                                self.events.push_back(e);
                            }
                        }
                        self.first = page.first;
                    }
                    Err(e) => self.error = Some(e),
                }
                true
            }
            Msg::Toggle => {
                self.open = !self.open;
                if self.open {
                    self.unseen = 0;
                }
                remember_open(self.open);
                true
            }
            Msg::Filter(f) => {
                self.filter = f;
                true
            }
            Msg::Search(s) => {
                self.search = s;
                true
            }
            Msg::DragStart(y) => {
                self.drag = Some((y, self.height));
                true
            }
            Msg::DragMove(y) => {
                let Some((y0, h0)) = self.drag else {
                    return false;
                };
                // Dragging up makes it taller.
                self.height = (h0 + y0 - y).clamp(MIN_HEIGHT, max_height());
                true
            }
            Msg::DragEnd => {
                if self.drag.take().is_none() {
                    return false;
                }
                remember_height(Some(self.height));
                true
            }
            Msg::ResetHeight => {
                self.height = default_height();
                remember_height(None);
                true
            }
        }
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        let link = ctx.link();
        let toggle = link.callback(|_| Msg::Toggle);
        let latest = self.events.front();
        let bar = html! {
            <button class="activity-bar" onclick={toggle} aria-expanded={self.open.to_string()}>
                <span class="activity-title">
                    <span class={classes!("caret", self.open.then_some("open"))} aria-hidden="true">{ "▸" }</span>
                    { "Activity" }
                    if self.unseen > 0 {
                        <span class="tag bad">{ self.unseen }</span>
                    }
                </span>
                <span class="activity-latest">
                    if let Some(e) = &self.error {
                        <span class="muted">{ format!("Activity log unavailable: {}", e) }</span>
                    } else if let Some(e) = latest {
                        <span class={classes!("dot", level_class(e.level))}></span>
                        <span class="muted mono">{ when(e.ts_ms, true) }</span>
                        <span class="ellipsis">{ &e.message }</span>
                    } else if self.next.is_some() {
                        <span class="muted">{ "Nothing yet" }</span>
                    } else {
                        <span class="muted">{ "Loading…" }</span>
                    }
                </span>
            </button>
        };
        let body = if self.open {
            self.table(ctx)
        } else {
            Html::default()
        };
        // The top edge, dragged to resize (pointer capture keeps the moves
        // coming wherever the pointer goes).
        let grip = if self.open {
            let down = link.callback(|e: PointerEvent| {
                e.prevent_default();
                if let Some(t) = e
                    .target()
                    .and_then(|t| t.dyn_into::<web_sys::Element>().ok())
                {
                    let _ = t.set_pointer_capture(e.pointer_id());
                }
                Msg::DragStart(e.client_y() as f64)
            });
            let dragging = self.drag.is_some();
            let moved = link.batch_callback(move |e: PointerEvent| {
                dragging.then(|| Msg::DragMove(e.client_y() as f64))
            });
            let up = link.callback(|_: PointerEvent| Msg::DragEnd);
            html! {
                <div class={classes!("activity-grip", dragging.then_some("dragging"))}
                    role="separator" aria-orientation="horizontal"
                    title="Drag to resize; double-click to reset"
                    onpointerdown={down} onpointermove={moved}
                    onpointerup={up.clone()} onpointercancel={up}
                    ondblclick={link.callback(|_| Msg::ResetHeight)}></div>
            }
        } else {
            Html::default()
        };
        html! {
            <footer class={classes!("activity", self.open.then_some("open"), self.drag.is_some().then_some("resizing"))}>
                { grip }
                { bar }
                { body }
            </footer>
        }
    }
}

impl Activity {
    fn table(&self, ctx: &Context<Self>) -> Html {
        let link = ctx.link();
        let on_filter = link.callback(|e: Event_| {
            let v = e
                .target()
                .and_then(|t| t.dyn_into::<web_sys::HtmlSelectElement>().ok())
                .map(|s| s.value())
                .unwrap_or_default();
            Msg::Filter(match v.as_str() {
                "warn" => Filter::Warnings,
                "error" => Filter::Errors,
                _ => Filter::All,
            })
        });
        let on_search = link.callback(|e: InputEvent| {
            let v = e
                .target()
                .and_then(|t| t.dyn_into::<web_sys::HtmlInputElement>().ok())
                .map(|i| i.value())
                .unwrap_or_default();
            Msg::Search(v)
        });
        let needle = self.search.trim().to_lowercase();
        let shown: Vec<&Event> = self
            .events
            .iter()
            .filter(|e| self.filter.shows(e))
            .filter(|e| {
                needle.is_empty()
                    || e.message.to_lowercase().contains(&needle)
                    || e.node.to_lowercase().contains(&needle)
                    || e.subject.to_string().to_lowercase().contains(&needle)
                    || e.by
                        .as_deref()
                        .unwrap_or("")
                        .to_lowercase()
                        .contains(&needle)
            })
            .collect();
        let more = self
            .events
            .back()
            .is_some_and(|e| e.seq > self.first.max(1));
        html! {
            <div class="activity-body" style={format!("height: {:.0}px", self.height.min(max_height()))}>
                <div class="activity-tools">
                    <select class="btn small" onchange={on_filter} aria-label="Show">
                        <option value="all" selected={self.filter == Filter::All}>{ "All events" }</option>
                        <option value="warn" selected={self.filter == Filter::Warnings}>{ "Warnings and errors" }</option>
                        <option value="error" selected={self.filter == Filter::Errors}>{ "Errors" }</option>
                    </select>
                    <input class="activity-search" type="search" placeholder="Filter by node, VM, volume…"
                        value={self.search.clone()} oninput={on_search} />
                    <span class="muted small">{ format!("{} shown", shown.len()) }</span>
                </div>
                <div class="activity-scroll">
                    <table class="activity-table">
                        <thead>
                            <tr>
                                <th>{ "Time" }</th>
                                <th>{ "" }</th>
                                <th>{ "Event" }</th>
                                <th>{ "Object" }</th>
                                <th>{ "Node" }</th>
                                <th>{ "By" }</th>
                            </tr>
                        </thead>
                        <tbody>
                            { for shown.iter().map(|e| row(e)) }
                            if shown.is_empty() {
                                <tr><td colspan="6" class="muted">{ "No events" }</td></tr>
                            }
                        </tbody>
                    </table>
                    if more {
                        <div class="activity-more">
                            <button class="btn small" disabled={self.loading_older}
                                onclick={link.callback(|_| Msg::Older)}>
                                { if self.loading_older { "Loading…" } else { "Older events" } }
                            </button>
                        </div>
                    }
                </div>
            </div>
        }
    }
}

type Event_ = yew::events::Event;

fn row(e: &Event) -> Html {
    let named = |kind: &str, name: &dyn std::fmt::Display| {
        html! { <span>{ format!("{} {}", kind, name) }</span> }
    };
    let object = match &e.subject {
        Subject::Vm(name) => html! {
            <a href={Route::Vms(Some(name.clone())).href()}>{ format!("VM {}", name) }</a>
        },
        Subject::Node(n) => named("node", n),
        Subject::Disk(d) => named("disk", d),
        Subject::Dataset(f, n) => named(raidish_types::api_types::format_slug(*f), n),
        s @ (Subject::Pool | Subject::Cluster) => {
            html! { <span class="muted">{ s.to_string() }</span> }
        }
    };
    html! {
        <tr key={e.seq.to_string()} class={classes!(level_class(e.level))}>
            <td class="mono muted nowrap" title={full_time(e.ts_ms)}>{ when(e.ts_ms, false) }</td>
            <td><span class={classes!("tag", level_class(e.level))}>{ level_name(e.level) }</span></td>
            <td>{ &e.message }</td>
            <td class="nowrap">{ object }</td>
            <td class="nowrap muted">{ &e.node }</td>
            <td class="nowrap muted">{ e.by.clone().unwrap_or_default() }</td>
        </tr>
    }
}

fn level_class(level: Level) -> &'static str {
    match level {
        Level::Error => "bad",
        Level::Warn => "warn",
        Level::Info => "ok",
    }
}

fn level_name(level: Level) -> &'static str {
    match level {
        Level::Error => "ERROR",
        Level::Warn => "WARN",
        Level::Info => "INFO",
    }
}

/// `14:03:22` today, `Nov 12 14:03` before; `short`: ago for the last hour.
fn when(ts_ms: u64, short: bool) -> String {
    let now = now_ms();
    let t = ts_ms as f64;
    if short && now - t < 3_600_000.0 {
        return crate::fmt::ago(t, now);
    }
    let d = js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(t));
    let today = js_sys::Date::new_0();
    let same_day = d.get_full_year() == today.get_full_year()
        && d.get_month() == today.get_month()
        && d.get_date() == today.get_date();
    if same_day {
        format!(
            "{:02}:{:02}:{:02}",
            d.get_hours(),
            d.get_minutes(),
            d.get_seconds()
        )
    } else {
        const MONTHS: [&str; 12] = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ];
        format!(
            "{} {} {:02}:{:02}",
            MONTHS[d.get_month() as usize % 12],
            d.get_date(),
            d.get_hours(),
            d.get_minutes()
        )
    }
}

fn full_time(ts_ms: u64) -> String {
    let d = js_sys::Date::new(&wasm_bindgen::JsValue::from_f64(ts_ms as f64));
    String::from(d.to_locale_string("default", &wasm_bindgen::JsValue::UNDEFINED))
}
