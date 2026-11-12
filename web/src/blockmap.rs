//! The interactive block map: the pool's address space as a grid of cells,
//! each the blocks of one bucket. Hover for what's there; click a cell or
//! drag across cells to zoom in, down to one block per cell.

use crate::fmt;
use crate::session::{LayoutData, Shared};
use raidish::inspect::{BlockKind, Bucket};
use wasm_bindgen::JsCast;
use web_sys::{CanvasRenderingContext2d, HtmlCanvasElement, MouseEvent};
use yew::prelude::*;

const COLS: usize = 96;
const ROWS: usize = 32;
const CELL: f64 = 12.0;
const GAP: f64 = 2.0;

#[derive(Properties, PartialEq)]
pub struct BlockMapProps {
    pub layout: Shared<LayoutData>,
    pub loading: bool,
    pub on_reload: Callback<()>,
    pub age: AttrValue,
}

#[derive(Clone, Copy, PartialEq)]
struct Window {
    start: u64,
    len: u64,
}

#[function_component]
pub fn BlockMap(p: &BlockMapProps) -> Html {
    let layout = &p.layout.layout;
    let total = layout.total_blocks;
    // Zoom stack; the first entry is the whole pool.
    let stack = use_state(|| {
        vec![Window {
            start: 0,
            len: total,
        }]
    });
    let hover = use_state(|| None::<usize>);
    let drag = use_state(|| None::<usize>);
    let by_owner = use_state(|| false);
    let canvas = use_node_ref();

    // A reformatted (resized) pool invalidates the zoom.
    {
        let stack = stack.clone();
        use_effect_with(total, move |t| {
            if stack.first().is_none_or(|w| w.len != *t) {
                stack.set(vec![Window { start: 0, len: *t }]);
            }
        });
    }
    let win = *stack.last().unwrap();
    let buckets: std::rc::Rc<Vec<Bucket>> = use_memo((p.layout.clone(), win), |(l, w)| {
        l.layout.buckets(w.start, w.len, COLS * ROWS)
    });
    let rows = buckets.len().div_ceil(COLS).max(1);

    let selection = match (*drag, *hover) {
        (Some(a), Some(b)) => Some((a.min(b), a.max(b))),
        (Some(a), None) => Some((a, a)),
        _ => None,
    };

    // Draw.
    {
        let canvas = canvas.clone();
        let buckets = buckets.clone();
        let owners = *by_owner;
        let hover = *hover;
        use_effect_with((buckets.clone(), owners, selection, hover), move |_| {
            if let Some(c) = canvas.cast::<HtmlCanvasElement>() {
                draw(&c, &buckets, rows, owners, selection, hover);
            }
        });
    }

    let cell_at = {
        let canvas = canvas.clone();
        let n = buckets.len();
        move |e: &MouseEvent| -> Option<usize> {
            let c = canvas.cast::<HtmlCanvasElement>()?;
            let scale = c.width() as f64 / c.client_width().max(1) as f64;
            let x = e.offset_x() as f64 * scale;
            let y = e.offset_y() as f64 * scale;
            let col = (x / (CELL + GAP)) as usize;
            let row = (y / (CELL + GAP)) as usize;
            let i = row * COLS + col;
            (col < COLS && i < n).then_some(i)
        }
    };

    let onmousemove = {
        let hover = hover.clone();
        let cell_at = cell_at.clone();
        Callback::from(move |e: MouseEvent| {
            let c = cell_at(&e);
            if *hover != c {
                hover.set(c);
            }
        })
    };
    let onmouseleave = {
        let hover = hover.clone();
        let drag = drag.clone();
        Callback::from(move |_| {
            hover.set(None);
            drag.set(None);
        })
    };
    let onmousedown = {
        let drag = drag.clone();
        let cell_at = cell_at.clone();
        Callback::from(move |e: MouseEvent| {
            e.prevent_default();
            drag.set(cell_at(&e));
        })
    };
    let onmouseup = {
        let drag = drag.clone();
        let stack = stack.clone();
        let buckets = buckets.clone();
        Callback::from(move |e: MouseEvent| {
            let (Some(a), Some(b)) = (*drag, cell_at(&e)) else {
                drag.set(None);
                return;
            };
            drag.set(None);
            let (lo, hi) = (a.min(b), a.max(b));
            let start = buckets[lo].start;
            let end = buckets[hi].start + buckets[hi].len;
            // Already at one block per cell for this range: nothing to zoom.
            if end - start <= (hi - lo + 1) as u64 {
                return;
            }
            let mut s = (*stack).clone();
            s.push(Window {
                start,
                len: end - start,
            });
            stack.set(s);
        })
    };

    let crumbs = stack.iter().enumerate().map(|(i, w)| {
        let stack = stack.clone();
        let label = if i == 0 {
            "whole pool".to_string()
        } else {
            format!("{}–{}", fmt::count(w.start), fmt::count(w.start + w.len - 1))
        };
        let last = i + 1 == stack.len();
        let onclick = Callback::from(move |_| {
            let mut s = (*stack).clone();
            s.truncate(i + 1);
            stack.set(s);
        });
        html! {
            <>
                if i > 0 { <span class="muted">{ "›" }</span> }
                <button class={classes!("crumb", last.then_some("on"))} {onclick} disabled={last}>{ label }</button>
            </>
        }
    });

    let per = buckets.first().map_or(1, |b| b.len);
    let toggle_owner = {
        let b = by_owner.clone();
        Callback::from(move |_| b.set(!*b))
    };
    let reload = {
        let r = p.on_reload.clone();
        Callback::from(move |_| r.emit(()))
    };

    // Window totals for the legend.
    let mut totals = [0u64; 9];
    for b in buckets.iter() {
        for (t, c) in totals.iter_mut().zip(b.counts.iter()) {
            *t += c;
        }
    }
    let shown: u64 = totals.iter().sum::<u64>().max(1);

    let info = hover.and_then(|i| buckets.get(i)).map(|b| {
        let owner = b.owner.and_then(|o| layout.owners.get(o as usize));
        html! {
            <div class="map-info">
                <div class="mono">
                    { if b.len == 1 { format!("block {}", fmt::count(b.start)) }
                      else { format!("blocks {}–{}", fmt::count(b.start), fmt::count(b.start + b.len - 1)) } }
                    <span class="muted">{ format!(" · {}", fmt::blocks(b.len)) }</span>
                </div>
                if let Some(o) = owner { <div><span class="muted">{ "mostly " }</span><span class="mono">{ o.clone() }</span></div> }
                <div class="map-mix">
                    { for BlockKind::ALL.iter().zip(b.counts.iter()).filter(|(_, c)| **c > 0).map(|(k, c)| html! {
                        <span><i class="sw" style={format!("background:{}", fmt::kind_color(*k))}></i>
                        { format!("{} {:.0}%", k.name(), fmt::pct(*c, b.len)) }</span>
                    }) }
                </div>
            </div>
        }
    });

    html! {
        <section class="card">
            <div class="card-head">
                <h2>{ "Block map" }</h2>
                <div class="map-tools">
                    <label class="switch">
                        <input type="checkbox" checked={*by_owner} onchange={toggle_owner} />
                        { "color by owner" }
                    </label>
                    <button class="btn ghost" onclick={reload} disabled={p.loading}>
                        { if p.loading { "reading…".to_string() } else { format!("refresh (read {})", p.age) } }
                    </button>
                </div>
            </div>
            <div class="crumbs">
                { for crumbs }
                <span class="muted small">{ format!(" · {} per cell · click or drag to zoom", fmt::blocks(per)) }</span>
            </div>
            <canvas ref={canvas} class="map"
                width={(((CELL + GAP) * COLS as f64) as u32).to_string()}
                height={(((CELL + GAP) * rows as f64) as u32).to_string()}
                {onmousemove} {onmouseleave} {onmousedown} {onmouseup} />
            <div class="map-foot">
                { info.unwrap_or_else(|| html! { <div class="map-info muted">{ "Hover a cell to see what's there." }</div> }) }
                <div class="legend">
                    { for BlockKind::ALL.iter().zip(totals.iter()).map(|(k, t)| html! {
                        <span class={classes!((*t == 0).then_some("zero"))}>
                            <i class="sw" style={format!("background:{}", fmt::kind_color(*k))}></i>
                            { k.name() }
                            <span class="muted">{ format!(" {:.1}%", fmt::pct(*t, shown)) }</span>
                        </span>
                    }) }
                </div>
            </div>
        </section>
    }
}

fn css_var(c: &HtmlCanvasElement, name: &str, fallback: &str) -> String {
    web_sys::window()
        .and_then(|w| w.get_computed_style(c).ok().flatten())
        .and_then(|s| s.get_property_value(name).ok())
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn draw(
    c: &HtmlCanvasElement,
    buckets: &[Bucket],
    rows: usize,
    by_owner: bool,
    selection: Option<(usize, usize)>,
    hover: Option<usize>,
) {
    let Some(ctx) = c
        .get_context("2d")
        .ok()
        .flatten()
        .and_then(|x| x.dyn_into::<CanvasRenderingContext2d>().ok())
    else {
        return;
    };
    let free = css_var(c, "--free", "#2a2f3a");
    let slack = css_var(c, "--slack", "#151a23");
    let ink = css_var(c, "--ink", "#ffffff");
    ctx.clear_rect(0.0, 0.0, c.width() as f64, (CELL + GAP) * rows as f64);
    for (i, b) in buckets.iter().enumerate() {
        let x = (i % COLS) as f64 * (CELL + GAP);
        let y = (i / COLS) as f64 * (CELL + GAP);
        let kind = b.dominant();
        if kind == BlockKind::Slack {
            ctx.set_fill_style_str(&slack);
            ctx.fill_rect(x, y, CELL, CELL);
            continue;
        }
        let color = if kind == BlockKind::Free {
            free.clone()
        } else if by_owner && let Some(o) = b.owner {
            fmt::owner_color(o)
        } else {
            fmt::kind_color(kind).to_string()
        };
        // Free background, then the allocated share as a filled bottom part.
        ctx.set_fill_style_str(&free);
        ctx.fill_rect(x, y, CELL, CELL);
        if kind != BlockKind::Free || b.fill() > 0.0 {
            let fill = if kind == BlockKind::Free {
                b.fill()
            } else {
                b.fill().max(0.35)
            };
            let h = (CELL * fill).max(2.0);
            let color = if kind == BlockKind::Free {
                fmt::kind_color(BlockKind::Data).to_string()
            } else {
                color
            };
            ctx.set_fill_style_str(&color);
            ctx.fill_rect(x, y + CELL - h, CELL, h);
        }
        let selected = selection.is_some_and(|(a, z)| i >= a && i <= z);
        if selected || hover == Some(i) {
            ctx.set_stroke_style_str(&ink);
            ctx.set_line_width(if selected { 2.0 } else { 1.5 });
            ctx.stroke_rect(x + 0.75, y + 0.75, CELL - 1.5, CELL - 1.5);
        }
    }
}
