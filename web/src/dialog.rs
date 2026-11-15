//! A small modal form: a title, a message, a few fields, OK / Cancel. Used
//! for names, sizes and confirmations in the file manager.

use std::collections::BTreeMap;
use wasm_bindgen::JsCast;
use web_sys::{HtmlInputElement, HtmlSelectElement};
use yew::prelude::*;

#[derive(Clone, PartialEq)]
pub enum Kind {
    Text,
    /// (value, label) pairs.
    Select(Vec<(String, String)>),
    Check,
}

#[derive(Clone, PartialEq)]
pub struct Field {
    pub key: &'static str,
    pub label: String,
    pub value: String,
    pub kind: Kind,
    pub placeholder: String,
    pub hint: String,
}

impl Field {
    pub fn text(key: &'static str, label: &str, value: &str) -> Self {
        Field {
            key,
            label: label.into(),
            value: value.into(),
            kind: Kind::Text,
            placeholder: String::new(),
            hint: String::new(),
        }
    }
    pub fn select(key: &'static str, label: &str, value: &str, options: &[(&str, &str)]) -> Self {
        Field {
            kind: Kind::Select(
                options
                    .iter()
                    .map(|(v, l)| (v.to_string(), l.to_string()))
                    .collect(),
            ),
            ..Field::text(key, label, value)
        }
    }
    pub fn check(key: &'static str, label: &str, on: bool) -> Self {
        Field {
            kind: Kind::Check,
            ..Field::text(key, label, if on { "true" } else { "" })
        }
    }
    pub fn placeholder(mut self, p: &str) -> Self {
        self.placeholder = p.into();
        self
    }
    pub fn hint(mut self, h: &str) -> Self {
        self.hint = h.into();
        self
    }
}

#[derive(Clone, PartialEq)]
pub struct Spec {
    pub title: String,
    pub message: Option<Html>,
    pub fields: Vec<Field>,
    pub submit: String,
    /// A destructive action (red button).
    pub danger: bool,
}

impl Spec {
    pub fn new(title: &str, submit: &str) -> Self {
        Spec {
            title: title.into(),
            message: None,
            fields: Vec::new(),
            submit: submit.into(),
            danger: false,
        }
    }
    pub fn message(mut self, m: Html) -> Self {
        self.message = Some(m);
        self
    }
    pub fn field(mut self, f: Field) -> Self {
        self.fields.push(f);
        self
    }
    pub fn danger(mut self) -> Self {
        self.danger = true;
        self
    }
}

pub type Values = BTreeMap<&'static str, String>;

#[derive(Properties, PartialEq)]
pub struct Props {
    pub spec: Spec,
    pub busy: bool,
    pub error: Option<String>,
    pub on_submit: Callback<Values>,
    pub on_cancel: Callback<()>,
}

#[function_component]
pub fn Dialog(p: &Props) -> Html {
    let values = use_state(|| {
        p.spec
            .fields
            .iter()
            .map(|f| (f.key, f.value.clone()))
            .collect::<Values>()
    });
    let first = use_node_ref();
    {
        let first = first.clone();
        use_effect_with((), move |_| {
            if let Some(el) = first.cast::<web_sys::HtmlElement>() {
                let _ = el.focus();
                if let Some(input) = el.dyn_ref::<HtmlInputElement>() {
                    input.select();
                }
            }
        });
    }
    let onsubmit = {
        let (values, cb) = (values.clone(), p.on_submit.clone());
        Callback::from(move |e: SubmitEvent| {
            e.prevent_default();
            cb.emit((*values).clone());
        })
    };
    let cancel = p.on_cancel.reform(|_: MouseEvent| ());
    let onkeydown = {
        let cb = p.on_cancel.clone();
        Callback::from(move |e: KeyboardEvent| {
            if e.key() == "Escape" {
                cb.emit(());
            }
        })
    };
    let backdrop = {
        let cb = p.on_cancel.clone();
        let busy = p.busy;
        Callback::from(move |e: MouseEvent| {
            // Only a click on the backdrop itself, not one inside the form.
            if !busy && e.target() == e.current_target() {
                cb.emit(());
            }
        })
    };
    html! {
        <div class="modal-back" onclick={backdrop} {onkeydown}>
            <form class="modal card" {onsubmit}>
                <h2>{ &p.spec.title }</h2>
                if let Some(m) = &p.spec.message {
                    <div class="modal-msg">{ m.clone() }</div>
                }
                { for p.spec.fields.iter().enumerate().map(|(i, f)| {
                    let key = f.key;
                    let r = if i == 0 { first.clone() } else { NodeRef::default() };
                    let set = {
                        let values = values.clone();
                        move |v: String| {
                            let mut m = (*values).clone();
                            m.insert(key, v);
                            values.set(m);
                        }
                    };
                    let value = values.get(key).cloned().unwrap_or_default();
                    let control = match &f.kind {
                        Kind::Text => {
                            let oninput = Callback::from(move |e: InputEvent| {
                                set(e.target_unchecked_into::<HtmlInputElement>().value())
                            });
                            html! { <input ref={r} type="text" {value} {oninput}
                                placeholder={f.placeholder.clone()} autocomplete="off" spellcheck="false" /> }
                        }
                        Kind::Select(options) => {
                            let onchange = Callback::from(move |e: Event| {
                                set(e.target_unchecked_into::<HtmlSelectElement>().value())
                            });
                            html! {
                                <select ref={r} {onchange}>
                                    { for options.iter().map(|(v, l)| html! {
                                        <option value={v.clone()} selected={*v == value}>{ l }</option>
                                    }) }
                                </select>
                            }
                        }
                        Kind::Check => {
                            let onchange = Callback::from(move |e: Event| {
                                let on = e.target_unchecked_into::<HtmlInputElement>().checked();
                                set(if on { "true".into() } else { String::new() })
                            });
                            return html! {
                                <label class="field check">
                                    <input ref={r} type="checkbox" checked={!value.is_empty()} {onchange} />
                                    <span>{ &f.label }</span>
                                    if !f.hint.is_empty() { <span class="muted small">{ &f.hint }</span> }
                                </label>
                            };
                        }
                    };
                    html! {
                        <label class="field">
                            <span>{ &f.label }</span>
                            { control }
                            if !f.hint.is_empty() { <span class="muted small">{ &f.hint }</span> }
                        </label>
                    }
                }) }
                if let Some(e) = &p.error {
                    <p class="bad">{ e }</p>
                }
                <div class="modal-actions">
                    <button type="button" class="btn" onclick={cancel} disabled={p.busy}>{ "Cancel" }</button>
                    <button type="submit" class={classes!("btn", "primary", p.spec.danger.then_some("danger"))} disabled={p.busy}>
                        { if p.busy { "Working…".to_string() } else { p.spec.submit.clone() } }
                    </button>
                </div>
            </form>
        </div>
    }
}
