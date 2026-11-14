//! Signing in: a node with TLS wants a token (`raidish token create`) from
//! the dashboard. It's kept in localStorage and sent with every request and
//! websocket. A node without TLS lets everyone in, so this is skipped.

use crate::app::App;
use crate::session::{self, Whoami};
use web_sys::HtmlTextAreaElement;
use yew::prelude::*;

pub enum Msg {
    Checked(Result<Whoami, Option<String>>),
    Submit,
    SignOut,
}

enum State {
    Checking,
    /// Signed out; the reason the last attempt failed, if any.
    Login(Option<String>),
    In(Whoami),
}

pub struct Root {
    state: State,
    input: NodeRef,
}

fn check(ctx: &Context<Root>) {
    ctx.link()
        .send_future(async { Msg::Checked(session::whoami().await) });
}

impl Component for Root {
    type Message = Msg;
    type Properties = ();

    fn create(ctx: &Context<Self>) -> Self {
        session::set_token(session::token().as_deref());
        check(ctx);
        Root {
            state: State::Checking,
            input: NodeRef::default(),
        }
    }

    fn update(&mut self, ctx: &Context<Self>, msg: Msg) -> bool {
        match msg {
            Msg::Checked(Ok(who)) => self.state = State::In(who),
            Msg::Checked(Err(why)) => {
                let had_token = session::token().is_some();
                if had_token {
                    // Expired, revoked or from another cluster: forget it.
                    session::set_token(None);
                }
                // A first visit just asks; a rejected token says why.
                self.state = State::Login(why.filter(|_| had_token));
            }
            Msg::Submit => {
                let Some(input) = self.input.cast::<HtmlTextAreaElement>() else {
                    return false;
                };
                let token = input.value().trim().to_string();
                if token.is_empty() {
                    return false;
                }
                session::set_token(Some(&token));
                self.state = State::Checking;
                check(ctx);
            }
            Msg::SignOut => {
                session::set_token(None);
                // Drop every open websocket along with the app.
                if let Some(w) = web_sys::window() {
                    let _ = w.location().reload();
                }
            }
        }
        true
    }

    fn view(&self, ctx: &Context<Self>) -> Html {
        match &self.state {
            State::Checking => html! {
                <main class="wrap"><div class="card empty"><h2>{ "Connecting…" }</h2></div></main>
            },
            State::In(who) => html! {
                <App
                    user={(!who.open()).then(|| format!("{} ({})", who.name, who.role))}
                    on_sign_out={ctx.link().callback(|_| Msg::SignOut)}
                />
            },
            State::Login(error) => {
                let onsubmit = ctx.link().callback(|e: SubmitEvent| {
                    e.prevent_default();
                    Msg::Submit
                });
                html! {
                    <main class="wrap">
                        <form class="card login" {onsubmit}>
                            <h2>{ "Sign in" }</h2>
                            <p class="muted">
                                { "This node requires a token. Create one where your admin certificate is:" }
                            </p>
                            <pre class="mono">{ "raidish token create <your name>" }</pre>
                            <textarea ref={self.input.clone()} rows="4" placeholder="rdt1.…"
                                autocomplete="off" spellcheck="false" />
                            if let Some(e) = error {
                                <p class="bad">{ e }</p>
                            }
                            <button class="btn" type="submit">{ "Sign in" }</button>
                        </form>
                    </main>
                }
            }
        }
    }
}
