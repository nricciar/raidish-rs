//! File uploads with progress: `fetch` can't report how much of a request
//! body has been sent, so uploads use `XMLHttpRequest`. The browser streams
//! the file from disk (it's never read into memory), which matters for
//! multi-gigabyte images.

use crate::api::encode_path;
use crate::session::token;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use web_sys::{File, ProgressEvent, XmlHttpRequest};

/// What an upload reports.
pub enum Event {
    /// Bytes of the body sent so far.
    Progress(f64),
    /// The body is all sent; the node is committing it.
    Sent,
    Done(Result<(), String>),
}

/// An upload in flight. Dropping it doesn't stop it; `abort` does.
pub struct Upload {
    xhr: XmlHttpRequest,
    _handlers: Vec<Closure<dyn FnMut(web_sys::Event)>>,
}

impl Upload {
    /// `PUT` `file` to `path` (creating or replacing it).
    pub fn start(path: &str, file: &File, on: impl Fn(Event) + 'static) -> Result<Upload, String> {
        let err = |e: wasm_bindgen::JsValue| format!("{:?}", e);
        let xhr = XmlHttpRequest::new().map_err(err)?;
        xhr.open("PUT", &format!("/api/v1/files/{}", encode_path(path)))
            .map_err(err)?;
        if let Some(t) = token() {
            xhr.set_request_header("Authorization", &format!("Bearer {}", t))
                .map_err(err)?;
        }
        xhr.set_request_header("Content-Type", "application/octet-stream")
            .map_err(err)?;

        let on = std::rc::Rc::new(on);
        let mut handlers = Vec::new();
        let upload = xhr.upload().map_err(err)?;

        let o = on.clone();
        let progress = Closure::<dyn FnMut(web_sys::Event)>::new(move |e: web_sys::Event| {
            if let Ok(p) = e.dyn_into::<ProgressEvent>() {
                o(Event::Progress(p.loaded()));
            }
        });
        upload.set_onprogress(Some(progress.as_ref().unchecked_ref()));
        handlers.push(progress);

        let o = on.clone();
        let sent = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| o(Event::Sent));
        upload.set_onload(Some(sent.as_ref().unchecked_ref()));
        handlers.push(sent);

        let o = on.clone();
        let x = xhr.clone();
        let load = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            let status = x.status().unwrap_or(0);
            o(Event::Done(if (200..300).contains(&status) {
                Ok(())
            } else {
                let text = x.response_text().ok().flatten().unwrap_or_default();
                let text = text.trim();
                Err(if text.is_empty() || text.starts_with('<') {
                    format!("HTTP {}", status)
                } else {
                    text.to_string()
                })
            }))
        });
        xhr.set_onload(Some(load.as_ref().unchecked_ref()));
        handlers.push(load);

        let o = on.clone();
        let error = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            o(Event::Done(Err("connection lost".into())))
        });
        xhr.set_onerror(Some(error.as_ref().unchecked_ref()));
        handlers.push(error);

        let o = on;
        let abort = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            o(Event::Done(Err("cancelled".into())))
        });
        xhr.set_onabort(Some(abort.as_ref().unchecked_ref()));
        handlers.push(abort);

        xhr.send_with_opt_blob(Some(file)).map_err(err)?;
        Ok(Upload {
            xhr,
            _handlers: handlers,
        })
    }

    pub fn abort(&self) {
        let _ = self.xhr.abort();
    }
}
