//! Remote disks from the browser: the same websocket block protocol as the
//! native client (`disk_ws.rs`), over `web_sys::WebSocket`.
//!
//! Browser objects aren't `Send`, but `BlockDevice` futures must be. So one
//! `spawn_local` task owns the socket (and every JS callback and timer), and
//! callers only ever touch `Send` channels: a request goes in over an mpsc,
//! its reply comes back on a oneshot. Timeouts are enforced by that task.

use super::*;
use crate::websocket::{WS_RESPONSE_HEADER_LEN, WsOp, decode_response, encode_request};
use futures::channel::{mpsc, oneshot};
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Mutex, Weak};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::Closure;
use web_sys::{BinaryType, MessageEvent, WebSocket};

/// What the socket task returns to a caller: (status, payload).
type Reply = (u8, Vec<u8>);

struct Job {
    id: u64,
    msg: Vec<u8>,
    reply: oneshot::Sender<Reply>,
}

#[derive(Debug, Default)]
struct Counters {
    reads: AtomicU64,
    writes: AtomicU64,
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
    errors: AtomicU64,
    rtt_us_total: AtomicU64,
    rtt_us_last: AtomicU64,
}

pub struct RemoteDisk {
    url: String,
    disk_id: u16,
    opts: DiskOptions,
    /// The live connection's request queue (None while disconnected).
    queue: Mutex<Option<mpsc::UnboundedSender<Job>>>,
    next_id: AtomicU64,
    reconnecting: AtomicBool,
    generation: watch::Sender<u64>,
    counters: Counters,
}

impl fmt::Debug for RemoteDisk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteDisk")
            .field("url", &self.url)
            .field("disk_id", &self.disk_id)
            .field("online", &self.is_online())
            .finish()
    }
}

fn now_ms() -> f64 {
    js_sys::Date::now()
}

fn log(msg: &str) {
    web_sys::console::log_1(&msg.into());
}

impl RemoteDisk {
    pub(super) async fn open(url: String, disk_id: u16, opts: DiskOptions) -> Arc<Self> {
        let disk = Arc::new(RemoteDisk {
            url,
            disk_id,
            opts,
            queue: Mutex::new(None),
            next_id: AtomicU64::new(1),
            reconnecting: AtomicBool::new(false),
            generation: watch::channel(0).0,
            counters: Counters::default(),
        });
        if let Err(e) = connect(&disk).await {
            log(&format!("disk {} ({}): offline: {}", disk_id, disk.url, e));
            disk.ensure_reconnect();
        }
        disk
    }

    pub(super) fn is_online(&self) -> bool {
        self.queue
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|q| !q.is_closed())
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<u64> {
        self.generation.subscribe()
    }

    pub fn io_stats(&self) -> IoStats {
        let c = &self.counters;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        IoStats {
            reads: get(&c.reads),
            writes: get(&c.writes),
            read_bytes: get(&c.read_bytes),
            write_bytes: get(&c.write_bytes),
            errors: get(&c.errors),
            rtt_us_total: get(&c.rtt_us_total),
            rtt_us_last: get(&c.rtt_us_last),
        }
    }

    fn ensure_reconnect(self: &Arc<Self>) {
        if self.reconnecting.swap(true, Ordering::SeqCst) {
            return;
        }
        let weak = Arc::downgrade(self);
        let (min, max) = (self.opts.reconnect_min, self.opts.reconnect_max);
        wasm_bindgen_futures::spawn_local(async move {
            let mut delay = min;
            loop {
                gloo_timers::future::sleep(delay).await;
                let Some(disk) = weak.upgrade() else { return };
                if connect(&disk).await.is_ok() {
                    disk.reconnecting.store(false, Ordering::SeqCst);
                    disk.generation.send_modify(|g| *g += 1);
                    log(&format!(
                        "disk {} ({}): reconnected",
                        disk.disk_id, disk.url
                    ));
                    return;
                }
                delay = (delay * 2).min(max);
            }
        });
    }

    pub(super) async fn request(
        self: &Arc<Self>,
        op: WsOp,
        block: u64,
        count: u32,
        data: Option<&[u8]>,
    ) -> Result<Vec<u8>, DiskError> {
        let Some(queue) = self.queue.lock().unwrap().clone() else {
            self.ensure_reconnect();
            return Err(DiskError::Disconnected);
        };
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let msg = encode_request(op, self.disk_id, block, count, id, lease_epoch(), data);
        let (tx, rx) = oneshot::channel();
        let started = now_ms();
        if queue.unbounded_send(Job { id, msg, reply: tx }).is_err() {
            self.ensure_reconnect();
            return Err(DiskError::Disconnected);
        }
        let c = &self.counters;
        let result = match rx.await {
            // The socket task fails timed-out requests with this status.
            Ok((TIMED_OUT, _)) => Err(DiskError::Timeout),
            Ok((status, payload)) => check_status(status).map(|_| payload),
            Err(_) => {
                self.ensure_reconnect();
                Err(DiskError::Disconnected)
            }
        };
        let rtt_us = ((now_ms() - started) * 1000.0) as u64;
        c.rtt_us_total.fetch_add(rtt_us, Ordering::Relaxed);
        c.rtt_us_last.store(rtt_us, Ordering::Relaxed);
        let bytes = count as u64 * BLOCK_SIZE as u64;
        match (&result, op) {
            (Err(_), _) => {
                c.errors.fetch_add(1, Ordering::Relaxed);
            }
            (Ok(_), WsOp::Read) => {
                c.reads.fetch_add(1, Ordering::Relaxed);
                c.read_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            (Ok(_), WsOp::Write) => {
                c.writes.fetch_add(1, Ordering::Relaxed);
                c.write_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            _ => {}
        }
        result
    }
}

/// Not a protocol status: the socket task's "no reply in time".
const TIMED_OUT: u8 = 0xff;

enum SocketEvent {
    Open,
    Message(Vec<u8>),
    Closed(String),
}

/// Open the socket, wait for it to connect, and install its request queue.
async fn connect(disk: &Arc<RemoteDisk>) -> Result<(), String> {
    let ws = WebSocket::new(&disk.url).map_err(|e| format!("{:?}", e))?;
    ws.set_binary_type(BinaryType::Arraybuffer);
    let (opened_tx, opened_rx) = oneshot::channel::<Result<(), String>>();
    let (jobs_tx, jobs_rx) = mpsc::unbounded::<Job>();
    wasm_bindgen_futures::spawn_local(run_socket(
        ws,
        Arc::downgrade(disk),
        disk.opts,
        opened_tx,
        jobs_rx,
    ));
    match opened_rx.await {
        Ok(Ok(())) => {
            *disk.queue.lock().unwrap() = Some(jobs_tx);
            Ok(())
        }
        Ok(Err(e)) => Err(e),
        Err(_) => Err("connection task ended".into()),
    }
}

/// Owns one websocket: sends queued requests, routes replies by id, fails
/// requests that take longer than `request_timeout`, and on close fails
/// everything in flight and starts reconnecting.
async fn run_socket(
    ws: WebSocket,
    disk: Weak<RemoteDisk>,
    opts: DiskOptions,
    opened: oneshot::Sender<Result<(), String>>,
    mut jobs: mpsc::UnboundedReceiver<Job>,
) {
    let (ev_tx, mut events) = mpsc::unbounded::<SocketEvent>();

    let tx = ev_tx.clone();
    let on_open = Closure::<dyn FnMut()>::new(move || {
        let _ = tx.unbounded_send(SocketEvent::Open);
    });
    let tx = ev_tx.clone();
    let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |e: MessageEvent| {
        if let Ok(buf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
            let _ = tx.unbounded_send(SocketEvent::Message(js_sys::Uint8Array::new(&buf).to_vec()));
        }
    });
    let tx = ev_tx.clone();
    let on_close = Closure::<dyn FnMut(web_sys::CloseEvent)>::new(move |e: web_sys::CloseEvent| {
        let _ = tx.unbounded_send(SocketEvent::Closed(format!("closed ({})", e.code())));
    });
    let tx = ev_tx;
    let on_error = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
        let _ = tx.unbounded_send(SocketEvent::Closed("connection error".into()));
    });
    ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
    ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));
    ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));

    // Wait for the handshake (or give up after connect_timeout).
    let mut opened = Some(opened);
    let connect_deadline = gloo_timers::future::sleep(opts.connect_timeout).fuse();
    futures::pin_mut!(connect_deadline);
    let ok = loop {
        futures::select! {
            ev = events.next() => match ev {
                Some(SocketEvent::Open) => break true,
                Some(SocketEvent::Closed(why)) => {
                    let _ = opened.take().unwrap().send(Err(why));
                    break false;
                }
                _ => {}
            },
            _ = connect_deadline => {
                let _ = opened.take().unwrap().send(Err("connect timed out".into()));
                break false;
            }
        }
    };
    if !ok {
        detach(&ws);
        return;
    }
    let _ = opened.take().unwrap().send(Ok(()));

    let mut pending: HashMap<u64, (f64, oneshot::Sender<Reply>)> = HashMap::new();
    let timeout_ms = opts.request_timeout.as_millis() as f64;
    let mut ticks = gloo_timers::future::IntervalStream::new(500).fuse();
    let why = loop {
        futures::select! {
            job = jobs.next() => match job {
                Some(job) => {
                    if ws.send_with_u8_array(&job.msg).is_err() {
                        break "send failed".to_string();
                    }
                    pending.insert(job.id, (now_ms(), job.reply));
                }
                // Every sender dropped: the disk is gone.
                None => break "disk dropped".to_string(),
            },
            ev = events.next() => match ev {
                Some(SocketEvent::Message(data)) => {
                    if let Some((status, id)) = decode_response(&data)
                        && let Some((_, reply)) = pending.remove(&id)
                    {
                        let _ = reply.send((status, data[WS_RESPONSE_HEADER_LEN..].to_vec()));
                    }
                }
                Some(SocketEvent::Closed(why)) => break why,
                Some(SocketEvent::Open) => {}
                None => break "event stream ended".to_string(),
            },
            _ = ticks.next() => {
                let now = now_ms();
                let late: Vec<u64> = pending
                    .iter()
                    .filter(|(_, (sent, _))| now - sent > timeout_ms)
                    .map(|(id, _)| *id)
                    .collect();
                if !late.is_empty() {
                    // Like the native client: a server that stops answering
                    // is treated as gone.
                    for id in late {
                        if let Some((_, reply)) = pending.remove(&id) {
                            let _ = reply.send((TIMED_OUT, Vec::new()));
                        }
                    }
                    break "request timed out".to_string();
                }
            }
        }
    };

    // Dropping the pending senders fails their callers with Disconnected.
    drop(pending);
    detach(&ws);
    if let Some(disk) = disk.upgrade() {
        *disk.queue.lock().unwrap() = None;
        log(&format!(
            "disk {} ({}): connection lost: {}",
            disk.disk_id, disk.url, why
        ));
        disk.ensure_reconnect();
    }
}

/// Close the socket and unhook its callbacks before they're dropped (an
/// error is followed by a close event, which must not reach a dropped
/// closure).
fn detach(ws: &WebSocket) {
    ws.set_onopen(None);
    ws.set_onmessage(None);
    ws.set_onclose(None);
    ws.set_onerror(None);
    let _ = ws.close();
}
