//! The RPC record layer: what a connection may have in flight.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

/// Holds every call until opened, counting how many got in.
struct Gate {
    entered: AtomicUsize,
    open: tokio::sync::watch::Sender<bool>,
}

#[async_trait::async_trait]
impl Dispatch for Gate {
    async fn call(&self, _call: &Call, _args: &mut Reader<'_>) -> Reply {
        self.entered.fetch_add(1, SeqCst);
        let _ = self.open.subscribe().wait_for(|o| *o).await;
        Reply::Success(Writer::new())
    }
}

fn null_call(xid: u32) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(xid).u32(0).u32(2).u32(100003).u32(4).u32(0);
    w.u32(AUTH_NONE).opaque(&[]);
    w.u32(AUTH_NONE).opaque(&[]);
    frame(&w.buf)
}

/// Wait until `n` stops changing (nothing more is coming in).
async fn settled(n: &AtomicUsize) -> usize {
    let mut last = usize::MAX;
    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let now = n.load(SeqCst);
        if now == last {
            return now;
        }
        last = now;
    }
}

/// A client pipelining calls without limit gets `MAX_IN_FLIGHT` of them
/// dispatched at once; the rest wait unread until one replies. Every call
/// used to get a task of its own at once.
#[tokio::test]
async fn a_connection_has_at_most_so_many_calls_in_flight() {
    let gate = Arc::new(Gate {
        entered: AtomicUsize::new(0),
        open: tokio::sync::watch::Sender::new(false),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve(listener, gate.clone()));

    let calls = MAX_IN_FLIGHT + 50;
    let (mut rd, mut wr) = tokio::net::TcpStream::connect(addr)
        .await
        .unwrap()
        .into_split();
    let send = tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        for xid in 0..calls as u32 {
            wr.write_all(&null_call(xid)).await.unwrap();
        }
        wr
    });
    assert_eq!(settled(&gate.entered).await, MAX_IN_FLIGHT);

    gate.open.send_replace(true);
    let mut xids = std::collections::BTreeSet::new();
    for _ in 0..calls {
        let rec = tokio::time::timeout(Duration::from_secs(10), read_record(&mut rd))
            .await
            .expect("every call answered")
            .unwrap()
            .unwrap();
        xids.insert(Reader::new(&rec).u32().unwrap());
    }
    assert_eq!(xids.len(), calls);
    assert_eq!(gate.entered.load(SeqCst), calls);
    let _wr = send.await.unwrap();
}
