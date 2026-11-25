//! Shared helpers for tests above the storage core: the core's fixtures
//! (`raidish_core::test_util`) plus an in-process disk server.

pub use raidish_core::test_util::*;
use std::path::PathBuf;

/// How a `TestDiskServer` treats requests.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum ServerMode {
    Normal = 0,
    /// Keep reading (and answering pings) but never reply to a request.
    Silent = 1,
    /// Stop reading the connection entirely (no pongs either).
    Frozen = 2,
}

/// An in-process disk server speaking the websocket block protocol (the same
/// request handler `listen` uses), on an ephemeral port that can be stopped,
/// restarted and made to misbehave.
pub struct TestDiskServer {
    pub port: u16,
    pub disks: std::sync::Arc<crate::fenced_disks::FencedDisks>,
    mode: std::sync::Arc<std::sync::atomic::AtomicU8>,
    /// The server's lease epoch floor (see `api::EpochFloor`). Ready: a
    /// test that wants the startup barrier swaps in one that isn't.
    pub floor: std::sync::Arc<crate::api::EpochFloor>,
    tasks: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>>,
}

impl TestDiskServer {
    /// Serve `paths` (local files) as disks 0..n.
    pub async fn start(paths: &[PathBuf]) -> Self {
        let mut disks = Vec::new();
        for p in paths {
            let p = p.to_str().unwrap();
            disks.push(crate::disks::Caddy::Healthy(
                p.to_string(),
                crate::disk::Disk::open(p).await.unwrap(),
            ));
        }
        Self::start_with(disks, std::sync::Arc::new(crate::api::EpochFloor::ready())).await
    }

    /// Serve `disks` behind `floor`.
    pub async fn start_with(
        disks: Vec<crate::disks::Caddy>,
        floor: std::sync::Arc<crate::api::EpochFloor>,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = TestDiskServer {
            port: listener.local_addr().unwrap().port(),
            disks: std::sync::Arc::new(crate::fenced_disks::FencedDisks::new(
                std::sync::Arc::new(disks),
                floor.clone(),
            )),
            mode: Default::default(),
            floor,
            tasks: Default::default(),
        };
        server.serve(listener);
        server
    }

    pub fn url(&self, disk: usize) -> String {
        format!("http://127.0.0.1:{}/api/v1/disks/{}", self.port, disk)
    }

    pub fn set_mode(&self, mode: ServerMode) {
        self.mode
            .store(mode as u8, std::sync::atomic::Ordering::SeqCst);
    }

    /// Close the listener and every connection.
    pub fn stop(&self) {
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }

    /// Listen again on the same port.
    pub async fn restart(&self) {
        self.stop();
        let mut tries = 0;
        let listener = loop {
            match tokio::net::TcpListener::bind(("127.0.0.1", self.port)).await {
                Ok(l) => break l,
                Err(e) if tries < 50 => {
                    tries += 1;
                    let _ = e;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(e) => panic!("rebind {}: {}", self.port, e),
            }
        };
        self.serve(listener);
    }

    fn serve(&self, listener: tokio::net::TcpListener) {
        use futures_util::{SinkExt, StreamExt};
        let (disks, mode, tasks) = (self.disks.clone(), self.mode.clone(), self.tasks.clone());
        let accept = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (disks, mode) = (disks.clone(), mode.clone());
                let conn = tokio::spawn(async move {
                    let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                        return;
                    };
                    let (mut sink, mut source) = ws.split();
                    // Set however the connection ends (`stop` aborts it).
                    struct Closed(std::sync::Arc<std::sync::atomic::AtomicBool>);
                    impl Drop for Closed {
                        fn drop(&mut self) {
                            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                    let closed = Closed(Default::default());
                    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
                    let writer = tokio::spawn(async move {
                        while let Some(r) = rx.recv().await {
                            if sink
                                .send(tungstenite::Message::Binary(r.into()))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });
                    loop {
                        if mode.load(std::sync::atomic::Ordering::SeqCst)
                            == ServerMode::Frozen as u8
                        {
                            std::future::pending::<()>().await;
                        }
                        match source.next().await {
                            Some(Ok(tungstenite::Message::Binary(data))) => {
                                if mode.load(std::sync::atomic::Ordering::SeqCst)
                                    == ServerMode::Silent as u8
                                {
                                    continue;
                                }
                                let received = tokio::time::Instant::now();
                                let (disks, tx, closed) =
                                    (disks.clone(), tx.clone(), closed.0.clone());
                                tokio::spawn(async move {
                                    let _ = tx.send(
                                        crate::api::handle_ws_message_as(
                                            &data, &disks, true, received, closed,
                                        )
                                        .await,
                                    );
                                });
                            }
                            Some(Ok(_)) => {}
                            _ => break,
                        }
                    }
                    writer.abort();
                });
                tasks.lock().unwrap().push(conn.abort_handle());
            }
        });
        self.tasks.lock().unwrap().push(accept.abort_handle());
    }
}

impl Drop for TestDiskServer {
    fn drop(&mut self) {
        self.stop();
    }
}

impl DiskUrls for TestDiskServer {
    fn url(&self, disk: usize) -> String {
        TestDiskServer::url(self, disk)
    }
}
