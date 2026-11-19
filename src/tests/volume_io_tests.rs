//! `SwitchableIo`: pausing and switching backends under running requests.

use super::*;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Semaphore;

/// A backend that records which requests reached it, and whose writes
/// wait for a permit when it's gated.
struct Fake {
    name: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    gate: Option<Arc<Semaphore>>,
}

impl Fake {
    fn new(name: &'static str, log: &Arc<Mutex<Vec<String>>>) -> Arc<Self> {
        Arc::new(Fake {
            name,
            log: log.clone(),
            gate: None,
        })
    }
    fn gated(
        name: &'static str,
        log: &Arc<Mutex<Vec<String>>>,
        gate: &Arc<Semaphore>,
    ) -> Arc<Self> {
        Arc::new(Fake {
            name,
            log: log.clone(),
            gate: Some(gate.clone()),
        })
    }
    fn note(&self, what: &str) {
        self.log
            .lock()
            .unwrap()
            .push(format!("{} {}", self.name, what));
    }
}

#[async_trait]
impl VolumeIo for Fake {
    async fn read(&self, offset: u64, buf: &mut [u8]) -> Result<usize, FileSystemError> {
        self.note(&format!("read {}", offset));
        buf.fill(self.name.as_bytes()[0]);
        Ok(buf.len())
    }
    async fn write(&self, offset: u64, data: &[u8]) -> Result<usize, FileSystemError> {
        if let Some(g) = &self.gate {
            g.acquire().await.unwrap().forget();
        }
        self.note(&format!("write {}", offset));
        Ok(data.len())
    }
    async fn zero(&self, offset: u64, _: usize) -> Result<(), FileSystemError> {
        self.note(&format!("zero {}", offset));
        Ok(())
    }
    async fn flush(&self) -> Result<(), FileSystemError> {
        self.note("flush");
        Ok(())
    }
    async fn sync(&self) -> Result<(), FileSystemError> {
        self.note("sync");
        Ok(())
    }
}

fn log() -> Arc<Mutex<Vec<String>>> {
    Arc::new(Mutex::new(Vec::new()))
}

/// Not finished within a short while (it's waiting on something).
async fn still_waiting<T>(h: &mut tokio::task::JoinHandle<T>) -> bool {
    tokio::time::timeout(Duration::from_millis(200), h)
        .await
        .is_err()
}

#[tokio::test]
async fn pause_waits_for_requests_in_flight_and_holds_new_ones() {
    let log = log();
    let gate = Arc::new(Semaphore::new(0));
    let io = Arc::new(SwitchableIo::new(Fake::gated("a", &log, &gate)));

    // A write in flight on `a`, stuck until the gate opens.
    let w = io.clone();
    let write = tokio::spawn(async move { w.write(1, b"x").await.unwrap() });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The pause waits for it.
    let p = io.clone();
    let (paused_tx, paused_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<Arc<dyn VolumeIo>>();
    let mut pauser = tokio::spawn(async move {
        let mut paused = p.pause().await;
        paused_tx.send(()).unwrap();
        let next = release_rx.await.unwrap();
        paused.set(next);
    });
    assert!(
        still_waiting(&mut pauser).await,
        "paused with a write in flight"
    );

    // A request arriving meanwhile queues behind the pause.
    let r = io.clone();
    let mut read = tokio::spawn(async move {
        let mut b = [0u8; 1];
        r.read(2, &mut b).await.unwrap();
        b[0]
    });
    assert!(still_waiting(&mut read).await);

    // The write finishes; only then is it paused, and the read still waits.
    gate.add_permits(1);
    assert_eq!(write.await.unwrap(), 1);
    paused_rx.await.unwrap();
    assert!(still_waiting(&mut read).await, "a request ran while paused");

    // Switched to `b`: the held read goes there.
    assert!(release_tx.send(Fake::new("b", &log)).is_ok());
    pauser.await.unwrap();
    assert_eq!(read.await.unwrap(), b'b');
    io.flush().await.unwrap();
    assert_eq!(
        *log.lock().unwrap(),
        vec!["a write 1", "b read 2", "b flush"]
    );
}

#[tokio::test]
async fn a_pause_dropped_without_a_new_backend_keeps_the_old_one() {
    let log = log();
    let io = SwitchableIo::new(Fake::new("a", &log));
    {
        let paused = io.pause().await;
        // What the held requests would go to.
        let mut b = [0u8; 1];
        paused.current().read(0, &mut b).await.unwrap();
    }
    io.zero(4, 1).await.unwrap();
    io.switch(Fake::new("b", &log)).await;
    io.sync().await.unwrap();
    assert_eq!(*log.lock().unwrap(), vec!["a read 0", "a zero 4", "b sync"]);
}
