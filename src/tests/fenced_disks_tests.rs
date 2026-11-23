use super::*;
use crate::disk::{BLOCK_SIZE, Disk};
use crate::fence::FenceMap;
use crate::test_util::{FaultDisk, FaultHandle};
use crate::types::ep;
use std::time::Duration;

const MB: u64 = 1024 * 1024;

/// One served fault disk behind `floor`.
fn served(floor: ServerFence) -> (tempfile::NamedTempFile, FaultHandle, Arc<FencedDisks>) {
    let f = tempfile::NamedTempFile::new().unwrap();
    f.as_file().set_len(MB).unwrap();
    let h = FaultDisk::open(f.path()).unwrap();
    let disks = vec![Caddy::Healthy(
        f.path().to_string_lossy().into_owned(),
        Disk::faulty(h.clone()),
    )];
    (
        f,
        h,
        Arc::new(FencedDisks::new(Arc::new(disks), Arc::new(floor))),
    )
}

fn blocks(v: u8, n: usize) -> Vec<u8> {
    vec![v; n * BLOCK_SIZE]
}

async fn read_back(d: &FencedDisks, block: u64) -> u8 {
    let r = d.validate_read(0, block, 1).unwrap();
    d.read(r, None).await.unwrap()[0]
}

async fn write(
    d: &FencedDisks,
    block: u64,
    v: u8,
    epoch: u64,
    expiry: &Expiry,
) -> Result<(), WsStatus> {
    let data = blocks(v, 1);
    let w = d.validate_write(0, block, 1, &data)?;
    d.write(w, Stamp::Pool(ep(epoch)), expiry).await
}

/// R1a: every bad shape is refused when the request is built, so nothing
/// about it is ever admitted (or raises a floor).
#[tokio::test]
async fn r1a_only_well_formed_requests_are_built() {
    let (_f, _h, d) = served(ServerFence::ready());
    let one = blocks(1, 1);
    let bad = |r: Result<ValidatedWrite<'_>, WsStatus>| r.err();
    assert_eq!(
        bad(d.validate_write(1, 0, 1, &one)),
        Some(WsStatus::ErrorNotFound)
    );
    assert_eq!(
        bad(d.validate_write(0, 0, 0, &[])),
        Some(WsStatus::InvalidRequest)
    );
    let max = blocks(1, WS_MAX_BLOCKS as usize + 1);
    assert_eq!(
        bad(d.validate_write(0, 0, WS_MAX_BLOCKS + 1, &max)),
        Some(WsStatus::InvalidRequest)
    );
    assert_eq!(
        bad(d.validate_write(0, u64::MAX, 1, &one)),
        Some(WsStatus::InvalidRequest),
        "a range that wraps"
    );
    assert_eq!(
        bad(d.validate_write(0, 0, 2, &one)),
        Some(WsStatus::InvalidRequest)
    );
    assert_eq!(
        bad(d.validate_write(0, 0, 1, &one[..BLOCK_SIZE - 1])),
        Some(WsStatus::InvalidRequest),
        "not whole blocks"
    );
    assert!(d.validate_read(0, 0, 0).is_err());
    assert!(d.validate_read(0, u64::MAX - 1, 4).is_err());
    assert!(d.validate_read(0, 0, WS_MAX_BLOCKS).is_ok());
    assert!(d.validate_write(0, 7, 1, &one).is_ok());
    assert_eq!(d.floor().pool_floor(), ep(0));
}

/// R1b: a fence that hasn't started refuses writes (and serves reads)
/// until the domain map is known and the cluster's grants are in.
#[tokio::test]
async fn r1b_writes_wait_for_the_map_and_a_linearizable_read() {
    let (_f, _h, d) = served(ServerFence::default());
    let never = Expiry::never();
    assert_eq!(write(&d, 3, 1, 5, &never).await, Err(WsStatus::NotReady));
    assert_eq!(read_back(&d, 3).await, 0, "reads are served");
    // The map alone isn't enough...
    d.floor().set_map(Some(FenceMap::new(
        crate::vdev::Topology::Mirror { disks: vec![0] },
        crate::metaslab::MetaslabLayout::default(),
    )));
    assert_eq!(write(&d, 3, 1, 5, &never).await, Err(WsStatus::NotReady));
    // ...nor the grants alone, on a fresh fence.
    let (_f2, _h2, d2) = served(ServerFence::default());
    d2.floor().mark_synced();
    assert_eq!(write(&d2, 3, 1, 5, &never).await, Err(WsStatus::NotReady));
    // Both: writes are admitted, at the floor the grants set.
    d.floor().raise_pool(ep(4));
    d.floor().mark_synced();
    assert_eq!(write(&d, 3, 1, 3, &never).await, Err(WsStatus::StaleEpoch));
    assert_eq!(write(&d, 3, 1, 5, &never).await, Ok(()));
    assert_eq!(read_back(&d, 3).await, 1);
}

/// R1c: two writes to one block, an older epoch's admitted first while
/// its disk write is held up. The newer one must not be admitted until the
/// older has landed, so the older can't land last.
#[tokio::test]
async fn r1c_an_older_epochs_write_cant_land_after_a_newer_one() {
    let (_f, h, d) = served(ServerFence::ready());
    h.hold_writes(true);
    let old = {
        let d = d.clone();
        tokio::spawn(async move { write(&d, 9, 0xA, 5, &Expiry::never()).await })
    };
    while h.held() == 0 {
        tokio::task::yield_now().await;
    }
    let new = {
        let d = d.clone();
        tokio::spawn(async move { write(&d, 9, 0xB, 6, &Expiry::never()).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    // Land whatever is held, newest first (the worst order for us).
    while !(old.is_finished() && new.is_finished()) {
        h.release_newest();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(old.await.unwrap(), Ok(()));
    assert_eq!(new.await.unwrap(), Ok(()));
    h.hold_writes(false);
    assert_eq!(
        read_back(&d, 9).await,
        0xB,
        "the older epoch's write landed last"
    );
}

/// A newer epoch's stamped read of blocks an older write was admitted to
/// waits for that write to land (a new committer reading a label must not
/// see it before the old committer's write to it does), and fences out
/// that epoch's later writes once it's admitted.
#[tokio::test]
async fn a_stamped_read_waits_for_an_admitted_write() {
    let (_f, h, d) = served(ServerFence::ready());
    h.hold_writes(true);
    let old = {
        let d = d.clone();
        tokio::spawn(async move { write(&d, 9, 0xA, 5, &Expiry::never()).await })
    };
    while h.held() == 0 {
        tokio::task::yield_now().await;
    }
    let read = {
        let d = d.clone();
        tokio::spawn(async move {
            let r = d.validate_read(0, 9, 1).unwrap();
            d.read(r, Some(Stamp::Pool(ep(6)))).await.map(|b| b[0])
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!read.is_finished(), "the read didn't wait for the write");
    h.hold_writes(false);
    h.release_all();
    assert_eq!(old.await.unwrap(), Ok(()));
    assert_eq!(
        read.await.unwrap(),
        Ok(0xA),
        "the read saw the block before the write"
    );
    assert_eq!(
        write(&d, 9, 0xC, 5, &Expiry::never()).await,
        Err(WsStatus::StaleEpoch)
    );
    // Unstamped reads raise nothing and wait for nothing.
    assert_eq!(read_back(&d, 9).await, 0xA);
}

/// D5b: a write that waited past its deadline (behind another on its
/// blocks) is refused, not written after its client gave up; so is one
/// whose connection has closed.
#[tokio::test]
async fn d5b_a_write_whose_client_gave_up_is_refused() {
    let (_f, h, d) = served(ServerFence::ready());
    h.hold_writes(true);
    let first = {
        let d = d.clone();
        tokio::spawn(async move { write(&d, 2, 0xA, 1, &Expiry::never()).await })
    };
    while h.held() == 0 {
        tokio::task::yield_now().await;
    }
    let late = {
        let d = d.clone();
        let expiry = Expiry::new(Some(Instant::now() + Duration::from_millis(30)), None);
        tokio::spawn(async move { write(&d, 2, 0xB, 1, &expiry).await })
    };
    tokio::time::sleep(Duration::from_millis(80)).await;
    h.hold_writes(false);
    h.release_all();
    assert_eq!(first.await.unwrap(), Ok(()));
    assert_eq!(late.await.unwrap(), Err(WsStatus::Expired));
    assert_eq!(read_back(&d, 2).await, 0xA);

    let closed = Arc::new(AtomicBool::new(true));
    let gone = Expiry::new(None, Some(closed));
    assert_eq!(write(&d, 2, 0xC, 1, &gone).await, Err(WsStatus::Expired));
    assert_eq!(read_back(&d, 2).await, 0xA);
}

/// D5b, over the wire: the deadline is half the budget the client sent,
/// counted from when the request arrived.
#[tokio::test]
async fn d5b_the_deadline_comes_from_the_requests_budget() {
    use crate::websocket::{WsOp, decode_response, encode_request};
    let (_f, _h, d) = served(ServerFence::ready());
    let data = blocks(7, 1);
    let req = |budget_ms| {
        encode_request(
            WsOp::Write,
            0,
            4,
            1,
            1,
            Stamp::Pool(ep(1)),
            Duration::from_millis(budget_ms),
            Some(&data),
        )
    };
    let status = |resp: Vec<u8>| WsStatus::try_from(decode_response(&resp).unwrap().0).unwrap();
    let closed = || Arc::new(AtomicBool::new(false));
    let arrived = Instant::now() - Duration::from_millis(200);
    // 300 ms of budget, 200 ms gone: past half of it.
    let r = crate::api::handle_ws_message_as(&req(300), &d, true, arrived, closed()).await;
    assert_eq!(status(r), WsStatus::Expired);
    assert_eq!(read_back(&d, 4).await, 0);
    // 1 s of budget: still in time; no budget: no deadline.
    let r = crate::api::handle_ws_message_as(&req(1000), &d, true, arrived, closed()).await;
    assert_eq!(status(r), WsStatus::Success);
    let r = crate::api::handle_ws_message_as(&req(0), &d, true, arrived, closed()).await;
    assert_eq!(status(r), WsStatus::Success);
}

/// R1b: with no map, every block is the pool's. That fences rightly only
/// while nothing is lent: once a lend is known (so the pool is formatted,
/// and its superblock merely went unread), a server without a map refuses
/// writes until it has one, rather than letting pool-stamped writes into
/// the lent space.
#[tokio::test]
async fn r1b_no_map_is_ready_only_while_nothing_is_lent() {
    use crate::fence::Grant;
    let (_f, _h, d) = served(ServerFence::default());
    d.floor().mark_unmapped();
    d.floor().mark_synced();
    let never = Expiry::never();
    assert_eq!(write(&d, 3, 1, 1, &never).await, Ok(()));
    d.floor().observe(
        ep(2),
        [(crate::types::MetaslabIdx::from_raw(0), Grant::Lent(ep(3)))],
    );
    assert_eq!(d.floor().readiness(), Ready::Starting);
    assert_eq!(write(&d, 3, 2, 2, &never).await, Err(WsStatus::NotReady));
    assert_eq!(read_back(&d, 3).await, 1);
    d.floor().set_map(Some(FenceMap::new(
        crate::vdev::Topology::Mirror { disks: vec![0] },
        crate::metaslab::MetaslabLayout::default(),
    )));
    assert_eq!(write(&d, 3, 2, 2, &never).await, Ok(()));
}
