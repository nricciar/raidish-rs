//! etcd's gRPC services over an [`EtcdStore`]: KV, Watch, Lease, and
//! enough of Maintenance, Cluster and Auth for etcdctl and the client
//! libraries (status, alarms, the member list, "auth is off").

use super::pb::etcdserverpb as pb;
use super::pb::etcdserverpb::{
    auth_server, cluster_server, kv_server, lease_server, maintenance_server, watch_server,
};
use super::store::{EtcdError, EtcdStore, KeyRange, MAX_REQUEST_BYTES};
use crate::disk::BlockDevice;
use crate::pki::Role;
use futures::Stream;
use pb::watch_request::RequestUnion;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status, Streaming};

type Res<T> = std::result::Result<Response<T>, Status>;
type Out<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send>>;

/// Events per watch response, at most (about: whole revisions).
const WATCH_BATCH: usize = 1000;
/// Bytes of events per watch response, about (clients take 4 MiB by
/// default; a single event may go over).
const WATCH_BYTES: usize = 1 << 20;
/// How often watchers that asked for it get a progress notification.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(600);

/// The services of one etcd dataset, as this node serves them.
pub struct EtcdServer<D: BlockDevice> {
    pub store: Arc<EtcdStore<D>>,
    /// This node's name (the member's).
    pub node: String,
    /// Where clients reach the dataset (every node: they forward).
    pub client_urls: Vec<String>,
}

fn status(e: EtcdError) -> Status {
    e.status()
}

/// Who is on a connection: the role of the client certificate it came
/// with (see `crate::frontend`), in every request's extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caller {
    pub role: Role,
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// A client connection (TLS, or forwarded by another node), and who.
pub struct EtcdConn {
    io: Box<dyn Io>,
    pub caller: Caller,
}

impl EtcdConn {
    pub fn new(io: impl AsyncRead + AsyncWrite + Unpin + Send + 'static, caller: Caller) -> Self {
        EtcdConn {
            io: Box::new(io),
            caller,
        }
    }
}

impl tonic::transport::server::Connected for EtcdConn {
    type ConnectInfo = Caller;
    fn connect_info(&self) -> Caller {
        self.caller
    }
}

impl AsyncRead for EtcdConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for EtcdConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

/// Writes need an admin (or node) certificate; readonly ones read.
#[allow(clippy::result_large_err)]
fn may_write<T>(r: &Request<T>) -> std::result::Result<(), Status> {
    match r.extensions().get::<Caller>() {
        Some(c) if c.role >= Role::Admin => Ok(()),
        _ => Err(Status::permission_denied("etcdserver: permission denied")),
    }
}

/// A stream from a channel.
fn out<T: Send + 'static>(rx: mpsc::Receiver<std::result::Result<T, Status>>) -> Out<T> {
    Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
}

impl<D: BlockDevice + 'static> EtcdServer<D> {
    /// The gRPC router of every service, on `store`.
    pub fn router(self) -> tonic::transport::server::Router {
        let s = Arc::new(self);
        let max = MAX_REQUEST_BYTES + 64 * 1024;
        tonic::transport::Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(30)))
            .add_service(
                kv_server::KvServer::from_arc(s.clone())
                    .max_decoding_message_size(max)
                    .max_encoding_message_size(usize::MAX),
            )
            .add_service(
                watch_server::WatchServer::from_arc(s.clone()).max_decoding_message_size(max),
            )
            .add_service(lease_server::LeaseServer::from_arc(s.clone()))
            .add_service(maintenance_server::MaintenanceServer::from_arc(s.clone()))
            .add_service(cluster_server::ClusterServer::from_arc(s.clone()))
            .add_service(auth_server::AuthServer::from_arc(s))
    }
}

#[tonic::async_trait]
impl<D: BlockDevice + 'static> kv_server::Kv for EtcdServer<D> {
    async fn range(&self, r: Request<pb::RangeRequest>) -> Res<pb::RangeResponse> {
        self.store
            .range(r.into_inner())
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn put(&self, r: Request<pb::PutRequest>) -> Res<pb::PutResponse> {
        may_write(&r)?;
        self.store
            .put(r.into_inner())
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn delete_range(
        &self,
        r: Request<pb::DeleteRangeRequest>,
    ) -> Res<pb::DeleteRangeResponse> {
        may_write(&r)?;
        self.store
            .delete_range(r.into_inner())
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn txn(&self, r: Request<pb::TxnRequest>) -> Res<pb::TxnResponse> {
        if !super::store::txn_is_read_only(r.get_ref()) {
            may_write(&r)?;
        }
        self.store
            .txn(r.into_inner())
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn compact(&self, r: Request<pb::CompactionRequest>) -> Res<pb::CompactionResponse> {
        may_write(&r)?;
        let r = r.into_inner();
        self.store
            .compact(r.revision, r.physical)
            .await
            .map(Response::new)
            .map_err(status)
    }
}

/// A watcher on a watch stream.
struct Watcher {
    range: KeyRange,
    /// The next revision to send.
    next: i64,
    prev_kv: bool,
    no_put: bool,
    no_delete: bool,
    progress_notify: bool,
}

#[tonic::async_trait]
impl<D: BlockDevice + 'static> watch_server::Watch for EtcdServer<D> {
    type WatchStream = Out<pb::WatchResponse>;

    async fn watch(&self, r: Request<Streaming<pb::WatchRequest>>) -> Res<Self::WatchStream> {
        if self.store.is_stopped() {
            return Err(status(EtcdError::NotServing));
        }
        let (tx, rx) = mpsc::channel(64);
        let store = self.store.clone();
        tokio::spawn(watch_session(store, r.into_inner(), tx));
        Ok(Response::new(out(rx)))
    }
}

/// One watch stream: its watchers, each sent the events of its keys in
/// revision order, durable ones only.
async fn watch_session<D: BlockDevice + 'static>(
    store: Arc<EtcdStore<D>>,
    mut inbound: Streaming<pb::WatchRequest>,
    tx: mpsc::Sender<std::result::Result<pb::WatchResponse, Status>>,
) {
    let mut durable = store.durable_rev();
    let mut stopped = store.stopped();
    let mut watchers: BTreeMap<i64, Watcher> = BTreeMap::new();
    let mut next_id: i64 = 0;
    let mut progress = tokio::time::interval(PROGRESS_INTERVAL);
    progress.tick().await;
    let mut progress_asked = false;
    let send = |resp: pb::WatchResponse| {
        let tx = tx.clone();
        async move { tx.send(Ok(resp)).await.is_ok() }
    };
    loop {
        // Bring every watcher up to the durable revision.
        let cur = *durable.borrow_and_update();
        let ids: Vec<i64> = watchers.keys().copied().collect();
        for id in ids {
            loop {
                let w = &watchers[&id];
                if w.next > cur {
                    break;
                }
                match store
                    .events(&w.range, w.next, cur, w.prev_kv, WATCH_BATCH)
                    .await
                {
                    Ok((events, upto)) => {
                        let events: Vec<_> = events
                            .into_iter()
                            .filter(|e| {
                                if e.r#type == 0 {
                                    !w.no_put
                                } else {
                                    !w.no_delete
                                }
                            })
                            .collect();
                        for chunk in chunks(events) {
                            if !send(pb::WatchResponse {
                                header: store.header(cur),
                                watch_id: id,
                                events: chunk,
                                ..Default::default()
                            })
                            .await
                            {
                                return;
                            }
                        }
                        watchers.get_mut(&id).unwrap().next = upto + 1;
                    }
                    Err(EtcdError::Compacted) => {
                        let compact = store.revisions().await.map(|r| r.1).unwrap_or(0);
                        watchers.remove(&id);
                        if !send(pb::WatchResponse {
                            header: store.header(cur),
                            watch_id: id,
                            canceled: true,
                            compact_revision: compact,
                            ..Default::default()
                        })
                        .await
                        {
                            return;
                        }
                        break;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(status(e))).await;
                        return;
                    }
                }
            }
        }
        if progress_asked {
            progress_asked = false;
            // Every watcher has everything up to `cur`.
            if !send(pb::WatchResponse {
                header: store.header(cur),
                watch_id: -1,
                ..Default::default()
            })
            .await
            {
                return;
            }
        }
        tokio::select! {
            m = inbound.message() => {
                let Ok(Some(m)) = m else { return };
                match m.request_union {
                    Some(RequestUnion::CreateRequest(c)) => {
                        let id = if c.watch_id != 0 {
                            if watchers.contains_key(&c.watch_id) {
                                // etcd's answer to a watch id in use.
                                if !send(pb::WatchResponse {
                                    header: store.header(cur),
                                    watch_id: c.watch_id,
                                    created: true,
                                    canceled: true,
                                    cancel_reason: "etcdserver: duplicate watch ID".into(),
                                    ..Default::default()
                                }).await {
                                    return;
                                }
                                continue;
                            }
                            c.watch_id
                        } else {
                            while watchers.contains_key(&next_id) {
                                next_id += 1;
                            }
                            next_id
                        };
                        let w = Watcher {
                            range: KeyRange::new(&c.key, &c.range_end),
                            next: if c.start_revision > 0 { c.start_revision } else { cur + 1 },
                            prev_kv: c.prev_kv,
                            no_put: c.filters.contains(&0),
                            no_delete: c.filters.contains(&1),
                            progress_notify: c.progress_notify,
                        };
                        if !send(pb::WatchResponse {
                            header: store.header(cur),
                            watch_id: id,
                            created: true,
                            ..Default::default()
                        }).await {
                            return;
                        }
                        watchers.insert(id, w);
                    }
                    Some(RequestUnion::CancelRequest(c)) => {
                        if watchers.remove(&c.watch_id).is_some()
                            && !send(pb::WatchResponse {
                                header: store.header(cur),
                                watch_id: c.watch_id,
                                canceled: true,
                                ..Default::default()
                            }).await
                        {
                            return;
                        }
                    }
                    Some(RequestUnion::ProgressRequest(_)) => progress_asked = true,
                    None => {}
                }
            }
            r = durable.changed() => if r.is_err() {
                // The store went away (stopped): say so, as below.
                let _ = tx.send(Err(status(EtcdError::NotServing))).await;
                return;
            },
            _ = stopped.changed() => {
                let _ = tx.send(Err(status(EtcdError::NotServing))).await;
                return;
            }
            _ = progress.tick() => {
                for (id, w) in &watchers {
                    if w.progress_notify && w.next > cur
                        && !send(pb::WatchResponse {
                            header: store.header(cur),
                            watch_id: *id,
                            ..Default::default()
                        }).await
                    {
                        return;
                    }
                }
            }
            _ = tx.closed() => return,
        }
    }
}

/// Events split into responses of about `WATCH_BYTES` (none if none).
fn chunks(events: Vec<super::pb::mvccpb::Event>) -> Vec<Vec<super::pb::mvccpb::Event>> {
    let mut out = Vec::new();
    let mut cur = Vec::new();
    let mut bytes = 0;
    for e in events {
        let n = e.kv.as_ref().map_or(0, |kv| kv.key.len() + kv.value.len())
            + e.prev_kv
                .as_ref()
                .map_or(0, |kv| kv.key.len() + kv.value.len());
        if !cur.is_empty() && bytes + n > WATCH_BYTES {
            out.push(std::mem::take(&mut cur));
            bytes = 0;
        }
        bytes += n;
        cur.push(e);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

#[tonic::async_trait]
impl<D: BlockDevice + 'static> lease_server::Lease for EtcdServer<D> {
    type LeaseKeepAliveStream = Out<pb::LeaseKeepAliveResponse>;

    async fn lease_grant(&self, r: Request<pb::LeaseGrantRequest>) -> Res<pb::LeaseGrantResponse> {
        may_write(&r)?;
        self.store
            .lease_grant(r.into_inner())
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn lease_revoke(
        &self,
        r: Request<pb::LeaseRevokeRequest>,
    ) -> Res<pb::LeaseRevokeResponse> {
        may_write(&r)?;
        self.store
            .lease_revoke(r.into_inner().id)
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn lease_keep_alive(
        &self,
        r: Request<Streaming<pb::LeaseKeepAliveRequest>>,
    ) -> Res<Self::LeaseKeepAliveStream> {
        may_write(&r)?;
        let mut inbound = r.into_inner();
        let store = self.store.clone();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            let mut stopped = store.stopped();
            loop {
                tokio::select! {
                    m = inbound.message() => {
                        let Ok(Some(m)) = m else { return };
                        let resp = store.lease_keep_alive(m.id).await.map_err(status);
                        let end = resp.is_err();
                        if tx.send(resp).await.is_err() || end {
                            return;
                        }
                    }
                    _ = stopped.changed() => {
                        let _ = tx.send(Err(status(EtcdError::NotServing))).await;
                        return;
                    }
                }
            }
        });
        Ok(Response::new(out(rx)))
    }
    async fn lease_time_to_live(
        &self,
        r: Request<pb::LeaseTimeToLiveRequest>,
    ) -> Res<pb::LeaseTimeToLiveResponse> {
        self.store
            .lease_time_to_live(r.into_inner())
            .await
            .map(Response::new)
            .map_err(status)
    }
    async fn lease_leases(
        &self,
        _: Request<pb::LeaseLeasesRequest>,
    ) -> Res<pb::LeaseLeasesResponse> {
        self.store
            .lease_leases()
            .await
            .map(Response::new)
            .map_err(status)
    }
}

#[allow(clippy::result_large_err)]
fn unimplemented<T>(what: &str) -> Res<T> {
    Err(Status::unimplemented(format!(
        "etcdserver: {} isn't supported by raidish",
        what
    )))
}

#[tonic::async_trait]
impl<D: BlockDevice + 'static> maintenance_server::Maintenance for EtcdServer<D> {
    type SnapshotStream = Out<pb::SnapshotResponse>;

    async fn alarm(&self, _: Request<pb::AlarmRequest>) -> Res<pb::AlarmResponse> {
        let (rev, _) = self.store.revisions().await.map_err(status)?;
        Ok(Response::new(pb::AlarmResponse {
            header: self.store.header(rev),
            alarms: Vec::new(),
        }))
    }
    async fn status(&self, _: Request<pb::StatusRequest>) -> Res<pb::StatusResponse> {
        let (rev, _) = self.store.revisions().await.map_err(status)?;
        let (size, in_use) = self.store.db_size().await.map_err(status)?;
        Ok(Response::new(pb::StatusResponse {
            header: self.store.header(rev),
            version: super::ETCD_VERSION.into(),
            db_size: size,
            leader: self.store.member_id,
            raft_index: rev as u64,
            raft_term: self.store.term,
            raft_applied_index: rev as u64,
            errors: Vec::new(),
            db_size_in_use: in_use,
            is_learner: false,
        }))
    }
    async fn defragment(&self, _: Request<pb::DefragmentRequest>) -> Res<pb::DefragmentResponse> {
        // Nothing to do: freed nodes are reused, and holes take no space.
        let (rev, _) = self.store.revisions().await.map_err(status)?;
        Ok(Response::new(pb::DefragmentResponse {
            header: self.store.header(rev),
        }))
    }
    async fn hash(&self, _: Request<pb::HashRequest>) -> Res<pb::HashResponse> {
        unimplemented("Hash")
    }
    async fn hash_kv(&self, _: Request<pb::HashKvRequest>) -> Res<pb::HashKvResponse> {
        unimplemented("HashKV")
    }
    async fn snapshot(&self, _: Request<pb::SnapshotRequest>) -> Res<Self::SnapshotStream> {
        unimplemented("Snapshot (use raidish snapshots and send)")
    }
    async fn move_leader(&self, _: Request<pb::MoveLeaderRequest>) -> Res<pb::MoveLeaderResponse> {
        unimplemented("MoveLeader (use dataset move)")
    }
    async fn downgrade(&self, _: Request<pb::DowngradeRequest>) -> Res<pb::DowngradeResponse> {
        unimplemented("Downgrade")
    }
}

#[tonic::async_trait]
impl<D: BlockDevice + 'static> cluster_server::Cluster for EtcdServer<D> {
    async fn member_add(&self, _: Request<pb::MemberAddRequest>) -> Res<pb::MemberAddResponse> {
        unimplemented("MemberAdd")
    }
    async fn member_remove(
        &self,
        _: Request<pb::MemberRemoveRequest>,
    ) -> Res<pb::MemberRemoveResponse> {
        unimplemented("MemberRemove")
    }
    async fn member_update(
        &self,
        _: Request<pb::MemberUpdateRequest>,
    ) -> Res<pb::MemberUpdateResponse> {
        unimplemented("MemberUpdate")
    }
    async fn member_list(&self, _: Request<pb::MemberListRequest>) -> Res<pb::MemberListResponse> {
        let (rev, _) = self.store.revisions().await.map_err(status)?;
        Ok(Response::new(pb::MemberListResponse {
            header: self.store.header(rev),
            members: vec![pb::Member {
                id: self.store.member_id,
                name: self.node.clone(),
                peer_ur_ls: Vec::new(),
                client_ur_ls: self.client_urls.clone(),
                is_learner: false,
            }],
        }))
    }
    async fn member_promote(
        &self,
        _: Request<pb::MemberPromoteRequest>,
    ) -> Res<pb::MemberPromoteResponse> {
        unimplemented("MemberPromote")
    }
}

#[allow(clippy::result_large_err)]
fn auth_off<T>() -> Res<T> {
    Err(Status::failed_precondition(
        "etcdserver: authentication is not enabled",
    ))
}

#[tonic::async_trait]
impl<D: BlockDevice + 'static> auth_server::Auth for EtcdServer<D> {
    async fn auth_status(&self, _: Request<pb::AuthStatusRequest>) -> Res<pb::AuthStatusResponse> {
        let (rev, _) = self.store.revisions().await.map_err(status)?;
        Ok(Response::new(pb::AuthStatusResponse {
            header: self.store.header(rev),
            enabled: false,
            auth_revision: 0,
        }))
    }
    async fn auth_enable(&self, _: Request<pb::AuthEnableRequest>) -> Res<pb::AuthEnableResponse> {
        unimplemented("auth")
    }
    async fn auth_disable(
        &self,
        _: Request<pb::AuthDisableRequest>,
    ) -> Res<pb::AuthDisableResponse> {
        let (rev, _) = self.store.revisions().await.map_err(status)?;
        Ok(Response::new(pb::AuthDisableResponse {
            header: self.store.header(rev),
        }))
    }
    async fn authenticate(
        &self,
        _: Request<pb::AuthenticateRequest>,
    ) -> Res<pb::AuthenticateResponse> {
        auth_off()
    }
    async fn user_add(&self, _: Request<pb::AuthUserAddRequest>) -> Res<pb::AuthUserAddResponse> {
        auth_off()
    }
    async fn user_get(&self, _: Request<pb::AuthUserGetRequest>) -> Res<pb::AuthUserGetResponse> {
        auth_off()
    }
    async fn user_list(
        &self,
        _: Request<pb::AuthUserListRequest>,
    ) -> Res<pb::AuthUserListResponse> {
        auth_off()
    }
    async fn user_delete(
        &self,
        _: Request<pb::AuthUserDeleteRequest>,
    ) -> Res<pb::AuthUserDeleteResponse> {
        auth_off()
    }
    async fn user_change_password(
        &self,
        _: Request<pb::AuthUserChangePasswordRequest>,
    ) -> Res<pb::AuthUserChangePasswordResponse> {
        auth_off()
    }
    async fn user_grant_role(
        &self,
        _: Request<pb::AuthUserGrantRoleRequest>,
    ) -> Res<pb::AuthUserGrantRoleResponse> {
        auth_off()
    }
    async fn user_revoke_role(
        &self,
        _: Request<pb::AuthUserRevokeRoleRequest>,
    ) -> Res<pb::AuthUserRevokeRoleResponse> {
        auth_off()
    }
    async fn role_add(&self, _: Request<pb::AuthRoleAddRequest>) -> Res<pb::AuthRoleAddResponse> {
        auth_off()
    }
    async fn role_get(&self, _: Request<pb::AuthRoleGetRequest>) -> Res<pb::AuthRoleGetResponse> {
        auth_off()
    }
    async fn role_list(
        &self,
        _: Request<pb::AuthRoleListRequest>,
    ) -> Res<pb::AuthRoleListResponse> {
        auth_off()
    }
    async fn role_delete(
        &self,
        _: Request<pb::AuthRoleDeleteRequest>,
    ) -> Res<pb::AuthRoleDeleteResponse> {
        auth_off()
    }
    async fn role_grant_permission(
        &self,
        _: Request<pb::AuthRoleGrantPermissionRequest>,
    ) -> Res<pb::AuthRoleGrantPermissionResponse> {
        auth_off()
    }
    async fn role_revoke_permission(
        &self,
        _: Request<pb::AuthRoleRevokePermissionRequest>,
    ) -> Res<pb::AuthRoleRevokePermissionResponse> {
        auth_off()
    }
}
