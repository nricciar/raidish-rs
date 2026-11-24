//! `CommitClient`: the committer's API over HTTP, from any node (see
//! `super::routes`).

use super::admin::PoolAdmin;
use super::*;
use crate::send::SendHeader;
use crate::types::MetaslabIdx;
use std::sync::atomic::AtomicUsize;

/// Talks to whichever `listen` node is the committer.
#[derive(Clone)]
pub struct CommitClient {
    endpoints: Vec<String>,
    http: reqwest::Client,
    writer: Option<LeaseRef>,
    /// The admin this node asks for (see `crate::auth::Actor`).
    on_behalf: Option<String>,
    /// Set when the writer gives up (its lease is gone): stop resending.
    pub cancel: Arc<AtomicBool>,
    /// The endpoint the committer answered on last: asked first, so a
    /// call doesn't go through every other node's "not the committer"
    /// each time. Shared by the client's clones.
    committer: Arc<AtomicUsize>,
}

const ROUND_BACKOFF: Duration = Duration::from_millis(500);

impl CommitClient {
    pub fn new(endpoints: Vec<String>) -> Self {
        CommitClient {
            endpoints: endpoints
                .into_iter()
                .map(|e| e.trim_end_matches('/').to_string())
                .collect(),
            http: crate::tls::http_client(Some(Duration::from_secs(30))).expect("http client"),
            writer: None,
            on_behalf: None,
            cancel: Default::default(),
            committer: Default::default(),
        }
    }

    /// The nodes it asks.
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// The same client, asking on behalf of the admin named `who`.
    pub fn on_behalf_of(&self, who: &str) -> Self {
        let mut c = self.clone();
        c.on_behalf = Some(who.to_string());
        c
    }

    /// The same client, committing as `writer` at `epoch`.
    pub fn as_writer(mut self, writer: LeaseRef) -> Self {
        self.writer = Some(writer);
        self
    }

    /// The endpoints' indexes, the committer's last known one first.
    fn by_preference(&self) -> impl Iterator<Item = usize> + use<> {
        let n = self.endpoints.len();
        let first = self.committer.load(Ordering::Relaxed);
        (0..n).map(move |k| (first + k) % n)
    }

    /// One pass over the endpoints: the first definite answer, else the
    /// reasons it couldn't get one.
    async fn once<T: for<'de> Deserialize<'de>>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&(impl Serialize + Sync)>,
    ) -> Result<T, CommitError> {
        let mut why = Vec::new();
        for i in self.by_preference() {
            let ep = &self.endpoints[i];
            let mut req = self.http.request(method.clone(), format!("{}{}", ep, path));
            if let Some(who) = &self.on_behalf {
                req = req.header(crate::auth::ON_BEHALF, who);
            }
            if let Some(b) = body {
                req = req.json(b);
            }
            match req.send().await {
                Ok(resp) => match resp.json::<Result<T, CommitError>>().await {
                    Ok(Err(CommitError::Retry(m))) => why.push(format!("{}: {}", ep, m)),
                    // The committer's answer, refusals included.
                    Ok(r) => {
                        self.committer.store(i, Ordering::Relaxed);
                        return r;
                    }
                    Err(e) => why.push(format!("{}: {}", ep, e)),
                },
                Err(e) => why.push(format!("{}: {}", ep, e)),
            }
        }
        Err(CommitError::Retry(if why.is_empty() {
            "no endpoints".into()
        } else {
            why.join("; ")
        }))
    }

    /// `once`, retried for `rounds` passes (None: until cancelled).
    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&(impl Serialize + Sync)>,
        rounds: Option<usize>,
    ) -> Result<T, CommitError> {
        let mut n = 0;
        loop {
            match self.once(method.clone(), path, body).await {
                Err(CommitError::Retry(m)) => {
                    n += 1;
                    if rounds.is_some_and(|r| n >= r) || self.cancel.load(Ordering::SeqCst) {
                        return Err(CommitError::Retry(m));
                    }
                    if n == 1 || n % 20 == 0 {
                        eprintln!("committer unavailable (will retry): {}", m);
                    }
                    tokio::time::sleep(ROUND_BACKOFF).await;
                }
                other => return other,
            }
        }
    }

    pub async fn committer(&self) -> Option<CommitterInfo> {
        self.once::<CommitterInfo>(reqwest::Method::GET, "/api/v1/pool/committer", None::<&()>)
            .await
            .ok()
    }

    /// Stream `r` (a send stream) to the committer, received into volume
    /// `name`. Not retried: a stream can't be sent twice.
    pub async fn recv<R: tokio::io::AsyncRead + Unpin + Send + 'static>(
        &self,
        name: &str,
        force: bool,
        mut r: R,
    ) -> Result<StreamReport, CommitError> {
        use tokio::io::AsyncReadExt;
        // Asked first, so a refusal comes back as such (not as a stream
        // cut off midway).
        let (header, raw) = crate::send::peek_header(&mut r)
            .await
            .map_err(|e| CommitError::refused(ErrorKind::Invalid, e.to_string()))?;
        self.recv_check(name, &header, force).await?;
        let r = std::io::Cursor::new(raw).chain(r);
        // Find the committer first: the stream goes to it alone (`once`
        // remembers where it answered).
        self.once::<CommitterInfo>(reqwest::Method::GET, "/api/v1/pool/committer", None::<&()>)
            .await?;
        let ep = self.endpoints[self.committer.load(Ordering::Relaxed)].clone();
        let chunks = futures::stream::unfold(Some(r), |r| async move {
            let mut r = r?;
            let mut buf = vec![0u8; 1 << 20];
            match r.read(&mut buf).await {
                Ok(0) => None,
                Ok(n) => {
                    buf.truncate(n);
                    Some((Ok::<_, std::io::Error>(buf), Some(r)))
                }
                Err(e) => Some((Err(e), None)),
            }
        });
        // No overall timeout: a stream takes as long as it takes.
        let http = crate::tls::http_client(None).map_err(CommitError::Retry)?;
        let url = format!("{}/api/v1/pool/volumes/{}/recv?force={}", ep, name, force);
        let resp = http
            .post(url)
            .body(reqwest::Body::wrap_stream(chunks))
            .send()
            .await
            .map_err(|e| CommitError::Retry(format!("{}: {}", ep, e)))?;
        resp.json::<Result<StreamReport, CommitError>>()
            .await
            .map_err(|e| CommitError::Retry(format!("{}: {}", ep, e)))?
    }
}

#[async_trait]
impl PoolAdmin for CommitClient {
    /// Report events (to whichever node is the committer), one try.
    async fn report_events(&self, events: &[NewEvent]) -> Result<Vec<u64>, CommitError> {
        self.once(reqwest::Method::POST, "/api/v1/pool/events", Some(&events))
            .await
    }

    /// A page of the activity log.
    async fn events(
        &self,
        q: &crate::events::EventQuery,
    ) -> Result<crate::events::EventPage, CommitError> {
        let mut qs = Vec::new();
        if let Some(a) = q.after {
            qs.push(format!("after={}", a));
        }
        if let Some(b) = q.before {
            qs.push(format!("before={}", b));
        }
        if let Some(l) = q.limit {
            qs.push(format!("limit={}", l));
        }
        let path = format!("/api/v1/pool/events?{}", qs.join("&"));
        self.once(reqwest::Method::GET, &path, None::<&()>).await
    }

    async fn volumes(&self) -> Result<Vec<VolumeInfo>, CommitError> {
        self.call(
            reqwest::Method::GET,
            "/api/v1/pool/volumes",
            None::<&()>,
            Some(10),
        )
        .await
    }

    async fn create_volume(
        &self,
        name: &crate::types::DatasetName,
        size_bytes: u64,
    ) -> Result<(), CommitError> {
        let body = CreateVolume {
            name: name.clone(),
            size_bytes,
        };

        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/volumes",
            Some(&body),
            Some(10),
        )
        .await
    }

    async fn delete_volume(&self, name: &str) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}", name);
        self.call(reqwest::Method::DELETE, &path, None::<&()>, Some(10))
            .await
    }

    /// Every VM definition.
    async fn vms(
        &self,
    ) -> Result<std::collections::BTreeMap<String, crate::api_types::VmDef>, CommitError> {
        self.call(
            reqwest::Method::GET,
            "/api/v1/pool/vms",
            None::<&()>,
            Some(10),
        )
        .await
    }

    /// What each node last reported about itself (`crate::node_facts`).
    async fn node_facts(
        &self,
    ) -> Result<std::collections::BTreeMap<String, crate::api_types::NodeFacts>, CommitError> {
        self.call(
            reqwest::Method::GET,
            "/api/v1/pool/nodes/facts",
            None::<&()>,
            Some(10),
        )
        .await
    }

    /// Store this node's facts (nodes only).
    async fn put_node_facts(&self, facts: &crate::api_types::NodeFacts) -> Result<(), CommitError> {
        self.call(
            reqwest::Method::PUT,
            "/api/v1/pool/nodes/facts",
            Some(facts),
            Some(10),
        )
        .await
    }

    /// Store VM `req.name`'s definition (see `crate::vm_store::VmStore::put`).
    async fn put_vm(&self, req: &PutVm) -> Result<crate::api_types::VmDef, CommitError> {
        self.call(
            reqwest::Method::PUT,
            "/api/v1/pool/vms",
            Some(req),
            Some(10),
        )
        .await
    }

    /// Delete VM `name`'s definition (`change`: only if it's that one).
    async fn delete_vm(&self, name: &str, change: Option<u64>) -> Result<(), CommitError> {
        let mut path = format!("/api/v1/pool/vms/{}", name);
        if let Some(c) = change {
            path.push_str(&format!("?change={}", c));
        }
        self.call(reqwest::Method::DELETE, &path, None::<&()>, Some(10))
            .await
    }

    async fn datasets(&self) -> Result<Vec<DatasetInfo>, CommitError> {
        self.call(
            reqwest::Method::GET,
            "/api/v1/pool/datasets",
            None::<&()>,
            Some(10),
        )
        .await
    }

    async fn create_dataset(&self, req: &CreateDataset) -> Result<(), CommitError> {
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/datasets",
            Some(req),
            Some(10),
        )
        .await
    }

    async fn delete_dataset(&self, name: &str) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/datasets/{}", name);
        self.call(reqwest::Method::DELETE, &path, None::<&()>, Some(10))
            .await
    }

    async fn set_props(&self, name: &str, props: &DatasetProps) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/props", name);
        self.call(reqwest::Method::PUT, &path, Some(props), Some(10))
            .await
    }

    async fn snapshots(&self, name: &str) -> Result<Vec<SnapshotInfo>, CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/snapshots", name);
        self.call(reqwest::Method::GET, &path, None::<&()>, Some(10))
            .await
    }

    async fn create_snapshot(&self, name: &str, snap: &str) -> Result<Txg, CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/snapshots", name);
        let body = NewName { name: snap.into() };
        self.call(reqwest::Method::POST, &path, Some(&body), Some(10))
            .await
    }

    async fn destroy_snapshot(&self, name: &str, snap: &str) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/snapshots/{}", name, snap);
        self.call(reqwest::Method::DELETE, &path, None::<&()>, Some(10))
            .await
    }

    async fn clone_snapshot(&self, name: &str, snap: &str, new: &str) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/snapshots/{}/clone", name, snap);
        let body = NewName { name: new.into() };
        self.call(reqwest::Method::POST, &path, Some(&body), Some(10))
            .await
    }

    async fn rollback(&self, name: &str) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/rollback", name);
        self.call(reqwest::Method::POST, &path, None::<&()>, Some(10))
            .await
    }

    async fn recv_check(
        &self,
        name: &str,
        header: &SendHeader,
        force: bool,
    ) -> Result<(), CommitError> {
        let path = format!("/api/v1/pool/volumes/{}/recv/check?force={}", name, force);
        self.call(reqwest::Method::POST, &path, Some(header), Some(10))
            .await
    }
}

#[async_trait]
impl CommitSink for CommitClient {
    fn writer(&self) -> LeaseRef {
        // Not a writer's client: a lend under no lease, which is refused.
        self.writer
            .clone()
            .unwrap_or_else(|| LeaseRef::new(LeaseResource::writer("-"), Epoch::NONE))
    }
    async fn lease_metaslab(&self, want_blocks: u64) -> Result<LentMetaslab, CommitError> {
        let body = LendRequest {
            writer: self.writer(),
            want_blocks,
        };
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/lend",
            Some(&body),
            Some(20),
        )
        .await
    }
    async fn return_metaslab(&self, idx: MetaslabIdx) -> Result<(), CommitError> {
        let body = ReturnRequest {
            writer: self.writer(),
            idx,
        };
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/return",
            Some(&body),
            Some(20),
        )
        .await
    }
    async fn commit(&self, c: DatasetCommit) -> Result<Committed, CommitError> {
        // A flush waits for the committer as long as it takes (a hard mount),
        // unless the writer gives up.
        self.call(reqwest::Method::POST, "/api/v1/pool/commit", Some(&c), None)
            .await
    }
    async fn commit_fs(&self, c: FsCommit) -> Result<FsCommitted, CommitError> {
        // A namespace op is a single call from a client: give up after a
        // while instead of hanging it (the outcome is then unknown, and a
        // resend is acknowledged if it was applied).
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/commit_fs",
            Some(&c),
            Some(20),
        )
        .await
    }
    async fn claim_dataset(&self, volume: &str, ds: &Holding) -> Result<(), CommitError> {
        let body = ClaimRequest {
            dataset: volume.into(),
            ds: ds.clone(),
        };
        self.call(
            reqwest::Method::POST,
            "/api/v1/pool/claim",
            Some(&body),
            Some(20),
        )
        .await
    }
}
