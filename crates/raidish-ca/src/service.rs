//! [`CaService`]: one node's part in the cluster CA.

use crate::api::{
    DEFAULT_TOKEN_TTL_SECS, EnrollmentCreated, EnrollmentRequest, Issued, JoinRequest,
    MAX_TOKEN_TTL_SECS, SignRequest,
};
use crate::seal::NodeKeySource;
use crate::token::JoinToken;
use crate::{bootstrap_path, node_sans, now_ms, unseal_once_path, url_host};
use raidish_auth::ca::{LeafSpec, Signer, X509Checker};
use raidish_auth::envelope::{self, Key};
use raidish_cluster::Cluster;
use raidish_cluster::state::{LeaseOp, Response};
use raidish_types::api_types::Principal;
use raidish_types::pki::{
    self, EnrollScope, Enrollment, Identity, IssuedCert, PkiOp, PkiState, PkiStatus, ProfileKind,
    TokenId,
};
use raidish_types::types::NodeId;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

/// A refusal, with the HTTP status it's answered with.
#[derive(Debug, Clone)]
pub struct CaError {
    pub status: u16,
    pub msg: String,
}

impl CaError {
    pub fn new(status: u16, msg: impl Into<String>) -> CaError {
        CaError {
            status,
            msg: msg.into(),
        }
    }
    pub(crate) fn bad(msg: impl Into<String>) -> CaError {
        CaError::new(400, msg)
    }
    fn denied(msg: impl Into<String>) -> CaError {
        CaError::new(403, msg)
    }
    pub(crate) fn conflict(msg: impl Into<String>) -> CaError {
        CaError::new(409, msg)
    }
    fn internal(msg: impl Into<String>) -> CaError {
        CaError::new(500, msg)
    }
    /// This node can't do it now; another may (`503`: try the next).
    pub(crate) fn unavailable(msg: impl Into<String>) -> CaError {
        CaError::new(503, msg)
    }
    fn sealed() -> CaError {
        CaError::unavailable(
            "this node is sealed (it can't sign): ask another node, or unseal this one \
             with `raidish ca unseal`",
        )
    }
    pub(crate) fn no_ca() -> CaError {
        CaError::unavailable("no CA here (yet): set one up with `raidish ca init`")
    }
}

impl std::fmt::Display for CaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.msg, self.status)
    }
}

pub struct CaOptions {
    /// Unseal from `data_dir/pki/unseal-once` at start (see
    /// `crate::unseal_once_path`). Off: such a file is ignored (and left,
    /// with a warning).
    pub unseal_once: bool,
    /// Argon2id cost for new passphrase and recovery-key slots (m KiB, t,
    /// p). Tests make it cheap.
    pub kdf: (u32, u32, u32),
    /// Where this node's own key is (`ca.unseal_key` or
    /// `ca.unseal_credential`): it unseals itself with it from a node-key
    /// slot naming it (CA-3b).
    pub node_key: Option<NodeKeySource>,
    /// The activity log (CA-3c): unseals, seals, slots, and a cluster no
    /// node can sign for.
    pub events: Option<Arc<dyn crate::CaEvents>>,
}

impl Default for CaOptions {
    fn default() -> Self {
        CaOptions {
            node_key: None,
            events: None,
            unseal_once: true,
            kdf: (envelope::KDF_M_KIB, envelope::KDF_T, envelope::KDF_P),
        }
    }
}

/// Joins a client IP may try per minute.
const JOINS_PER_MINUTE: u32 = 20;
/// Unseals a client IP may try per minute.
const UNSEALS_PER_MINUTE: u32 = 10;

fn per_minute(map: &Mutex<HashMap<IpAddr, (Instant, u32)>>, ip: Option<IpAddr>, max: u32) -> bool {
    let Some(ip) = ip else { return true };
    let mut m = map.lock().unwrap();
    let now = Instant::now();
    m.retain(|_, (t, _)| now.duration_since(*t) < Duration::from_secs(60));
    let (_, n) = m.entry(ip).or_insert((now, 0));
    *n += 1;
    *n <= max
}

pub struct CaService {
    cluster: Arc<Cluster>,
    /// The cluster DEK, the seal epoch it was opened at and how (for
    /// `ca status`), if this node is unsealed. Never stored. Dropped when
    /// a newer epoch is applied (`ca seal`).
    dek: RwLock<Option<(Key, u64, &'static str)>>,
    /// The CA as staged by `ca init`, until it's in Raft. Held across a
    /// change to it and across proposing it, so nothing recorded into it is
    /// lost in the hand-over.
    staged: tokio::sync::Mutex<Option<PkiState>>,
    warnings: Mutex<Vec<String>>,
    http: reqwest::Client,
    joins: Mutex<HashMap<IpAddr, (Instant, u32)>>,
    unseals: Mutex<HashMap<IpAddr, (Instant, u32)>>,
    pub(crate) kdf: (u32, u32, u32),
    /// Wakes the CRL re-signer (`crate::crl`): the CA changed, or this
    /// node was unsealed.
    pub(crate) crl_wake: Arc<tokio::sync::Notify>,
    /// This node's own key, if configured.
    pub(crate) node_key: Option<NodeKeySource>,
    /// The node-key slots naming this node it has tried: each once, until
    /// a restart (`crate::seal`).
    pub(crate) node_key_seen: Mutex<HashSet<String>>,
    /// Wakes the node-key unsealer: the CA changed.
    pub(crate) node_key_wake: Arc<tokio::sync::Notify>,
    pub(crate) events: Option<Arc<dyn crate::CaEvents>>,
    /// A `ca.unsealed_none` is on (cleared when a node is unsealed).
    pub(crate) warned_unsealed_none: std::sync::atomic::AtomicBool,
}

impl CaService {
    /// Start this node's CA service: pick up a staged CA and the one-shot
    /// DEK, and (staged) hand the CA to Raft once there's a leader.
    pub fn start(cluster: Arc<Cluster>, opts: CaOptions) -> Result<Arc<CaService>, String> {
        let dir = cluster.data_dir().to_path_buf();
        let staged_path = bootstrap_path(&dir);
        let mut staged: Option<PkiState> = match std::fs::read(&staged_path) {
            Ok(b) => Some(
                serde_json::from_slice(&b)
                    .map_err(|e| format!("{}: {}", staged_path.display(), e))?,
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(format!("{}: {}", staged_path.display(), e)),
        };
        let mut warnings = Vec::new();
        if let (Some(s), Some(committed)) = (&staged, cluster.pki()) {
            if s.anchor == committed.anchor {
                // Proposed before a restart: done.
                let _ = std::fs::remove_file(&staged_path);
            } else {
                warnings.push(format!(
                    "{} stages another CA than the cluster has (root {}): ignored",
                    staged_path.display(),
                    s.anchor.fingerprint
                ));
            }
            staged = None;
        }
        let once = unseal_once_path(&dir);
        let mut dek = None;
        if once.exists() {
            if opts.unseal_once {
                let read = std::fs::read_to_string(&once);
                // Gone whatever comes of it: it's only good once.
                if let Err(e) = std::fs::remove_file(&once) {
                    warnings.push(format!("couldn't remove {}: {}", once.display(), e));
                }
                let pki = staged
                    .clone()
                    .or_else(|| cluster.pki().map(|p| (*p).clone()));
                match (read, pki) {
                    (Err(e), _) => warnings.push(format!("{}: {}", once.display(), e)),
                    (Ok(_), None) => {
                        warnings.push(format!("{}: no CA to unseal (ignored)", once.display()))
                    }
                    (Ok(text), Some(pki)) => {
                        match parse_dek(&text).and_then(|k| check_dek(&pki, &k).map(|_| k)) {
                            Ok(k) => {
                                println!("ca: unsealed (one-shot key from `ca init`, now removed)");
                                dek = Some((k, pki.seal.epoch, "unseal-once"));
                            }
                            Err(e) => warnings.push(format!("{}: {}", once.display(), e)),
                        }
                    }
                }
            } else {
                warnings.push(format!(
                    "{} is ignored (ca.unseal_once is off): delete it",
                    once.display()
                ));
            }
        }
        for w in &warnings {
            eprintln!("ca: WARNING: {}", w);
        }
        let is_staged = staged.is_some();
        let ca = Arc::new(CaService {
            cluster,
            dek: RwLock::new(dek),
            staged: tokio::sync::Mutex::new(staged),
            warnings: Mutex::new(warnings),
            http: raidish_auth::tls::http_client(Some(Duration::from_secs(20)))?,
            joins: Mutex::new(HashMap::new()),
            unseals: Mutex::new(HashMap::new()),
            kdf: opts.kdf,
            crl_wake: Arc::new(tokio::sync::Notify::new()),
            node_key: opts.node_key,
            node_key_seen: Mutex::new(HashSet::new()),
            node_key_wake: Arc::new(tokio::sync::Notify::new()),
            events: opts.events,
            warned_unsealed_none: Default::default(),
        });
        if let Some((_, _, by)) = ca.dek.read().unwrap().as_ref() {
            ca.event(
                raidish_types::api_types::EventKind::CaUnsealed,
                ca.me_subject(),
                format!("{} unsealed by {}", ca.cluster.name, by),
                None,
            );
        }
        tokio::spawn(crate::loud::announce(Arc::downgrade(&ca)));
        if ca.node_key.is_some() {
            // Before the observer: its first call (the CA as it is) wakes it.
            crate::seal::start_node_key(&ca);
        }
        ca.cluster.observe_pki(Arc::new(Watch(Arc::downgrade(&ca))));
        crate::crl::start(&ca);
        if is_staged {
            println!(
                "ca: staged by `ca init`: answering joins from {} until the cluster's first quorum",
                staged_path.display()
            );
            tokio::spawn(hand_over(Arc::downgrade(&ca)));
        }
        Ok(ca)
    }

    /// The CA as this node has it: staged, else Raft's.
    pub async fn current(&self) -> Option<Arc<PkiState>> {
        if let Some(s) = self.staged.lock().await.as_ref() {
            return Some(Arc::new(s.clone()));
        }
        self.cluster.pki()
    }

    /// Whether the CA is only staged here (before the first quorum).
    pub(crate) async fn is_staged(&self) -> bool {
        self.staged.lock().await.is_some()
    }

    pub(crate) fn cluster(&self) -> &Arc<Cluster> {
        &self.cluster
    }

    /// The CA as the cluster has it now: staged, else Raft's after a read
    /// index, so a change just made through another node is seen (for
    /// what's checked before it's changed: cancelling, revoking).
    pub(crate) async fn current_linearizable(&self) -> Result<Arc<PkiState>, CaError> {
        if let Some(s) = self.staged.lock().await.as_ref() {
            return Ok(Arc::new(s.clone()));
        }
        let state = self
            .cluster
            .linearizable_state(Duration::from_secs(5))
            .await
            .map_err(CaError::unavailable)?;
        state.pki.ok_or_else(CaError::no_ca)
    }

    /// The CA as the cluster has it now, for an action `caller` (the
    /// certificate the request came with, over TLS) asks for: refused if
    /// that certificate, or its issuer, is revoked. This node's own view,
    /// and so its TLS check, can be behind a revocation committed through
    /// another node; this reads after a read index.
    pub(crate) async fn for_caller(&self, caller: &[u8]) -> Result<Arc<PkiState>, CaError> {
        let pki = self.current_linearizable().await?;
        caller_ok(&pki, caller)?;
        Ok(pki)
    }

    pub fn unsealed(&self) -> bool {
        self.dek.read().unwrap().is_some()
    }

    /// How this node was unsealed, if it is.
    pub fn unsealed_by(&self) -> Option<String> {
        self.dek
            .read()
            .unwrap()
            .as_ref()
            .map(|(_, _, by)| by.to_string())
    }

    pub(crate) fn http(&self) -> reqwest::Client {
        self.http.clone()
    }

    /// The DEK, to make something for `pki` with: only if it was opened
    /// at `pki`'s seal epoch. Opened at an older one (a `Seal` applied
    /// since), it's dropped. `pki` behind it (a stale view): 503.
    pub(crate) fn dek_at(&self, pki: &PkiState) -> Result<Key, CaError> {
        let mut held = self.dek.write().unwrap();
        match held.as_ref() {
            None => Err(CaError::sealed()),
            Some((k, e, _)) if *e == pki.seal.epoch => Ok(k.clone()),
            Some((_, e, _)) if *e < pki.seal.epoch => {
                *held = None;
                println!(
                    "ca: sealed (the CA was sealed: seal epoch {})",
                    pki.seal.epoch
                );
                Err(CaError::sealed())
            }
            Some(_) => Err(CaError::unavailable(
                "this node's view of the CA is behind: try again",
            )),
        }
    }

    /// Told of the CA as it's applied: drop the DEK on a newer seal epoch.
    fn observed(&self, pki: Option<&PkiState>) {
        if let Some(pki) = pki {
            let stale = self
                .dek
                .read()
                .unwrap()
                .as_ref()
                .is_some_and(|(_, e, _)| *e < pki.seal.epoch);
            if stale {
                let _ = self.dek_at(pki);
            }
        }
        self.crl_wake.notify_one();
        self.node_key_wake.notify_one();
    }

    pub async fn status(&self) -> PkiStatus {
        self.status_of(true).await
    }

    /// This node's status (`crl_warning`: with its own warning about a CRL
    /// it can't re-sign; `?all=true` says it cluster-wide instead).
    pub(crate) async fn status_of(&self, crl_warning: bool) -> PkiStatus {
        let staged = self.staged.lock().await.is_some();
        let mut warnings = self.warnings.lock().unwrap().clone();
        let once = unseal_once_path(self.cluster.data_dir());
        if once.exists() {
            warnings.push(format!("{} is still there: delete it", once.display()));
        }
        let pki = self.current().await;
        if let Some(w) = pki
            .as_deref()
            .filter(|_| crl_warning)
            .and_then(|p| crate::crl::warning(p, self.unsealed(), now_ms()))
        {
            warnings.push(w);
        }
        PkiStatus {
            node: self.cluster.name.clone(),
            unsealed: self.unsealed(),
            unsealed_by: self.unsealed_by(),
            staged,
            pki: pki.map(|p| p.view()),
            warnings,
            nodes: vec![],
        }
    }

    /// The active issuer, unsealed.
    pub(crate) fn signer(&self, pki: &PkiState) -> Result<Signer, CaError> {
        let dek = self.dek_at(pki)?;
        open_signer(pki, &dek).map_err(CaError::internal)
    }

    /// Record `op`, made with the DEK for `pki` (so at its seal epoch).
    pub(crate) async fn commit_at(&self, pki: &PkiState, op: PkiOp) -> Result<(), CaError> {
        self.commit(op.at_epoch(pki.seal.epoch)).await
    }

    /// Record `op`: in the staged CA while there is one, else through Raft.
    pub(crate) async fn commit(&self, op: PkiOp) -> Result<(), CaError> {
        {
            let mut staged = self.staged.lock().await;
            if let Some(s) = staged.as_mut() {
                let mut next = Some(Arc::new(s.clone()));
                pki::apply(&mut next, &op, now_ms(), &X509Checker).map_err(CaError::conflict)?;
                let next = (*next.unwrap()).clone();
                write_staged(self.cluster.data_dir(), &next).map_err(CaError::internal)?;
                *s = next;
                return Ok(());
            }
        }
        match self.cluster.lease(&LeaseOp::Pki(op), false).await {
            Ok(Response::Recorded) => {
                // Applied here too before answering, so what's asked of
                // this node next sees it (a token just made, a cert).
                if let Err(e) = self
                    .cluster
                    .linearizable_state(Duration::from_secs(5))
                    .await
                {
                    eprintln!("ca: recorded, but not applied here yet: {}", e);
                }
                Ok(())
            }
            Ok(Response::Refused(e)) => Err(CaError::conflict(e)),
            Ok(other) => Err(CaError::internal(format!("unexpected answer {:?}", other))),
            Err(e) => Err(CaError::unavailable(e)),
        }
    }

    /// May `ip` try another join now? (Guessing secrets is pointless at
    /// 256 bits, but nobody should be able to make the signer busy.)
    pub fn join_allowed(&self, ip: Option<IpAddr>) -> bool {
        per_minute(&self.joins, ip, JOINS_PER_MINUTE)
    }

    /// May `ip` try another unseal now? (Each try is an Argon2id per slot,
    /// and a guess at a passphrase.)
    pub fn unseal_allowed(&self, ip: Option<IpAddr>) -> bool {
        per_minute(&self.unseals, ip, UNSEALS_PER_MINUTE)
    }

    /// This node now holds `dek`, opened at `epoch` (`by`: how, as
    /// `ca status` says it).
    pub(crate) fn set_dek(&self, dek: Key, epoch: u64, by: &'static str) {
        *self.dek.write().unwrap() = Some((dek, epoch, by));
        self.event(
            raidish_types::api_types::EventKind::CaUnsealed,
            self.me_subject(),
            format!("{} unsealed by {}", self.cluster.name, by),
            None,
        );
        self.crl_wake.notify_one();
    }

    /// `POST /api/v1/pki/join`. A sealed node passes it on to one that
    /// can sign (unless it was passed on already).
    pub async fn join(&self, req: JoinRequest, forwarded: bool) -> Result<Issued, CaError> {
        let pki = self.current().await.ok_or_else(CaError::no_ca)?;
        if !self.unsealed() {
            if forwarded {
                return Err(CaError::sealed());
            }
            return self.forward_join(&req).await;
        }
        let secret = crate::token::decode_secret(&req.secret).map_err(CaError::denied)?;
        let hash = envelope::sha256(&secret);
        let e = pki
            .enrollments
            .get(&req.id)
            .ok_or_else(|| CaError::denied("unknown, expired or used-up join token"))?;
        if !envelope::ct_eq(&e.secret_sha256, &hash) {
            return Err(CaError::denied("unknown, expired or used-up join token"));
        }
        let dek = self.dek_at(&pki)?;
        if !envelope::enrollment_mac_ok(&dek, &req.id, e) {
            return Err(CaError::denied(
                "this join token wasn't made by the cluster's signer (its MAC is wrong)",
            ));
        }
        let EnrollScope::Node {
            fingerprint,
            member,
            extra_sans,
            ..
        } = &e.scope
        else {
            return Err(CaError::denied("not a node join token"));
        };
        let mine = &self.cluster.members;
        if req.fingerprint != mine.fingerprint || *fingerprint != mine.fingerprint {
            return Err(CaError::conflict(
                "your pool config describes another pool layout than this node's: \
                 make the configs agree first",
            ));
        }
        if member.is_some_and(|m| m != req.member) {
            return Err(CaError::denied(format!(
                "this token is for {}, not {}",
                member.unwrap(),
                req.member
            )));
        }
        let host = self.member_host(req.member)?;
        let mut hosts = vec![host.clone()];
        hosts.extend(extra_sans.iter().cloned());
        let identity = Identity {
            profile: ProfileKind::Node,
            cn: host,
            sans: node_sans(&hosts),
            member: Some(req.member),
        };
        let signer = self.signer(&pki)?;
        let days = pki.params(ProfileKind::Node).default_days;
        // Signed first: a bad CSR mustn't use the token up.
        let issued = sign(&signer, &req.csr_pem, &identity, days)?;
        self.commit(PkiOp::ConsumeEnrollment {
            id: req.id.clone(),
            secret_sha256: hash,
            member: Some(req.member),
        })
        .await?;
        self.record(&pki, &signer, &identity, &issued, None).await?;
        println!(
            "ca: {} joined as '{}' (certificate {})",
            req.member, identity.cn, issued.serial
        );
        Ok(answer(&pki, &signer, issued))
    }

    async fn forward_join(&self, req: &JoinRequest) -> Result<Issued, CaError> {
        let mut why = Vec::new();
        for (id, base) in &self.cluster.members.peers {
            let url = format!("{}/api/v1/pki/join?forwarded=true", base);
            match self.http.post(&url).json(req).send().await {
                Err(e) => why.push(format!("{}: {}", id, e)),
                Ok(r) if r.status().as_u16() == 503 => {
                    why.push(format!("{}: {}", id, r.text().await.unwrap_or_default()))
                }
                Ok(r) if r.status().is_success() => {
                    return r
                        .json()
                        .await
                        .map_err(|e| CaError::internal(format!("from {}: {}", id, e)));
                }
                Ok(r) => {
                    let status = r.status().as_u16();
                    return Err(CaError::new(status, r.text().await.unwrap_or_default()));
                }
            }
        }
        Err(CaError::unavailable(format!(
            "no node can sign now (all sealed?): {}",
            why.join("; ")
        )))
    }

    /// The host `member` is reached at, by this node's pool config.
    fn member_host(&self, member: NodeId) -> Result<String, CaError> {
        let base = self.cluster.members.peers.get(&member).ok_or_else(|| {
            if member == self.cluster.members.me {
                CaError::bad(format!(
                    "{} is the node signing: it can't join itself",
                    member
                ))
            } else {
                CaError::bad(format!("{} is not a member of this pool", member))
            }
        })?;
        url_host(base).ok_or_else(|| CaError::internal(format!("no host in {}", base)))
    }

    async fn record(
        &self,
        pki: &PkiState,
        signer: &Signer,
        identity: &Identity,
        issued: &raidish_auth::ca::Issued,
        renews: Option<pki::Serial>,
    ) -> Result<(), CaError> {
        self.commit_at(
            pki,
            PkiOp::RecordIssued {
                serial: issued.serial.clone(),
                cert: IssuedCert {
                    identity: identity.clone(),
                    issuer: signer.id,
                    not_before_ms: issued.not_before_ms,
                    not_after_ms: issued.not_after_ms,
                    renews,
                },
            },
        )
        .await
    }

    /// `POST /api/v1/pki/renew`: the caller's own identity again, from its
    /// current certificate (`cert_der`, which TLS has verified).
    pub async fn renew(&self, cert_der: &[u8], csr_pem: &str) -> Result<Issued, CaError> {
        let pki = self.for_caller(cert_der).await?;
        let facts = raidish_auth::ca::facts(cert_der).map_err(CaError::bad)?;
        let now = now_ms();
        let cert = pki
            .certs
            .get(&facts.serial)
            .filter(|_| pki.is_live(&facts.serial, now))
            .ok_or_else(|| {
                CaError::denied(format!(
                    "certificate {} isn't on record here, or is revoked or expired",
                    facts.serial
                ))
            })?;
        let profile = cert.identity.profile;
        if !profile.spec().self_renew {
            return Err(CaError::denied(format!(
                "{} certificates aren't renewed by their holder: an admin reissues them \
                 (`raidish ca issue`)",
                profile
            )));
        }
        let signer = self.signer(&pki)?;
        let identity = cert.identity.clone();
        let issued = sign(
            &signer,
            csr_pem,
            &identity,
            pki.params(profile).default_days,
        )?;
        self.record(
            &pki,
            &signer,
            &identity,
            &issued,
            Some(facts.serial.clone()),
        )
        .await?;
        println!(
            "ca: renewed '{}' ({}): {} → {}",
            identity.cn, profile, facts.serial, issued.serial
        );
        Ok(answer(&pki, &signer, issued))
    }

    /// `POST /api/v1/pki/sign`: an admin issuing a certificate.
    pub async fn sign(
        &self,
        admin: &Principal,
        caller: &[u8],
        req: SignRequest,
    ) -> Result<Issued, CaError> {
        let pki = self.for_caller(caller).await?;
        if !req.profile.admin_issuable() {
            return Err(CaError::bad(format!(
                "{} certificates aren't issued this way{}",
                req.profile,
                if req.profile == ProfileKind::Node {
                    " (nodes join: `raidish ca token create --member …`)"
                } else {
                    ""
                }
            )));
        }
        let cn = req.cn.trim().to_string();
        if cn.is_empty() || cn.len() > 64 || cn.chars().any(|c| c.is_control()) {
            return Err(CaError::bad(
                "a certificate needs a name (CN) of 1-64 characters",
            ));
        }
        let params = pki.params(req.profile);
        let days = req.days.unwrap_or(params.default_days);
        if days == 0 || days > params.max_days {
            return Err(CaError::bad(format!(
                "{} certificates live 1-{} days",
                req.profile, params.max_days
            )));
        }
        let mut sans = Vec::new();
        if req.profile.spec().server_auth {
            // A server is reached by its name.
            sans.push(cn.to_ascii_lowercase());
        }
        for s in &req.sans {
            let s = s.trim().to_ascii_lowercase();
            if !s.is_empty() && !sans.contains(&s) {
                sans.push(s);
            }
        }
        let member = match req.profile {
            ProfileKind::Libvirt => req.member,
            _ if req.member.is_some() => {
                return Err(CaError::bad("only libvirt certificates belong to a member"));
            }
            _ => None,
        };
        let identity = Identity {
            profile: req.profile,
            cn,
            sans,
            member,
        };
        let signer = self.signer(&pki)?;
        let issued = sign(&signer, &req.csr_pem, &identity, days)?;
        self.record(&pki, &signer, &identity, &issued, None).await?;
        println!(
            "ca: {} issued a {} certificate for '{}' ({})",
            admin.name, req.profile, identity.cn, issued.serial
        );
        Ok(answer(&pki, &signer, issued))
    }

    /// `POST /api/v1/pki/enrollments`: a join token for a member.
    pub async fn create_enrollment(
        &self,
        admin: &Principal,
        caller: &[u8],
        req: EnrollmentRequest,
    ) -> Result<EnrollmentCreated, CaError> {
        let pki = self.for_caller(caller).await?;
        let dek = self.dek_at(&pki)?;
        if req.member == self.cluster.members.me {
            // Its pool config has no address for itself: another node's has.
            return Err(CaError::unavailable(format!(
                "{} can't make its own join token: ask another node",
                req.member
            )));
        }
        self.member_host(req.member)?;
        if req.compromised && !req.replace {
            return Err(CaError::bad("--compromised goes with --replace"));
        }
        let ttl = req.ttl_secs.unwrap_or(DEFAULT_TOKEN_TTL_SECS);
        if ttl == 0 || ttl > MAX_TOKEN_TTL_SECS {
            return Err(CaError::bad(format!(
                "a join token lasts up to {} days",
                MAX_TOKEN_TTL_SECS / 86400
            )));
        }
        let mut extra_sans: Vec<String> = Vec::new();
        for s in &req.extra_sans {
            let s = s.trim().to_ascii_lowercase();
            if !s.is_empty() && !extra_sans.contains(&s) {
                extra_sans.push(s);
            }
        }
        let token = JoinToken::generate(pki.anchor.fingerprint).map_err(CaError::internal)?;
        let enrollment = node_enrollment(
            &dek,
            &token,
            NodeScope {
                fingerprint: self.cluster.members.fingerprint.clone(),
                member: req.member,
                extra_sans,
                replace: req.replace,
                compromised: req.compromised,
            },
            now_ms() + ttl * 1000,
            &admin.name,
        );
        let expires_ms = enrollment.expires_ms;
        self.commit_at(
            &pki,
            PkiOp::CreateEnrollment {
                id: token.id.clone(),
                enrollment: Box::new(enrollment),
            },
        )
        .await?;
        println!(
            "ca: {} made join token {} for {}{}",
            admin.name,
            token.id,
            req.member,
            if req.replace { " (replacing)" } else { "" }
        );
        Ok(EnrollmentCreated {
            id: token.id.clone(),
            member: req.member,
            token: token.encode().to_string(),
            expires_ms,
        })
    }

    /// `DELETE /api/v1/pki/enrollments/<id>`.
    pub async fn cancel_enrollment(&self, id: TokenId) -> Result<(), CaError> {
        let pki = self.current_linearizable().await?;
        if !pki.enrollments.contains_key(&id) {
            return Err(CaError::new(404, format!("no join token {}", id)));
        }
        self.commit(PkiOp::CancelEnrollment { id }).await
    }
}

/// What a node join token is for.
pub(crate) struct NodeScope {
    pub fingerprint: String,
    pub member: NodeId,
    pub extra_sans: Vec<String>,
    pub replace: bool,
    pub compromised: bool,
}

/// A single-use enrollment for `token`, MACed under `dek`.
pub(crate) fn node_enrollment(
    dek: &[u8; 32],
    token: &JoinToken,
    scope: NodeScope,
    expires_ms: u64,
    created_by: &str,
) -> Enrollment {
    let mut e = Enrollment {
        scope: EnrollScope::Node {
            fingerprint: scope.fingerprint,
            member: Some(scope.member),
            extra_sans: scope.extra_sans,
            replace: scope.replace,
            compromised: scope.compromised,
        },
        secret_sha256: token.secret_sha256(),
        uses: 1,
        uses_left: 1,
        expires_ms,
        created_by: created_by.into(),
        mac: [0; 32],
    };
    e.mac = envelope::enrollment_mac(dek, &token.id, &e);
    e
}

/// The active issuer of `pki`, its key opened with `dek`.
pub(crate) fn open_signer(pki: &PkiState, dek: &[u8; 32]) -> Result<Signer, String> {
    let (id, issuer) = pki.active_issuer().ok_or("the CA has no active issuer")?;
    let key = envelope::open(
        dek,
        &envelope::issuer_aad(&pki.anchor.fingerprint, id),
        &issuer.sealed_key,
    )
    .map_err(|e| format!("{}'s key: {}", id, e))?;
    let key = Zeroizing::new(
        String::from_utf8(key.to_vec()).map_err(|_| format!("{}'s key is not PEM", id))?,
    );
    Signer::new(id, &issuer.cert_pem, &key)
}

/// Does `dek` open the CA's active issuer?
/// Whether certificate `der` may still act, by `pki`: not revoked, and not
/// from a revoked issuer (on record, or by its key id for one that isn't).
fn caller_ok(pki: &PkiState, der: &[u8]) -> Result<(), CaError> {
    let facts = raidish_auth::ca::facts(der).map_err(CaError::bad)?;
    let refused = |why: &str| {
        Err(CaError::new(
            401,
            format!("certificate {} {}", facts.serial, why),
        ))
    };
    if pki.is_revoked(&facts.serial) {
        return refused("is revoked");
    }
    if let Some(c) = pki.certs.get(&facts.serial)
        && pki.issuer_revoked(c.issuer)
    {
        return refused("is from a revoked issuer");
    }
    if let Some(aki) = raidish_auth::ca::authority_key_id(der)
        && pki
            .issuers
            .values()
            .any(|i| i.ski == aki && matches!(i.status, pki::IssuerStatus::Revoked { .. }))
    {
        return refused("is from a revoked issuer");
    }
    Ok(())
}

/// The CA's view of the cluster, for this node's [`CaService`].
struct Watch(std::sync::Weak<CaService>);

impl pki::PkiObserver for Watch {
    fn observe_pki(&self, pki: Option<&PkiState>) {
        if let Some(ca) = self.0.upgrade() {
            ca.observed(pki);
        }
    }
}

pub(crate) fn check_dek(pki: &PkiState, dek: &[u8; 32]) -> Result<(), String> {
    open_signer(pki, dek)
        .map(|_| ())
        .map_err(|_| "that key doesn't open this CA".to_string())
}

fn parse_dek(text: &str) -> Result<Key, String> {
    let b = Zeroizing::new(hex::decode(text.trim()).map_err(|_| "not a key")?);
    let k: [u8; 32] = b[..].try_into().map_err(|_| "not a 256-bit key")?;
    Ok(Zeroizing::new(k))
}

fn sign(
    signer: &Signer,
    csr_pem: &str,
    identity: &Identity,
    days: u32,
) -> Result<raidish_auth::ca::Issued, CaError> {
    signer
        .sign(
            csr_pem,
            &LeafSpec {
                profile: identity.profile,
                cn: identity.cn.clone(),
                sans: identity.sans.clone(),
                days,
            },
        )
        .map_err(CaError::bad)
}

fn answer(pki: &PkiState, signer: &Signer, issued: raidish_auth::ca::Issued) -> Issued {
    Issued {
        serial: issued.serial,
        cert_pem: signer.chain(&issued.cert_pem),
        ca_pem: pki.anchor.cert_pem.clone(),
        not_after_ms: issued.not_after_ms,
    }
}

pub(crate) fn write_staged(data_dir: &std::path::Path, state: &PkiState) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(state).map_err(|e| e.to_string())?;
    crate::write_file(&bootstrap_path(data_dir), &bytes, 0o600)
}

/// Propose the staged CA once there's a leader, then drop the file.
async fn hand_over(ca: std::sync::Weak<CaService>) {
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let Some(ca) = ca.upgrade() else { return };
        if !ca.cluster.has_leader() {
            continue;
        }
        let mut staged = ca.staged.lock().await;
        let Some(state) = staged.clone() else { return };
        let path = bootstrap_path(ca.cluster.data_dir());
        if let Some(committed) = ca.cluster.pki() {
            if committed.anchor == state.anchor {
                let _ = std::fs::remove_file(&path);
            } else {
                let w = format!(
                    "the cluster already has another CA (root {}): {} ignored",
                    committed.anchor.fingerprint,
                    path.display()
                );
                eprintln!("ca: WARNING: {}", w);
                ca.warnings.lock().unwrap().push(w);
            }
            *staged = None;
            return;
        }
        let op = LeaseOp::Pki(PkiOp::Bootstrap(Box::new(state.clone())));
        match ca.cluster.lease(&op, false).await {
            Ok(Response::Recorded) => {
                // Applied here before the file goes, so nothing answers
                // from neither.
                for _ in 0..100 {
                    if ca.cluster.pki().is_some() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                if let Err(e) = std::fs::remove_file(&path) {
                    eprintln!("ca: removing {}: {}", path.display(), e);
                }
                *staged = None;
                println!(
                    "ca: the CA is in the cluster now (root {})",
                    state.anchor.fingerprint
                );
                return;
            }
            // Another node's went in first, say: the next round sees it.
            Ok(Response::Refused(e)) => eprintln!("ca: bootstrap refused: {}", e),
            Ok(other) => eprintln!("ca: bootstrap: unexpected answer {:?}", other),
            Err(e) => eprintln!("ca: bootstrap: {} (retrying)", e),
        }
        drop(staged);
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
