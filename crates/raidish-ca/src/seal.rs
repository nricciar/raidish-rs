//! Sealing (CA-3a, `docs/ca-service-plan.md`): unsealing a node with a
//! recovery key or a passphrase, sealing every node, and the key slots.
//!
//! A node holds the DEK with the seal epoch it was opened at
//! (`CaService::dek_at`); `ca seal` moves the epoch, and every node drops
//! its DEK as it applies that. What a node makes with the DEK carries the
//! epoch (`PkiOp::AtEpoch`), so nothing it signs after a seal it hasn't
//! seen yet is recorded.
//!
//! Node keys (CA-3b): a node configured with its own key
//! (`NodeKeySource`) unseals itself from a node-key slot naming it, when
//! it starts and when such a slot appears. Each slot is tried once: a
//! failure is logged, and it waits for a new slot or a restart.

use crate::api::{NewSlotKind, SealRequest, SlotCreated, SlotRequest};
use crate::service::{CaError, CaService, check_dek};
use raidish_auth::envelope::{self, Key, Secret};
use raidish_types::api_types::{EventKind, Subject};
use raidish_types::pki::{Mac, PkiOp, PkiState, Rekey, SealConfig, SlotKind};
use std::path::PathBuf;
use std::sync::{Arc, Weak};

/// Where a node's own key is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeKeySource {
    /// A plain file (`ca.unseal_key`): a copy of the disk it's on opens
    /// the CA.
    File(PathBuf),
    /// A systemd credential (`ca.unseal_credential`), read from
    /// `$CREDENTIALS_DIRECTORY/<name>`: `LoadCredentialEncrypted=` keeps
    /// it sealed to this machine's TPM or host key.
    Credential(String),
}

impl NodeKeySource {
    /// How `ca status` says a node was unsealed with it.
    fn by(&self) -> &'static str {
        match self {
            NodeKeySource::File(_) => "node-key (file)",
            NodeKeySource::Credential(_) => "node-key (credential)",
        }
    }

    fn path(&self) -> Result<PathBuf, String> {
        match self {
            NodeKeySource::File(p) => Ok(p.clone()),
            NodeKeySource::Credential(name) => {
                if name.is_empty() || name.contains('/') {
                    return Err(format!("credential name '{}'", name));
                }
                let dir = std::env::var_os("CREDENTIALS_DIRECTORY").ok_or_else(|| {
                    format!(
                        "credential {}: $CREDENTIALS_DIRECTORY isn't set (run under systemd \
                         with LoadCredentialEncrypted={})",
                        name, name
                    )
                })?;
                Ok(PathBuf::from(dir).join(name))
            }
        }
    }

    /// The key.
    pub fn read(&self) -> Result<Key, String> {
        let path = self.path()?;
        let text = zeroize::Zeroizing::new(
            std::fs::read_to_string(&path).map_err(|e| format!("{}: {}", path.display(), e))?,
        );
        envelope::parse_node_key(&text).map_err(|e| format!("{}: {}", path.display(), e))
    }
}

/// Start `ca`'s node-key unsealer (woken by `CaService`'s observer).
pub(crate) fn start_node_key(ca: &Arc<CaService>) {
    tokio::spawn(run_node_key(Arc::downgrade(ca), ca.node_key_wake.clone()));
}

async fn run_node_key(ca: Weak<CaService>, wake: Arc<tokio::sync::Notify>) {
    loop {
        wake.notified().await;
        let Some(ca) = ca.upgrade() else { return };
        ca.try_node_key().await;
    }
}

/// A new slot's id: its kind and 8 random hex digits.
fn slot_id(kind: &str) -> Result<String, CaError> {
    let b: [u8; 4] = envelope::random().map_err(|e| CaError::new(500, e))?;
    Ok(format!(
        "{}-{}",
        kind,
        b.iter().map(|x| format!("{:02x}", x)).collect::<String>()
    ))
}

fn is_recovery(kind: &SlotKind) -> bool {
    matches!(kind, SlotKind::RecoveryKey(_))
}

/// The node-key slots naming `me`.
fn my_slots(pki: &PkiState, me: raidish_types::types::NodeId) -> Vec<String> {
    pki.seal
        .slots
        .iter()
        .filter(|s| matches!(s.kind, SlotKind::NodeKey { node } if node == me))
        .map(|s| s.id.clone())
        .collect()
}

impl CaService {
    /// Unseal with this node's own key, if it's sealed and a node-key slot
    /// naming it hasn't been tried yet.
    pub(crate) async fn try_node_key(&self) {
        let Some(src) = &self.node_key else { return };
        if self.unsealed() {
            return;
        }
        let Some(pki) = self.current().await else {
            return;
        };
        let me = self.cluster().members.me;
        let fresh: Vec<String> = {
            let mut seen = self.node_key_seen.lock().unwrap();
            let fresh: Vec<String> = my_slots(&pki, me)
                .into_iter()
                .filter(|id| !seen.contains(id))
                .collect();
            seen.extend(fresh.iter().cloned());
            fresh
        };
        if fresh.is_empty() {
            return;
        }
        let opened = src.read().and_then(|key| {
            let dek = envelope::unwrap_dek_node(&pki.seal.slots, &pki.seal.cluster_id, me, &key)?;
            check_dek(&pki, &dek)?;
            Ok(dek)
        });
        match opened {
            Ok(dek) => {
                self.set_dek(dek, pki.seal.epoch, src.by());
                println!("ca: unsealed by {}", src.by());
                if let NodeKeySource::File(p) = src {
                    eprintln!(
                        "ca: WARNING: unsealed from a plain file ({}): a copy of this node's \
                         disk opens the CA. Keep the key in a systemd credential instead \
                         (ca.unseal_credential)",
                        p.display()
                    );
                }
            }
            Err(e) => eprintln!(
                "ca: WARNING: this node's key didn't unseal the CA (slot {}): {}. Not tried \
                 again until a new node-key slot for {} or a restart",
                fresh.join(", "),
                e,
                me
            ),
        }
    }

    /// Unseal this node with a recovery key or a passphrase.
    pub async fn unseal(&self, secret: &str) -> Result<(), CaError> {
        let pki = self.current_linearizable().await?;
        let epoch = pki.seal.epoch;
        let secret = Secret::parse(secret).map_err(CaError::bad)?;
        let by_recovery_key = matches!(secret, Secret::RecoveryKey(_));
        // Argon2id at 64 MiB per slot tried: off the async threads.
        let dek = tokio::task::spawn_blocking(move || {
            envelope::unwrap_dek(&pki.seal.slots, &pki.seal.cluster_id, &secret)
                .and_then(|k| check_dek(&pki, &k).map(|_| k))
        })
        .await
        .map_err(|e| CaError::new(500, e.to_string()))?
        .map_err(|e| CaError::new(403, e))?;
        // Sealed again meanwhile: the observer has run already, and
        // `dek_at` drops it at first use.
        let by = match by_recovery_key {
            true => "recovery-key",
            false => "passphrase",
        };
        self.set_dek(dek, epoch, by);
        println!("ca: unsealed");
        Ok(())
    }

    /// Seal every node: they drop the DEK as they apply it. Node-key slots
    /// go too, unless kept.
    pub async fn seal(&self, req: SealRequest, by: Option<String>) -> Result<(), CaError> {
        let pki = self.current_linearizable().await?;
        let remove_slots = if req.keep_node_keys {
            vec![]
        } else {
            pki.seal
                .slots
                .iter()
                .filter(|s| matches!(s.kind, SlotKind::NodeKey { .. }))
                .map(|s| s.id.clone())
                .collect()
        };
        let removed = remove_slots.len();
        self.commit(PkiOp::Seal { remove_slots }).await?;
        println!("ca: sealed (every node)");
        self.event(
            EventKind::CaSealed,
            Subject::Cluster,
            match (req.keep_node_keys, removed) {
                (true, _) => "CA sealed on every node (node keys kept: those nodes unseal \
                              again when they restart)"
                    .to_string(),
                (false, 0) => "CA sealed on every node".to_string(),
                (false, n) => format!("CA sealed on every node; {} node-key slots removed", n),
            },
            by,
        );
        Ok(())
    }

    /// Another way to unseal, wrapping the DEK this node holds.
    pub async fn add_slot(
        &self,
        caller: &[u8],
        req: SlotRequest,
        by: Option<String>,
    ) -> Result<SlotCreated, CaError> {
        let pki = self.for_caller(caller).await?;
        let dek = self.dek_at(&pki)?;
        let (m, t, p) = self.kdf;
        let kdf = envelope::kdf_params(m, t, p).map_err(|e| CaError::new(500, e))?;
        let label = req.label.trim().to_string();
        let cluster_id = pki.seal.cluster_id;
        let (slot, recovery_key, node_key) = match req.kind {
            NewSlotKind::RecoveryKey => {
                let id = slot_id("recovery-key")?;
                let key = envelope::new_recovery_key().map_err(|e| CaError::new(500, e))?;
                let bytes = envelope::parse_recovery_key(&key).map_err(|e| CaError::new(500, e))?;
                let slot = tokio::task::spawn_blocking(move || {
                    envelope::recovery_slot(&dek, &cluster_id, &id, &label, &bytes, kdf)
                })
                .await
                .map_err(|e| CaError::new(500, e.to_string()))?
                .map_err(|e| CaError::new(500, e))?;
                (slot, Some(key.to_string()), None)
            }
            NewSlotKind::NodeKey => {
                let node = req
                    .node
                    .ok_or_else(|| CaError::bad("a node-key slot names its node"))?;
                let members = &self.cluster().members;
                if node != members.me && !members.peers.contains_key(&node) {
                    return Err(CaError::bad(format!(
                        "{} isn't a member of this pool",
                        node
                    )));
                }
                let (key, text) = envelope::new_node_key().map_err(|e| CaError::new(500, e))?;
                let id = slot_id(&format!("node-key-{}", node))?;
                let label = if label.is_empty() {
                    format!("{}'s own key", node)
                } else {
                    label
                };
                let slot = envelope::node_key_slot(&dek, &cluster_id, &id, &label, node, &key)
                    .map_err(|e| CaError::new(500, e))?;
                (slot, None, Some(text.to_string()))
            }
            NewSlotKind::Passphrase => {
                let phrase = zeroize::Zeroizing::new(
                    req.passphrase
                        .ok_or_else(|| CaError::bad("a passphrase slot needs its passphrase"))?,
                );
                let id = slot_id("passphrase")?;
                let slot = tokio::task::spawn_blocking(move || {
                    envelope::passphrase_slot(&dek, &cluster_id, &id, &label, &phrase, kdf)
                })
                .await
                .map_err(|e| CaError::new(500, e.to_string()))?
                .map_err(CaError::bad)?;
                (slot, None, None)
            }
        };
        let id = slot.id.clone();
        let what = format!("key slot {} ({}) added", id, slot.kind);
        self.commit_at(&pki, PkiOp::AddSlot(Box::new(slot))).await?;
        println!("ca: {}", what);
        self.event(EventKind::CaSlotAdded, Subject::Cluster, what, by);
        // A staged CA has no observer: a slot for this node is tried now.
        self.node_key_wake.notify_one();
        Ok(SlotCreated {
            id,
            recovery_key,
            node_key,
        })
    }

    /// Remove slot `id`: never the last, nor the last recovery-key slot
    /// without `force`.
    pub async fn remove_slot(
        &self,
        id: &str,
        force: bool,
        by: Option<String>,
    ) -> Result<(), CaError> {
        let pki = self.current_linearizable().await?;
        let slot = pki
            .seal
            .slots
            .iter()
            .find(|s| s.id == id)
            .ok_or_else(|| CaError::new(404, format!("no key slot {}", id)))?;
        let recovery_left = pki
            .seal
            .slots
            .iter()
            .filter(|s| s.id != id && is_recovery(&s.kind))
            .count();
        if is_recovery(&slot.kind) && recovery_left == 0 && !force {
            return Err(CaError::conflict(format!(
                "{} is the last recovery-key slot: without one, only passphrases (or node \
                 keys) unseal the CA. Again with --force to do it",
                id
            )));
        }
        self.commit(PkiOp::RemoveSlot { id: id.to_string() })
            .await?;
        println!("ca: key slot {} removed", id);
        self.event(
            EventKind::CaSlotRemoved,
            Subject::Cluster,
            format!("key slot {} removed", id),
            by,
        );
        Ok(())
    }
}

/// Tries at a rekey that the CA changed under (an enrollment made or used
/// meanwhile): each is made again from the CA as it is.
const REKEY_TRIES: usize = 3;

impl CaService {
    /// `ca rekey` (CA-3d): a new DEK, everything under the old one sealed
    /// again under it, and one new recovery key in place of every slot.
    /// Every other node is sealed by it; this one keeps the new DEK.
    pub async fn rekey(
        &self,
        caller: &[u8],
        by: Option<String>,
    ) -> Result<crate::api::Rekeyed, CaError> {
        let mut tries = 0;
        loop {
            tries += 1;
            let pki = self.for_caller(caller).await?;
            let old = self.dek_at(&pki)?;
            let new = envelope::new_key().map_err(|e| CaError::new(500, e))?;
            let (op, slot_id, recovery_key) = self.rekey_of(&pki, &old, &new).await?;
            let removed_slots: Vec<String> = pki.seal.slots.iter().map(|s| s.id.clone()).collect();
            match self.commit_at(&pki, op).await {
                Ok(()) => {
                    // Applied here already (`commit` waits for it): the old
                    // DEK is dropped; hold the new one.
                    self.keep_dek(new, pki.seal.epoch + 1, "rekey");
                    println!(
                        "ca: rekeyed: a new DEK, one recovery-key slot ({}); {} slots removed",
                        slot_id,
                        removed_slots.len()
                    );
                    self.event(
                        EventKind::CaRekeyed,
                        Subject::Cluster,
                        format!(
                            "CA rekeyed: a new DEK and recovery key ({}); every other node is \
                             sealed, and slots {} are gone",
                            slot_id,
                            removed_slots.join(", ")
                        ),
                        by,
                    );
                    return Ok(crate::api::Rekeyed {
                        slot_id,
                        recovery_key: recovery_key.to_string(),
                        removed_slots,
                    });
                }
                Err(e)
                    if e.status == 409
                        && e.msg.contains("make it again")
                        && tries < REKEY_TRIES =>
                {
                    println!("ca: rekey: the CA changed meanwhile ({}): again", e.msg);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// The rekey of `pki` from `old` to `new`, its new slot's id and its
    /// recovery key.
    async fn rekey_of(
        &self,
        pki: &PkiState,
        old: &[u8; 32],
        new: &Key,
    ) -> Result<(PkiOp, String, zeroize::Zeroizing<String>), CaError> {
        let internal = |e: String| CaError::new(500, e);
        let anchor = &pki.anchor.fingerprint;
        let mut issuer_keys = std::collections::BTreeMap::new();
        for (id, r) in &pki.issuers {
            let aad = envelope::issuer_aad(anchor, *id);
            let key = envelope::open(old, &aad, &r.sealed_key)
                .map_err(|e| internal(format!("{}'s key: {}", id, e)))?;
            issuer_keys.insert(*id, envelope::seal(new, &aad, &key).map_err(internal)?);
        }
        let token_signer_key = match &pki.token_signer {
            None => None,
            Some(t) => {
                let aad = envelope::token_signer_aad(anchor, &t.serial);
                let key = envelope::open(old, &aad, &t.sealed_key)
                    .map_err(|e| internal(format!("the token signer's key: {}", e)))?;
                Some((
                    t.serial.clone(),
                    envelope::seal(new, &aad, &key).map_err(internal)?,
                ))
            }
        };
        let enrollment_macs = pki
            .enrollments
            .iter()
            .map(|(id, e)| (id.clone(), Mac(envelope::enrollment_mac(new, id, e))))
            .collect();
        let (m, t, p) = self.kdf;
        let kdf = envelope::kdf_params(m, t, p).map_err(internal)?;
        let recovery_key = envelope::new_recovery_key().map_err(internal)?;
        let bytes = envelope::parse_recovery_key(&recovery_key).map_err(internal)?;
        let slot_id = slot_id("recovery-key")?;
        let (dek, cluster_id, id) = (new.clone(), pki.seal.cluster_id, slot_id.clone());
        let slot = tokio::task::spawn_blocking(move || {
            envelope::recovery_slot(
                &dek,
                &cluster_id,
                &id,
                "recovery key (ca rekey)",
                &bytes,
                kdf,
            )
        })
        .await
        .map_err(|e| internal(e.to_string()))?
        .map_err(internal)?;
        let op = PkiOp::Rekey(Box::new(Rekey {
            seal: SealConfig {
                cluster_id: pki.seal.cluster_id,
                slots: vec![slot],
                epoch: pki.seal.epoch + 1,
            },
            issuer_keys,
            token_signer_key,
            enrollment_macs,
        }));
        Ok((op, slot_id, recovery_key))
    }
}
