//! Sealing the cluster CA's secrets (see `docs/ca-service-plan.md` §3).
//!
//! * One random 256-bit **cluster DEK** seals every CA key that lives in
//!   Raft (AES-256-GCM, with an AAD naming what the blob is, so blobs can't
//!   be swapped for one another).
//! * **Key slots** each wrap the DEK under a KEK: Argon2id of a recovery key
//!   (or, later, a passphrase), or a node's own raw key. Adding a way to
//!   unseal never re-encrypts the CA.
//! * Keys derived from the DEK with HKDF do other jobs, e.g. MACing
//!   enrollments, so a node without the DEK can't forge one through Raft.
//!
//! Every key in memory is `Zeroizing`.

use raidish_types::pki::{
    CertFingerprint, Enrollment, IssuerId, KdfParams, KeySlot, SealedBlob, SlotKind, TokenId,
};
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use zeroize::Zeroizing;

/// A 256-bit key, wiped when dropped.
pub type Key = Zeroizing<[u8; 32]>;

/// Argon2id cost for slots: 64 MiB, 3 passes (a fraction of a second per
/// unseal, and the same for every guess an attacker makes).
pub const KDF_M_KIB: u32 = 64 * 1024;
pub const KDF_T: u32 = 3;
pub const KDF_P: u32 = 1;

pub fn random<const N: usize>() -> Result<[u8; N], String> {
    let mut b = [0u8; N];
    SystemRandom::new()
        .fill(&mut b)
        .map_err(|_| "no randomness".to_string())?;
    Ok(b)
}

/// A fresh random key (the DEK, a node key).
pub fn new_key() -> Result<Key, String> {
    Ok(Zeroizing::new(random()?))
}

/// Encrypt `plaintext` under `key`, bound to `aad`.
pub fn seal(key: &[u8; 32], aad: &[u8], plaintext: &[u8]) -> Result<SealedBlob, String> {
    let k = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).map_err(|_| "bad key")?);
    let nonce: [u8; 12] = random()?;
    let mut ct = plaintext.to_vec();
    k.seal_in_place_append_tag(Nonce::assume_unique_for_key(nonce), Aad::from(aad), &mut ct)
        .map_err(|_| "sealing failed".to_string())?;
    Ok(SealedBlob { v: 1, nonce, ct })
}

/// Decrypt `blob`: fails if the key, the AAD or the blob is wrong.
pub fn open(key: &[u8; 32], aad: &[u8], blob: &SealedBlob) -> Result<Zeroizing<Vec<u8>>, String> {
    if blob.v != 1 {
        return Err(format!("sealed blob version {}", blob.v));
    }
    let k = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, key).map_err(|_| "bad key")?);
    let mut buf = Zeroizing::new(blob.ct.clone());
    let n = k
        .open_in_place(
            Nonce::assume_unique_for_key(blob.nonce),
            Aad::from(aad),
            &mut buf,
        )
        .map_err(|_| "wrong key, or the sealed data was changed".to_string())?
        .len();
    buf.truncate(n);
    Ok(buf)
}

/// Fresh Argon2id parameters (a new salt) at `m_kib`/`t`/`p`.
pub fn kdf_params(m_kib: u32, t: u32, p: u32) -> Result<KdfParams, String> {
    Ok(KdfParams {
        salt: random()?,
        m_kib,
        t,
        p,
    })
}

/// The KEK for `secret` under `p` (Argon2id).
pub fn derive_kek(secret: &[u8], p: &KdfParams) -> Result<Key, String> {
    let params = argon2::Params::new(p.m_kib, p.t, p.p, Some(32))
        .map_err(|e| format!("KDF parameters: {}", e))?;
    let argon = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    let mut out = Zeroizing::new([0u8; 32]);
    argon
        .hash_password_into(secret, &p.salt, &mut *out)
        .map_err(|e| format!("KDF: {}", e))?;
    Ok(out)
}

struct Len32;
impl ring::hkdf::KeyType for Len32 {
    fn len(&self) -> usize {
        32
    }
}

/// A key for job `info`, derived from `key` (HKDF-SHA256).
pub fn subkey(key: &[u8; 32], info: &str) -> Key {
    let prk = ring::hkdf::Salt::new(ring::hkdf::HKDF_SHA256, b"raidish").extract(key);
    let mut out = Zeroizing::new([0u8; 32]);
    let infos = [info.as_bytes()];
    prk.expand(&infos, Len32)
        .and_then(|okm| okm.fill(&mut *out))
        .expect("HKDF of 32 bytes");
    out
}

pub fn sha256(data: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, data)
        .as_ref()
        .try_into()
        .unwrap()
}

/// Equal, in time that doesn't depend on where they differ.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

// ---- the cluster DEK's uses ----------------------------------------------------

/// What an issuer's sealed key is bound to.
pub fn issuer_aad(anchor: &CertFingerprint, id: IssuerId) -> Vec<u8> {
    let mut aad = b"raidish-issuer".to_vec();
    aad.extend_from_slice(&anchor.0);
    aad.extend_from_slice(&id.0.to_be_bytes());
    aad
}

fn slot_aad(cluster_id: &[u8; 16], slot_id: &str) -> Vec<u8> {
    let mut aad = b"raidish-slot".to_vec();
    aad.extend_from_slice(cluster_id);
    aad.extend_from_slice(slot_id.as_bytes());
    aad
}

/// The enrollment MAC (see `Enrollment::mac`).
pub fn enrollment_mac(dek: &[u8; 32], id: &TokenId, e: &Enrollment) -> [u8; 32] {
    let key = subkey(dek, "raidish-enrollment-mac");
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &*key);
    ring::hmac::sign(&k, &e.mac_input(id))
        .as_ref()
        .try_into()
        .unwrap()
}

/// Was `e` made by a signer holding `dek`?
pub fn enrollment_mac_ok(dek: &[u8; 32], id: &TokenId, e: &Enrollment) -> bool {
    let key = subkey(dek, "raidish-enrollment-mac");
    let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &*key);
    ring::hmac::verify(&k, &e.mac_input(id), &e.mac).is_ok()
}

// ---- recovery keys ---------------------------------------------------------------

const RECOVERY_PREFIX: &str = "RDK1";

/// A new recovery key: 256 random bits, as `RDK1-` and eight groups of
/// eight hex digits.
pub fn new_recovery_key() -> Result<Zeroizing<String>, String> {
    let b: Zeroizing<[u8; 32]> = Zeroizing::new(random()?);
    let mut s = Zeroizing::new(String::from(RECOVERY_PREFIX));
    for chunk in b.chunks(4) {
        s.push('-');
        for x in chunk {
            s.push_str(&format!("{:02x}", x));
        }
    }
    Ok(s)
}

/// The 32 bytes of a recovery key as typed (case, dashes and spaces don't
/// matter).
pub fn parse_recovery_key(s: &str) -> Result<Zeroizing<Vec<u8>>, String> {
    let t = s.trim();
    let rest = t
        .strip_prefix(RECOVERY_PREFIX)
        .or_else(|| t.strip_prefix(&RECOVERY_PREFIX.to_ascii_lowercase()))
        .ok_or("not a recovery key (they start with RDK1-)")?;
    let hex: Zeroizing<String> = Zeroizing::new(
        rest.chars()
            .filter(|c| !matches!(c, '-' | ' ' | '\n' | '\t'))
            .map(|c| c.to_ascii_lowercase())
            .collect(),
    );
    if hex.len() != 64 {
        return Err("a recovery key has 64 hex digits".into());
    }
    let mut out = Zeroizing::new(Vec::with_capacity(32));
    for i in (0..64).step_by(2) {
        out.push(u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| "bad recovery key")?);
    }
    Ok(out)
}

// ---- key slots ---------------------------------------------------------------------

/// A slot wrapping `dek` under recovery key `recovery` (Argon2id at `kdf`).
pub fn recovery_slot(
    dek: &[u8; 32],
    cluster_id: &[u8; 16],
    id: &str,
    label: &str,
    recovery: &[u8],
    kdf: KdfParams,
) -> Result<KeySlot, String> {
    let kek = derive_kek(recovery, &kdf)?;
    Ok(KeySlot {
        id: id.into(),
        label: label.into(),
        wrapped_dek: seal(&kek, &slot_aad(cluster_id, id), dek)?,
        kind: SlotKind::RecoveryKey(kdf),
    })
}

/// The DEK, from whichever recovery or passphrase slot `secret` opens.
pub fn unwrap_dek(slots: &[KeySlot], cluster_id: &[u8; 16], secret: &[u8]) -> Result<Key, String> {
    for slot in slots {
        let kdf = match &slot.kind {
            SlotKind::RecoveryKey(p) | SlotKind::Passphrase(p) => p,
            SlotKind::NodeKey { .. } => continue,
        };
        let kek = derive_kek(secret, kdf)?;
        if let Ok(dek) = open(&kek, &slot_aad(cluster_id, &slot.id), &slot.wrapped_dek) {
            let dek: [u8; 32] = dek[..]
                .try_into()
                .map_err(|_| "a slot's DEK is not 32 bytes")?;
            return Ok(Zeroizing::new(dek));
        }
    }
    Err("that key opens none of the CA's key slots".into())
}

// ---- the offline root ----------------------------------------------------------------

/// `root.enc`: the root CA's key, for the admin to keep off the cluster
/// (needed only to make a new issuer, every couple of years).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct RootEnvelope {
    pub v: u8,
    pub cert_pem: String,
    pub kdf: KdfParams,
    pub key: SealedBlob,
}

fn root_aad(cert_pem: &str) -> Vec<u8> {
    let mut aad = b"raidish-root".to_vec();
    aad.extend_from_slice(cert_pem.as_bytes());
    aad
}

impl RootEnvelope {
    /// Seal the root key (`key_pem`) under recovery key `recovery`.
    pub fn seal(
        cert_pem: &str,
        key_pem: &str,
        recovery: &[u8],
        kdf: KdfParams,
    ) -> Result<RootEnvelope, String> {
        let kek = derive_kek(recovery, &kdf)?;
        Ok(RootEnvelope {
            v: 1,
            cert_pem: cert_pem.into(),
            key: seal(&kek, &root_aad(cert_pem), key_pem.as_bytes())?,
            kdf,
        })
    }

    /// The root key's PEM.
    pub fn open(&self, recovery: &[u8]) -> Result<Zeroizing<String>, String> {
        let kek = derive_kek(recovery, &self.kdf)?;
        let pem = open(&kek, &root_aad(&self.cert_pem), &self.key)
            .map_err(|_| "wrong recovery key for this root".to_string())?;
        String::from_utf8(pem.to_vec())
            .map(Zeroizing::new)
            .map_err(|_| "the root key is not PEM".into())
    }
}

#[cfg(test)]
#[path = "tests/envelope_tests.rs"]
mod tests;
