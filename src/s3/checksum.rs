//! The checksums S3 clients send with uploads (`x-amz-checksum-*`, in a
//! header or an `aws-chunked` trailer): CRC32, CRC32C, CRC64NVME, SHA-1,
//! SHA-256, each base64 of its big-endian value.

use base64::Engine;
use sha1::Sha1;
use sha2::{Digest, Sha256};

/// A reflected CRC's table.
const fn table32(poly: u32) -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ poly } else { c >> 1 };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

const fn table64(poly: u64) -> [u64; 256] {
    let mut t = [0u64; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u64;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ poly } else { c >> 1 };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

static CRC32: [u32; 256] = table32(0xEDB8_8320);
static CRC32C: [u32; 256] = table32(0x82F6_3B78);
static CRC64NVME: [u64; 256] = table64(0x9A6C_9329_AC4B_C9B5);

/// One running checksum.
pub enum Hasher {
    Crc32(u32),
    Crc32c(u32),
    Crc64Nvme(u64),
    Sha1(Sha1),
    Sha256(Sha256),
}

impl Hasher {
    /// For header (or trailer) `name` (`x-amz-checksum-<algorithm>`).
    pub fn for_header(name: &str) -> Option<Hasher> {
        Some(match name.strip_prefix("x-amz-checksum-")? {
            "crc32" => Hasher::Crc32(!0),
            "crc32c" => Hasher::Crc32c(!0),
            "crc64nvme" => Hasher::Crc64Nvme(!0),
            "sha1" => Hasher::Sha1(Sha1::new()),
            "sha256" => Hasher::Sha256(Sha256::new()),
            _ => return None,
        })
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Hasher::Crc32(c) => {
                for &b in data {
                    *c = CRC32[((*c ^ b as u32) & 0xff) as usize] ^ (*c >> 8);
                }
            }
            Hasher::Crc32c(c) => {
                for &b in data {
                    *c = CRC32C[((*c ^ b as u32) & 0xff) as usize] ^ (*c >> 8);
                }
            }
            Hasher::Crc64Nvme(c) => {
                for &b in data {
                    *c = CRC64NVME[((*c ^ b as u64) & 0xff) as usize] ^ (*c >> 8);
                }
            }
            Hasher::Sha1(h) => h.update(data),
            Hasher::Sha256(h) => h.update(data),
        }
    }

    /// Its value as the header carries it.
    pub fn finish(self) -> String {
        let raw: Vec<u8> = match self {
            Hasher::Crc32(c) | Hasher::Crc32c(c) => (!c).to_be_bytes().to_vec(),
            Hasher::Crc64Nvme(c) => (!c).to_be_bytes().to_vec(),
            Hasher::Sha1(h) => h.finalize().to_vec(),
            Hasher::Sha256(h) => h.finalize().to_vec(),
        };
        base64::engine::general_purpose::STANDARD.encode(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(name: &str, data: &[u8]) -> Vec<u8> {
        let mut h = Hasher::for_header(name).unwrap();
        h.update(data);
        base64::engine::general_purpose::STANDARD
            .decode(h.finish())
            .unwrap()
    }

    #[test]
    fn check_values() {
        // The standard check input.
        let d = b"123456789";
        assert_eq!(of("x-amz-checksum-crc32", d), 0xCBF4_3926u32.to_be_bytes());
        assert_eq!(of("x-amz-checksum-crc32c", d), 0xE306_9283u32.to_be_bytes());
        assert_eq!(
            of("x-amz-checksum-crc64nvme", d),
            0xAE8B_1486_0A79_9888u64.to_be_bytes()
        );
        assert_eq!(of("x-amz-checksum-sha256", b"").len(), 32);
        assert!(Hasher::for_header("x-amz-checksum-md4").is_none());
    }
}
