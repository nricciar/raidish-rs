//! Block I/O over websocket.
//!
//! Every request carries an id, so a connection can have many requests in
//! flight and the server may answer them in any order. Reads and writes cover
//! a contiguous range of `count` blocks (at most `WS_MAX_BLOCKS`).
//!
//! ```text
//! request:  [op:1][disk:2][block:8][count:4][id:8][epoch:8][stamp:1][budget_ms:4][data: count*4096 for writes]
//! response: [status:1][id:8][data: count*4096 for reads]
//! ```
//! All integers are little endian.
//!
//! `epoch` is the sender's pool lease epoch (0 without one) with `stamp` 0,
//! or a metaslab's lend (1) or return (2) epoch (see `crate::fence::Stamp`).
//! The server remembers the newest epoch it has seen (from requests or from
//! the cluster) and refuses writes with an older one: `StaleEpoch`; and
//! writes stamped with the wrong kind for where they land (in a lent
//! metaslab, not its lend; in the pool domain, not a pool epoch): `Fenced`.
//! A request carries one stamp, so a client splits a range whose blocks
//! need different ones.
//!
//! `budget_ms` is how long the client waits for the reply (0: no limit). A
//! write the server can't start within half of it is refused, `Expired`,
//! rather than landing after the client gave up on it (audit D5b); so is
//! one whose connection has closed. A server that hasn't finished starting
//! (see `crate::fence::Ready`) refuses writes with `NotReady`.
//!
//! A client names the version it speaks when it connects (`/ws?v=N`,
//! [`BLOCK_PROTOCOL`]); a node refuses any other before the upgrade, so
//! nodes of different versions never misread each other's frames.

#[cfg(not(target_arch = "wasm32"))]
use crate::disk::BLOCK_SIZE;
use crate::disk::DiskError;
use crate::types::Epoch;

/// This framing's version. Bump it with any change to the messages.
pub const BLOCK_PROTOCOL: u32 = 4;

/// A disk's websocket `url`, asking for [`BLOCK_PROTOCOL`].
pub fn versioned(url: &str) -> String {
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{}{}v={}", url, sep, BLOCK_PROTOCOL)
}

pub const WS_REQUEST_HEADER_LEN: usize = 36;
pub const WS_RESPONSE_HEADER_LEN: usize = 9;
/// Largest range in one message (1 MiB of data).
pub const WS_MAX_BLOCKS: u32 = 256;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsOp {
    Read = 0,
    Write = 1,
    Flush = 2,
    /// Capacity in blocks, returned as a little-endian u64.
    Info = 3,
}

impl TryFrom<u8> for WsOp {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(WsOp::Read),
            1 => Ok(WsOp::Write),
            2 => Ok(WsOp::Flush),
            3 => Ok(WsOp::Info),
            _ => Err(()),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsStatus {
    Success = 0,
    ErrorNotFound = 1,
    InvalidRequest = 2,
    /// A write or flush from a writer whose lease epoch is out of date.
    StaleEpoch = 3,
    /// The connection's role may not write (see `crate::auth`).
    Forbidden = 4,
    /// A write into a metaslab lent to a writer, not stamped with that lend.
    Fenced = 5,
    /// A write before the server's fence is ready (`crate::fence::Ready`).
    NotReady = 6,
    /// A write the client had given up on before it could start.
    Expired = 7,
}

impl TryFrom<u8> for WsStatus {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(WsStatus::Success),
            1 => Ok(WsStatus::ErrorNotFound),
            2 => Ok(WsStatus::InvalidRequest),
            3 => Ok(WsStatus::StaleEpoch),
            4 => Ok(WsStatus::Forbidden),
            5 => Ok(WsStatus::Fenced),
            6 => Ok(WsStatus::NotReady),
            7 => Ok(WsStatus::Expired),
            _ => Err(()),
        }
    }
}

impl From<DiskError> for WsStatus {
    fn from(error: DiskError) -> Self {
        match error {
            DiskError::StaleEpoch => WsStatus::StaleEpoch,
            DiskError::Fenced => WsStatus::Fenced,
            DiskError::NotReady => WsStatus::NotReady,
            _ => WsStatus::ErrorNotFound,
        }
    }
}

#[allow(clippy::too_many_arguments)] // the header's fields
pub fn encode_request(
    op: WsOp,
    disk: u16,
    block: u64,
    count: u32,
    id: u64,
    stamp: crate::fence::Stamp,
    budget: std::time::Duration,
    data: Option<&[u8]>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(WS_REQUEST_HEADER_LEN + data.map_or(0, |d| d.len()));
    out.push(op as u8);
    out.extend_from_slice(&disk.to_le_bytes());
    out.extend_from_slice(&block.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&stamp.epoch().to_le_bytes());
    out.push(stamp.kind());
    let budget_ms = u32::try_from(budget.as_millis()).unwrap_or(u32::MAX);
    out.extend_from_slice(&budget_ms.to_le_bytes());
    if let Some(data) = data {
        out.extend_from_slice(data);
    }
    out
}

#[cfg(not(target_arch = "wasm32"))]
pub fn encode_response(status: WsStatus, id: u64, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(WS_RESPONSE_HEADER_LEN + data.len());
    out.push(status as u8);
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(data);
    out
}

/// Returns `(status, id)`; the payload follows `WS_RESPONSE_HEADER_LEN`.
pub fn decode_response(msg: &[u8]) -> Option<(u8, u64)> {
    if msg.len() < WS_RESPONSE_HEADER_LEN {
        return None;
    }
    Some((msg[0], u64::from_le_bytes(msg[1..9].try_into().unwrap())))
}

#[cfg(not(target_arch = "wasm32"))]
pub struct WsRequest<'a> {
    data: &'a [u8],
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a> WsRequest<'a> {
    pub fn parse(data: &'a [u8]) -> Option<Self> {
        (data.len() >= WS_REQUEST_HEADER_LEN).then_some(Self { data })
    }

    pub fn op(&self) -> Option<WsOp> {
        WsOp::try_from(self.data[0]).ok()
    }

    pub fn disk_index(&self) -> u16 {
        u16::from_le_bytes([self.data[1], self.data[2]])
    }

    pub fn block_id(&self) -> u64 {
        u64::from_le_bytes(self.data[3..11].try_into().unwrap())
    }

    pub fn count(&self) -> u32 {
        u32::from_le_bytes(self.data[11..15].try_into().unwrap())
    }

    pub fn id(&self) -> u64 {
        u64::from_le_bytes(self.data[15..23].try_into().unwrap())
    }

    pub fn epoch(&self) -> Epoch {
        Epoch::from_le_bytes(self.data[23..31].try_into().unwrap())
    }

    /// The stamp, or None for a kind this version doesn't know.
    pub fn stamp(&self) -> Option<crate::fence::Stamp> {
        crate::fence::Stamp::from_wire(self.data[31], self.epoch())
    }

    /// How long the client waits for the reply (None: no limit).
    pub fn budget(&self) -> Option<std::time::Duration> {
        let ms = u32::from_le_bytes(self.data[32..36].try_into().unwrap());
        (ms > 0).then(|| std::time::Duration::from_millis(ms as u64))
    }

    /// Write payload, if it is exactly `count` blocks.
    pub fn write_data(&self) -> Option<&[u8]> {
        let payload = &self.data[WS_REQUEST_HEADER_LEN..];
        (payload.len() == self.count() as usize * BLOCK_SIZE).then_some(payload)
    }
}
