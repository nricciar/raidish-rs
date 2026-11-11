//! Block I/O over websocket.
//!
//! Every request carries an id, so a connection can have many requests in
//! flight and the server may answer them in any order. Reads and writes cover
//! a contiguous range of `count` blocks (at most `WS_MAX_BLOCKS`).
//!
//! ```text
//! request:  [op:1][disk:2][block:8][count:4][id:8][data: count*4096 for writes]
//! response: [status:1][id:8][data: count*4096 for reads]
//! ```
//! All integers are little endian.

use crate::disk::{BLOCK_SIZE, DiskError};

pub const WS_REQUEST_HEADER_LEN: usize = 23;
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
}

impl TryFrom<u8> for WsStatus {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(WsStatus::Success),
            1 => Ok(WsStatus::ErrorNotFound),
            2 => Ok(WsStatus::InvalidRequest),
            _ => Err(()),
        }
    }
}

impl From<DiskError> for WsStatus {
    fn from(_error: DiskError) -> Self {
        WsStatus::ErrorNotFound
    }
}

pub fn encode_request(
    op: WsOp,
    disk: u16,
    block: u64,
    count: u32,
    id: u64,
    data: Option<&[u8]>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(WS_REQUEST_HEADER_LEN + data.map_or(0, |d| d.len()));
    out.push(op as u8);
    out.extend_from_slice(&disk.to_le_bytes());
    out.extend_from_slice(&block.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&id.to_le_bytes());
    if let Some(data) = data {
        out.extend_from_slice(data);
    }
    out
}

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

pub struct WsRequest<'a> {
    data: &'a [u8],
}

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

    /// Write payload, if it is exactly `count` blocks.
    pub fn write_data(&self) -> Option<&[u8]> {
        let payload = &self.data[WS_REQUEST_HEADER_LEN..];
        (payload.len() == self.count() as usize * BLOCK_SIZE).then_some(payload)
    }
}
