use crate::disk::{BLOCK_SIZE, DiskError};

/// BlockDevice WebSocket operation codes
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsOp {
    Read = 0,
    Write = 1,
    Flush = 2,
}

impl TryFrom<u8> for WsOp {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(WsOp::Read),
            1 => Ok(WsOp::Write),
            2 => Ok(WsOp::Flush),
            _ => Err(()),
        }
    }
}

/// WebSocket response status codes
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsStatus {
    Success = 0,
    ErrorNotFound = 1,
    InvalidRequest = 2,
}

impl From<DiskError> for WsStatus {
    fn from(_error: DiskError) -> Self {
        WsStatus::ErrorNotFound
    }
}

pub struct WsRequest<'a> {
    data: &'a [u8],
}

impl<'a> WsRequest<'a> {
    pub fn parse(data: &'a [u8]) -> Result<Self, WsStatus> {
        if data.len() < 11 {
            return Err(WsStatus::InvalidRequest);
        }
        Ok(Self { data })
    }

    #[inline]
    pub fn op(&self) -> Result<WsOp, WsStatus> {
        WsOp::try_from(self.data[0]).map_err(|_| WsStatus::InvalidRequest)
    }

    #[inline]
    pub fn disk_index(&self) -> u16 {
        u16::from_le_bytes([self.data[1], self.data[2]])
    }

    #[inline]
    pub fn block_id(&self) -> u64 {
        u64::from_le_bytes(
            self.data[3..11]
                .try_into()
                .expect("slice length checked in parse"),
        )
    }

    #[inline]
    pub fn write_data(&self) -> Option<&[u8]> {
        if self.data.len() >= 11 + BLOCK_SIZE {
            Some(&self.data[11..11 + BLOCK_SIZE])
        } else {
            None
        }
    }
}

pub struct WsResponseBuilder {
    buffer: Vec<u8>,
}

impl WsResponseBuilder {
    #[inline]
    pub fn success() -> Vec<u8> {
        vec![WsStatus::Success as u8]
    }

    #[inline]
    pub fn error(status: WsStatus) -> Vec<u8> {
        vec![status as u8]
    }

    #[inline]
    pub fn with_data(data: &[u8]) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(1 + data.len());
        buffer.push(WsStatus::Success as u8);
        buffer.extend_from_slice(data);
        buffer
    }
}
