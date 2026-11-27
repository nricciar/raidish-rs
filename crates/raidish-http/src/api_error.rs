//! What the node's HTTP API (`/api/v1/...`) answers a request it can't
//! carry out with: an HTTP status and a one-line reason, as plain text.
//! The status comes from the error's [`ErrorKind`](crate::types::ErrorKind),
//! so a route only has to `?` whatever failed.

use crate::fs::FileSystemError;
use crate::writer::CommitError;
use axum::http::StatusCode as Status;
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: Status,
    pub msg: String,
}

impl ApiError {
    pub fn new(status: Status, msg: impl Into<String>) -> Self {
        ApiError {
            status,
            msg: msg.into(),
        }
    }

    /// The caller asked for something malformed.
    pub fn bad(msg: impl Into<String>) -> Self {
        ApiError::new(Status::BAD_REQUEST, msg)
    }

    fn of(kind: crate::types::ErrorKind, msg: String) -> Self {
        let status = Status::from_u16(kind.http_status()).unwrap_or(Status::INTERNAL_SERVER_ERROR);
        ApiError::new(status, msg)
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.status, self.msg)
    }
}

impl From<FileSystemError> for ApiError {
    fn from(e: FileSystemError) -> Self {
        ApiError::of(e.kind(), e.to_string())
    }
}

impl From<CommitError> for ApiError {
    fn from(e: CommitError) -> Self {
        let msg = match &e {
            CommitError::Retry(m) => format!("no committer reachable: {}", m),
            CommitError::Refused { msg, .. } => msg.clone(),
            e @ CommitError::Restamp(_) => e.to_string(),
        };
        ApiError::of(e.kind(), msg)
    }
}

/// A bad definition is the caller's; the rest are this host's.
impl From<crate::vm::VmError> for ApiError {
    fn from(e: crate::vm::VmError) -> Self {
        ApiError::of(e.kind(), e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, self.msg).into_response()
    }
}

/// A refusal from an extractor (`raidish_auth::http::Refusal`).
impl From<(Status, String)> for ApiError {
    fn from((status, msg): (Status, String)) -> Self {
        ApiError::new(status, msg)
    }
}
