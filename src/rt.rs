//! Runtime shims so the storage core runs on tokio (native) and in the
//! browser (wasm, single threaded, no blocking pool).

/// Run CPU-heavy `f` off the async workers (native), or inline (wasm).
#[cfg(not(target_arch = "wasm32"))]
pub async fn spawn_blocking<T, F>(f: F) -> Result<T, tokio::task::JoinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await
}

/// Never produced; keeps callers' error handling the same as on native.
#[cfg(target_arch = "wasm32")]
#[derive(Debug)]
pub struct JoinError;

#[cfg(target_arch = "wasm32")]
impl std::fmt::Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("task failed")
    }
}

#[cfg(target_arch = "wasm32")]
pub async fn spawn_blocking<T, F>(f: F) -> Result<T, JoinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    Ok(f())
}
