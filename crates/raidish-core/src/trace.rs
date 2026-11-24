//! Phase timing for the write path, printed to stderr when `RAIDISH_TRACE`
//! is set (e.g. `RAIDISH_TRACE=1`). Off, a span costs a branch.
//!
//! ```text
//! let mut s = Span::new("zil.flush");
//! ...; s.mark("log_write");
//! ...; s.mark("dev_flush");
//! // on drop: trace zil.flush 12.31ms log_write=3.02 dev_flush=9.29
//! ```

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use std::fmt::Write as _;
    use std::sync::LazyLock;
    use std::time::Instant;

    static ENABLED: LazyLock<bool> = LazyLock::new(|| {
        std::env::var_os("RAIDISH_TRACE").is_some_and(|v| !v.is_empty() && v != "0")
    });

    pub fn enabled() -> bool {
        *ENABLED
    }

    struct Inner {
        what: &'static str,
        start: Instant,
        last: Instant,
        out: String,
    }

    /// A timed operation; each `mark` records the time since the previous
    /// one. Printed when dropped.
    pub struct Span(Option<Inner>);

    impl Span {
        pub fn new(what: &'static str) -> Self {
            Span(enabled().then(|| {
                let now = Instant::now();
                Inner {
                    what,
                    start: now,
                    last: now,
                    out: String::new(),
                }
            }))
        }

        pub fn mark(&mut self, phase: &str) {
            if let Some(i) = &mut self.0 {
                let now = Instant::now();
                let ms = (now - i.last).as_secs_f64() * 1e3;
                let _ = write!(i.out, " {}={:.2}", phase, ms);
                i.last = now;
            }
        }

        /// Attach a value (a count, a reason) to the line.
        pub fn note(&mut self, key: &str, value: impl std::fmt::Display) {
            if let Some(i) = &mut self.0 {
                let _ = write!(i.out, " {}:{}", key, value);
            }
        }
    }

    impl Drop for Span {
        fn drop(&mut self) {
            if let Some(i) = &self.0 {
                eprintln!(
                    "trace {} {:.2}ms{}",
                    i.what,
                    i.start.elapsed().as_secs_f64() * 1e3,
                    i.out
                );
            }
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    pub fn enabled() -> bool {
        false
    }
    pub struct Span;
    impl Span {
        pub fn new(_: &'static str) -> Self {
            Span
        }
        pub fn mark(&mut self, _: &str) {}
        pub fn note(&mut self, _: &str, _: impl std::fmt::Display) {}
    }
}

#[allow(unused_imports)]
pub(crate) use imp::{Span, enabled};
