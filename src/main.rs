#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::io::Result<()> {
    raidish::cli::main()
}

// The command line is native only; on wasm the crate is used as a library
// (the web dashboard), so the binary is an empty stub.
#[cfg(target_arch = "wasm32")]
fn main() {}
