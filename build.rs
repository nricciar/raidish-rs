//! Bundles the web dashboard (`web/`, a Yew app built with trunk) into
//! release builds of the native binary.
//!
//! Writes `$OUT_DIR/ui_assets.rs`: a table of `(path, mime, bytes)`. It is
//! empty (the server then serves `web/dist` from disk, if present) for
//! debug builds, wasm builds of this crate (that's trunk building the
//! dashboard, which depends on us) and when `RAIDISH_SKIP_UI` is set.

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RAIDISH_SKIP_UI");
    println!("cargo:rerun-if-env-changed=PROFILE");
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let target = std::env::var("TARGET").unwrap_or_default();
    let profile = std::env::var("PROFILE").unwrap_or_default();

    let bundle = !target.starts_with("wasm32")
        && profile == "release"
        && std::env::var_os("RAIDISH_SKIP_UI").is_none();
    if !bundle {
        write_table(&out, &[]);
        return;
    }

    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    for dir in [
        "web/src",
        "web/index.html",
        "web/style.css",
        "web/Trunk.toml",
        "web/Cargo.toml",
        "src",
    ] {
        println!("cargo:rerun-if-changed={}", manifest.join(dir).display());
    }

    let dist = out.join("web-dist");
    let _ = std::fs::remove_dir_all(&dist);
    let status = Command::new("trunk")
        .current_dir(manifest.join("web"))
        .args(["build", "--release", "--locked", "--dist"])
        .arg(&dist)
        // A separate target dir: the outer build holds the lock on ours.
        .env("CARGO_TARGET_DIR", manifest.join("target/web"))
        // Don't leak this build's settings into the nested cargo.
        .env_remove("RUSTFLAGS")
        .env_remove("CARGO_ENCODED_RUSTFLAGS")
        .env_remove("CARGO_BUILD_TARGET")
        .env_remove("CARGO_TARGET_DIR_OVERRIDE")
        .status();
    match status {
        Ok(s) if s.success() => {}
        Ok(s) => fail(&format!("`trunk build` failed ({})", s)),
        Err(e) => fail(&format!("cannot run trunk ({})", e)),
    }

    let mut files = Vec::new();
    collect(&dist, &dist, &mut files);
    files.sort();
    write_table(&out, &files);
}

fn fail(why: &str) -> ! {
    panic!(
        "\n\nraidish: bundling the web dashboard: {}\n\
         Needs trunk and the wasm target:\n\
         \x20   cargo install trunk --locked\n\
         \x20   rustup target add wasm32-unknown-unknown\n\
         or build without the dashboard: RAIDISH_SKIP_UI=1 cargo build --release\n",
        why
    );
}

fn collect(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        // Trunk's lock file and the like.
        if path
            .file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with('.'))
        {
            continue;
        }
        if path.is_dir() {
            collect(root, &path, out);
        } else {
            let rel = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            out.push((rel, path));
        }
    }
}

fn mime(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript",
        "wasm" => "application/wasm",
        "css" => "text/css",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "ico" => "image/x-icon",
        "json" => "application/json",
        _ => "application/octet-stream",
    }
}

fn write_table(out: &Path, files: &[(String, PathBuf)]) {
    let mut src = String::from("pub static ASSETS: &[(&str, &str, &[u8])] = &[\n");
    for (rel, path) in files {
        src.push_str(&format!(
            "    ({:?}, {:?}, include_bytes!({:?})),\n",
            rel,
            mime(rel),
            path.display().to_string()
        ));
    }
    src.push_str("];\n");
    std::fs::write(out.join("ui_assets.rs"), src).unwrap();
}
