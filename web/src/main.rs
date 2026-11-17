//! The raidish web dashboard. It runs the raidish storage core in the
//! browser: the pool is opened read-only over the `listen` nodes' websocket
//! block protocol, and everything shown is computed here.

// Only the wasm build runs; the native build just type-checks.
#![cfg_attr(not(target_arch = "wasm32"), allow(dead_code, unused_imports))]

mod activity;
mod api;
mod app;
mod blockmap;
mod cluster;
mod dialog;
mod files;
mod fmt;
mod login;
mod panels;
mod session;
mod upload;
mod vm_new;
mod vm_page;
mod vms;
mod volumes;

fn main() {
    #[cfg(target_arch = "wasm32")]
    {
        console_error_panic_hook::set_once();
        yew::Renderer::<login::Root>::new().render();
    }
}
