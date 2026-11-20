//! The NFSv4.1 operations, by area: each file adds its ops to `Server`
//! (dispatched from `super::Server::op`).

mod layout;
mod lock;
mod open;
mod read;
mod session;
mod write;
