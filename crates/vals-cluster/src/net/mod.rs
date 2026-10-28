pub use vals_client::net::*;

mod server;

pub(crate) use server::{
    accept_loop, serve_scan_keys, server_poll, ReadGate, ReadPause, ServerShared,
};
pub use server::{ReplicaServer, ServerHandle, ShardedServer};

#[cfg(test)]
pub(crate) use server::{catalog_to_wire, serve_request, serve_scan, serve_scan_counted};
#[cfg(test)]
mod tests;
