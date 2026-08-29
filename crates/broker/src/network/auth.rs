//! Per-connection SASL authentication state machine.
//!
//! Drives `SaslHandshake` (17) and `SaslAuthenticate` (36).
//!
//! The state machine is deliberately separate from the byte-level I/O loop
//! in `dispatch.rs`. Handlers mutate `ConnectionAuth` from decoded request
//! bodies. The dispatcher only reads the state, to gate non-allowlisted
//! requests before authentication completes.
//!
//! The state itself lives in the `state` child module. Each SASL mechanism
//! owns one child module of its own, and `handshake` negotiates which of them
//! a connection runs.

mod gssapi;
mod handshake;
mod oauthbearer;
mod plain;
mod response;
mod scram;
mod state;
#[cfg(test)]
mod test_support;

// Only test code -- dispatch::session's tests and oauthbearer's -- builds an
// AuthenticatedSnapshot through this path, so the re-export is test-gated:
// in a normal build it is dead and -D warnings rejects it.
#[cfg(test)]
pub use self::state::AuthenticatedSnapshot;
pub use self::{
    gssapi::handle_authenticate_gssapi,
    handshake::handle_handshake,
    oauthbearer::handle_authenticate_oauthbearer,
    plain::handle_authenticate_plain,
    scram::handle_authenticate_scram,
    state::{ConnectionAuth, SaslExchange, is_pre_auth_allowed},
};
