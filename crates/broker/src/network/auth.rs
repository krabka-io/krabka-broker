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

// SASL/GSSAPI needs the `sspi` Kerberos stack, which does not build for
// wasm32-wasip1. That target builds the stand-in, whose configuration type has
// no values, so no broker there can enable the mechanism.
#[cfg_attr(target_family = "wasm", path = "auth/gssapi/wasm.rs")]
mod gssapi;
mod handshake;
mod java_regex;
// The Kerberos `auth_to_local` rules belong to the GSSAPI stack, so the
// wasm stand-in has no use for them.
#[cfg(not(target_family = "wasm"))]
mod kerberos_name;
mod oauthbearer;
mod plain;
mod response;
mod scram;
mod ssl_principal_mapper;
mod state;
mod subject_dn;
#[cfg(test)]
mod test_support;

#[cfg(not(target_family = "wasm"))]
pub use self::kerberos_name::{KerberosNameError, KerberosRule};
// Only test code -- dispatch::session's tests and oauthbearer's -- builds an
// AuthenticatedSnapshot through this path, so the re-export is test-gated:
// in a normal build it is dead and -D warnings rejects it.
#[cfg(test)]
pub use self::state::AuthenticatedSnapshot;
pub use self::{
    gssapi::{GssapiConfig, handle_authenticate_gssapi},
    handshake::{ReauthClock, handle_handshake},
    oauthbearer::{
        handle_authenticate_oauthbearer, handle_authenticate_oauthbearer_with_jwks_cache,
    },
    plain::handle_authenticate_plain,
    response::generic_failure_message,
    scram::handle_authenticate_scram,
    ssl_principal_mapper::{SslPrincipalMapper, SslPrincipalRuleError},
    state::ConnectionAuth,
    subject_dn::subject_dn_rfc2253,
};
