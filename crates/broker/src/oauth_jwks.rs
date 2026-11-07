//! Background JWKS refresher for SASL/OAUTHBEARER signed-token validation.
//!
//! `crates/security` is I/O-free. It parses a JWKS *string* and verifies
//! tokens against an in-memory key set behind a [`JwksHandle`]. This module is
//! the one place that reaches the network. It periodically GETs the identity
//! provider's JWKS endpoint, parses it, and atomically swaps the new key set
//! into the shared handle. The [`SignedJwsValidator`] therefore picks up
//! rotated keys with no restart and no lock.
//!
//! [`JwksHandle`]: krabka_security::JwksHandle
//! [`SignedJwsValidator`]: krabka_security::SignedJwsValidator

//!
//! The fetch and the refresher need an HTTP client stack, which wasm32-wasip1
//! does not have. The shared publication state in `runtime` builds everywhere,
//! because `[oauthbearer]` loading creates it on every platform.

#[cfg(not(target_family = "wasm"))]
mod fetch;
#[cfg(not(target_family = "wasm"))]
mod refresher;
mod runtime;
#[cfg(all(test, not(target_family = "wasm")))]
mod test_support;

pub use self::runtime::{JwksRuntime, JwksSnapshotState};
#[cfg(not(target_family = "wasm"))]
pub use self::{fetch::fetch_jwks, refresher::JwksRefresher};
