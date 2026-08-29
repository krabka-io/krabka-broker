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

mod fetch;
mod refresher;
#[cfg(test)]
mod test_support;

pub(crate) use self::{fetch::fetch_jwks, refresher::JwksRefresher};
