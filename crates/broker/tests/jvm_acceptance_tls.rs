//! TLS listeners: a plain SSL handshake, the `SASL_SSL` stack, and inter-broker
//! replication over authenticated and TLS-encrypted connections.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.
//!
//! The binary root carries only the module tree. [`ssl_handshake`] is the bare
//! TLS case, [`sasl_ssl_stack`] adds SASL over it on a single broker, and
//! [`inter_broker_plaintext`] and [`inter_broker_sasl_ssl`] are the two-broker
//! cases that authenticate peer to peer.
//!
//! Cargo compiles this file as its own test binary, so a `mod` declaration in
//! it resolves against `tests/` rather than against a directory named for the
//! file. Each child therefore carries an explicit `#[path]` onto the sibling
//! `jvm_acceptance_tls/` directory. `jvm_acceptance` and `support` are
//! `tests/<name>/mod.rs` helpers, which the crate-root rule already resolves.

#[path = "jvm_acceptance_tls/inter_broker_plaintext.rs"]
mod inter_broker_plaintext;
#[path = "jvm_acceptance_tls/inter_broker_sasl_ssl.rs"]
mod inter_broker_sasl_ssl;
mod jvm_acceptance;
#[path = "jvm_acceptance_tls/sasl_ssl_stack.rs"]
mod sasl_ssl_stack;
#[path = "jvm_acceptance_tls/ssl_handshake.rs"]
mod ssl_handshake;
mod support;
