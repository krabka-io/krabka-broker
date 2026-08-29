//! Broker-side auth tests. No Docker.
//!
//! The binary root carries only what the whole suite shares: the dev TLS
//! keypair, whose `include_str!` paths resolve against this directory, and
//! the module tree below. Each child covers one authentication surface --
//! TLS termination, SASL/PLAIN, SASL/SCRAM, SASL/OAUTHBEARER token
//! validation and KIP-368 sessions, `AlterUserScramCredentials`, the
//! pre-auth SASL gate, inter-broker authentication, GSSAPI, and a
//! two-broker SASL cluster -- and `harness` holds the wire plumbing and the
//! credential fixtures they share.

mod alter_scram;
mod alter_scram_validation;
mod gssapi;
mod handshake;
mod harness;
mod inter_broker;
mod oauthbearer;
mod oauthbearer_sessions;
mod oauthbearer_tokens;
mod plain;
mod scram;
mod support;
mod tls;
mod two_broker_sasl;

const DEV_CERT: &str = include_str!("fixtures/security/dev_cert.pem");
const DEV_KEY: &str = include_str!("fixtures/security/dev_key.pem");
