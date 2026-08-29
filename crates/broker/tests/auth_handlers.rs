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

#[path = "auth_handlers/alter_scram.rs"]
mod alter_scram;
#[path = "auth_handlers/alter_scram_validation.rs"]
mod alter_scram_validation;
#[path = "auth_handlers/gssapi.rs"]
mod gssapi;
#[path = "auth_handlers/handshake.rs"]
mod handshake;
#[path = "auth_handlers/harness.rs"]
mod harness;
#[path = "auth_handlers/inter_broker.rs"]
mod inter_broker;
#[path = "auth_handlers/oauthbearer.rs"]
mod oauthbearer;
#[path = "auth_handlers/oauthbearer_sessions.rs"]
mod oauthbearer_sessions;
#[path = "auth_handlers/oauthbearer_tokens.rs"]
mod oauthbearer_tokens;
#[path = "auth_handlers/plain.rs"]
mod plain;
#[path = "auth_handlers/scram.rs"]
mod scram;
mod support;
#[path = "auth_handlers/tls.rs"]
mod tls;
#[path = "auth_handlers/two_broker_sasl.rs"]
mod two_broker_sasl;

const DEV_CERT: &str = include_str!("fixtures/security/dev_cert.pem");
const DEV_KEY: &str = include_str!("fixtures/security/dev_key.pem");
