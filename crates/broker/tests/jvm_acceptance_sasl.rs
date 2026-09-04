//! SASL over PLAINTEXT -- PLAIN, SCRAM-SHA-256/512 and OAUTHBEARER -- plus the
//! ACL authorization cases that build on an authenticated broker.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on.
//!
//! The binary root carries only the module tree. One child covers each SASL
//! mechanism -- [`plain`], [`scram`] and [`oauthbearer`] -- and the remaining
//! children cover authorization on top of an authenticated broker:
//! [`acl_cli`] for the `kafka-acls` administration round-trip,
//! [`acl_authorized`] and [`acl_denied`] for what a literal binding permits and
//! refuses, and [`acl_prefixed`] for the `PREFIXED` pattern.
//!
//! Cargo compiles this file as its own test binary, so a `mod` declaration in
//! it resolves against `tests/` rather than against a directory named for the
//! file. Each child therefore carries an explicit `#[path]` onto the sibling
//! `jvm_acceptance_sasl/` directory. `jvm_acceptance` and `support` are
//! `tests/<name>/mod.rs` helpers, which the crate-root rule already resolves.

#[path = "jvm_acceptance_sasl/acl_authorized.rs"]
mod acl_authorized;
#[path = "jvm_acceptance_sasl/acl_cli.rs"]
mod acl_cli;
#[path = "jvm_acceptance_sasl/acl_output.rs"]
mod acl_output;
#[path = "jvm_acceptance_sasl/acl_denied.rs"]
mod acl_denied;
#[path = "jvm_acceptance_sasl/acl_prefixed.rs"]
mod acl_prefixed;
mod jvm_acceptance;
#[path = "jvm_acceptance_sasl/oauthbearer.rs"]
mod oauthbearer;
// The oracle harness `acl_cli` compares against. It is `jvm_acceptance_cli`'s
// file, shared rather than copied: see its own module documentation.
#[path = "jvm_acceptance_cli/oracle.rs"]
mod oracle;
#[path = "jvm_acceptance_sasl/plain.rs"]
mod plain;
#[path = "jvm_acceptance_sasl/scram.rs"]
mod scram;
mod support;
