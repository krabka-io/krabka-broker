//! Quota, SCRAM-credential, log-directory and delegation-token administration
//! against a three-broker SASL cluster.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on. The binary root carries only the module
//! tree, and each child covers one administrative surface.
#![cfg(test)]

mod jvm_acceptance;
mod support;

// Cargo compiles this file as its own test binary, so a plain `mod` here
// resolves against `tests/`. `#[path]` re-bases each declaration onto the
// sibling `jvm_acceptance_quotas/` directory, which keeps the parts out of
// `tests/`, where every `.rs` file would become another test binary.
#[path = "jvm_acceptance_quotas/client_quotas.rs"]
mod client_quotas;
#[path = "jvm_acceptance_quotas/delegation_tokens.rs"]
mod delegation_tokens;
#[path = "jvm_acceptance_quotas/log_dirs.rs"]
mod log_dirs;
#[path = "jvm_acceptance_quotas/scram_credentials.rs"]
mod scram_credentials;

// Every case in this binary uses the same process-wide advertised endpoints.
// Hold ownership until the brokers have completed shutdown before reusing them.
use tokio::sync::{Mutex, MutexGuard};

async fn claim_broker_ports() -> MutexGuard<'static, ()> {
    static PORTS: Mutex<()> = Mutex::const_new(());
    PORTS.lock().await
}
