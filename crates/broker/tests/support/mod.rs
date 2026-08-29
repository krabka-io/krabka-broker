//! Shared helpers for broker integration tests.
//!
//! # Single-broker helper
//!
//! [`start`] and [`InProcess`] boot one broker and one client for simple
//! unit-style integration tests.
//!
//! # Multi-broker helpers
//!
//! [`start_n_node_with_retry`] boots an `n`-broker cluster with
//! ephemeral ports and short raft timings. Each `tests/*.rs` integration-test
//! crate that needs a 3-broker cluster declares `mod support;` and calls
//! `start_n_node_with_retry`.
//!
//! # Fault injection
//!
//! [`relay`] is a test-only TCP forwarder. Point a broker at a relay instead of
//! at its peer and the test can cut the link — including the connections that
//! are already open — without stopping either node, which is the only way to
//! produce a live minority.
//!
//! # Layout
//!
//! One child module per role: [`single_broker`] and [`operator_keys`] boot a
//! single node, [`ports`] reserves the addresses a cluster binds and
//! [`cluster`] and [`cluster_boot`] boot it, [`containers`] addresses the JVM
//! container suites, [`audit`] reads the audit topic back, and [`relay`] cuts
//! links. Every helper is re-exported here, so a suite reaches all of them as
//! `support::<name>`. What stays in this file is the tracing setup, the
//! metadata round-trip that resolves a topic id, and the two pollers that wait
//! on the audit topic.
//!
//! Cargo treats `tests/support/mod.rs` (rather than `tests/support.rs`) as
//! a non-binary submodule, so it does not compile the file as its own test
//! crate.

#![allow(dead_code)]

use std::time::{Duration, Instant};

use assert2::assert;

mod audit;
mod cluster;
mod cluster_boot;
mod containers;
mod operator_keys;
mod ports;
mod single_broker;
// A cut-and-heal TCP relay for partition tests. Declared here so every suite
// that pulls in `support` can reach it as `support::relay`.
pub mod relay;

// Each suite declares `mod support;` and reaches the helpers it needs through
// this one re-export, so every binary compiles the whole surface and uses only
// part of it. That is why this statement carries the `unused_imports` allow:
// the same reason the module carries `allow(dead_code)`.
#[allow(unused_imports)]
pub use self::{
    audit::{audit_record_seqs, consume_audit_records},
    cluster::start_n_node_with,
    cluster_boot::{
        broker_config, start_n_node, start_n_node_with_retry, start_reusing_addrs,
        wait_for_all_brokers_registered,
    },
    containers::{JvmListeners, free_port, manifest_dir, unique_container_name},
    operator_keys::{
        ANONYMOUS, OperatorKey, mint_operator_key, sasl_client, sasl_plain_security,
        start_with_operator_key, start_with_operator_keys, start_with_operator_keys_sasl,
    },
    ports::{bind_and_drop_ports, bind_and_hold_ports},
    single_broker::{
        InProcess, start, start_with_audit_key, start_with_deny_all_authz, start_with_dir,
    },
};

/// Lazily-initialized tracing subscriber so `RUST_LOG=...` works in
/// integration tests. It is safe to call this many times, because `try_init`
/// is a no-op after the first success.
pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_test_writer()
        .try_init();
}

/// Fetch all records from `AUDIT_TOPIC` partition 0, JSON-decode each
/// record value, and return the decoded objects. Mirrors the
/// `broker_started_event_is_written_to_audit_topic` fetch pattern.
pub async fn wait_for_audit_record<F>(
    client: &krabka_client_core::Client,
    what: &str,
    mut predicate: F,
) -> Vec<serde_json::Value>
where
    F: FnMut(&serde_json::Value) -> bool,
{
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let records = consume_audit_records(client).await;
        if records.iter().any(&mut predicate) {
            return records;
        }
        assert!(
            Instant::now() <= deadline,
            "audit record '{what}' did not appear within 30s; last={records:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub async fn wait_for_audit_seq_count(
    client: &krabka_client_core::Client,
    min_count: usize,
) -> Vec<u64> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let seqs = audit_record_seqs(client).await;
        if seqs.len() >= min_count {
            return seqs;
        }
        assert!(
            Instant::now() <= deadline,
            "audit seq count did not reach {min_count} within 30s; last={seqs:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Round-trip a Metadata request to learn the topic's assigned UUID.
/// Produce / Fetch at v ≥ 13 carry only `topic_id` on the wire, so the
/// caller must plumb the real UUID through.
pub async fn topic_id_for(
    client: &krabka_client_core::Client,
    name: &str,
) -> krabka_protocol::primitives::uuid::Uuid {
    use krabka_protocol::owned::metadata_request::{MetadataRequest, MetadataRequestTopic};

    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata for topic_id");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}
