//! Broker-side integration tests for the `__krabka_audit` topic.
//!
//! The suite answers two questions. The first is whether the broker writes an
//! audit record for the things it promises to audit — its own start, an admin
//! operation, a signed checkpoint — and whether the hash chain over those
//! records stays whole across a restart. The second is KFC-9's claim that the
//! audit topic *alone*, with no metadata image to read and no broker to ask,
//! says who froze a topic and who approved the thaw.
//!
//! - [`emission`] — the topic exists, and startup and `CreateTopics` land on it.
//! - [`chain_integrity`] — signed checkpoints, and seq numbers that survive a
//!   restart.
//! - [`denied_operations`] — a denied request is refused and the broker lives.
//! - [`freeze_signing`] — the auditor's own copy of the signed-bytes layout.
//! - [`freeze_workflow`] — the freeze, proposal, approvals, thaw and withdrawal
//!   that the KFC-9 cases read back.
//! - [`freeze_evidence`] — that workflow, recovered from the audit topic alone.
//! - [`freeze_metrics`] — the KFC-9 gauges and counters under a real request.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `audit/` directory, which keeps the parts out of `tests/` where every `.rs`
// file would become another test binary.
#[path = "audit/chain_integrity.rs"]
mod chain_integrity;
#[path = "audit/denied_operations.rs"]
mod denied_operations;
#[path = "audit/emission.rs"]
mod emission;
#[path = "audit/freeze_evidence.rs"]
mod freeze_evidence;
#[path = "audit/freeze_metrics.rs"]
mod freeze_metrics;
#[path = "audit/freeze_signing.rs"]
mod freeze_signing;
#[path = "audit/freeze_workflow.rs"]
mod freeze_workflow;
