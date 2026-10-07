//! Boots the single-broker SASL/PLAINTEXT cluster every test in this suite
//! runs against.
//!
//! The listener has to speak SASL because the scenarios need two distinct
//! principals: a super-user that sets the quota through `AlterClientQuotas`,
//! and a named user whose mutations the quota is then measured against.

pub use crate::support::sasl::start_single_broker_sasl_plaintext_with_users;

// ─────────────────────────────────────────────────────────────────────────────
// Cluster setup helpers. Copied from `client_quotas.rs`.
// ─────────────────────────────────────────────────────────────────────────────
