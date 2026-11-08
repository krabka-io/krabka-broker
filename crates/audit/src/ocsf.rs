//! OCSF serialization for audit events.
//!
//! OCSF is the Open Cybersecurity Schema Framework.
//!
//! [`to_ocsf`] is the one entry point. It matches an [`AuditEvent`] variant and
//! hands it to the child module that builds the OCSF record class for that
//! variant, so each class's field names and id numbers live in exactly one
//! place. [`ProductInfo`] names the emitting product in every record.
//!
//! # Record versions
//!
//! `__krabka_audit` records are a persisted format, so their shape is chosen by
//! the cluster's finalized `krabka.version`, as an [`OcsfVersion`]:
//!
//! - [`OcsfVersion::V1`], below level [`OCSF_ACTIVITY_IDS_LEVEL`], is the
//!   record krabka 1.0.0 writes. Its `metadata` object carries no
//!   `log_version`, and that absence is what marks a record as version 1.
//! - [`OcsfVersion::V2`], from that level on, uses the activity ids OCSF 1.3.0
//!   defines: an Application Lifecycle start is activity 3 and a stop
//!   activity 4, a configuration change or TLS reload is 99 (Other), and the
//!   record names the broker application in `app`. An ACL denial is Authorize
//!   Session activity 99 (Other). An admin operation is API Activity 1
//!   (Create), 3 (Update) or 4 (Delete) when its Kafka API is one of those,
//!   and 0 (Unknown) otherwise. Every version 2 record, of every class, carries
//!   `"log_version": "2"` in its `metadata` object: OCSF's
//!   `metadata.log_version`, the event log schema version.
//!
//! The version is fixed when a record is first serialized. A record the
//! writer spools is replayed with the bytes it was serialized to, so a level
//! finalized in between does not change it. Records of both versions sit in
//! one hash chain, which commits to their bytes and not to their shape.

use {
    self::{
        api_activity::{PrivilegedFields, ocsf_admin_operation, ocsf_privileged_action},
        authentication::ocsf_authentication,
        authorization::ocsf_authorization_denied,
        envelope::Envelope,
        lifecycle::ocsf_lifecycle,
    },
    crate::{event::AuditEvent, ids::EpochMs},
    krabka_ids::NodeId,
    krabka_metadata::krabka_version::{KrabkaVersion, OCSF_ACTIVITY_IDS_LEVEL},
    std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

mod api_activity;
mod authentication;
mod authorization;
mod envelope;
mod lifecycle;

#[cfg(test)]
mod tests;

/// The version of the OCSF record layout a broker writes.
///
/// See the [module docs](self#record-versions) for what each version writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OcsfVersion {
    /// The krabka 1.0.0 record, written below `krabka.version` 3.
    V1,
    /// The record with OCSF's own activity ids, written from `krabka.version`
    /// 3 on.
    V2,
}

impl OcsfVersion {
    /// The record version a broker writes once `krabka_version` is
    /// finalized.
    #[must_use]
    pub fn at(krabka_version: KrabkaVersion) -> Self {
        if krabka_version.level() >= OCSF_ACTIVITY_IDS_LEVEL {
            Self::V2
        } else {
            Self::V1
        }
    }
}

/// The record version an audit writer serializes with, shared with whatever
/// follows the finalized `krabka.version`.
///
/// Clones share one value. The broker sets it on every metadata image change,
/// and the writer reads it once per event, when it serializes the event.
#[derive(Clone, Debug)]
pub struct OcsfVersionSwitch(Arc<AtomicBool>);

impl OcsfVersionSwitch {
    /// A switch that starts at `version`.
    #[must_use]
    pub fn new(version: OcsfVersion) -> Self {
        Self(Arc::new(AtomicBool::new(version == OcsfVersion::V2)))
    }

    /// The version the next record is serialized with.
    #[must_use]
    pub fn get(&self) -> OcsfVersion {
        // The flag guards no other memory, so a relaxed load is enough: a
        // record serialized just before a level change may keep the old
        // version, as it would had it been emitted a moment earlier.
        if self.0.load(Ordering::Relaxed) {
            OcsfVersion::V2
        } else {
            OcsfVersion::V1
        }
    }

    /// Serialize every later record with `version`.
    pub fn set(&self, version: OcsfVersion) {
        self.0.store(version == OcsfVersion::V2, Ordering::Relaxed);
    }
}

/// Product identity for every OCSF record's `metadata` field.
#[derive(Debug, Clone)]
pub struct ProductInfo {
    pub vendor_name: String,
    pub name: String,
    pub version: String,
}

/// Serialize an [`AuditEvent`] to an OCSF JSON object in record `version`.
#[must_use]
pub fn to_ocsf(
    event: &AuditEvent,
    product: &ProductInfo,
    version: OcsfVersion,
) -> serde_json::Value {
    let envelope = &Envelope { product, version };
    match event {
        AuditEvent::Authentication {
            outcome,
            mechanism,
            principal,
            source,
            reason,
            time_ms,
        } => ocsf_authentication(
            *outcome,
            mechanism,
            principal,
            source,
            reason.as_ref(),
            *time_ms,
            envelope,
        ),
        AuditEvent::AuthorizationDenied {
            principal,
            source,
            resource_type,
            resource_name,
            operation,
            time_ms,
        } => ocsf_authorization_denied(
            principal,
            source,
            resource_type,
            resource_name,
            operation,
            *time_ms,
            envelope,
        ),
        AuditEvent::AdminOperation {
            outcome,
            principal,
            source,
            operation,
            resources,
            time_ms,
        } => ocsf_admin_operation(
            *outcome, principal, source, operation, resources, *time_ms, envelope,
        ),
        AuditEvent::PrivilegedAction {
            outcome,
            phase,
            action,
            target,
            proposal_id,
            principal,
            counterparties,
            approver_set_fingerprint,
            key_id,
            signature,
            signature_verified,
            signed_at_ms,
            source,
            reason,
            time_ms,
        } => ocsf_privileged_action(
            &PrivilegedFields {
                outcome: *outcome,
                phase: *phase,
                action,
                target,
                proposal_id,
                principal,
                counterparties,
                approver_set_fingerprint,
                key_id,
                signature,
                signature_verified: *signature_verified,
                signed_at_ms: *signed_at_ms,
                source,
                reason,
                time_ms: *time_ms,
            },
            envelope,
        ),
        AuditEvent::Lifecycle {
            kind,
            node_id,
            time_ms,
        } => ocsf_lifecycle(
            *kind,
            // `node_id` is the Kafka `broker.id` (an `int32` widened to `i64`
            // at the broker boundary), always non-negative; wrap it into the
            // canonical `u64` `NodeId`.
            NodeId(u64::try_from(*node_id).unwrap_or(0)),
            EpochMs(*time_ms),
            envelope,
        ),
    }
}
