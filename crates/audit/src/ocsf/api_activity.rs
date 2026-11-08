//! The OCSF API Activity records (class 6003), which report an operation the
//! broker performed on a client's behalf. Two audit events map here: a plain
//! admin operation, and a privileged action, whose body additionally carries
//! the two-person-control evidence -- the proposal, the approvers, and the
//! signature over the request.

use {
    super::{
        OcsfVersion,
        envelope::{Envelope, status_id, user},
    },
    crate::{
        chain::to_hex,
        event::{AuditEndpoint, AuditOutcome, AuditPrincipal, AuditResource, PrivilegedPhase},
    },
    serde_json::json,
};

pub(super) fn ocsf_admin_operation(
    outcome: AuditOutcome,
    principal: &AuditPrincipal,
    source: &AuditEndpoint,
    operation: &str,
    resources: &[AuditResource],
    time_ms: i64,
    envelope: &Envelope<'_>,
) -> serde_json::Value {
    // class 6003 API Activity. The operation is named in `api` either way.
    let class_uid = 6003_i64;
    let activity_id = match envelope.version {
        OcsfVersion::V1 => UNKNOWN,
        OcsfVersion::V2 => admin_activity_id(operation),
    };
    let res: Vec<serde_json::Value> = resources
        .iter()
        .map(|r| json!({ "type": r.resource_type, "name": r.name }))
        .collect();
    json!({
        "class_uid": class_uid,
        "category_uid": 6,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "time": time_ms,
        "status_id": status_id(outcome),
        "api": { "operation": operation, "service": { "name": "kafka" } },
        "actor": { "user": user(principal) },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "resources": res,
        "metadata": envelope.metadata(),
    })
}

/// OCSF API Activity 0, Unknown.
const UNKNOWN: i64 = 0;

/// The OCSF API Activity id of the Kafka admin API `operation`, in a
/// version 2 record: 1 Create, 2 Read, 3 Update or 4 Delete, or 0 Unknown.
///
/// The table names every API the broker audits as an admin operation, by its
/// exact Kafka name, so a new audited API takes 0 until it is added here.
/// None of them reads: the broker audits only APIs that change cluster state.
fn admin_activity_id(operation: &str) -> i64 {
    const CREATE: i64 = 1;
    const UPDATE: i64 = 3;
    const DELETE: i64 = 4;
    match operation {
        "CreateTopics" | "CreatePartitions" | "CreateAcls" | "CreateDelegationToken" => CREATE,
        "AlterConfigs"
        | "IncrementalAlterConfigs"
        | "AlterClientQuotas"
        | "AlterPartitionReassignments"
        | "AlterUserScramCredentials"
        | "UpdateFeatures"
        | "ElectLeaders"
        | "SetTopicFreeze"
        | "AddRaftVoter"
        | "RemoveRaftVoter"
        | "RenewDelegationToken"
        | "ExpireDelegationToken" => UPDATE,
        "DeleteTopics"
        | "DeleteAcls"
        | "DeleteRecords"
        | "UnregisterBroker"
        | "UnregisterController" => DELETE,
        _ => UNKNOWN,
    }
}

/// Borrowed view of the [`AuditEvent::PrivilegedAction`] fields.
///
/// The variant carries more fields than an argument list reads well with, so
/// the dispatch arm groups them here.
///
/// [`AuditEvent::PrivilegedAction`]: crate::event::AuditEvent::PrivilegedAction
pub(super) struct PrivilegedFields<'a> {
    pub(super) outcome: AuditOutcome,
    pub(super) phase: PrivilegedPhase,
    pub(super) action: &'a str,
    pub(super) target: &'a str,
    pub(super) proposal_id: &'a str,
    pub(super) principal: &'a AuditPrincipal,
    pub(super) counterparties: &'a [AuditPrincipal],
    pub(super) approver_set_fingerprint: &'a str,
    pub(super) key_id: &'a str,
    pub(super) signature: &'a [u8],
    pub(super) signature_verified: bool,
    pub(super) signed_at_ms: i64,
    pub(super) source: &'a AuditEndpoint,
    pub(super) reason: &'a str,
    pub(super) time_ms: i64,
}

pub(super) fn ocsf_privileged_action(
    f: &PrivilegedFields<'_>,
    envelope: &Envelope<'_>,
) -> serde_json::Value {
    // class 6003 API Activity, activity 0 = Unknown in both versions: a
    // break-glass transition is named in `api` and `privileged_action`.
    let class_uid = 6003_i64;
    let activity_id = UNKNOWN;
    let counterparties: Vec<serde_json::Value> = f.counterparties.iter().map(user).collect();
    json!({
        "class_uid": class_uid,
        "category_uid": 6,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "time": f.time_ms,
        "status_id": status_id(f.outcome),
        "status_detail": f.reason,
        "api": {
            "operation": format!("{}.{}", f.action, f.phase.as_name()),
            "service": { "name": "kafka" },
        },
        "actor": { "user": user(f.principal) },
        "src_endpoint": { "ip": f.source.ip, "port": f.source.port },
        "privileged_action": {
            "phase": f.phase.as_name(),
            "action": f.action,
            "target": f.target,
            "proposal_id": f.proposal_id,
            "counterparties": counterparties,
            "approver_set_fingerprint": f.approver_set_fingerprint,
            "key_id": f.key_id,
            // Lowercase hex, as `Checkpoint::to_record` encodes its signature
            // and public key. An auditor re-verifies the signature from this
            // field alone, so the encoding must stay the crate's one encoding.
            "signature": to_hex(f.signature),
            "signature_verified": f.signature_verified,
            // The stamp the signature covers, which is not `time` above. An
            // auditor rebuilding the signed preimage reads it from here.
            "signed_at_ms": f.signed_at_ms,
        },
        "metadata": envelope.metadata(),
    })
}
