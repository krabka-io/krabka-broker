//! OCSF serialization for audit events.
//!
//! OCSF is the Open Cybersecurity Schema Framework.

use krabka_ids::NodeId;
use serde_json::json;

use crate::{
    chain::to_hex,
    event::{
        AuditEndpoint, AuditEvent, AuditOutcome, AuditPrincipal, LifecycleKind, PrivilegedPhase,
    },
    ids::EpochMs,
};

/// Product identity for every OCSF record's `metadata` field.
#[derive(Debug, Clone)]
pub struct ProductInfo {
    pub vendor_name: String,
    pub name: String,
    pub version: String,
}

const SCHEMA_VERSION: &str = "1.3.0";

fn status_id(outcome: AuditOutcome) -> i64 {
    match outcome {
        AuditOutcome::Success => 1,
        AuditOutcome::Failure => 2,
    }
}

fn metadata(product: &ProductInfo) -> serde_json::Value {
    json!({
        "version": SCHEMA_VERSION,
        "product": {
            "vendor_name": product.vendor_name,
            "name": product.name,
            "version": product.version,
        }
    })
}

fn ocsf_authentication(
    outcome: AuditOutcome,
    mechanism: &str,
    principal: &crate::event::AuditPrincipal,
    source: &crate::event::AuditEndpoint,
    reason: Option<&String>,
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 3002 Authentication, activity 1 = Logon.
    let class_uid = 3002_i64;
    let activity_id = 1_i64;
    json!({
        "class_uid": class_uid,
        "category_uid": 3,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": "Logon",
        "time": time_ms,
        "status_id": status_id(outcome),
        "status_detail": reason,
        "auth_protocol": mechanism,
        "actor": { "user": { "name": principal.name, "type": principal.auth_method } },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "metadata": metadata(product),
    })
}

fn ocsf_authorization_denied(
    principal: &crate::event::AuditPrincipal,
    source: &crate::event::AuditEndpoint,
    resource_type: &str,
    resource_name: &str,
    operation: &str,
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 3003 Authorize Session, activity 2 = Deny.
    let class_uid = 3003_i64;
    let activity_id = 2_i64;
    json!({
        "class_uid": class_uid,
        "category_uid": 3,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": "Deny",
        "time": time_ms,
        "status_id": 2,
        "operation": operation,
        "actor": { "user": { "name": principal.name, "type": principal.auth_method } },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "resources": [ { "type": resource_type, "name": resource_name } ],
        "metadata": metadata(product),
    })
}

fn ocsf_admin_operation(
    outcome: AuditOutcome,
    principal: &crate::event::AuditPrincipal,
    source: &crate::event::AuditEndpoint,
    operation: &str,
    resources: &[crate::event::AuditResource],
    time_ms: i64,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 6003 API Activity, activity 0 = Unknown/Other (operation in `api`).
    let class_uid = 6003_i64;
    let activity_id = 0_i64;
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
        "actor": { "user": { "name": principal.name, "type": principal.auth_method } },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "resources": res,
        "metadata": metadata(product),
    })
}

/// One principal in the `{ "name": …, "type": … }` shape the OCSF bodies use
/// for `actor.user`. The privileged-action body needs it for the actor and for
/// every counterparty, so the shape lives in one place.
fn user(principal: &AuditPrincipal) -> serde_json::Value {
    json!({ "name": principal.name, "type": principal.auth_method })
}

/// Borrowed view of the [`AuditEvent::PrivilegedAction`] fields.
///
/// The variant carries more fields than an argument list reads well with, so
/// the dispatch arm groups them here.
struct PrivilegedFields<'a> {
    outcome: AuditOutcome,
    phase: PrivilegedPhase,
    action: &'a str,
    target: &'a str,
    proposal_id: &'a str,
    principal: &'a AuditPrincipal,
    counterparties: &'a [AuditPrincipal],
    approver_set_fingerprint: &'a str,
    key_id: &'a str,
    signature: &'a [u8],
    signature_verified: bool,
    signed_at_ms: i64,
    source: &'a AuditEndpoint,
    reason: &'a str,
    time_ms: i64,
}

fn ocsf_privileged_action(f: &PrivilegedFields<'_>, product: &ProductInfo) -> serde_json::Value {
    // class 6003 API Activity, activity 0 = Unknown/Other (operation in `api`).
    let class_uid = 6003_i64;
    let activity_id = 0_i64;
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
        "metadata": metadata(product),
    })
}

fn ocsf_lifecycle(
    kind: LifecycleKind,
    node_id: NodeId,
    time_ms: EpochMs,
    product: &ProductInfo,
) -> serde_json::Value {
    // class 6002 Application Lifecycle.
    let class_uid = 6002_i64;
    let (activity_id, activity_name) = match kind {
        LifecycleKind::BrokerStarted => (1_i64, "BrokerStarted"),
        LifecycleKind::BrokerStopping => (4, "BrokerStopping"),
        LifecycleKind::ConfigApplied => (3, "ConfigApplied"),
        LifecycleKind::TlsReloaded => (3, "TlsReloaded"),
    };
    json!({
        "class_uid": class_uid,
        "category_uid": 6,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": activity_name,
        "time": time_ms.0,
        "status_id": 1,
        "device": { "uid": node_id.0.to_string(), "type_id": 1 },
        "metadata": metadata(product),
    })
}

/// Serialize an [`AuditEvent`] to an OCSF JSON object.
#[must_use]
pub fn to_ocsf(event: &AuditEvent, product: &ProductInfo) -> serde_json::Value {
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
            product,
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
            product,
        ),
        AuditEvent::AdminOperation {
            outcome,
            principal,
            source,
            operation,
            resources,
            time_ms,
        } => ocsf_admin_operation(
            *outcome, principal, source, operation, resources, *time_ms, product,
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
            product,
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
            product,
        ),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::event::*;

    fn product() -> ProductInfo {
        ProductInfo {
            vendor_name: "Krabka".into(),
            name: "krabka-broker".into(),
            version: "0.3.7".into(),
        }
    }

    #[test]
    fn authentication_failure_maps_to_3002() {
        let ev = AuditEvent::Authentication {
            outcome: AuditOutcome::Failure,
            mechanism: "SASL/PLAIN".into(),
            principal: AuditPrincipal {
                name: "alice".into(),
                auth_method: "SaslPlain".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.1".into(),
                port: 51120,
            },
            reason: Some("authentication failed".into()),
            time_ms: 1_700_000_000_000,
        };
        let j = to_ocsf(&ev, &product());
        check!(
            (
                j["class_uid"].clone(),
                j["category_uid"].clone(),
                j["status_id"].clone(),
                j["time"].clone(),
                j["actor"]["user"]["name"].clone(),
                j["src_endpoint"]["ip"].clone(),
                j["src_endpoint"]["port"].clone(),
                j["auth_protocol"].clone(),
                j["metadata"]["product"]["vendor_name"].clone(),
            ) == (
                serde_json::json!(3002),
                serde_json::json!(3),
                serde_json::json!(2),
                serde_json::json!(1_700_000_000_000_i64),
                serde_json::json!("alice"),
                serde_json::json!("10.0.0.1"),
                serde_json::json!(51120),
                serde_json::json!("SASL/PLAIN"),
                serde_json::json!("Krabka"),
            )
        );
    }

    #[test]
    fn authorization_denied_maps_to_3003_failure() {
        let ev = AuditEvent::AuthorizationDenied {
            principal: AuditPrincipal {
                name: "bob".into(),
                auth_method: "MTls".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.2".into(),
                port: 4444,
            },
            resource_type: "Topic".into(),
            resource_name: "secrets".into(),
            operation: "Write".into(),
            time_ms: 5,
        };
        let j = to_ocsf(&ev, &product());
        check!(
            (
                j["class_uid"].clone(),
                j["status_id"].clone(),
                j["type_uid"].clone(),
                j["actor"]["user"]["name"].clone(),
                j["resources"][0]["type"].clone(),
                j["resources"][0]["name"].clone(),
                j["operation"].clone(),
            ) == (
                serde_json::json!(3003),
                serde_json::json!(2),
                serde_json::json!(300_302_i64),
                serde_json::json!("bob"),
                serde_json::json!("Topic"),
                serde_json::json!("secrets"),
                serde_json::json!("Write"),
            )
        );
    }

    #[test]
    fn admin_operation_maps_to_6003_with_resources() {
        let ev = AuditEvent::AdminOperation {
            outcome: AuditOutcome::Success,
            principal: AuditPrincipal {
                name: "admin".into(),
                auth_method: "MTls".into(),
            },
            source: AuditEndpoint {
                ip: "10.0.0.3".into(),
                port: 9092,
            },
            operation: "CreateTopics".into(),
            resources: vec![AuditResource {
                resource_type: "Topic".into(),
                name: "orders".into(),
            }],
            time_ms: 6,
        };
        let j = to_ocsf(&ev, &product());
        check!(
            (
                j["class_uid"].clone(),
                j["category_uid"].clone(),
                j["status_id"].clone(),
                j["api"]["operation"].clone(),
                j["resources"][0]["name"].clone(),
            ) == (
                serde_json::json!(6003),
                serde_json::json!(6),
                serde_json::json!(1),
                serde_json::json!("CreateTopics"),
                serde_json::json!("orders"),
            )
        );
    }

    fn expected_metadata() -> serde_json::Value {
        serde_json::json!({
            "version": "1.3.0",
            "product": {
                "vendor_name": "Krabka",
                "name": "krabka-broker",
                "version": "0.3.7",
            }
        })
    }

    #[test]
    fn privileged_action_maps_to_6003_with_the_whole_body() {
        let alice = AuditPrincipal {
            name: "User:alice".into(),
            auth_method: "MTls".into(),
        };
        let bob = AuditPrincipal {
            name: "User:bob".into(),
            auth_method: "SaslScram".into(),
        };
        let carol = AuditPrincipal {
            name: "User:carol".into(),
            auth_method: "MTls".into(),
        };
        let source = AuditEndpoint {
            ip: "10.0.0.4".into(),
            port: 9092,
        };
        let cases = [
            (
                "signed freeze, verified, no proposal",
                AuditEvent::PrivilegedAction {
                    outcome: AuditOutcome::Success,
                    phase: PrivilegedPhase::Applied,
                    action: "topic_freeze".into(),
                    target: "orders".into(),
                    proposal_id: String::new(),
                    principal: alice.clone(),
                    counterparties: vec![],
                    approver_set_fingerprint: String::new(),
                    key_id: "op-1".into(),
                    signature: vec![0xde, 0xad, 0xbe, 0xef],
                    signature_verified: true,
                    signed_at_ms: 0,
                    source: source.clone(),
                    reason: "incident 42".into(),
                    time_ms: 10,
                },
                serde_json::json!({
                    "class_uid": 6003,
                    "category_uid": 6,
                    "type_uid": 600_300,
                    "activity_id": 0,
                    "time": 10,
                    "status_id": 1,
                    "status_detail": "incident 42",
                    "api": {
                        "operation": "topic_freeze.applied",
                        "service": { "name": "kafka" },
                    },
                    "actor": { "user": { "name": "User:alice", "type": "MTls" } },
                    "src_endpoint": { "ip": "10.0.0.4", "port": 9092 },
                    "privileged_action": {
                        "phase": "applied",
                        "action": "topic_freeze",
                        "target": "orders",
                        "proposal_id": "",
                        "counterparties": [],
                        "approver_set_fingerprint": "",
                        "key_id": "op-1",
                        "signature": "deadbeef",
                        "signature_verified": true,
                        "signed_at_ms": 0,
                    },
                    "metadata": expected_metadata(),
                }),
            ),
            (
                "unsigned two-person consumption",
                AuditEvent::PrivilegedAction {
                    outcome: AuditOutcome::Success,
                    phase: PrivilegedPhase::Consumed,
                    action: "unclean_elect_leaders".into(),
                    target: "orders-3".into(),
                    proposal_id: "bg-7".into(),
                    principal: carol.clone(),
                    counterparties: vec![alice.clone(), bob.clone()],
                    approver_set_fingerprint: "f00dcafe".into(),
                    key_id: String::new(),
                    signature: vec![],
                    signature_verified: false,
                    signed_at_ms: 0,
                    source: source.clone(),
                    reason: String::new(),
                    time_ms: 11,
                },
                serde_json::json!({
                    "class_uid": 6003,
                    "category_uid": 6,
                    "type_uid": 600_300,
                    "activity_id": 0,
                    "time": 11,
                    "status_id": 1,
                    "status_detail": "",
                    "api": {
                        "operation": "unclean_elect_leaders.consumed",
                        "service": { "name": "kafka" },
                    },
                    "actor": { "user": { "name": "User:carol", "type": "MTls" } },
                    "src_endpoint": { "ip": "10.0.0.4", "port": 9092 },
                    "privileged_action": {
                        "phase": "consumed",
                        "action": "unclean_elect_leaders",
                        "target": "orders-3",
                        "proposal_id": "bg-7",
                        "counterparties": [
                            { "name": "User:alice", "type": "MTls" },
                            { "name": "User:bob", "type": "SaslScram" },
                        ],
                        "approver_set_fingerprint": "f00dcafe",
                        "key_id": "",
                        "signature": "",
                        "signature_verified": false,
                        "signed_at_ms": 0,
                    },
                    "metadata": expected_metadata(),
                }),
            ),
            (
                "bypassed gate reports failure",
                AuditEvent::PrivilegedAction {
                    outcome: AuditOutcome::Failure,
                    phase: PrivilegedPhase::Bypassed,
                    action: "unclean_recovery".into(),
                    target: "orders-9".into(),
                    proposal_id: String::new(),
                    principal: AuditPrincipal {
                        name: "broker".into(),
                        auth_method: "Internal".into(),
                    },
                    counterparties: vec![],
                    approver_set_fingerprint: "f00dcafe".into(),
                    key_id: String::new(),
                    signature: vec![],
                    signature_verified: false,
                    signed_at_ms: 0,
                    source: source.clone(),
                    reason: "background recovery ran without an approval".into(),
                    time_ms: 12,
                },
                serde_json::json!({
                    "class_uid": 6003,
                    "category_uid": 6,
                    "type_uid": 600_300,
                    "activity_id": 0,
                    "time": 12,
                    "status_id": 2,
                    "status_detail": "background recovery ran without an approval",
                    "api": {
                        "operation": "unclean_recovery.bypassed",
                        "service": { "name": "kafka" },
                    },
                    "actor": { "user": { "name": "broker", "type": "Internal" } },
                    "src_endpoint": { "ip": "10.0.0.4", "port": 9092 },
                    "privileged_action": {
                        "phase": "bypassed",
                        "action": "unclean_recovery",
                        "target": "orders-9",
                        "proposal_id": "",
                        "counterparties": [],
                        "approver_set_fingerprint": "f00dcafe",
                        "key_id": "",
                        "signature": "",
                        "signature_verified": false,
                        "signed_at_ms": 0,
                    },
                    "metadata": expected_metadata(),
                }),
            ),
        ];
        for (label, event, expected) in cases {
            check!(to_ocsf(&event, &product()) == expected, "case {label}");
        }
    }

    #[test]
    fn lifecycle_maps_to_6002() {
        let ev = AuditEvent::Lifecycle {
            kind: LifecycleKind::BrokerStarted,
            node_id: 1,
            time_ms: 7,
        };
        let j = to_ocsf(&ev, &product());
        check!(
            (
                j["class_uid"].clone(),
                j["status_id"].clone(),
                j["activity_name"].clone(),
                j["device"]["uid"].clone(),
            ) == (
                serde_json::json!(6002),
                serde_json::json!(1),
                serde_json::json!("BrokerStarted"),
                serde_json::json!("1"),
            )
        );
    }
}
