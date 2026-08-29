//! Unit tests for the OCSF mapping.
//!
//! Each test pins one audit event to the class, category, and activity ids the
//! OCSF schema assigns it, because those numbers are what a downstream SIEM
//! matches on and a silent change to one is not otherwise visible.

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
