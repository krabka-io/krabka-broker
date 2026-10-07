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

fn check_fields(value: &serde_json::Value, expected: &[(&str, serde_json::Value)]) {
    for (path, expected) in expected {
        check!(value.pointer(path) == Some(expected), "field {path}");
    }
}

#[test]
fn authentication_failure_maps_to_3002() {
    let ev = crate::test_support::authentication(
        51120,
        Some("authentication failed"),
        1_700_000_000_000,
    );
    let j = to_ocsf(&ev, &product());
    check_fields(
        &j,
        &[
            ("/class_uid", serde_json::json!(3002)),
            ("/category_uid", serde_json::json!(3)),
            ("/status_id", serde_json::json!(2)),
            ("/time", serde_json::json!(1_700_000_000_000_i64)),
            ("/actor/user/name", serde_json::json!("alice")),
            ("/src_endpoint/ip", serde_json::json!("10.0.0.1")),
            ("/src_endpoint/port", serde_json::json!(51120)),
            ("/auth_protocol", serde_json::json!("SASL/PLAIN")),
            ("/metadata/product/vendor_name", serde_json::json!("Krabka")),
        ],
    );
}

#[test]
fn authorization_denied_maps_to_3003_failure() {
    let ev = crate::test_support::denied(5);
    let j = to_ocsf(&ev, &product());
    check_fields(
        &j,
        &[
            ("/class_uid", serde_json::json!(3003)),
            ("/status_id", serde_json::json!(2)),
            ("/type_uid", serde_json::json!(300_302_i64)),
            ("/actor/user/name", serde_json::json!("bob")),
            ("/resources/0/type", serde_json::json!("Topic")),
            ("/resources/0/name", serde_json::json!("secrets")),
            ("/operation", serde_json::json!("Write")),
        ],
    );
}

#[test]
fn admin_operation_maps_to_6003_with_resources() {
    let ev = crate::test_support::admin(6);
    let j = to_ocsf(&ev, &product());
    check_fields(
        &j,
        &[
            ("/class_uid", serde_json::json!(6003)),
            ("/category_uid", serde_json::json!(6)),
            ("/status_id", serde_json::json!(1)),
            ("/api/operation", serde_json::json!("CreateTopics")),
            ("/resources/0/name", serde_json::json!("orders")),
        ],
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

fn unsigned_privilege(
    identity: (AuditOutcome, PrivilegedPhase, &str, &str, &str),
    actors: (AuditPrincipal, Vec<AuditPrincipal>),
    source: &AuditEndpoint,
    reason: &str,
    time_ms: i64,
) -> AuditEvent {
    let (outcome, phase, action, target, proposal_id) = identity;
    let (principal, counterparties) = actors;
    AuditEvent::PrivilegedAction {
        outcome,
        phase,
        action: action.into(),
        target: target.into(),
        proposal_id: proposal_id.into(),
        principal,
        counterparties,
        approver_set_fingerprint: "f00dcafe".into(),
        key_id: String::new(),
        signature: vec![],
        signature_verified: false,
        signed_at_ms: 0,
        source: source.clone(),
        reason: reason.into(),
        time_ms,
    }
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
            crate::test_support::privileged_freeze(vec![0xde, 0xad, 0xbe, 0xef], 0, 10),
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
            unsigned_privilege(
                (
                    AuditOutcome::Success,
                    PrivilegedPhase::Consumed,
                    "unclean_elect_leaders",
                    "orders-3",
                    "bg-7",
                ),
                (carol.clone(), vec![alice.clone(), bob.clone()]),
                &source,
                "",
                11,
            ),
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
            unsigned_privilege(
                (
                    AuditOutcome::Failure,
                    PrivilegedPhase::Bypassed,
                    "unclean_recovery",
                    "orders-9",
                    "",
                ),
                (
                    AuditPrincipal {
                        name: "broker".into(),
                        auth_method: "Internal".into(),
                    },
                    vec![],
                ),
                &source,
                "background recovery ran without an approval",
                12,
            ),
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
