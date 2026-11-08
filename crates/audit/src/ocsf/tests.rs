//! Unit tests for the OCSF mapping.
//!
//! Each test pins one audit event to the class, category, and activity ids the
//! OCSF schema assigns it, because those numbers are what a downstream SIEM
//! matches on and a silent change to one is not otherwise visible.

use {
    super::*,
    crate::{
        event::*,
        sink::AuditRecord,
        test_support::{admin, authentication, denied, privileged_freeze},
    },
    assert2::check,
    krabka_metadata::krabka_version::KrabkaVersion,
    std::str,
};

fn product() -> ProductInfo {
    ProductInfo {
        vendor_name: "Krabka".into(),
        name: "krabka-broker".into(),
        version: "0.3.7".into(),
    }
}

/// A JSON pointer into an OCSF document and the value expected there.
struct ExpectedField {
    path: &'static str,
    value: serde_json::Value,
}

fn field(path: &'static str, value: serde_json::Value) -> ExpectedField {
    ExpectedField { path, value }
}

fn check_fields(value: &serde_json::Value, expected: &[ExpectedField]) {
    for ExpectedField {
        path,
        value: expected,
    } in expected
    {
        check!(value.pointer(path) == Some(expected), "field {path}");
    }
}

#[test]
fn authentication_failure_maps_to_3002() {
    let ev = authentication(51120, Some("authentication failed"), 1_700_000_000_000);
    let j = to_ocsf(&ev, &product(), OcsfVersion::V1);
    check_fields(&j, &[
        field("/class_uid", serde_json::json!(3002)),
        field("/category_uid", serde_json::json!(3)),
        field("/status_id", serde_json::json!(2)),
        field("/time", serde_json::json!(1_700_000_000_000_i64)),
        field("/actor/user/name", serde_json::json!("alice")),
        field("/src_endpoint/ip", serde_json::json!("10.0.0.1")),
        field("/src_endpoint/port", serde_json::json!(51120)),
        field("/auth_protocol", serde_json::json!("SASL/PLAIN")),
        field("/metadata/product/vendor_name", serde_json::json!("Krabka")),
    ]);
}

#[test]
fn authorization_denied_maps_to_3003_failure() {
    let ev = denied(5);
    let j = to_ocsf(&ev, &product(), OcsfVersion::V1);
    check_fields(&j, &[
        field("/class_uid", serde_json::json!(3003)),
        field("/status_id", serde_json::json!(2)),
        field("/type_uid", serde_json::json!(300_302_i64)),
        field("/actor/user/name", serde_json::json!("bob")),
        field("/resources/0/type", serde_json::json!("Topic")),
        field("/resources/0/name", serde_json::json!("secrets")),
        field("/operation", serde_json::json!("Write")),
    ]);
}

#[test]
fn admin_operation_maps_to_6003_with_resources() {
    let ev = admin(6);
    let j = to_ocsf(&ev, &product(), OcsfVersion::V1);
    check_fields(&j, &[
        field("/class_uid", serde_json::json!(6003)),
        field("/category_uid", serde_json::json!(6)),
        field("/status_id", serde_json::json!(1)),
        field("/api/operation", serde_json::json!("CreateTopics")),
        field("/resources/0/name", serde_json::json!("orders")),
    ]);
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

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
struct EventTimeMillis(i64);

#[bon::builder]
fn unsigned_privilege(
    #[builder(default = AuditOutcome :: Success)] outcome: AuditOutcome,
    #[builder(default = PrivilegedPhase :: Consumed)] phase: PrivilegedPhase,
    #[builder(default = "unclean_elect_leaders")] action: &'_ str,
    #[builder(default = "orders-3")] target: &'_ str,
    #[builder(default = "bg-7")] proposal_id: &'_ str,
    #[builder(default = AuditPrincipal { name : "User:alice" . into () , auth_method : "MTls" . into () })]
    principal: AuditPrincipal,
    #[builder(default)] counterparties: Vec<AuditPrincipal>,
    #[builder(default = AuditEndpoint { ip : "10.0.0.4" . into () , port : 9092 })]
    source: AuditEndpoint,
    #[builder(default)] reason: &'_ str,
    #[builder(into, default)] time_ms: EventTimeMillis,
) -> AuditEvent {
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
        source,
        reason: reason.into(),
        time_ms: time_ms.0,
    }
}

fn signed_freeze_ocsf_case() -> (AuditEvent, serde_json::Value) {
    (
        privileged_freeze(vec![0xde, 0xad, 0xbe, 0xef], 0, 10),
        serde_json::json!({
            "class_uid": 6003,
            "category_uid": 6,
            "type_uid": 600_300,
            "activity_id": 0,
            "time": 10,
            "status_id": 1,
            "status_detail": "incident 42",
            "api": { "operation": "topic_freeze.applied", "service": { "name": "kafka" } },
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
    )
}

fn unsigned_two_person_ocsf_case() -> (AuditEvent, serde_json::Value) {
    (
        unsigned_privilege()
            .principal(AuditPrincipal {
                name: "User:carol".into(),
                auth_method: "MTls".into(),
            })
            .counterparties(vec![
                AuditPrincipal {
                    name: "User:alice".into(),
                    auth_method: "MTls".into(),
                },
                AuditPrincipal {
                    name: "User:bob".into(),
                    auth_method: "SaslScram".into(),
                },
            ])
            .time_ms(11)
            .call(),
        serde_json::json!({
            "class_uid": 6003,
            "category_uid": 6,
            "type_uid": 600_300,
            "activity_id": 0,
            "time": 11,
            "status_id": 1,
            "status_detail": "",
            "api": { "operation": "unclean_elect_leaders.consumed", "service": { "name": "kafka" } },
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
    )
}

fn bypassed_failure_ocsf_case() -> (AuditEvent, serde_json::Value) {
    (
        unsigned_privilege()
            .outcome(AuditOutcome::Failure)
            .phase(PrivilegedPhase::Bypassed)
            .action("unclean_recovery")
            .target("orders-9")
            .proposal_id("")
            .principal(AuditPrincipal {
                name: "broker".into(),
                auth_method: "Internal".into(),
            })
            .reason("background recovery ran without an approval")
            .time_ms(12)
            .call(),
        serde_json::json!({
            "class_uid": 6003,
            "category_uid": 6,
            "type_uid": 600_300,
            "activity_id": 0,
            "time": 12,
            "status_id": 2,
            "status_detail": "background recovery ran without an approval",
            "api": { "operation": "unclean_recovery.bypassed", "service": { "name": "kafka" } },
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
    )
}

#[yare::parameterized(
    signed_freeze = { signed_freeze_ocsf_case() },
    unsigned_two_person_consumption = { unsigned_two_person_ocsf_case() },
    bypassed_gate_failure = { bypassed_failure_ocsf_case() },
)]
fn privileged_action_maps_to_6003_with_the_whole_body(case: (AuditEvent, serde_json::Value)) {
    let (event, expected) = case;
    check!(to_ocsf(&event, &product(), OcsfVersion::V1) == expected);
}

#[test]
fn lifecycle_maps_to_6002() {
    let ev = AuditEvent::Lifecycle {
        kind: LifecycleKind::BrokerStarted,
        node_id: 1,
        time_ms: 7,
    };
    let j = to_ocsf(&ev, &product(), OcsfVersion::V1);
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

#[yare::parameterized(
    below_the_level = { 0, OcsfVersion::V1 },
    at_the_1_0_0_formats = { 1, OcsfVersion::V1 },
    one_level_below = { 2, OcsfVersion::V1 },
    at_the_level = { 3, OcsfVersion::V2 },
)]
fn ocsf_version_follows_the_finalized_krabka_version(level: i16, expected: OcsfVersion) {
    check!(OcsfVersion::at(KrabkaVersion::new(level)) == expected);
}

#[test]
fn version_switch_clones_share_one_version() {
    let writer = OcsfVersionSwitch::new(OcsfVersion::V1);
    let follower = writer.clone();
    let before = writer.get();
    follower.set(OcsfVersion::V2);
    check!((before, writer.get()) == (OcsfVersion::V1, OcsfVersion::V2));
}

fn lifecycle(kind: LifecycleKind) -> AuditEvent {
    AuditEvent::Lifecycle {
        kind,
        node_id: 1,
        time_ms: 7,
    }
}

/// A successful admin operation of the Kafka API `api`.
fn admin_named(api: &str) -> AuditEvent {
    match admin(6) {
        AuditEvent::AdminOperation {
            outcome,
            principal,
            source,
            resources,
            time_ms,
            ..
        } => AuditEvent::AdminOperation {
            outcome,
            principal,
            source,
            operation: api.into(),
            resources,
            time_ms,
        },
        other => other,
    }
}

fn failed_logon() -> AuditEvent {
    authentication(51120, Some("authentication failed"), 1_700_000_000_000)
}

fn signed_freeze() -> AuditEvent { privileged_freeze(vec![0xde, 0xad, 0xbe, 0xef], 0, 10) }

// Version 1: the bytes krabka 1.1.5 writes to `__krabka_audit`. They are
// persisted, so they must never change.
const V1_BROKER_STARTED: &[u8] = br#"{"activity_id":1,"activity_name":"BrokerStarted","category_uid":6,"class_uid":6002,"device":{"type_id":1,"uid":"1"},"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"status_id":1,"time":7,"type_uid":600201}"#;
const V1_BROKER_STOPPING: &[u8] = br#"{"activity_id":4,"activity_name":"BrokerStopping","category_uid":6,"class_uid":6002,"device":{"type_id":1,"uid":"1"},"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"status_id":1,"time":7,"type_uid":600204}"#;
const V1_CONFIG_APPLIED: &[u8] = br#"{"activity_id":3,"activity_name":"ConfigApplied","category_uid":6,"class_uid":6002,"device":{"type_id":1,"uid":"1"},"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"status_id":1,"time":7,"type_uid":600203}"#;
const V1_TLS_RELOADED: &[u8] = br#"{"activity_id":3,"activity_name":"TlsReloaded","category_uid":6,"class_uid":6002,"device":{"type_id":1,"uid":"1"},"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"status_id":1,"time":7,"type_uid":600203}"#;
const V1_ACL_DENIAL: &[u8] = br#"{"activity_id":2,"activity_name":"Deny","actor":{"user":{"name":"bob","type":"MTls"}},"category_uid":3,"class_uid":3003,"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"operation":"Write","resources":[{"name":"secrets","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.2","port":4444},"status_id":2,"time":5,"type_uid":300302}"#;
const V1_ADMIN_CREATE: &[u8] = br#"{"activity_id":0,"actor":{"user":{"name":"admin","type":"MTls"}},"api":{"operation":"CreateTopics","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"resources":[{"name":"orders","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.3","port":9092},"status_id":1,"time":6,"type_uid":600300}"#;
const V1_ADMIN_UPDATE: &[u8] = br#"{"activity_id":0,"actor":{"user":{"name":"admin","type":"MTls"}},"api":{"operation":"IncrementalAlterConfigs","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"resources":[{"name":"orders","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.3","port":9092},"status_id":1,"time":6,"type_uid":600300}"#;
const V1_ADMIN_DELETE: &[u8] = br#"{"activity_id":0,"actor":{"user":{"name":"admin","type":"MTls"}},"api":{"operation":"DeleteTopics","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"resources":[{"name":"orders","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.3","port":9092},"status_id":1,"time":6,"type_uid":600300}"#;
const V1_ADMIN_UNKNOWN: &[u8] = br#"{"activity_id":0,"actor":{"user":{"name":"admin","type":"MTls"}},"api":{"operation":"DescribeCluster","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"resources":[{"name":"orders","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.3","port":9092},"status_id":1,"time":6,"type_uid":600300}"#;
const V1_AUTHENTICATION: &[u8] = br#"{"activity_id":1,"activity_name":"Logon","actor":{"user":{"name":"alice","type":"SaslPlain"}},"auth_protocol":"SASL/PLAIN","category_uid":3,"class_uid":3002,"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"src_endpoint":{"ip":"10.0.0.1","port":51120},"status_detail":"authentication failed","status_id":2,"time":1700000000000,"type_uid":300201}"#;
const V1_PRIVILEGED: &[u8] = br#"{"activity_id":0,"actor":{"user":{"name":"User:alice","type":"MTls"}},"api":{"operation":"topic_freeze.applied","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"privileged_action":{"action":"topic_freeze","approver_set_fingerprint":"","counterparties":[],"key_id":"op-1","phase":"applied","proposal_id":"","signature":"deadbeef","signature_verified":true,"signed_at_ms":0,"target":"orders"},"src_endpoint":{"ip":"10.0.0.4","port":9092},"status_detail":"incident 42","status_id":1,"time":10,"type_uid":600300}"#;

// Version 2, written from `krabka.version` 3 on: the activity ids OCSF 1.3.0
// defines, the lifecycle `app`, and `metadata.log_version`.
const V2_BROKER_STARTED: &[u8] = br#"{"activity_id":3,"activity_name":"Start","app":{"name":"krabka-broker","version":"0.3.7"},"category_uid":6,"class_uid":6002,"device":{"type_id":1,"uid":"1"},"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"status_id":1,"time":7,"type_uid":600203}"#;
const V2_BROKER_STOPPING: &[u8] = br#"{"activity_id":4,"activity_name":"Stop","app":{"name":"krabka-broker","version":"0.3.7"},"category_uid":6,"class_uid":6002,"device":{"type_id":1,"uid":"1"},"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"status_id":1,"time":7,"type_uid":600204}"#;
const V2_CONFIG_APPLIED: &[u8] = br#"{"activity_id":99,"activity_name":"ConfigApplied","app":{"name":"krabka-broker","version":"0.3.7"},"category_uid":6,"class_uid":6002,"device":{"type_id":1,"uid":"1"},"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"status_id":1,"time":7,"type_uid":600299}"#;
const V2_TLS_RELOADED: &[u8] = br#"{"activity_id":99,"activity_name":"TlsReloaded","app":{"name":"krabka-broker","version":"0.3.7"},"category_uid":6,"class_uid":6002,"device":{"type_id":1,"uid":"1"},"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"status_id":1,"time":7,"type_uid":600299}"#;
const V2_ACL_DENIAL: &[u8] = br#"{"activity_id":99,"activity_name":"Deny","actor":{"user":{"name":"bob","type":"MTls"}},"category_uid":3,"class_uid":3003,"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"operation":"Write","resources":[{"name":"secrets","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.2","port":4444},"status_id":2,"time":5,"type_uid":300399}"#;
const V2_ADMIN_CREATE: &[u8] = br#"{"activity_id":1,"actor":{"user":{"name":"admin","type":"MTls"}},"api":{"operation":"CreateTopics","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"resources":[{"name":"orders","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.3","port":9092},"status_id":1,"time":6,"type_uid":600301}"#;
const V2_ADMIN_UPDATE: &[u8] = br#"{"activity_id":3,"actor":{"user":{"name":"admin","type":"MTls"}},"api":{"operation":"IncrementalAlterConfigs","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"resources":[{"name":"orders","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.3","port":9092},"status_id":1,"time":6,"type_uid":600303}"#;
const V2_ADMIN_DELETE: &[u8] = br#"{"activity_id":4,"actor":{"user":{"name":"admin","type":"MTls"}},"api":{"operation":"DeleteTopics","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"resources":[{"name":"orders","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.3","port":9092},"status_id":1,"time":6,"type_uid":600304}"#;
const V2_ADMIN_UNKNOWN: &[u8] = br#"{"activity_id":0,"actor":{"user":{"name":"admin","type":"MTls"}},"api":{"operation":"DescribeCluster","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"resources":[{"name":"orders","type":"Topic"}],"src_endpoint":{"ip":"10.0.0.3","port":9092},"status_id":1,"time":6,"type_uid":600300}"#;
const V2_AUTHENTICATION: &[u8] = br#"{"activity_id":1,"activity_name":"Logon","actor":{"user":{"name":"alice","type":"SaslPlain"}},"auth_protocol":"SASL/PLAIN","category_uid":3,"class_uid":3002,"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"src_endpoint":{"ip":"10.0.0.1","port":51120},"status_detail":"authentication failed","status_id":2,"time":1700000000000,"type_uid":300201}"#;
const V2_PRIVILEGED: &[u8] = br#"{"activity_id":0,"actor":{"user":{"name":"User:alice","type":"MTls"}},"api":{"operation":"topic_freeze.applied","service":{"name":"kafka"}},"category_uid":6,"class_uid":6003,"metadata":{"log_version":"2","product":{"name":"krabka-broker","vendor_name":"Krabka","version":"0.3.7"},"version":"1.3.0"},"privileged_action":{"action":"topic_freeze","approver_set_fingerprint":"","counterparties":[],"key_id":"op-1","phase":"applied","proposal_id":"","signature":"deadbeef","signature_verified":true,"signed_at_ms":0,"target":"orders"},"src_endpoint":{"ip":"10.0.0.4","port":9092},"status_detail":"incident 42","status_id":1,"time":10,"type_uid":600300}"#;

/// The record bytes, the persisted form, are pinned for each record version:
/// a version 1 record is what an older broker wrote and an auditor's chain
/// still covers, and a version 2 record is what a broker writes from
/// `krabka.version` 3 on.
#[yare::parameterized(
    v1_broker_started = { lifecycle(LifecycleKind::BrokerStarted), OcsfVersion::V1, V1_BROKER_STARTED },
    v1_broker_stopping = { lifecycle(LifecycleKind::BrokerStopping), OcsfVersion::V1, V1_BROKER_STOPPING },
    v1_config_applied = { lifecycle(LifecycleKind::ConfigApplied), OcsfVersion::V1, V1_CONFIG_APPLIED },
    v1_tls_reloaded = { lifecycle(LifecycleKind::TlsReloaded), OcsfVersion::V1, V1_TLS_RELOADED },
    v1_acl_denial = { denied(5), OcsfVersion::V1, V1_ACL_DENIAL },
    v1_admin_create = { admin_named("CreateTopics"), OcsfVersion::V1, V1_ADMIN_CREATE },
    v1_admin_update = { admin_named("IncrementalAlterConfigs"), OcsfVersion::V1, V1_ADMIN_UPDATE },
    v1_admin_delete = { admin_named("DeleteTopics"), OcsfVersion::V1, V1_ADMIN_DELETE },
    v1_admin_unknown = { admin_named("DescribeCluster"), OcsfVersion::V1, V1_ADMIN_UNKNOWN },
    v1_authentication = { failed_logon(), OcsfVersion::V1, V1_AUTHENTICATION },
    v1_privileged_action = { signed_freeze(), OcsfVersion::V1, V1_PRIVILEGED },
    v2_broker_started = { lifecycle(LifecycleKind::BrokerStarted), OcsfVersion::V2, V2_BROKER_STARTED },
    v2_broker_stopping = { lifecycle(LifecycleKind::BrokerStopping), OcsfVersion::V2, V2_BROKER_STOPPING },
    v2_config_applied = { lifecycle(LifecycleKind::ConfigApplied), OcsfVersion::V2, V2_CONFIG_APPLIED },
    v2_tls_reloaded = { lifecycle(LifecycleKind::TlsReloaded), OcsfVersion::V2, V2_TLS_RELOADED },
    v2_acl_denial = { denied(5), OcsfVersion::V2, V2_ACL_DENIAL },
    v2_admin_create = { admin_named("CreateTopics"), OcsfVersion::V2, V2_ADMIN_CREATE },
    v2_admin_update = { admin_named("IncrementalAlterConfigs"), OcsfVersion::V2, V2_ADMIN_UPDATE },
    v2_admin_delete = { admin_named("DeleteTopics"), OcsfVersion::V2, V2_ADMIN_DELETE },
    v2_admin_unknown = { admin_named("DescribeCluster"), OcsfVersion::V2, V2_ADMIN_UNKNOWN },
    v2_authentication = { failed_logon(), OcsfVersion::V2, V2_AUTHENTICATION },
    v2_privileged_action = { signed_freeze(), OcsfVersion::V2, V2_PRIVILEGED },
)]
fn record_bytes_match_the_golden_fixture(event: AuditEvent, version: OcsfVersion, golden: &[u8]) {
    let written = AuditRecord::from_event(&event, &product(), version).value;
    check!(str::from_utf8(&written) == str::from_utf8(golden));
}

/// Every Kafka API the broker audits as an admin operation, with the OCSF
/// API Activity a version 2 record gives it.
#[yare::parameterized(
    create_topics = { "CreateTopics", 1 },
    create_partitions = { "CreatePartitions", 1 },
    create_acls = { "CreateAcls", 1 },
    create_delegation_token = { "CreateDelegationToken", 1 },
    alter_configs = { "AlterConfigs", 3 },
    incremental_alter_configs = { "IncrementalAlterConfigs", 3 },
    alter_client_quotas = { "AlterClientQuotas", 3 },
    alter_partition_reassignments = { "AlterPartitionReassignments", 3 },
    alter_user_scram_credentials = { "AlterUserScramCredentials", 3 },
    update_features = { "UpdateFeatures", 3 },
    elect_leaders = { "ElectLeaders", 3 },
    set_topic_freeze = { "SetTopicFreeze", 3 },
    add_raft_voter = { "AddRaftVoter", 3 },
    remove_raft_voter = { "RemoveRaftVoter", 3 },
    renew_delegation_token = { "RenewDelegationToken", 3 },
    expire_delegation_token = { "ExpireDelegationToken", 3 },
    delete_topics = { "DeleteTopics", 4 },
    delete_acls = { "DeleteAcls", 4 },
    delete_records = { "DeleteRecords", 4 },
    unregister_broker = { "UnregisterBroker", 4 },
    unregister_controller = { "UnregisterController", 4 },
    an_api_no_activity_covers = { "DescribeCluster", 0 },
    a_name_that_only_starts_like_one = { "CreateTopicsV2", 0 },
)]
fn admin_operation_v2_activity_follows_its_kafka_api(api: &str, activity_id: i64) {
    let j = to_ocsf(&admin_named(api), &product(), OcsfVersion::V2);
    check!(
        (j["activity_id"].clone(), j["type_uid"].clone())
            == (
                serde_json::json!(activity_id),
                serde_json::json!(600_300 + activity_id)
            )
    );
}
