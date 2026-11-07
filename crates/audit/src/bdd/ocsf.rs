//! Steps for `tests/features/ocsf_mapping`: how an audit event is recorded as
//! an OCSF 1.3.0 record.
//!
//! Every event goes through [`AuditRecord::from_event`], the path that reaches
//! the audit topic, and the steps read the record back as JSON.

use {
    crate::{
        event::{AuditEvent, AuditOutcome, LifecycleKind},
        ocsf::{OcsfVersion, ProductInfo},
        sink::AuditRecord,
        test_support::{admin, authentication, denied, privileged_freeze},
    },
    krabka_bdd::Given,
    krabka_metadata::krabka_version::KrabkaVersion,
    serde::Deserialize,
};

const RECORDS: &str = "When the broker records an audit event";
const SIGNS_FREEZE: &str = "Given an operator signed a topic freeze before the broker applied it";

/// When the broker applied the topic freeze, in epoch milliseconds.
const FREEZE_APPLIED_AT_MS: i64 = 1_700_000_005_000;

/// When the operator signed the topic freeze: before the broker applied it.
const FREEZE_SIGNED_AT_MS: i64 = 1_700_000_000_000;

/// Records audit events the way the broker's audit writer does.
///
/// The recorder writes the record version of the finalized `krabka.version`,
/// level 0 until a step finalizes another.
struct OcsfRecorder {
    product: ProductInfo,
    version: OcsfVersion,
}

impl OcsfRecorder {
    fn new() -> Self {
        Self {
            product: ProductInfo {
                vendor_name: "Krabka".into(),
                name: "krabka-broker".into(),
                version: "1.0.0".into(),
            },
            version: OcsfVersion::V1,
        }
    }

    /// The OCSF JSON of the record the broker writes for `event`.
    fn record(&self, event: &AuditEvent) -> serde_json::Value {
        let record = AuditRecord::from_event(event, &self.product, self.version);
        serde_json::from_slice(&record.value).unwrap()
    }
}

/// A `CreateTopics` admin operation that ended in `outcome`.
fn admin_operation(outcome: AuditOutcome) -> AuditEvent {
    match admin(1_700_000_000_000) {
        AuditEvent::AdminOperation {
            principal,
            source,
            operation,
            resources,
            time_ms,
            ..
        } => AuditEvent::AdminOperation {
            outcome,
            principal,
            source,
            operation,
            resources,
            time_ms,
        },
        other => other,
    }
}

/// A successful admin operation of the Kafka API `api`.
fn admin_request(api: String) -> AuditEvent {
    match admin(1_700_000_000_000) {
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
            operation: api,
            resources,
            time_ms,
        },
        other => other,
    }
}

/// A lifecycle event of `kind` on broker 1.
fn lifecycle(kind: LifecycleKind) -> AuditEvent {
    AuditEvent::Lifecycle {
        kind,
        node_id: 1,
        time_ms: 1_700_000_000_000,
    }
}

/// A topic freeze an operator signed, as the broker applied it.
#[derive(Debug, Clone)]
struct SignedFreeze {
    signature: Vec<u8>,
    signed_at_ms: i64,
    applied_at_ms: i64,
}

impl SignedFreeze {
    fn event(&self) -> AuditEvent {
        privileged_freeze(
            self.signature.clone(),
            self.signed_at_ms,
            self.applied_at_ms,
        )
    }
}

/// The ids that route a record by OCSF class.
#[derive(Debug, PartialEq, Eq, Deserialize)]
struct OcsfClass {
    class_uid: i64,
    category_uid: i64,
    activity_id: i64,
    type_uid: i64,
}

/// The application a lifecycle record names.
#[derive(Debug, PartialEq, Eq, Deserialize)]
struct Application {
    name: String,
    version: String,
}

/// The request an auditor re-verifies a privileged action's signature
/// against.
#[derive(Debug, PartialEq, Eq)]
struct SignedRequest {
    signature: String,
    signed_at_ms: i64,
    applied_at_ms: i64,
}

struct OcsfScenario {
    recorder: OcsfRecorder,
    freeze: Given<SignedFreeze>,
    record: Given<serde_json::Value>,
}

#[rstest::fixture]
fn ocsf() -> OcsfScenario {
    OcsfScenario {
        recorder: OcsfRecorder::new(),
        freeze: Given::new("freeze", SIGNS_FREEZE),
        record: Given::new("record", RECORDS),
    }
}

impl OcsfScenario {
    fn records(&mut self, event: &AuditEvent) {
        let record = self.recorder.record(event);
        self.record.set(record);
    }
}

#[rstest_bdd_macros::when("the broker records an authentication")]
fn records_authentication(ocsf: &mut OcsfScenario) {
    ocsf.records(&authentication(51120, None, 1_700_000_000_000));
}

#[rstest_bdd_macros::when("the broker records an authorization denial")]
fn records_authorization_denial(ocsf: &mut OcsfScenario) {
    ocsf.records(&denied(1_700_000_000_000));
}

#[rstest_bdd_macros::when("the broker records an admin operation")]
fn records_admin_operation(ocsf: &mut OcsfScenario) {
    ocsf.records(&admin_operation(AuditOutcome::Success));
}

#[rstest_bdd_macros::when("the broker records a privileged action")]
fn records_privileged_action(ocsf: &mut OcsfScenario) {
    ocsf.records(&privileged_freeze(
        vec![0xab, 0xcd],
        FREEZE_SIGNED_AT_MS,
        FREEZE_APPLIED_AT_MS,
    ));
}

#[rstest_bdd_macros::when("the broker records a broker start")]
fn records_broker_start(ocsf: &mut OcsfScenario) {
    ocsf.records(&lifecycle(LifecycleKind::BrokerStarted));
}

#[rstest_bdd_macros::when("the broker records a broker stop")]
fn records_broker_stop(ocsf: &mut OcsfScenario) {
    ocsf.records(&lifecycle(LifecycleKind::BrokerStopping));
}

#[rstest_bdd_macros::when("the broker records a configuration change")]
fn records_configuration_change(ocsf: &mut OcsfScenario) {
    ocsf.records(&lifecycle(LifecycleKind::ConfigApplied));
}

#[rstest_bdd_macros::when("the broker records a TLS reload")]
fn records_tls_reload(ocsf: &mut OcsfScenario) {
    ocsf.records(&lifecycle(LifecycleKind::TlsReloaded));
}

#[rstest_bdd_macros::when("the broker records a successful {api} request")]
fn records_successful_request(ocsf: &mut OcsfScenario, api: String) {
    ocsf.records(&admin_request(api));
}

#[rstest_bdd_macros::given("the cluster has finalized krabka.version {level}")]
fn cluster_has_finalized(ocsf: &mut OcsfScenario, level: i16) {
    ocsf.recorder.version = OcsfVersion::at(KrabkaVersion::new(level));
}

#[rstest_bdd_macros::when("the broker records an admin operation that ended in {outcome}")]
fn records_admin_operation_with_outcome(ocsf: &mut OcsfScenario, outcome: AuditOutcome) {
    ocsf.records(&admin_operation(outcome));
}

#[rstest_bdd_macros::given("an operator signed a topic freeze before the broker applied it")]
fn operator_signed_freeze(ocsf: &mut OcsfScenario) {
    ocsf.freeze.set(SignedFreeze {
        signature: vec![0xde, 0xad, 0xbe, 0xef, 0x0a],
        signed_at_ms: FREEZE_SIGNED_AT_MS,
        applied_at_ms: FREEZE_APPLIED_AT_MS,
    });
}

#[rstest_bdd_macros::when("the broker records the applied freeze")]
fn records_applied_freeze(ocsf: &mut OcsfScenario) {
    let event = ocsf.freeze.get().event();
    ocsf.records(&event);
}

/// OCSF fixes a record's type id as its class id times 100 plus its activity
/// id.
#[rstest_bdd_macros::then(
    "the record has OCSF class {class_uid}, category {category_uid} and activity {activity_id}, \
     with the type id they imply"
)]
fn record_has_class(ocsf: &OcsfScenario, class_uid: i64, category_uid: i64, activity_id: i64) {
    let recorded: OcsfClass = serde_json::from_value(ocsf.record.get().clone()).unwrap();
    assert2::assert!(
        recorded
            == OcsfClass {
                class_uid,
                category_uid,
                activity_id,
                type_uid: class_uid * 100 + activity_id,
            }
    );
}

#[rstest_bdd_macros::then("the record has OCSF status {status_id}")]
fn record_has_status(ocsf: &OcsfScenario, status_id: i64) {
    assert2::assert!(ocsf.record.get()["status_id"] == serde_json::json!(status_id));
}

#[rstest_bdd_macros::then("the record names the operation {operation}")]
fn record_names_operation(ocsf: &OcsfScenario, operation: String) {
    assert2::assert!(ocsf.record.get()["api"]["operation"] == serde_json::json!(operation));
}

#[rstest_bdd_macros::then(
    "the record carries the signature as lowercase hex with the time it was signed"
)]
fn record_carries_signature(ocsf: &OcsfScenario) {
    let record = ocsf.record.get();
    let freeze = ocsf.freeze.get();
    let carried = SignedRequest {
        signature: record["privileged_action"]["signature"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        signed_at_ms: record["privileged_action"]["signed_at_ms"]
            .as_i64()
            .unwrap_or_default(),
        applied_at_ms: record["time"].as_i64().unwrap_or_default(),
    };
    assert2::assert!(
        carried
            == SignedRequest {
                signature: hex::encode(&freeze.signature),
                signed_at_ms: freeze.signed_at_ms,
                applied_at_ms: freeze.applied_at_ms,
            }
    );
}

#[rstest_bdd_macros::then("the record declares OCSF schema version {version}")]
fn record_declares_schema(ocsf: &OcsfScenario, version: String) {
    assert2::assert!(ocsf.record.get()["metadata"]["version"] == serde_json::json!(version));
}

/// OCSF 1.3.0 Application Lifecycle requires the `app` the event is about.
#[rstest_bdd_macros::then("the record names the application {name} at version {version}")]
fn record_names_application(ocsf: &OcsfScenario, name: String, version: String) {
    check_application(ocsf, Some(&Application { name, version }));
}

#[rstest_bdd_macros::then("the record names no application")]
fn record_names_no_application(ocsf: &OcsfScenario) { check_application(ocsf, None); }

fn check_application(ocsf: &OcsfScenario, expected: Option<&Application>) {
    let app: Option<Application> =
        serde_json::from_value(ocsf.record.get()["app"].clone()).unwrap();
    assert2::assert!(app.as_ref() == expected);
}

/// OCSF `metadata.log_version`, the event log schema version: absent on a
/// version 1 record.
#[rstest_bdd_macros::then("the record declares log version {log_version}")]
fn record_declares_log_version(ocsf: &OcsfScenario, log_version: String) {
    assert2::assert!(
        ocsf.record.get()["metadata"].get("log_version") == Some(&serde_json::json!(log_version))
    );
}

#[rstest_bdd_macros::then("the record declares no log version")]
fn record_declares_no_log_version(ocsf: &OcsfScenario) {
    assert2::assert!(ocsf.record.get()["metadata"].get("log_version").is_none());
}

krabka_bdd::scenarios!("tests/features/ocsf_mapping", fixtures = [ocsf: OcsfScenario]);
