//! The OCSF Application Lifecycle record (class 6002), which reports that a
//! broker started, began stopping, applied a configuration change, or reloaded
//! its TLS material. The record names the broker as the OCSF device rather
//! than a principal, because no client requested it.

use {
    super::{OcsfVersion, envelope::Envelope},
    crate::{event::LifecycleKind, ids::EpochMs},
    krabka_ids::NodeId,
    serde_json::json,
};

pub(super) fn ocsf_lifecycle(
    kind: LifecycleKind,
    node_id: NodeId,
    time_ms: EpochMs,
    envelope: &Envelope<'_>,
) -> serde_json::Value {
    // class 6002 Application Lifecycle.
    let class_uid = 6002_i64;
    let (activity_id, activity_name) = activity(kind, envelope.version);
    let mut record = json!({
        "class_uid": class_uid,
        "category_uid": 6,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": activity_name,
        "time": time_ms.0,
        "status_id": 1,
        "device": { "uid": node_id.0.to_string(), "type_id": 1 },
        "metadata": envelope.metadata(),
    });
    // OCSF 1.3.0 requires the application the event is about.
    if envelope.version == OcsfVersion::V2
        && let Some(fields) = record.as_object_mut()
    {
        fields.insert(
            "app".to_owned(),
            json!({
                "name": envelope.product.name,
                "version": envelope.product.version,
            }),
        );
    }
    record
}

/// The activity id and name of a `kind` event in record `version`.
///
/// Version 1 is the krabka 1.0.0 mapping, which gave a start activity 1
/// (OCSF's Install) and a configuration change or TLS reload activity 3 (OCSF's
/// Start). Version 2 uses OCSF's own ids: 3 Start, 4 Stop, and 99 Other for
/// the two events OCSF has no activity for, which keep their krabka names.
fn activity(kind: LifecycleKind, version: OcsfVersion) -> (i64, &'static str) {
    match (version, kind) {
        (OcsfVersion::V1, LifecycleKind::BrokerStarted) => (1, "BrokerStarted"),
        (OcsfVersion::V1, LifecycleKind::BrokerStopping) => (4, "BrokerStopping"),
        (OcsfVersion::V1, LifecycleKind::ConfigApplied) => (3, "ConfigApplied"),
        (OcsfVersion::V1, LifecycleKind::TlsReloaded) => (3, "TlsReloaded"),
        (OcsfVersion::V2, LifecycleKind::BrokerStarted) => (3, "Start"),
        (OcsfVersion::V2, LifecycleKind::BrokerStopping) => (4, "Stop"),
        (OcsfVersion::V2, LifecycleKind::ConfigApplied) => (99, "ConfigApplied"),
        (OcsfVersion::V2, LifecycleKind::TlsReloaded) => (99, "TlsReloaded"),
    }
}
