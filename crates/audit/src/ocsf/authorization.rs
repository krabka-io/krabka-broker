//! The OCSF Authorize Session record (class 3003), which reports an ACL check
//! that denied a principal access to a named resource. Only denials reach this
//! module, so the record is always a failure.

use {
    super::{
        OcsfVersion,
        envelope::{Envelope, user},
    },
    crate::event::{AuditEndpoint, AuditPrincipal},
    serde_json::json,
};

pub(super) fn ocsf_authorization_denied(
    principal: &AuditPrincipal,
    source: &AuditEndpoint,
    resource_type: &str,
    resource_name: &str,
    operation: &str,
    time_ms: i64,
    envelope: &Envelope<'_>,
) -> serde_json::Value {
    // class 3003 Authorize Session. OCSF 1.3.0 has no Deny activity: it
    // defines 1 Assign Privileges and 2 Assign Groups, so a version 2 denial
    // is 99 Other, named Deny. Version 1 is the krabka 1.0.0 record, which
    // wrote 2.
    let class_uid = 3003_i64;
    let activity_id = match envelope.version {
        OcsfVersion::V1 => 2_i64,
        OcsfVersion::V2 => 99,
    };
    json!({
        "class_uid": class_uid,
        "category_uid": 3,
        "type_uid": class_uid * 100 + activity_id,
        "activity_id": activity_id,
        "activity_name": "Deny",
        "time": time_ms,
        "status_id": 2,
        "operation": operation,
        "actor": { "user": user(principal) },
        "src_endpoint": { "ip": source.ip, "port": source.port },
        "resources": [ { "type": resource_type, "name": resource_name } ],
        "metadata": envelope.metadata(),
    })
}
