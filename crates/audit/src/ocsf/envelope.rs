//! The parts of an OCSF record that do not vary with its class: the `metadata`
//! block that stamps the schema version, the record version and the emitting
//! product, and the `status_id` code that reports whether the audited
//! operation succeeded.

use {
    super::{OcsfVersion, ProductInfo},
    crate::event::{AuditOutcome, AuditPrincipal},
    serde_json::json,
};

const SCHEMA_VERSION: &str = "1.3.0";

/// The `metadata.log_version` of a version 2 record. A version 1 record
/// carries none: the field's absence is what marks it as version 1.
const LOG_VERSION_V2: &str = "2";

/// What every record of one serialization shares: the product that emits it
/// and the record version it is written in.
pub(super) struct Envelope<'a> {
    pub(super) product: &'a ProductInfo,
    pub(super) version: OcsfVersion,
}

impl Envelope<'_> {
    /// The record's `metadata` object. A version 2 record also states its
    /// `log_version`.
    pub(super) fn metadata(&self) -> serde_json::Value {
        let mut metadata = json!({
            "version": SCHEMA_VERSION,
            "product": {
                "vendor_name": self.product.vendor_name,
                "name": self.product.name,
                "version": self.product.version,
            }
        });
        if self.version == OcsfVersion::V2
            && let Some(fields) = metadata.as_object_mut()
        {
            fields.insert("log_version".to_owned(), json!(LOG_VERSION_V2));
        }
        metadata
    }
}

pub(super) fn status_id(outcome: AuditOutcome) -> i64 {
    match outcome {
        AuditOutcome::Success => 1,
        AuditOutcome::Failure => 2,
    }
}

pub(super) fn user(principal: &AuditPrincipal) -> serde_json::Value {
    json!({ "name": principal.name, "type": principal.auth_method })
}
