//! `AlterConfigs` (`api_key=33`) for topic and broker resources.
//!
//! The handler builds each resource's full override map from the request.
//! That map is the *complete* set of non-default values for the resource.
//! Topic configs use one authoritative `V1TopicConfig` record. Broker configs
//! use Kafka-compatible per-key `V1BrokerConfig` records, including tombstones
//! for overrides omitted from the replacement. An empty broker resource name
//! targets Kafka's cluster-wide default broker config.
//!
//! Controller-managed broker keys stand outside the replacement. The handler
//! rejects a request that names one, and the tombstone sweep leaves them in
//! place. See [`crate::config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS`].
//!
//! A topic resource has the same rule. KFC-9's
//! [`crate::config_keys::WRITE_FREEZE`] is synthesised for `DescribeConfigs`
//! and is never stored, so the handler rejects a request that names it. See
//! [`crate::config_keys::topic_scope::CONTROLLER_MANAGED_TOPIC_CONFIGS`].
//!
//! This file holds the wire entry point and the resource-type constants. The
//! per-resource work lives in `resource`, and the record builders it
//! dispatches to live in `topic_configs` and `broker_configs`.

use bytes::Bytes;
use krabka_protocol::{
    Decode, UnknownTaggedFields,
    owned::{
        alter_configs_request::AlterConfigsRequest,
        alter_configs_response::{AlterConfigsResourceResponse, AlterConfigsResponse},
    },
};

mod broker_configs;
mod resource;
mod topic_configs;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::resource::process_resource;
use crate::{broker::Broker, error::BrokerError};

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;

#[tracing::instrument(
    name = "handle_alter_configs",
    level = "info",
    skip_all,
    fields(api = "AlterConfigs", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = AlterConfigsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();
    let mut responses: Vec<AlterConfigsResourceResponse> = Vec::with_capacity(req.resources.len());
    let validate_only = req.validate_only;
    let mut audited: Vec<krabka_audit::AuditResource> = Vec::new();

    for resource in req.resources {
        let named = audit_resources_for(&resource);
        let response = process_resource(broker, &image, ctx, resource, validate_only).await;
        // A `--dry-run` request stores nothing, so it changed no resource.
        if response.error_code == crate::codes::NONE && !validate_only {
            audited.extend(named);
        }
        responses.push(response);
    }
    crate::handlers::audit_admin_success(broker.audit_log.as_ref(), ctx, "AlterConfigs", audited);

    let resp = AlterConfigsResponse {
        responses,
        throttle_time_ms: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    crate::handlers::encode_response(&resp, version)
}

/// Names the audited resource and the keys the replacement sets on it.
///
/// The values never reach the audit record. A config value can be a password
/// or a key store path, and the record only has to say who changed what.
fn audit_resources_for(
    resource: &krabka_protocol::owned::alter_configs_request::AlterConfigsResource,
) -> Vec<krabka_audit::AuditResource> {
    let mut out = vec![crate::handlers::audit_resource(
        config_resource_type(resource.resource_type),
        resource.resource_name.clone(),
    )];
    out.extend(
        resource
            .configs
            .iter()
            .map(|config| crate::handlers::audit_resource("ConfigKey", config.name.clone())),
    );
    out
}

/// The audit `resource_type` for a KIP-133 config resource-type discriminant.
pub(super) fn config_resource_type(resource_type: i8) -> &'static str {
    match resource_type {
        RESOURCE_TYPE_TOPIC => "Topic",
        RESOURCE_TYPE_BROKER => "Broker",
        8 => "BrokerLogger",
        16 => "ClientMetrics",
        32 => "Group",
        _ => "Unknown",
    }
}
