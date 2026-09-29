//! `IncrementalAlterConfigs` (`api_key=44`). Same target as `AlterConfigs`
//! (a single `V1TopicConfig` record per resource) but the wire request
//! carries per-key operations (SET/DELETE/APPEND/SUBTRACT). The handler
//! reads the current overrides from the metadata image, applies the ops,
//! validates the result, and submits the merged map.
//!
//! Supported operations:
//! - SET (0): set or replace
//! - DELETE (1): remove
//! - APPEND (2) and SUBTRACT (3) merge items into, or out of, a key Kafka
//!   types `LIST`, starting from its default when the resource does not hold
//!   it. Any other key refuses them with `INVALID_CONFIG`, in Kafka's words.
//!
//! `BROKER_LOGGER` (8) is the one resource type that stages no metadata
//! record at all: it retargets this node's live `tracing` filter and nothing
//! else, which is what a JVM broker does with its log4j2 context. See
//! [`broker_logger_scope`].
//!
//! A controller-managed key is refused whatever the operation is. KFC-9's
//! [`crate::config_keys::WRITE_FREEZE`] is synthesised for `DescribeConfigs`
//! and is never stored, so a DELETE of it is an attempt to lift the freeze and
//! gets the same refusal a SET gets. See
//! [`crate::config_keys::topic_scope::CONTROLLER_MANAGED_TOPIC_CONFIGS`].
//!
//! This file holds the request entry point and the per-resource dispatch. Each
//! resource type has its own submodule that owns the key whitelist, the value
//! validation, and the metadata record that it stages.

use bytes::Bytes;
use krabka_metadata::{AclOperation, MetadataImage, MetadataRecord, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        incremental_alter_configs_request::{AlterConfigsResource, IncrementalAlterConfigsRequest},
        incremental_alter_configs_response::{
            AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
        },
    },
};
use krabka_raft::RaftError;

mod broker_logger_scope;
mod broker_scope;
mod client_metrics_scope;
mod group_scope;
#[cfg(test)]
mod test_support;
mod topic_scope;

use self::{
    broker_logger_scope::handle_broker_logger_scoped, broker_scope::handle_broker_scoped,
    client_metrics_scope::handle_client_metrics_scoped, group_scope::handle_group_scoped,
    topic_scope::topic_config_record,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
};

const RESOURCE_TYPE_TOPIC: i8 = 2;
const RESOURCE_TYPE_BROKER: i8 = 4;
const RESOURCE_TYPE_BROKER_LOGGER: i8 = 8;
const RESOURCE_TYPE_CLIENT_METRICS: i8 = 16;
const RESOURCE_TYPE_GROUP: i8 = 32;
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;
const OP_APPEND: i8 = 2;
const OP_SUBTRACT: i8 = 3;

#[tracing::instrument(
    name = "handle_incremental_alter_configs",
    level = "info",
    skip_all,
    fields(api = "IncrementalAlterConfigs", version, req_bytes = req_bytes.len()),
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
    let req = IncrementalAlterConfigsRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();
    let mut responses: Vec<AlterConfigsResourceResponse> = Vec::with_capacity(req.resources.len());
    let validate_only = req.validate_only;
    let mut audited: Vec<krabka_audit::AuditResource> = Vec::new();
    let duplicate_flags = duplicate_resource_flags(&req.resources);

    for (resource, is_duplicate) in req.resources.into_iter().zip(duplicate_flags) {
        // The keys, never the values: a config value can be a password or a
        // key store path, and the record only has to say who changed what.
        let named: Vec<krabka_audit::AuditResource> =
            std::iter::once(crate::handlers::audit_resource(
                super::alter_configs::config_resource_type(resource.resource_type),
                resource.resource_name.clone(),
            ))
            .chain(
                resource.configs.iter().map(|config| {
                    crate::handlers::audit_resource("ConfigKey", config.name.clone())
                }),
            )
            .collect();
        let response =
            process_resource(broker, &image, ctx, resource, validate_only, is_duplicate).await;
        // A `--dry-run` request stores nothing, so it changed no resource.
        if response.error_code == codes::NONE && !validate_only {
            audited.extend(named);
        }
        responses.push(response);
    }
    crate::handlers::audit_admin_success(
        broker.audit_log.as_ref(),
        ctx,
        "IncrementalAlterConfigs",
        audited,
    );

    let resp = IncrementalAlterConfigsResponse {
        responses,
        throttle_time_ms: 0,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

/// Kafka's `ConfigAdminManager.preprocess` rejects a request that names the
/// same `(resource_type, resource_name)` pair more than once, on every row
/// that names it. This computes that flag for each resource in request order.
fn duplicate_resource_flags(resources: &[AlterConfigsResource]) -> Vec<bool> {
    let mut counts: std::collections::HashMap<(i8, &str), usize> = std::collections::HashMap::new();
    for resource in resources {
        *counts
            .entry((resource.resource_type, resource.resource_name.as_str()))
            .or_insert(0) += 1;
    }
    resources
        .iter()
        .map(|resource| counts[&(resource.resource_type, resource.resource_name.as_str())] > 1)
        .collect()
}

/// Kafka's `ConfigAdminManager.preprocess` shape checks, which run before any
/// authorization: a resource named twice, a key named twice within one
/// resource, and a null value on any operation but DELETE.
fn validate_resource_shape(
    resource: &AlterConfigsResource,
    is_duplicate: bool,
) -> Result<(), (i16, String)> {
    if is_duplicate {
        return Err((
            codes::INVALID_REQUEST,
            "Each resource must appear at most once.".into(),
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    if resource
        .configs
        .iter()
        .any(|config| !seen.insert(config.name.as_str()))
    {
        return Err((
            codes::INVALID_REQUEST,
            "Error due to duplicate config keys".into(),
        ));
    }
    let null_names: Vec<&str> = resource
        .configs
        .iter()
        .filter(|config| config.config_operation != OP_DELETE && config.value.is_none())
        .map(|config| config.name.as_str())
        .collect();
    if !null_names.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            format!("Null value not supported for : {}", null_names.join(", ")),
        ));
    }
    Ok(())
}

async fn process_resource(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> AlterConfigsResourceResponse {
    let mut out = AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_code: codes::NONE,
        error_message: None,
        ..Default::default()
    };

    // ── Kafka validates the request shape before it authorizes ──
    if let Err((code, message)) = validate_resource_shape(&resource, is_duplicate) {
        out.error_code = code;
        out.error_message = Some(message);
        return out;
    }

    // ── ACL preamble ────────────────────────────────────────
    // `ControllerApis.authorizeAlterResource`, and `preprocess` for the
    // broker types: Topic → AlterConfigs on Topic(name), Group → on
    // Group(name), Broker, BrokerLogger and ClientMetrics → on the cluster.
    // `preprocess` refuses any other resource type before it authorizes.
    let (acl_type, acl_name, denied_code, denied_message) = match resource.resource_type {
        RESOURCE_TYPE_TOPIC => (
            ResourceType::Topic,
            resource.resource_name.as_str(),
            codes::TOPIC_AUTHORIZATION_FAILED,
            "Topic authorization failed.",
        ),
        RESOURCE_TYPE_GROUP => (
            ResourceType::Group,
            resource.resource_name.as_str(),
            codes::GROUP_AUTHORIZATION_FAILED,
            "Group authorization failed.",
        ),
        RESOURCE_TYPE_BROKER | RESOURCE_TYPE_BROKER_LOGGER | RESOURCE_TYPE_CLIENT_METRICS => (
            ResourceType::Cluster,
            crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
        ),
        other => {
            out.error_code = codes::INVALID_REQUEST;
            out.error_message = Some(format!("Unknown resource type {other}"));
            return out;
        }
    };
    let acl_result = broker.config.authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: acl_type,
            resource_name: acl_name,
            operation: AclOperation::AlterConfigs,
        },
    );
    if acl_result == AuthorizationResult::Deny {
        out.error_code = denied_code;
        out.error_message = Some(denied_message.into());
        return out;
    }

    // After ACL pass: dispatch by resource type.
    let mut to_submit: Vec<MetadataRecord> = Vec::new();

    match resource.resource_type {
        RESOURCE_TYPE_TOPIC => {
            match topic_config_record(
                &resource,
                image,
                &broker.config.topic_policy,
                broker.config.remote_storage_backend.is_some(),
                broker.config.features.unstable_api_versions,
            ) {
                Ok(record) => to_submit.push(record),
                Err((code, message)) => {
                    out.error_code = code;
                    out.error_message = Some(message);
                    return out;
                }
            }
        }
        RESOURCE_TYPE_BROKER => {
            handle_broker_scoped(
                &resource,
                image,
                krabka_metadata::NodeId(broker.config.node_id.0),
                (
                    &broker.config.broker_log_dirs(),
                    broker.config.features.unstable_api_versions,
                ),
                &mut out,
                &mut to_submit,
            );
            if out.error_code != codes::NONE {
                return out;
            }
        }
        RESOURCE_TYPE_BROKER_LOGGER => {
            // Node-local and never persisted, so this returns without
            // reaching `submit_change` whatever the outcome.
            handle_broker_logger_scoped(
                &resource,
                broker.config.broker_id,
                &broker.config.log_levels,
                validate_only,
                &mut out,
            );
            return out;
        }
        RESOURCE_TYPE_CLIENT_METRICS => {
            handle_client_metrics_scoped(&resource, image, &mut out, &mut to_submit);
            if out.error_code != codes::NONE {
                return out;
            }
        }
        RESOURCE_TYPE_GROUP => {
            handle_group_scoped(
                &resource,
                image,
                &broker.config.streams_group,
                broker.config.features.unstable_api_versions,
                &mut out,
                &mut to_submit,
            );
            if out.error_code != codes::NONE {
                return out;
            }
        }
        other => unreachable!("resource type {other} passed the ACL dispatch"),
    }

    if to_submit.iter().any(|record| match record {
        MetadataRecord::V1TopicConfig(config) => {
            resource
                .configs
                .iter()
                .any(|item| item.name == crate::config_keys::MIN_INSYNC_REPLICAS)
                && image
                    .topic_config(&config.topic)
                    .and_then(|current| current.get(crate::config_keys::MIN_INSYNC_REPLICAS))
                    != config
                        .overrides
                        .get(crate::config_keys::MIN_INSYNC_REPLICAS)
        }
        MetadataRecord::V1BrokerConfig(config) => {
            config.node_id == krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
                && config.config_name == crate::config_keys::MIN_INSYNC_REPLICAS
                && image
                    .broker_config(config.node_id)
                    .and_then(|current| current.get(&config.config_name))
                    != config.config_value.as_ref()
        }
        _ => false,
    }) {
        let topic = (resource.resource_type == RESOURCE_TYPE_TOPIC)
            .then_some(resource.resource_name.as_str());
        to_submit.extend(crate::config_keys::clear_elr_records(image, topic));
    }

    if validate_only {
        // Validation pass already happened above (per-config loop). Nothing
        // to submit; the response already carries the per-resource result
        // (NONE if all configs validated, INVALID_CONFIG with reason on any
        // rejection). This matches Apache Kafka's --dry-run behavior.
        return out;
    }
    match broker.controller.submit_change(to_submit).await {
        Ok(_) => {}
        Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
            out.error_code = codes::NOT_CONTROLLER;
        }
        Err(e) => {
            tracing::error!(error = %e, "IncrementalAlterConfigs submit_change failed");
            out.error_code = codes::UNKNOWN_SERVER_ERROR;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;

    use super::*;

    fn config(name: &str, operation: i8, value: Option<&str>) -> AlterableConfig {
        AlterableConfig {
            name: name.into(),
            config_operation: operation,
            value: value.map(str::to_owned),
            ..Default::default()
        }
    }

    /// Kafka's `ConfigAdminManager.preprocess` shape checks, which answer
    /// before any authorization. Each row is a request's resources and the
    /// outcome for each.
    #[test]
    fn the_request_shape_is_checked_the_way_kafka_preprocesses_it() {
        let resource =
            |resource_type: i8, name: &str, configs: Vec<AlterableConfig>| AlterConfigsResource {
                resource_type,
                resource_name: name.into(),
                configs,
                ..Default::default()
            };
        let duplicate = Err((
            codes::INVALID_REQUEST,
            "Each resource must appear at most once.".to_owned(),
        ));
        let cases = [
            (
                vec![
                    resource(RESOURCE_TYPE_TOPIC, "t", vec![]),
                    resource(RESOURCE_TYPE_TOPIC, "t", vec![]),
                    resource(RESOURCE_TYPE_GROUP, "t", vec![]),
                ],
                vec![duplicate.clone(), duplicate, Ok(())],
            ),
            (
                vec![resource(
                    RESOURCE_TYPE_TOPIC,
                    "t",
                    vec![
                        config("retention.ms", OP_SET, Some("1")),
                        config("retention.ms", OP_DELETE, None),
                    ],
                )],
                vec![Err((
                    codes::INVALID_REQUEST,
                    "Error due to duplicate config keys".to_owned(),
                ))],
            ),
            (
                vec![resource(
                    RESOURCE_TYPE_GROUP,
                    "g",
                    vec![
                        config("a", OP_SET, None),
                        config("b", OP_DELETE, None),
                        config("c", OP_APPEND, None),
                    ],
                )],
                vec![Err((
                    codes::INVALID_REQUEST,
                    "Null value not supported for : a, c".to_owned(),
                ))],
            ),
        ];
        for (resources, want) in cases {
            let flags = duplicate_resource_flags(&resources);
            let got: Vec<Result<(), (i16, String)>> = resources
                .iter()
                .zip(flags)
                .map(|(resource, is_duplicate)| validate_resource_shape(resource, is_duplicate))
                .collect();
            check!(got == want);
        }
    }
}
