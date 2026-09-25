//! The per-resource `AlterConfigs` work: the Kafka `preprocess` shape checks,
//! the authorization preamble, the dispatch to the resource's record
//! builder, and the metadata submit.
//!
//! One resource's outcome never depends on another's beyond whether it is a
//! duplicate resource reference (computed for the whole request up front), so
//! this module turns a single `AlterConfigsResource` into the one response
//! row it earns and the request entry point only loops over it.
//!
//! Kafka's `ConfigAdminManager.preprocess` validates the request shape before
//! `ControllerApis.authorizeAlterResource` runs, so a malformed row —
//! a resource named twice, a config key named twice, or a config with no
//! value — earns `INVALID_REQUEST` whether or not the principal may touch the
//! resource. That check runs first here too.

use std::collections::BTreeSet;

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        alter_configs_request::AlterConfigsResource,
        alter_configs_response::AlterConfigsResourceResponse,
    },
};
use krabka_raft::RaftError;

use super::{
    RESOURCE_TYPE_BROKER, RESOURCE_TYPE_CLIENT_METRICS, RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
    broker_configs::broker_config_records, client_metrics_configs::client_metrics_config_record,
    group_configs::group_config_record, topic_configs::topic_config_record,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

/// The Kafka `ConfigAdminManager.preprocess` shape checks, run before
/// authorization: a resource named more than once in the request, a config
/// key named more than once within a resource, and a config with no value
/// (legacy `AlterConfigs` never deletes by omitting a value the way
/// `IncrementalAlterConfigs`' DELETE operation does).
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
    let mut seen_keys = BTreeSet::new();
    if resource
        .configs
        .iter()
        .any(|cfg| !seen_keys.insert(cfg.name.as_str()))
    {
        return Err((
            codes::INVALID_REQUEST,
            "Error due to duplicate config keys".into(),
        ));
    }
    let null_names: Vec<&str> = resource
        .configs
        .iter()
        .filter(|cfg| cfg.value.is_none())
        .map(|cfg| cfg.name.as_str())
        .collect();
    if !null_names.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            format!("Null value not supported for : {}", null_names.join(",")),
        ));
    }
    Ok(())
}

pub(super) async fn process_resource(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
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
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };

    // ── Kafka validates the request shape before it authorizes ──
    if let Err((code, message)) = validate_resource_shape(&resource, is_duplicate) {
        out.error_code = code;
        out.error_message = Some(message);
        return out;
    }

    // ── ACL preamble ────────────────────────────────────────
    // Per-resource authorization based on resource_type, matching
    // `ControllerApis.authorizeAlterResource` for the types it handles
    // (Topic, ClientMetrics, Group) and the legacy in-broker path for Broker.
    // Topic (2)          → AlterConfigs on Topic(resource_name)     → TOPIC_AUTHORIZATION_FAILED, "Topic authorization failed."
    // Broker (4)         → AlterConfigs on Cluster("kafka-cluster") → CLUSTER_AUTHORIZATION_FAILED, no message (legacy in-broker path).
    // ClientMetrics (16) → AlterConfigs on Cluster("kafka-cluster") → CLUSTER_AUTHORIZATION_FAILED, "Cluster authorization failed."
    // Group (32)         → AlterConfigs on Group(resource_name)     → GROUP_AUTHORIZATION_FAILED, "Group authorization failed."
    // Other resource types are unsupported; Kafka assigns no distinct code for
    // that, so they get INVALID_REQUEST.
    let acl_result = match resource.resource_type {
        RESOURCE_TYPE_TOPIC => broker.config.authorizer.authorize(
            image,
            &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::Topic,
                resource_name: &resource.resource_name,
                operation: AclOperation::AlterConfigs,
            },
        ),
        RESOURCE_TYPE_BROKER | RESOURCE_TYPE_CLIENT_METRICS => broker.config.authorizer.authorize(
            image,
            &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: AclOperation::AlterConfigs,
            },
        ),
        RESOURCE_TYPE_GROUP => broker.config.authorizer.authorize(
            image,
            &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::Group,
                resource_name: &resource.resource_name,
                operation: AclOperation::AlterConfigs,
            },
        ),
        _ => {
            out.error_code = codes::INVALID_REQUEST;
            out.error_message = Some(format!(
                "resource_type={} not supported",
                resource.resource_type
            ));
            return out;
        }
    };
    if acl_result == AuthorizationResult::Deny {
        out.error_code = match resource.resource_type {
            RESOURCE_TYPE_TOPIC => codes::TOPIC_AUTHORIZATION_FAILED,
            RESOURCE_TYPE_GROUP => codes::GROUP_AUTHORIZATION_FAILED,
            _ => codes::CLUSTER_AUTHORIZATION_FAILED,
        };
        out.error_message = match resource.resource_type {
            RESOURCE_TYPE_TOPIC => Some("Topic authorization failed.".into()),
            RESOURCE_TYPE_GROUP => Some("Group authorization failed.".into()),
            RESOURCE_TYPE_CLIENT_METRICS => Some("Cluster authorization failed.".into()),
            RESOURCE_TYPE_BROKER => None,
            _ => unreachable!("resource type passed ACL dispatch"),
        };
        return out;
    }

    let mut records = match resource.resource_type {
        RESOURCE_TYPE_TOPIC => {
            match topic_config_record(&resource, image, &broker.config.topic_policy) {
                Ok(record) => vec![record],
                Err((code, message)) => {
                    out.error_code = code;
                    out.error_message = Some(message);
                    return out;
                }
            }
        }
        RESOURCE_TYPE_BROKER => match broker_config_records(&resource, image) {
            Ok(records) => records,
            Err((code, message)) => {
                out.error_code = code;
                out.error_message = Some(message);
                return out;
            }
        },
        RESOURCE_TYPE_GROUP => match group_config_record(&resource, &broker.config.streams_group) {
            Ok(record) => vec![record],
            Err((code, message)) => {
                out.error_code = code;
                out.error_message = Some(message);
                return out;
            }
        },
        RESOURCE_TYPE_CLIENT_METRICS => match client_metrics_config_record(&resource) {
            Ok(record) => vec![record],
            Err((code, message)) => {
                out.error_code = code;
                out.error_message = Some(message);
                return out;
            }
        },
        _ => unreachable!("resource type passed ACL dispatch"),
    };
    let min_isr_changed = records.iter().any(|record| match record {
        krabka_metadata::MetadataRecord::V1TopicConfig(config) => {
            image
                .topic_config(&config.topic)
                .and_then(|current| current.get(crate::config_keys::MIN_INSYNC_REPLICAS))
                != config
                    .overrides
                    .get(crate::config_keys::MIN_INSYNC_REPLICAS)
        }
        krabka_metadata::MetadataRecord::V1BrokerConfig(config) => {
            config.node_id == krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
                && config.config_name == crate::config_keys::MIN_INSYNC_REPLICAS
                && image
                    .broker_config(config.node_id)
                    .and_then(|current| current.get(&config.config_name))
                    != config.config_value.as_ref()
        }
        _ => false,
    });
    if min_isr_changed {
        let topic = (resource.resource_type == RESOURCE_TYPE_TOPIC)
            .then_some(resource.resource_name.as_str());
        records.extend(crate::config_keys::clear_elr_records(image, topic));
    }
    if validate_only {
        // Validation pass already happened above (per-config loop). Nothing
        // to submit; the response already carries the per-resource result
        // (NONE if all configs validated, INVALID_CONFIG with reason on any
        // rejection). This matches Apache Kafka's --dry-run behavior.
        return out;
    }
    if records.is_empty() {
        return out;
    }
    match broker.controller.submit_change(records).await {
        Ok(_) => {}
        Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
            out.error_code = codes::NOT_CONTROLLER;
        }
        Err(e) => {
            tracing::error!(error = %e, "AlterConfigs submit_change failed");
            out.error_code = codes::UNKNOWN_SERVER_ERROR;
        }
    }
    out
}
