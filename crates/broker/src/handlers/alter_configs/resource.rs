//! The per-resource `AlterConfigs` work: the authorization preamble, the
//! dispatch to the topic or broker record builder, and the metadata submit.
//!
//! One resource's outcome never depends on another's, so this module turns a
//! single `AlterConfigsResource` into the one response row it earns and the
//! request entry point only loops over it.

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
    RESOURCE_TYPE_BROKER, RESOURCE_TYPE_TOPIC, broker_configs::broker_config_records,
    topic_configs::topic_config_record,
};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
};

pub(super) async fn process_resource(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
) -> AlterConfigsResourceResponse {
    let mut out = AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_code: codes::NONE,
        error_message: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };

    // ── ACL preamble ────────────────────────────────────────
    // Per-resource authorization based on resource_type.
    // Topic (2) → AlterConfigs on Topic(resource_name) → TOPIC_AUTHORIZATION_FAILED on Deny.
    // Broker (4) → AlterConfigs on Cluster("kafka-cluster") → CLUSTER_AUTHORIZATION_FAILED on Deny.
    // Other resource types are unsupported; Kafka assigns no distinct code for
    // that, so they get INVALID_REQUEST — checked after ACL.
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
        RESOURCE_TYPE_BROKER => broker.config.authorizer.authorize(
            image,
            &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
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
            _ => codes::CLUSTER_AUTHORIZATION_FAILED,
        };
        return out;
    }

    let records = match resource.resource_type {
        RESOURCE_TYPE_TOPIC => match topic_config_record(&resource, image) {
            Ok(record) => vec![record],
            Err((code, message)) => {
                out.error_code = code;
                out.error_message = Some(message);
                return out;
            }
        },
        RESOURCE_TYPE_BROKER => match broker_config_records(&resource, image) {
            Ok(records) => records,
            Err((code, message)) => {
                out.error_code = code;
                out.error_message = Some(message);
                return out;
            }
        },
        _ => unreachable!("resource type passed ACL dispatch"),
    };
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
