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
//! This file holds the wire entry point, the broker-side preprocessing that
//! runs before the forward, and the audit names. The per-resource work lives
//! in `resource`, and the record builders it dispatches to live in
//! `topic_configs`, `broker_configs`, `group_configs` and
//! `client_metrics_configs`. What `IncrementalAlterConfigs` shares with this
//! API lives in [`super::config_alter`].

use {
    crate::handlers::{ApiVersion, DispatchResult},
    krabka_protocol::owned::{
        alter_configs_request::{self, AlterConfigsRequest, AlterConfigsResource},
        alter_configs_response::{AlterConfigsResourceResponse, AlterConfigsResponse},
    },
};

mod broker_configs;
mod client_metrics_configs;
mod group_configs;
mod resource;
mod topic_configs;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::resource::process_resource,
    crate::{
        broker::Broker,
        config_keys::is_controller_managed_topic_config,
        handlers::{
            RequestContext, audit_resource,
            config_alter::{
                BrokerPreprocess, ConfigResourceType, broker_preprocess_refusal,
                config_resource_type, duplicate_resource_flags, ordered_resource_responses,
                resource_error_rows, validate_resource_shape,
            },
            encode_response,
            forward_to_controller::to_active_controller_or_handle,
            response_encoding::ErrorRow as _,
        },
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        mem,
    },
};

krabka_macros::decoded_dispatch! {
    /// Validate broker-local values before sending replacement resources to the controller.
    pub async fn dispatch(
        broker: &Broker,
        version: ApiVersion,
        req_bytes: &[u8],
        ctx: &RequestContext<'_>,
    )
        [request = AlterConfigsRequest, binding(req)] -> DispatchResult
    {
        let mut req = krabka_macros::controller_request!(broker, ctx, req, version; self::handle, self::request_refusal);
        let duplicates = duplicate_resource_flags(req.resources.iter());
        let mut indexed_responses = Vec::new();
        let mut forwarded_indices = Vec::new();
        req.resources = mem::take(&mut req.resources)
            .into_iter()
            .zip(duplicates)
            .enumerate()
            .filter_map(|(index, (resource, duplicate))| {
                let refusal = preprocess_resource(broker, ctx, &resource, duplicate);
                refusal.map_or_else(
                    || {
                        forwarded_indices.push(index);
                        Some(resource)
                    },
                    |refusal| {
                        indexed_responses.push((index, refusal));
                        None
                    },
                )
            })
            .collect();
        let mut response = if req.resources.is_empty() {
            AlterConfigsResponse::default()
        } else {
            to_active_controller_or_handle(
                broker,
                req,
                alter_configs_request::API_KEY,
                version,
                ctx,
                request_refusal,
                |req| handle(broker, req, version, ctx),
            )
            .await?
        };
        indexed_responses.extend(forwarded_indices.into_iter().zip(response.responses));
        response.responses = ordered_resource_responses(indexed_responses);
        encode_response(&response, version)
    }
}

fn request_refusal(
    req: &AlterConfigsRequest,
    code: i16,
    message: Option<&str>,
) -> AlterConfigsResponse {
    AlterConfigsResponse {
        responses: resource_error_rows(req.resources.iter(), code, message),
        ..Default::default()
    }
}

fn preprocess_resource(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    resource: &AlterConfigsResource,
    duplicate: bool,
) -> Option<AlterConfigsResourceResponse> {
    let shape = validate_resource_shape(
        duplicate,
        resource
            .configs
            .iter()
            .map(|c| (c.name.as_str(), c.value.is_none())),
    );
    let image = broker.controller.current_image();
    let (code, message) = broker_preprocess_refusal(broker, ctx, &image, BrokerPreprocess {
        resource,
        shape,
        runs_preprocess: resource.resource_type == ConfigResourceType::Broker.to_wire(),
        preprocess: broker_configs::preprocess_broker_resource,
        broker_logger_supported: false,
    })?;
    Some(
        AlterConfigsResourceResponse {
            resource_type: resource.resource_type,
            resource_name: resource.resource_name.clone(),
            ..Default::default()
        }
        .with_error(code, message),
    )
}

krabka_macros::config_alter_handler! { request = AlterConfigsRequest, response = AlterConfigsResponse, operation = "AlterConfigs", audit = audit_resources_for, process = process_resource, audits_in_process = true }

/// Names the audited resource and the keys the request changes on it.
///
/// The values never reach the audit record. A config value can be a password
/// or a key store path, and the record only has to say who changed what.
fn audit_resources_for(
    resource: &AlterConfigsResource,
    image: &krabka_metadata::MetadataImage,
) -> Vec<krabka_audit::AuditResource> {
    let mut out = vec![audit_resource(
        config_resource_type(resource.resource_type),
        resource.resource_name.clone(),
    )];
    let keys = match ConfigResourceType::from_wire(resource.resource_type) {
        Some(ConfigResourceType::Topic) => changed_config_keys(
            resource,
            image.topic_config(&resource.resource_name),
            is_controller_managed_topic_config,
        ),
        Some(ConfigResourceType::Group) => changed_config_keys(
            resource,
            image.group_config(&resource.resource_name),
            |_| false,
        ),
        Some(ConfigResourceType::ClientMetrics) => changed_config_keys(
            resource,
            image.client_metrics_config(&resource.resource_name),
            |_| false,
        ),
        _ => resource
            .configs
            .iter()
            .map(|config| config.name.clone())
            .collect(),
    };
    out.extend(keys.into_iter().map(|key| audit_resource("ConfigKey", key)));
    out
}

/// The config keys a full-map-replacement resource's request changes.
///
/// `AlterConfigs` replaces the whole override map for a Topic, Group, or
/// `ClientMetrics` resource, so a stored key the request omits is deleted by
/// the request as surely as one it restates with a new value. Naming only
/// the request's own keys would leave those deletions out of the audit
/// trail: replacing `{retention.ms, cleanup.policy}` with `{retention.ms}`
/// removes the compaction policy and would record nothing. A key whose
/// stored value the request restates unchanged changed nothing and is left
/// out.
///
/// `is_controller_managed` excludes keys that stand outside the
/// replacement: no client can name one and the record builder carries the
/// stored value forward, so their absence from the request is not a
/// deletion. Only Topic resources have such keys; Group and `ClientMetrics`
/// pass a predicate that always returns `false`.
fn changed_config_keys(
    resource: &AlterConfigsResource,
    stored: Option<&BTreeMap<String, String>>,
    is_controller_managed: impl Fn(&str) -> bool,
) -> Vec<String> {
    let replacement: BTreeMap<&str, &str> = resource
        .configs
        .iter()
        .map(|config| (config.name.as_str(), config.value.as_deref().unwrap_or("")))
        .collect();
    let mut changed: BTreeSet<String> = replacement
        .iter()
        .filter(|(key, value)| {
            stored
                .and_then(|stored| stored.get(**key))
                .map(String::as_str)
                != Some(**value)
        })
        .map(|(key, _)| (*key).to_owned())
        .collect();
    changed.extend(
        stored
            .into_iter()
            .flatten()
            .filter(|(key, _)| {
                !is_controller_managed(key) && !replacement.contains_key(key.as_str())
            })
            .map(|(key, _)| key.clone()),
    );
    changed.into_iter().collect()
}
