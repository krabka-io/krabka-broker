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
//! [`crate::config_keys::CONTROLLER_MANAGED_TOPIC_CONFIGS`].

use bytes::Bytes;
use krabka_metadata::{
    AclOperation, BrokerConfigRecord, MetadataRecord, ResourceType, TopicConfigRecord,
};
use krabka_protocol::{
    Decode, UnknownTaggedFields,
    owned::{
        alter_configs_request::{AlterConfigsRequest, AlterConfigsResource},
        alter_configs_response::{AlterConfigsResourceResponse, AlterConfigsResponse},
    },
};
use krabka_raft::RaftError;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes, config_keys,
    error::BrokerError,
};

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

    for resource in req.resources {
        responses.push(process_resource(broker, &image, ctx, resource, req.validate_only).await);
    }

    let resp = AlterConfigsResponse {
        responses,
        throttle_time_ms: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    crate::handlers::encode_response(&resp, version)
}

async fn process_resource(
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
    // Other resource types are unsupported (INVALID_RESOURCE_TYPE) — checked after ACL.
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
            out.error_code = codes::INVALID_RESOURCE_TYPE;
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

/// Build the authoritative `V1TopicConfig` record for a topic resource. The
/// request carries the *complete* set of non-default values, so the map this
/// builds is the whole override map and the cross-key rules apply to it.
fn topic_config_record(
    resource: &AlterConfigsResource,
    image: &krabka_metadata::MetadataImage,
) -> Result<MetadataRecord, (i16, String)> {
    if image.topic(&resource.resource_name).is_none() {
        return Err((
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            format!("unknown topic `{}`", resource.resource_name),
        ));
    }
    let mut overrides = std::collections::BTreeMap::new();
    for cfg in &resource.configs {
        // A controller-managed key is never stored, so it cannot take part
        // in the replacement. The check comes before the whitelist, so the
        // operator reads the refusal that names `crabka-guard` and not
        // `unrecognized config key`.
        if config_keys::is_controller_managed_topic_config(&cfg.name) {
            return Err((
                codes::INVALID_CONFIG,
                config_keys::controller_managed_topic_config_message(&cfg.name),
            ));
        }
        let value = cfg.value.clone().unwrap_or_default();
        config_keys::validate_topic_config(&cfg.name, &value)
            .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
        overrides.insert(cfg.name.clone(), value);
    }
    config_keys::validate_config_combination(&overrides)
        .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
    Ok(MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: resource.resource_name.clone(),
        overrides,
    }))
}

fn broker_config_records(
    resource: &AlterConfigsResource,
    image: &krabka_metadata::MetadataImage,
) -> Result<Vec<MetadataRecord>, (i16, String)> {
    let node_id =
        super::incremental_alter_configs::broker_config_node_id(&resource.resource_name, image)?;
    let mut replacement = std::collections::BTreeMap::new();
    for config in &resource.configs {
        if config_keys::is_controller_managed_broker_config(&config.name) {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is controller-managed and read-only",
                    config.name
                ),
            ));
        }
        if !super::incremental_alter_configs::is_known_broker_config(&config.name) {
            return Err((
                codes::INVALID_CONFIG,
                format!("unknown broker config {}", config.name),
            ));
        }
        if node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
            && super::incremental_alter_configs::is_cluster_default_topic_config(&config.name)
        {
            return Err((
                codes::INVALID_CONFIG,
                format!(
                    "broker config {} is valid only on the cluster-default resource",
                    config.name
                ),
            ));
        }
        let value = config.value.as_deref().ok_or_else(|| {
            (
                codes::INVALID_CONFIG,
                format!("broker config {} requires a value", config.name),
            )
        })?;
        super::incremental_alter_configs::validate_broker_config_value(&config.name, value)
            .map_err(|message| (codes::INVALID_CONFIG, message))?;
        replacement.insert(config.name.clone(), value.to_owned());
    }

    let current = image.broker_config(node_id);
    let capacity = replacement.len() + current.map_or(0, std::collections::BTreeMap::len);
    let mut records = Vec::with_capacity(capacity);
    records.extend(replacement.iter().map(|(name, value)| {
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.clone(),
            config_value: Some(value.clone()),
        })
    }));
    if let Some(current) = current {
        records.extend(
            current
                .keys()
                .filter(|name| {
                    !replacement.contains_key(*name)
                        && !config_keys::is_controller_managed_broker_config(name)
                })
                .map(|name| {
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id,
                        config_name: name.clone(),
                        config_value: None,
                    })
                }),
        );
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::{assert, check};
    use krabka_protocol::owned::alter_configs_request::{
        AlterConfigsRequest, AlterConfigsResource, AlterableConfig,
    };
    use krabka_security::{AuthMethod, Principal};

    use super::*;
    use crate::{authorizer::Authorizer, test_support::DenyAll};

    crate::test_support::wire_helpers!(
        AlterConfigsRequest,
        AlterConfigsResponse,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer as start_broker;

    fn resource(resource_type: i8, resource_name: &str) -> AlterConfigsResource {
        AlterConfigsResource {
            resource_type,
            resource_name: resource_name.into(),
            configs: vec![AlterableConfig {
                name: "retention.ms".into(),
                value: Some("60000".into()),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// A metadata image that holds one registered broker and nothing else.
    fn image_with_broker(node_id: u64) -> krabka_metadata::MetadataImage {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                node_id: krabka_metadata::NodeId(node_id),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: Vec::new(),
                features: std::collections::BTreeMap::new(),
            },
        ));
        image
    }

    fn topic_resource(resource_name: &str, configs: &[(&str, &str)]) -> AlterConfigsResource {
        AlterConfigsResource {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: resource_name.into(),
            configs: configs
                .iter()
                .map(|(name, value)| AlterableConfig {
                    name: (*name).into(),
                    value: Some((*value).into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn image_with_topic(name: &str) -> krabka_metadata::MetadataImage {
        let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: name.into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 1,
        }));
        image
    }

    fn broker_resource(resource_name: &str, configs: &[(&str, &str)]) -> AlterConfigsResource {
        AlterConfigsResource {
            resource_type: RESOURCE_TYPE_BROKER,
            resource_name: resource_name.into(),
            configs: configs
                .iter()
                .map(|(name, value)| AlterableConfig {
                    name: (*name).into(),
                    value: Some((*value).into()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    async fn drive_one(
        authorizer: Arc<dyn Authorizer>,
        resource: AlterConfigsResource,
    ) -> AlterConfigsResponse {
        let version = 2;
        let (broker_handle, _dir) = start_broker(authorizer).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = AlterConfigsRequest {
            resources: vec![resource],
            validate_only: false,
            ..Default::default()
        };
        let req_bytes = encode_request(&req, version);

        let resp = handle(&broker, version, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp, version);
        broker_handle.shutdown().await;
        resp
    }

    #[tokio::test]
    async fn handle_preserves_resource_identity_for_unsupported_type() {
        let resp = Box::pin(drive_one(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            resource(77, "mystery"),
        ))
        .await;

        let expected = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResourceResponse {
                error_code: codes::INVALID_RESOURCE_TYPE,
                error_message: Some("resource_type=77 not supported".to_string()),
                resource_type: 77,
                resource_name: "mystery".to_string(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn topic_resource_denial_uses_topic_authorization_error() {
        let resp = Box::pin(drive_one(
            Arc::new(DenyAll),
            resource(RESOURCE_TYPE_TOPIC, "orders"),
        ))
        .await;

        let expected = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResourceResponse {
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: None,
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: "orders".to_string(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn broker_resource_denial_uses_cluster_authorization_error() {
        let resp = Box::pin(drive_one(
            Arc::new(DenyAll),
            resource(RESOURCE_TYPE_BROKER, "1"),
        ))
        .await;

        let expected = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResourceResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: None,
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "1".to_string(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn authorized_broker_resource_is_applied() {
        let resp = Box::pin(drive_one(
            Arc::new(crate::authorizer::AllowAllAuthorizer),
            broker_resource("1", &[(crate::throttle::LEADER_THROTTLED_RATE_KEY, "2048")]),
        ))
        .await;

        let expected = AlterConfigsResponse {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResourceResponse {
                error_code: codes::NONE,
                error_message: None,
                resource_type: RESOURCE_TYPE_BROKER,
                resource_name: "1".to_string(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[test]
    fn topic_replacement_rejects_compaction_on_a_scheduled_topic() {
        let image = image_with_topic("orders");

        let (code, message) = topic_config_record(
            &topic_resource(
                "orders",
                &[
                    (crate::config_keys::CLEANUP_POLICY, "compact"),
                    (crate::config_keys::DELIVERY_MODE, "scheduled"),
                ],
            ),
            &image,
        )
        .expect_err("compaction on a scheduled topic must be rejected");

        assert!(code == codes::INVALID_CONFIG);
        assert!(
            message.contains(crate::config_keys::CLEANUP_POLICY),
            "got: {message}"
        );
        assert!(
            message.contains(crate::config_keys::DELIVERY_MODE),
            "got: {message}"
        );
    }

    #[test]
    fn topic_replacement_accepts_a_scheduled_topic_without_compaction() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource(
                "orders",
                &[
                    (crate::config_keys::CLEANUP_POLICY, "delete"),
                    (crate::config_keys::DELIVERY_MODE, "scheduled"),
                ],
            ),
            &image,
        )
        .expect("a scheduled delete-policy topic is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: std::collections::BTreeMap::from([
                (
                    crate::config_keys::CLEANUP_POLICY.to_string(),
                    "delete".to_string(),
                ),
                (
                    crate::config_keys::DELIVERY_MODE.to_string(),
                    "scheduled".to_string(),
                ),
            ]),
        });
        assert!(record == expected);
    }

    #[test]
    fn topic_replacement_rejects_controller_managed_configs() {
        let image = image_with_topic("orders");

        for key in config_keys::CONTROLLER_MANAGED_TOPIC_CONFIGS {
            // A full replacement carries a value for every key it names, so
            // both a `true` and an empty value are a write of the key.
            for value in ["true", "false", ""] {
                let error = topic_config_record(&topic_resource("orders", &[(key, value)]), &image)
                    .expect_err("controller-managed key must be rejected");

                check!(
                    error.0 == codes::INVALID_CONFIG,
                    "key {key} value {value:?}"
                );
                check!(
                    error.1 == config_keys::controller_managed_topic_config_message(key),
                    "key {key} value {value:?}"
                );
                check!(!error.1.is_empty(), "key {key} value {value:?}");
            }
        }
    }

    #[test]
    fn topic_replacement_leaves_an_ordinary_config_unaffected() {
        let image = image_with_topic("orders");

        let record = topic_config_record(
            &topic_resource("orders", &[(crate::config_keys::RETENTION_MS, "60000")]),
            &image,
        )
        .expect("an ordinary topic config is valid");

        let expected = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".into(),
            overrides: std::collections::BTreeMap::from([(
                crate::config_keys::RETENTION_MS.to_string(),
                "60000".to_string(),
            )]),
        });
        assert!(record == expected);
    }

    #[test]
    fn broker_full_replacement_sets_requested_and_deletes_omitted_configs() {
        let mut image = image_with_broker(1);
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::NodeId(1),
            config_name: crate::throttle::LEADER_THROTTLED_RATE_KEY.into(),
            config_value: Some("1024".into()),
        }));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::NodeId(1),
            config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
            config_value: Some("512".into()),
        }));

        let records = broker_config_records(
            &broker_resource("1", &[(crate::throttle::LEADER_THROTTLED_RATE_KEY, "2048")]),
            &image,
        )
        .expect("valid broker replacement");

        let expected = vec![
            MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: krabka_metadata::NodeId(1),
                config_name: crate::throttle::LEADER_THROTTLED_RATE_KEY.into(),
                config_value: Some("2048".into()),
            }),
            MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: krabka_metadata::NodeId(1),
                config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
                config_value: None,
            }),
        ];
        assert!(records == expected);
    }

    #[test]
    fn broker_full_replacement_accepts_cluster_default_resource() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let records = broker_config_records(
            &broker_resource(
                "",
                &[
                    (crate::throttle::FOLLOWER_THROTTLED_RATE_KEY, "4096"),
                    (crate::config_keys::UNCLEAN_RECOVERY_STRATEGY, "Balanced"),
                ],
            ),
            &image,
        )
        .expect("valid broker default replacement");

        assert!(
            records
                == vec![
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                        config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
                        config_value: Some("4096".into()),
                    }),
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                        config_name: crate::config_keys::UNCLEAN_RECOVERY_STRATEGY.into(),
                        config_value: Some("Balanced".into()),
                    }),
                ]
        );
    }

    #[test]
    fn broker_full_replacement_rejects_controller_managed_configs() {
        let image = image_with_broker(1);
        for key in config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS {
            for resource_name in ["1", ""] {
                let error = broker_config_records(
                    &broker_resource(resource_name, &[(key, "true")]),
                    &image,
                )
                .expect_err("controller-managed key must be rejected");

                check!(error.0 == codes::INVALID_CONFIG, "key {key}");
                check!(
                    error.1 == format!("broker config {key} is controller-managed and read-only"),
                    "key {key}"
                );
            }
        }
    }

    #[test]
    fn broker_full_replacement_leaves_controller_managed_configs_alone() {
        let mut image = image_with_broker(1);
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::NodeId(1),
            config_name: crate::config_keys::BROKER_WITNESS.into(),
            config_value: Some(crate::config_keys::WITNESS_TRUE.into()),
        }));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::NodeId(1),
            config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
            config_value: Some("512".into()),
        }));

        let records = broker_config_records(
            &broker_resource("1", &[(crate::throttle::LEADER_THROTTLED_RATE_KEY, "2048")]),
            &image,
        )
        .expect("valid broker replacement");

        assert!(
            records
                == vec![
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::NodeId(1),
                        config_name: crate::throttle::LEADER_THROTTLED_RATE_KEY.into(),
                        config_value: Some("2048".into()),
                    }),
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::NodeId(1),
                        config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.into(),
                        config_value: None,
                    }),
                ]
        );
    }

    #[test]
    fn broker_full_replacement_rejects_per_broker_recovery_setting() {
        let image = image_with_broker(1);

        let error = broker_config_records(
            &broker_resource(
                "1",
                &[(crate::config_keys::UNCLEAN_RECOVERY_STRATEGY, "Balanced")],
            ),
            &image,
        )
        .expect_err("per-broker recovery setting must be rejected");

        assert!(error.0 == codes::INVALID_CONFIG);
    }
}
