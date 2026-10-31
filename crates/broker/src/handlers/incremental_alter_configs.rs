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

use {
    crate::{
        config_keys::{MIN_INSYNC_REPLICAS, clear_elr_records},
        handlers::{
            ApiVersion, DispatchResult, RequestContext,
            acl_gates::config_resource_refusal,
            alter_configs::{
                BrokerPreprocess, RecordBuilders, broker_preprocess_refusal,
                guarded_resource_response, resource_preamble_refusal, resource_records,
            },
            audit_resource, encode_response,
            forward_to_controller::to_active_controller_or_handle,
            response_encoding::ErrorRow as _,
        },
    },
    krabka_metadata::{MetadataImage, MetadataRecord},
    krabka_protocol::owned::{
        incremental_alter_configs_request::{
            self, AlterConfigsResource, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::{
            AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
        },
    },
    krabka_raft::RaftError,
    std::sync::Arc,
};

mod broker_logger_scope;
mod broker_scope;
mod client_metrics_scope;
mod group_scope;
#[cfg(test)]
mod test_support;
mod topic_scope;

use {
    self::{
        broker_logger_scope::handle_broker_logger_scoped, broker_scope::broker_records,
        client_metrics_scope::client_metrics_records, group_scope::group_record,
        topic_scope::topic_config_record,
    },
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        handlers::describe_configs::{
            RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
            RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
        },
    },
    std::{iter, mem},
};
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;
const OP_APPEND: i8 = 2;
const OP_SUBTRACT: i8 = 3;

/// Whether `AlterConfigOp.OpType.forId` knows `operation`.
const fn is_known_operation(operation: i8) -> bool { matches!(operation, OP_SET..=OP_SUBTRACT) }

krabka_macros::decoded_dispatch! {
    /// Keep logger changes on this node and validate broker keys against local settings.
    /// Topic, group, and other resource operations use the controller's image.
    pub async fn dispatch(
        broker: &Broker,
        version: ApiVersion,
        req_bytes: &[u8],
        ctx: &RequestContext<'_>,
    )
        [request = IncrementalAlterConfigsRequest, binding(req)] -> DispatchResult
    {
        let mut req = krabka_macros::controller_request!(broker, ctx, req, version; self::handle_on_controller, self::request_refusal);
        let image = broker.controller.current_image();
        let duplicates = super::alter_configs::duplicate_resource_flags(req.resources.iter());
        let mut local = Vec::new();
        let mut local_indices = Vec::new();
        let mut indexed_responses = Vec::new();
        let mut forwarded = Vec::new();
        let mut forwarded_indices = Vec::new();
        for (index, (resource, duplicate)) in mem::take(&mut req.resources)
            .into_iter()
            .zip(duplicates)
            .enumerate()
        {
            if resource.resource_type == RESOURCE_TYPE_BROKER_LOGGER {
                local_indices.push(index);
                local.push(resource);
                continue;
            }
            let refusal = (resource.resource_type == RESOURCE_TYPE_BROKER).then(|| {
                broker_preprocess_refusal(broker, ctx, &image, BrokerPreprocess {
                    resource: &resource,
                    shape: validate_resource_shape(&resource, duplicate),
                    runs_preprocess: true,
                    preprocess: broker_scope::preprocess_broker_resource,
                    broker_logger_supported: true,
                })
            });
            if let Some((code, message)) = refusal.flatten() {
                indexed_responses.push((
                    index,
                    resource_refusal(&resource, code).with_error(code, message),
                ));
                continue;
            }
            forwarded_indices.push(index);
            forwarded.push(resource);
        }
        req.resources = forwarded;
        let mut response = if req.resources.is_empty() {
            IncrementalAlterConfigsResponse::default()
        } else {
            to_active_controller_or_handle(
                broker,
                req.clone(),
                incremental_alter_configs_request::API_KEY,
                version,
                ctx,
                request_refusal,
                |incremental_request| handle_on_controller(broker, incremental_request, version, ctx),
            )
            .await?
        };
        indexed_responses.extend(forwarded_indices.into_iter().zip(response.responses));
        if !local.is_empty() {
            req.resources = local;
            indexed_responses.extend(
                local_indices
                    .into_iter()
                    .zip(handle(broker, req, version, ctx).await?.responses),
            );
        }
        response.responses = super::alter_configs::ordered_resource_responses(indexed_responses);
        encode_response(&response, version)
    }
}

fn request_refusal(
    req: &IncrementalAlterConfigsRequest,
    code: i16,
    message: Option<&str>,
) -> IncrementalAlterConfigsResponse {
    IncrementalAlterConfigsResponse {
        responses: super::alter_configs::resource_error_rows(req.resources.iter(), code, message),
        ..Default::default()
    }
}

/// The resources that reach the controller, answered as Kafka 4.4's
/// controller answers them.
///
/// `ControllerApis.handleIncrementalAlterConfigs` stores
/// `AlterConfigOp.OpType.forId(op)`, which is null for an unknown code, and
/// `ConfigurationControlManager.incrementalAlterConfigResource` then throws on
/// its `switch`. The controller fails the whole request: every resource in it
/// answers `UNKNOWN_SERVER_ERROR` with no message, and nothing is written.
async fn handle_on_controller(
    broker: &Broker,
    req: IncrementalAlterConfigsRequest,
    version: i16,
    ctx: &RequestContext<'_>,
) -> Result<IncrementalAlterConfigsResponse, BrokerError> {
    if reaches_unknown_operation(broker, ctx, &req) {
        return Ok(request_refusal(&req, codes::UNKNOWN_SERVER_ERROR, None));
    }
    handle(broker, req, version, ctx).await
}

/// Whether a resource that Kafka's controller would merge carries an unknown
/// operation code. `ControllerApis` sets aside `BROKER_LOGGER`, unknown
/// resource types, duplicate resources and unauthorized ones before the
/// merge, and the broker's `preprocess` has refused a resource whose shape is
/// wrong.
fn reaches_unknown_operation(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    req: &IncrementalAlterConfigsRequest,
) -> bool {
    let image = broker.controller.current_image();
    let duplicates = super::alter_configs::duplicate_resource_flags(req.resources.iter());
    req.resources
        .iter()
        .zip(duplicates)
        .any(|(resource, duplicate)| {
            matches!(
                resource.resource_type,
                RESOURCE_TYPE_TOPIC
                    | RESOURCE_TYPE_BROKER
                    | RESOURCE_TYPE_GROUP
                    | RESOURCE_TYPE_CLIENT_METRICS
            ) && resource
                .configs
                .iter()
                .any(|config| !is_known_operation(config.config_operation))
                && validate_resource_shape(resource, duplicate).is_ok()
                && config_resource_refusal(
                    broker.config.authorizer.as_ref(),
                    &image,
                    ctx,
                    (resource.resource_type, &resource.resource_name),
                    false,
                )
                .is_none()
        })
}

krabka_macros::config_alter_handler! { request = IncrementalAlterConfigsRequest, response = IncrementalAlterConfigsResponse, operation = "IncrementalAlterConfigs", audit = audit_resources_for, process = process_resource, audits_in_process = false }

/// Audit names include the resource and keys, never sensitive config values.
fn audit_resources_for(
    resource: &AlterConfigsResource,
    _image: &MetadataImage,
) -> Vec<krabka_audit::AuditResource> {
    iter::once(audit_resource(
        super::alter_configs::config_resource_type(resource.resource_type),
        resource.resource_name.clone(),
    ))
    .chain(
        resource
            .configs
            .iter()
            .map(|config| audit_resource("ConfigKey", config.name.clone())),
    )
    .collect()
}

/// Flags each resource named more than once in the request. See
/// [`super::alter_configs::duplicate_resource_flags`].
#[cfg(test)]
fn resource_duplicate_flags(resources: &[AlterConfigsResource]) -> Vec<bool> {
    super::alter_configs::duplicate_resource_flags(resources.iter())
}

/// Kafka's `ConfigAdminManager.preprocess` shape checks, with a null value
/// legal on DELETE only. See
/// [`super::alter_configs::validate_resource_shape`].
fn validate_resource_shape(
    resource: &AlterConfigsResource,
    is_duplicate: bool,
) -> Result<(), (i16, String)> {
    super::alter_configs::validate_resource_shape(
        is_duplicate,
        resource.configs.iter().map(|config| {
            (
                config.name.as_str(),
                config.config_operation != OP_DELETE && config.value.is_none(),
            )
        }),
    )
}

/// `IncrementalAlterConfigs`' builder for each resource type. Each merges the
/// operations onto the map the image holds.
const RECORD_BUILDERS: RecordBuilders<AlterConfigsResource> = RecordBuilders {
    topic: topic_config_record,
    broker: broker_records,
    group: group_record,
    client_metrics: client_metrics_records,
};

async fn process_resource(
    broker: &Broker,
    _image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> AlterConfigsResourceResponse {
    // Read and admission are optimistic; the engine atomically checks that
    // image when it appends. Rebuild every operation after a conflicting commit.
    guarded_resource_response(broker, "IncrementalAlterConfigs", |image| {
        process_resource_once(
            broker,
            image,
            ctx,
            resource.clone(),
            validate_only,
            is_duplicate,
        )
    })
    .await
    .unwrap_or_else(|code| resource_refusal(&resource, code))
}

fn resource_refusal(resource: &AlterConfigsResource, code: i16) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_code: code,
        error_message: None,
        ..Default::default()
    }
}

async fn process_resource_once(
    broker: &Broker,
    image: Arc<MetadataImage>,
    ctx: &RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> Result<AlterConfigsResourceResponse, RaftError> {
    let mut out: AlterConfigsResourceResponse =
        super::alter_configs::empty_resource_response(&resource);
    let shape = validate_resource_shape(&resource, is_duplicate);
    if let Some((code, message)) =
        resource_preamble_refusal(broker, &image, ctx, &resource, shape, true)
    {
        return Ok(out.with_error(code, message));
    }

    if resource.resource_type == RESOURCE_TYPE_BROKER_LOGGER {
        // Node-local and never persisted, so this returns without
        // reaching `submit_change` whatever the outcome.
        handle_broker_logger_scoped(
            &resource,
            broker.config.broker_id,
            &broker.config.log_levels,
            validate_only,
            &mut out,
        );
        return Ok(out);
    }
    let mut to_submit = match resource_records(broker, &image, ctx, &resource, &RECORD_BUILDERS) {
        Ok(records) => records,
        Err((code, message)) => return Ok(out.with_error(code, message)),
    };

    if let Some((code, message)) = super::alter_configs::config_value_size_error(&to_submit) {
        return Ok(out.with_error(code, message));
    }

    if to_submit.iter().any(|record| {
        let include_topic = !matches!(record, MetadataRecord::V1TopicConfig(_))
            || resource
                .configs
                .iter()
                .any(|item| item.name == MIN_INSYNC_REPLICAS);
        super::alter_configs::changes_min_isr(&image, record, include_topic)
    }) {
        let topic = (resource.resource_type == RESOURCE_TYPE_TOPIC)
            .then_some(resource.resource_name.as_str());
        to_submit.extend(clear_elr_records(&image, topic));
    }

    if validate_only {
        // Validation pass already happened above (per-config loop). Nothing
        // to submit; the response already carries the per-resource result
        // (NONE if all configs validated, INVALID_CONFIG with reason on any
        // rejection). This matches Apache Kafka's --dry-run behavior.
        return Ok(out);
    }
    broker
        .controller
        .submit_change_if_image(to_submit, image)
        .await?;
    Ok(out)
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{
            api_catalog::UnstableApiVersions,
            config_keys::{RETENTION_MS, SEGMENT_BYTES},
            handlers::{
                forward_to_controller::guarded_operation_on_source,
                incremental_alter_configs::test_support::{
                    AlterableConfig, StoredOverride, check, image_with_topic_config,
                    make_scoped_resource, make_set_cfg, make_topic_resource, overrides_map,
                },
                test_support::{
                    assert_dispatch_response, config_resource_error_response, ordered_rows,
                },
            },
            metadata_source::MetadataSource,
            test_support::{
                DenyAll, FakeMetadataSource, start_broker_with_authorizer as start_broker,
            },
            topic_policy::TopicPolicy,
        },
        krabka_metadata::TopicConfigRecord,
        std::time::Duration,
        tokio::time::Instant,
    };

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    /// Dispatch `request` at version 1 and compare the whole wire response
    /// with `responses`.
    async fn assert_incremental_dispatch(
        broker: &Broker,
        ctx: &RequestContext<'_>,
        request: &IncrementalAlterConfigsRequest,
        responses: Vec<AlterConfigsResourceResponse>,
    ) {
        let expected = IncrementalAlterConfigsResponse {
            responses,
            ..Default::default()
        };
        assert_dispatch_response(request, &expected, 1, |body| async move {
            dispatch(broker, 1, &body, ctx).await
        })
        .await;
    }

    /// The wire response interleaves logger results, local broker refusals and
    /// controller results in the original order, keeping duplicate rows.
    #[tokio::test]
    async fn dispatch_preserves_mixed_resource_order_and_duplicates() {
        allow_all_broker_setup!((handle, _dir, broker, ctx), start_broker, "admin");
        let resource = |kind, name| make_scoped_resource(kind, name, vec![]);
        let duplicate_topic = krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "duplicate".into(),
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Each resource must appear at most once.".into())
        });
        let duplicate_logger = krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
            resource_type: RESOURCE_TYPE_BROKER_LOGGER,
            resource_name: "2".into(),
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Each resource must appear at most once.".into())
        });
        let rows = [
            (
                resource(RESOURCE_TYPE_BROKER_LOGGER, "1"),
                krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_BROKER_LOGGER,
                    resource_name: "1".into(),
                    error_code: codes::NONE,
                    error_message: None
                }),
            ),
            (
                resource(RESOURCE_TYPE_BROKER, "2"),
                config_resource_error_response(
                    RESOURCE_TYPE_BROKER,
                    "2",
                    codes::INVALID_REQUEST,
                    Some("Unexpected broker id, expected 1, but received 2".into()),
                ),
            ),
            (
                resource(RESOURCE_TYPE_TOPIC, "duplicate"),
                duplicate_topic.clone(),
            ),
            (
                resource(RESOURCE_TYPE_BROKER, "1"),
                krabka_macros::tagged_wire!(AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_BROKER,
                    resource_name: "1".into(),
                    error_code: codes::NONE,
                    error_message: None
                }),
            ),
            (
                resource(RESOURCE_TYPE_TOPIC, "missing"),
                config_resource_error_response(
                    RESOURCE_TYPE_TOPIC,
                    "missing",
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    Some("The topic 'missing' does not exist.".into()),
                ),
            ),
            (resource(RESOURCE_TYPE_TOPIC, "duplicate"), duplicate_topic),
            (
                resource(RESOURCE_TYPE_BROKER_LOGGER, "2"),
                duplicate_logger.clone(),
            ),
            (resource(RESOURCE_TYPE_BROKER_LOGGER, "2"), duplicate_logger),
        ];
        for order in [[0, 1, 2, 3, 4, 5, 6, 7], [7, 6, 5, 4, 3, 2, 1, 0]] {
            let ordered = ordered_rows(&rows, &order);
            let request = IncrementalAlterConfigsRequest {
                resources: ordered.requests,
                validate_only: true,
                ..Default::default()
            };
            assert_incremental_dispatch(broker.as_ref(), &ctx, &request, ordered.responses).await;
        }
        handle.shutdown().await;
    }

    /// Kafka's error precedence for a BROKER resource that the principal may
    /// not alter: a request-shape error answers before authorization, and an
    /// authorization error answers before the broker-side preprocess checks.
    #[yare::parameterized(
        authorization_beats_preprocess = {
            vec![make_set_cfg("log.retention.ms", "1")],
            "2",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
        },
        shape_beats_authorization = {
            vec![make_set_cfg("log.retention.ms", "1"), make_set_cfg("log.retention.ms", "2")],
            "1",
            codes::INVALID_REQUEST,
            "Error due to duplicate config keys",
        },
    )]
    #[test_macro(tokio::test)]
    async fn unauthorized_broker_resource_precedence(
        configs: Vec<AlterableConfig>,
        name: &str,
        error_code: i16,
        error_message: &str,
    ) {
        let started = start_broker(Arc::new(DenyAll)).await;
        let broker = started.broker.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["admin"];
        };
        let request = IncrementalAlterConfigsRequest {
            resources: vec![make_scoped_resource(RESOURCE_TYPE_BROKER, name, configs)],
            ..Default::default()
        };
        let refusal = config_resource_error_response(
            RESOURCE_TYPE_BROKER,
            name,
            error_code,
            Some(error_message.into()),
        );
        assert_incremental_dispatch(broker.as_ref(), &ctx, &request, vec![refusal]).await;
        started.broker.shutdown().await;
    }

    /// An operation code outside 0-3 gets Kafka 4.4's answer for each resource
    /// type. BROKER and `BROKER_LOGGER` are refused on the broker, by
    /// `ConfigAdminManager.validateBrokerConfigChange` and
    /// `RuntimeLoggerManager.validateLogLevelConfigs`. TOPIC, GROUP and
    /// `CLIENT_METRICS` reach the controller, whose
    /// `ConfigurationControlManager` throws on the null `OpType`, and so answer
    /// `UNKNOWN_SERVER_ERROR` with no message. A null value is refused by
    /// `preprocess` before the operation is looked at.
    #[yare::parameterized(
        topic_four = { RESOURCE_TYPE_TOPIC, "orders", 4, Some("1"), codes::UNKNOWN_SERVER_ERROR, None },
        topic_negative = { RESOURCE_TYPE_TOPIC, "orders", -1, Some("1"), codes::UNKNOWN_SERVER_ERROR, None },
        topic_null_value = {
            RESOURCE_TYPE_TOPIC, "orders", 4, None, codes::INVALID_REQUEST,
            Some("Null value not supported for : retention.ms"),
        },
        group_four = { RESOURCE_TYPE_GROUP, "workers", 4, Some("1"), codes::UNKNOWN_SERVER_ERROR, None },
        group_negative = { RESOURCE_TYPE_GROUP, "workers", -1, Some("1"), codes::UNKNOWN_SERVER_ERROR, None },
        client_metrics_four = {
            RESOURCE_TYPE_CLIENT_METRICS, "metrics", 4, Some("1"), codes::UNKNOWN_SERVER_ERROR, None,
        },
        client_metrics_negative = {
            RESOURCE_TYPE_CLIENT_METRICS, "metrics", -1, Some("1"), codes::UNKNOWN_SERVER_ERROR, None,
        },
        broker_four = {
            RESOURCE_TYPE_BROKER, "1", 4, Some("1"), codes::INVALID_REQUEST,
            Some("Unknown operations type 4"),
        },
        cluster_negative = {
            RESOURCE_TYPE_BROKER, "", -1, Some("1"), codes::INVALID_REQUEST,
            Some("Unknown operations type -1"),
        },
        broker_logger_four = {
            RESOURCE_TYPE_BROKER_LOGGER, "1", 4, Some("1"), codes::INVALID_REQUEST,
            Some("Unknown operation type 4 is not allowed for the BROKER_LOGGER resource"),
        },
        broker_logger_negative = {
            RESOURCE_TYPE_BROKER_LOGGER, "1", -1, Some("1"), codes::INVALID_REQUEST,
            Some("Unknown operation type -1 is not allowed for the BROKER_LOGGER resource"),
        },
    )]
    #[test_macro(tokio::test)]
    async fn unknown_operation_code_matches_kafka(
        resource_type: i8,
        name: &str,
        operation: i8,
        value: Option<&str>,
        error_code: i16,
        error_message: Option<&str>,
    ) {
        allow_all_broker_setup!((handle, _dir, broker, ctx), start_broker, "admin");
        let request = IncrementalAlterConfigsRequest {
            resources: vec![make_scoped_resource(resource_type, name, vec![config(
                RETENTION_MS,
                operation,
                value,
            )])],
            ..Default::default()
        };
        let expected = config_resource_error_response(
            resource_type,
            name,
            error_code,
            error_message.map(str::to_owned),
        );
        assert_incremental_dispatch(broker.as_ref(), &ctx, &request, vec![expected]).await;
        handle.shutdown().await;
    }

    /// Kafka's controller fails the whole forwarded request when one resource
    /// carries an unknown operation: every resource that reached it answers
    /// `UNKNOWN_SERVER_ERROR` and nothing is written. Resources the broker
    /// answered itself (`BROKER_LOGGER`, and a BROKER that fails preprocess)
    /// keep their own answers.
    #[tokio::test]
    async fn an_unknown_operation_fails_every_forwarded_resource() {
        allow_all_broker_setup!((handle, _dir, broker, ctx), start_broker, "admin");
        let unknown = || vec![config(RETENTION_MS, 4, Some("1"))];
        let failed = |resource_type, name| {
            config_resource_error_response(resource_type, name, codes::UNKNOWN_SERVER_ERROR, None)
        };
        let request = IncrementalAlterConfigsRequest {
            resources: vec![
                make_scoped_resource(RESOURCE_TYPE_TOPIC, "missing", vec![]),
                make_scoped_resource(RESOURCE_TYPE_BROKER_LOGGER, "1", vec![]),
                make_scoped_resource(RESOURCE_TYPE_TOPIC, "orders", unknown()),
                make_scoped_resource(RESOURCE_TYPE_BROKER, "1", vec![]),
                make_scoped_resource(RESOURCE_TYPE_BROKER, "", unknown()),
                make_scoped_resource(RESOURCE_TYPE_GROUP, "workers", vec![]),
            ],
            ..Default::default()
        };
        let responses = vec![
            failed(RESOURCE_TYPE_TOPIC, "missing"),
            config_resource_error_response(RESOURCE_TYPE_BROKER_LOGGER, "1", codes::NONE, None),
            failed(RESOURCE_TYPE_TOPIC, "orders"),
            failed(RESOURCE_TYPE_BROKER, "1"),
            config_resource_error_response(
                RESOURCE_TYPE_BROKER,
                "",
                codes::INVALID_REQUEST,
                Some("Unknown operations type 4".into()),
            ),
            failed(RESOURCE_TYPE_GROUP, "workers"),
        ];
        assert_incremental_dispatch(broker.as_ref(), &ctx, &request, responses).await;
        handle.shutdown().await;
    }

    /// Kafka's `ControllerApis` drops an unauthorized resource before the
    /// controller sees it, so its unknown operation never fails the request.
    #[tokio::test]
    async fn an_unauthorized_unknown_operation_answers_authorization() {
        let started = start_broker(Arc::new(DenyAll)).await;
        let broker = started.broker.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["admin"];
        };
        let request = IncrementalAlterConfigsRequest {
            resources: vec![make_topic_resource("orders", vec![config(
                RETENTION_MS,
                4,
                Some("1"),
            )])],
            ..Default::default()
        };
        let refusal = config_resource_error_response(
            RESOURCE_TYPE_TOPIC,
            "orders",
            codes::TOPIC_AUTHORIZATION_FAILED,
            Some("Topic authorization failed.".into()),
        );
        assert_incremental_dispatch(broker.as_ref(), &ctx, &request, vec![refusal]).await;
        started.broker.shutdown().await;
    }

    /// An incremental SET merges onto the image that the guarded retry reads
    /// after a conflict, so an override committed between the first planning
    /// attempt and the append survives in the submitted record.
    #[tokio::test]
    async fn topic_set_replans_onto_an_intervening_override() {
        let initial =
            image_with_topic_config("orders", &[StoredOverride::new(RETENTION_MS, "60000")]);
        let newer =
            image_with_topic_config("orders", &[StoredOverride::new(RETENTION_MS, "120000")]);
        let source = FakeMetadataSource::builder()
            .image(initial)
            .leader(Some(krabka_raft::NodeId(1)))
            .before_guarded_submit(move |source| source.set_image(newer))
            .build();
        let resource = make_topic_resource("orders", vec![make_set_cfg(SEGMENT_BYTES, "1048576")]);
        let result = guarded_operation_on_source(
            &source,
            Instant::now() + Duration::from_secs(1),
            |image| {
                let resource = &resource;
                let source = &source;
                async move {
                    let record = match topic_config_record(
                        resource,
                        &image,
                        &TopicPolicy::default(),
                        false,
                        UnstableApiVersions::Disabled,
                    ) {
                        Ok(record) => record,
                        Err(error) => return Ok(Err(error)),
                    };
                    source.submit_change_if_image(vec![record], image).await?;
                    Ok(Ok(()))
                }
            },
        )
        .await
        .expect("bounded retry")
        .expect("guarded submission");
        check!(result == Ok(()));
        check!(
            source.submitted_records()
                == vec![MetadataRecord::V1TopicConfig(TopicConfigRecord {
                    topic: "orders".into(),
                    overrides: overrides_map(&[
                        StoredOverride::new(RETENTION_MS, "120000"),
                        StoredOverride::new(SEGMENT_BYTES, "1048576"),
                    ]),
                })]
        );
    }

    fn config(name: &str, operation: i8, value: Option<&str>) -> AlterableConfig {
        AlterableConfig {
            name: name.into(),
            config_operation: operation,
            value: value.map(str::to_owned),
            ..Default::default()
        }
    }

    #[derive(Default)]
    struct IncrementalShapeScenario {
        resources: Vec<AlterConfigsResource>,
        outcomes: Option<Vec<Result<(), (i16, String)>>>,
    }
    #[rstest::fixture]
    fn incremental_shape() -> IncrementalShapeScenario {
        let resources = Vec::new();
        IncrementalShapeScenario {
            resources,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given("the incremental configuration request contains resources")]
    fn incremental_resources(
        incremental_shape: &mut IncrementalShapeScenario,
        datatable: Vec<Vec<String>>,
    ) {
        incremental_shape.resources = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [kind, name, configs] = row.as_slice() else {
                    panic!("resource type name and configuration operations required")
                };
                let resource_type = match kind.as_str() {
                    "topic" => RESOURCE_TYPE_TOPIC,
                    "group" => RESOURCE_TYPE_GROUP,
                    _ => panic!("unknown resource kind: {kind}"),
                };
                let configs = if configs == "none" {
                    Vec::new()
                } else {
                    configs
                        .split(',')
                        .map(|spec| {
                            let columns: Vec<_> = spec.split(':').collect();
                            let [name, operation, value] = columns.as_slice() else {
                                panic!("config name operation value required")
                            };
                            config(
                                name,
                                operation.parse().unwrap(),
                                if *value == "null" { None } else { Some(*value) },
                            )
                        })
                        .collect()
                };
                AlterConfigsResource {
                    resource_type,
                    resource_name: name.clone(),
                    configs,
                    ..Default::default()
                }
            })
            .collect();
    }
    #[rstest_bdd_macros::when(
        "the broker preprocesses duplicate resources duplicate keys and null operations"
    )]
    fn preprocess_incremental_shape(incremental_shape: &mut IncrementalShapeScenario) {
        let flags = resource_duplicate_flags(&incremental_shape.resources);
        incremental_shape.outcomes = Some(
            incremental_shape
                .resources
                .iter()
                .zip(flags)
                .map(|(resource, duplicate)| validate_resource_shape(resource, duplicate))
                .collect(),
        );
    }
    #[rstest_bdd_macros::then("the incremental request shape outcomes in order are")]
    fn check_incremental_shape(
        incremental_shape: &IncrementalShapeScenario,
        datatable: Vec<Vec<String>>,
    ) {
        let expected: Vec<_> = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [code, message] = row.as_slice() else {
                    panic!("error code and message required")
                };
                let code = code.parse::<i16>().unwrap();
                if code == codes::NONE {
                    Ok(())
                } else {
                    Err((code, message.clone()))
                }
            })
            .collect();
        check!(incremental_shape.outcomes.as_ref().unwrap() == &expected);
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_incremental_config_shape",fixtures=[incremental_shape:IncrementalShapeScenario]);
}
