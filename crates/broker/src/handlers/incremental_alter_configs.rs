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
        config_keys::{
            MIN_INSYNC_REPLICAS, broker_dynamic::BrokerConfigContext, clear_elr_records,
            group::GroupBounds,
        },
        handlers::{
            ApiVersion, DispatchResult, RequestContext,
            alter_configs::{BrokerPreprocess, broker_preprocess_refusal},
            audit_resource, config_resource_refusal, encode_response,
            forward_to_controller::{guarded_operation_on_source, to_active_controller_or_handle},
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
    std::{sync::Arc, time::Duration},
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
        handlers::describe_configs::{
            RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
            RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
        },
    },
    std::{iter, mem},
    tokio::time::Instant,
};
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;
const OP_APPEND: i8 = 2;
const OP_SUBTRACT: i8 = 3;

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
        let mut req = krabka_macros::controller_request!(broker, ctx, req, version; self::handle, self::request_refusal);
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
                |incremental_request| handle(broker, incremental_request, version, ctx),
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
    let result = guarded_operation_on_source(
        broker.controller.as_ref(),
        Instant::now() + Duration::from_secs(5),
        |image| {
            process_resource_once(
                broker,
                image,
                ctx,
                resource.clone(),
                validate_only,
                is_duplicate,
            )
        },
    )
    .await;
    let code = match result {
        Ok(Ok(response)) => return response,
        Err(_) => codes::REQUEST_TIMED_OUT,
        Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => codes::NOT_CONTROLLER,
        Ok(Err(error)) => {
            tracing::error!(%error, "IncrementalAlterConfigs submit_change failed");
            codes::UNKNOWN_SERVER_ERROR
        }
    };
    resource_refusal(&resource, code)
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

/// The records a TOPIC, BROKER, CLIENT_METRICS or GROUP resource stages, or
/// the error it answers with.
fn resource_records(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource: &AlterConfigsResource,
) -> Result<Vec<MetadataRecord>, (i16, String)> {
    match resource.resource_type {
        RESOURCE_TYPE_TOPIC => topic_config_record(
            resource,
            image,
            &broker.config.topic_policy,
            broker.config.remote_storage_backend.is_some(),
            broker.config.features.unstable_api_versions,
        )
        .map(|record| vec![record]),
        RESOURCE_TYPE_BROKER => {
            let serving = if ctx.forwarded && !resource.resource_name.is_empty() {
                resource
                    .resource_name
                    .parse::<u64>()
                    .map_or(broker.config.node_id, krabka_metadata::NodeId)
            } else {
                broker.config.node_id
            };
            let log_dirs = if ctx.forwarded {
                Vec::new()
            } else {
                broker.config.broker_log_dirs()
            };
            broker_records(resource, BrokerConfigContext {
                image,
                serving,
                log_dirs: &log_dirs,
                unstable: broker.config.features.unstable_api_versions,
            })
        }
        RESOURCE_TYPE_CLIENT_METRICS => client_metrics_records(resource, image),
        RESOURCE_TYPE_GROUP => group_record(
            resource,
            image,
            &GroupBounds::of(&broker.config),
            broker.config.features.unstable_api_versions,
        )
        .map(|record| vec![record]),
        other => unreachable!("resource type {other} passed the ACL dispatch"),
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
    if let Err((code, message)) = validate_resource_shape(&resource, is_duplicate) {
        return Ok(out.with_error(code, message));
    }
    if let Some((code, message)) = config_resource_refusal(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        (resource.resource_type, &resource.resource_name),
        true,
    ) {
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
    let mut to_submit = match resource_records(broker, &image, ctx, &resource) {
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
    };

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
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
            let expected = IncrementalAlterConfigsResponse {
                responses: ordered.responses,
                ..Default::default()
            };
            let broker_ref = broker.as_ref();
            let context_ref = &ctx;
            assert_dispatch_response(&request, &expected, 1, |body| async move {
                dispatch(broker_ref, 1, &body, context_ref).await
            })
            .await;
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
        let expected = IncrementalAlterConfigsResponse {
            responses: vec![config_resource_error_response(
                RESOURCE_TYPE_BROKER,
                name,
                error_code,
                Some(error_message.into()),
            )],
            ..Default::default()
        };
        let broker_ref = broker.as_ref();
        let context_ref = &ctx;
        assert_dispatch_response(&request, &expected, 1, |body| async move {
            dispatch(broker_ref, 1, &body, context_ref).await
        })
        .await;
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
