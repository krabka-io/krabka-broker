//! The machinery `AlterConfigs` and `IncrementalAlterConfigs` share.
//!
//! Both APIs run Kafka's `ConfigAdminManager.preprocess` shape checks and
//! broker-side checks before they forward, authorize each resource the same
//! way, dispatch it to an API-specific record builder per resource type, and
//! submit the records through the same guarded optimistic retry. Each API's
//! own module keeps its wire entry point and its record builders; this module
//! holds the rest, so a change to one API's behavior edits only that API's
//! module.

use {
    crate::{
        api_catalog::UnstableApiVersions,
        broker::Broker,
        codes::{
            INVALID_CONFIG, INVALID_REQUEST, NOT_CONTROLLER, POLICY_VIOLATION, REQUEST_TIMED_OUT,
            UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION,
        },
        config_keys::{
            self, MIN_INSYNC_REPLICAS,
            broker_dynamic::BrokerConfigContext,
            group::{GroupBounds, validate_group_configs},
            listener_reconfig::OwnBrokerChecks,
        },
        handlers::{
            RequestContext, config_resource_refusal,
            describe_configs::{
                RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
                RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
            },
            forward_to_controller::{guarded_operation_on_source, wrong_controller_message},
            response_encoding::ResourceErrorRow,
        },
        topic_policy::{TopicPolicy, check as check_topic_policy},
    },
    krabka_metadata::{GroupConfigRecord, MetadataImage, MetadataRecord, NodeId},
    krabka_protocol::owned::{
        alter_configs_request::AlterConfigsResource, incremental_alter_configs_request,
    },
    krabka_raft::RaftError,
    std::{
        collections::{BTreeMap, BTreeSet, HashMap},
        future::Future,
        path::PathBuf,
        sync::Arc,
        time::Duration,
    },
    tokio::time::{Instant, error::Elapsed},
};

#[cfg(test)]
mod guarded_failure_tests;

pub(super) trait ResourceErrorRequestResource {
    fn resource_type(&self) -> i8;
    fn resource_name(&self) -> &str;
}

impl ResourceErrorRequestResource for AlterConfigsResource {
    fn resource_type(&self) -> i8 { self.resource_type }

    fn resource_name(&self) -> &str { &self.resource_name }
}

impl ResourceErrorRequestResource for incremental_alter_configs_request::AlterConfigsResource {
    fn resource_type(&self) -> i8 { self.resource_type }

    fn resource_name(&self) -> &str { &self.resource_name }
}

pub(super) fn empty_resource_response<Resource, Row>(resource: &Resource) -> Row
where
    Resource: ResourceErrorRequestResource,
    Row: ResourceErrorRow,
{
    Row::for_resource(
        resource.resource_type(),
        resource.resource_name().to_owned(),
    )
}

pub(super) fn ordered_resource_responses<Row>(mut indexed: Vec<(usize, Row)>) -> Vec<Row> {
    indexed.sort_unstable_by_key(|(index, _)| *index);
    indexed.into_iter().map(|(_, row)| row).collect()
}

/// The row of `resource` refused with `refusal`, its error code and message.
pub(super) fn resource_refusal<Resource, Row>(
    resource: &Resource,
    (error_code, error_message): (i16, Option<String>),
) -> Row
where
    Resource: ResourceErrorRequestResource,
    Row: ResourceErrorRow,
{
    let mut row: Row = empty_resource_response(resource);
    row.set_error(error_code, error_message);
    row
}

pub(super) fn resource_error_rows<'a, Resource, Row>(
    resources: impl Iterator<Item = &'a Resource>,
    code: i16,
    message: Option<&str>,
) -> Vec<Row>
where
    Resource: ResourceErrorRequestResource + 'a,
    Row: ResourceErrorRow,
{
    resources
        .map(|resource| resource_refusal(resource, (code, message.map(str::to_owned))))
        .collect()
}

/// The checks both alter-config APIs run on a topic's resulting override map
/// once its own keys have validated: the storage transitions against the
/// stored map, then the existence check, then the operator's policy.
pub(super) fn finish_topic_overrides(
    image: &MetadataImage,
    topic: &str,
    current: Option<&BTreeMap<String, String>>,
    overrides: &BTreeMap<String, String>,
    policy: &TopicPolicy,
) -> Result<(), (i16, String)> {
    config_keys::validate_diskless_unchanged(current, overrides)
        .map_err(|reason| (INVALID_CONFIG, reason))?;
    config_keys::validate_remote_storage_disable(current, overrides)
        .map_err(|reason| (INVALID_CONFIG, reason))?;
    if image.topic(topic).is_none() {
        return Err((
            UNKNOWN_TOPIC_OR_PARTITION,
            format!("The topic '{topic}' does not exist."),
        ));
    }
    // KIP-133: the operator-declared policy, on the map the topic ends up
    // with. Kafka calls `AlterConfigPolicy.validate` on the same resolved map,
    // and its `RequestMetadata` carries no partition count and no replication
    // factor, so neither is passed here. It runs after the built-in
    // validators, as `ConfigAdminManager` does: a config the broker itself
    // refuses never reaches the policy.
    check_topic_policy(policy, topic, None, None, overrides)
        .map_err(|reason| (POLICY_VIOLATION, reason))
}

/// The `V1GroupConfig` record both alter-config APIs stage for a GROUP
/// resource's resulting config map, after
/// `ControllerConfigurationValidator.validateGroupName` and Kafka's
/// `GroupConfig.validate` against the broker's group bounds.
pub(super) fn group_config_record_from(
    name: &str,
    configs: BTreeMap<String, String>,
    bounds: &GroupBounds,
    unstable: UnstableApiVersions,
) -> Result<MetadataRecord, (i16, String)> {
    if name.is_empty() {
        return Err((
            INVALID_REQUEST,
            "Default group resources are not allowed.".into(),
        ));
    }
    validate_group_configs(&configs, bounds, unstable)
        .map_err(|reason| (INVALID_CONFIG, reason))?;
    Ok(MetadataRecord::V1GroupConfig(GroupConfigRecord {
        group_id: name.to_owned(),
        configs,
    }))
}

/// Run one resource's alter through the guarded optimistic retry: each attempt
/// plans against a fresh controller image and submits only if that image is
/// still current, so a conflicting commit replans every operation.
///
/// # Errors
/// Returns the code and message the resource answers with when the retry
/// gives up. See [`guarded_resource_outcome`].
pub(super) async fn guarded_resource_response<Row, Attempt, Fut>(
    broker: &Broker,
    operation: &str,
    attempt: Attempt,
) -> Result<Row, (i16, Option<String>)>
where
    Attempt: FnMut(Arc<MetadataImage>) -> Fut,
    Fut: Future<Output = Result<Row, RaftError>> + Send,
{
    let result = guarded_operation_on_source(
        broker.controller.as_ref(),
        Instant::now() + Duration::from_secs(5),
        attempt,
    )
    .await;
    guarded_resource_outcome(result, operation)
}

/// The row of a guarded resource alter, or the code and message the resource
/// answers with when the retry gave up.
///
/// Kafka's `ControllerApis.handleLegacyAlterConfigs` and
/// `handleIncrementalAlterConfigs` answer a failed write through
/// `requestHelper.handleError`, whose `AlterConfigsRequest.getErrorResponse`
/// and `IncrementalAlterConfigsRequest.getErrorResponse` set
/// `ApiError.message()` on every resource row. A controller that is not
/// active fails the write with `ControllerExceptions.newWrongControllerException`,
/// so a `NOT_CONTROLLER` row carries [`wrong_controller_message`] for the
/// leader the refusal redirected to.
///
/// # Errors
/// Returns `REQUEST_TIMED_OUT` at the deadline, `NOT_CONTROLLER` with Kafka's
/// message when this node is no longer the leader, and
/// `UNKNOWN_SERVER_ERROR`, logged under `operation`, for any other submit
/// failure.
pub(super) fn guarded_resource_outcome<Row>(
    result: Result<Result<Row, RaftError>, Elapsed>,
    operation: &str,
) -> Result<Row, (i16, Option<String>)> {
    match result {
        Ok(Ok(row)) => Ok(row),
        Err(_) => Err((REQUEST_TIMED_OUT, None)),
        Ok(Err(RaftError::NotLeader { current_leader })) => Err((
            NOT_CONTROLLER,
            Some(wrong_controller_message(current_leader)),
        )),
        Ok(Err(RaftError::LeaderUnknown)) => {
            Err((NOT_CONTROLLER, Some(wrong_controller_message(None))))
        }
        Ok(Err(error)) => {
            tracing::error!(%error, "{operation} submit_change failed");
            Err((UNKNOWN_SERVER_ERROR, None))
        }
    }
}

/// The refusal a resource answers with before any record is built: its
/// request-shape error, else its authorization error. Kafka's
/// `ConfigAdminManager.preprocess` checks the shape before
/// `ControllerApis.authorizeAlterResource` runs.
pub(super) fn resource_preamble_refusal<Resource>(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource: &Resource,
    shape: Result<(), (i16, String)>,
    broker_logger_supported: bool,
) -> Option<(i16, String)>
where
    Resource: ResourceErrorRequestResource,
{
    if let Err(error) = shape {
        return Some(error);
    }
    config_resource_refusal(
        broker.config.authorizer.as_ref(),
        image,
        ctx,
        (resource.resource_type(), resource.resource_name()),
        broker_logger_supported,
    )
}

/// The result of a builder that stages one record.
type RecordResult = Result<MetadataRecord, (i16, String)>;
/// The result of a builder that stages any number of records.
type RecordsResult = Result<Vec<MetadataRecord>, (i16, String)>;

/// Each API's record builder for one resource type, which
/// [`resource_records`] dispatches to.
pub(super) struct RecordBuilders<Resource> {
    pub(super) topic:
        fn(&Resource, &MetadataImage, &TopicPolicy, bool, UnstableApiVersions) -> RecordResult,
    pub(super) broker: fn(&Resource, BrokerConfigContext<'_>) -> RecordsResult,
    pub(super) group:
        fn(&Resource, &MetadataImage, &GroupBounds, UnstableApiVersions) -> RecordResult,
    pub(super) client_metrics: fn(&Resource, &MetadataImage) -> RecordsResult,
}

/// The records a TOPIC, BROKER, `CLIENT_METRICS` or GROUP resource stages, or
/// the error it answers with, from this broker's settings and the resource's
/// API-specific `builders`.
pub(super) fn resource_records<Resource>(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource: &Resource,
    builders: &RecordBuilders<Resource>,
) -> RecordsResult
where
    Resource: ResourceErrorRequestResource,
{
    let unstable = broker.config.features.unstable_api_versions;
    match resource.resource_type() {
        RESOURCE_TYPE_TOPIC => (builders.topic)(
            resource,
            image,
            &broker.config.topic_policy,
            broker.config.remote_storage_backend.is_some(),
            unstable,
        )
        .map(|record| vec![record]),
        RESOURCE_TYPE_BROKER => (builders.broker)(resource, BrokerConfigContext {
            image,
            serving: forwarded_serving_node(broker, ctx, resource.resource_name()),
            log_dirs: &forwarded_log_dirs(broker, ctx),
            unstable,
        }),
        RESOURCE_TYPE_GROUP => {
            (builders.group)(resource, image, &GroupBounds::of(&broker.config), unstable)
                .map(|record| vec![record])
        }
        RESOURCE_TYPE_CLIENT_METRICS => (builders.client_metrics)(resource, image),
        other => unreachable!("resource type {other} passed the ACL dispatch"),
    }
}

/// The node a BROKER resource's checks run for: the node the client
/// addressed when the request was forwarded to this controller, else this
/// node.
fn forwarded_serving_node(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    resource_name: &str,
) -> NodeId {
    if ctx.forwarded && !resource_name.is_empty() {
        resource_name
            .parse::<u64>()
            .map_or(broker.config.node_id, NodeId)
    } else {
        broker.config.node_id
    }
}

/// This node's log directories for a BROKER resource's checks, or none when
/// the request was forwarded and so addresses another node's directories.
fn forwarded_log_dirs(broker: &Broker, ctx: &RequestContext<'_>) -> Vec<PathBuf> {
    if ctx.forwarded {
        Vec::new()
    } else {
        broker.config.broker_log_dirs()
    }
}

/// The broker-side checks one API runs on a BROKER resource against this
/// node's own settings.
pub(super) type PreprocessBroker<Resource> =
    fn(&Resource, BrokerConfigContext<'_>, OwnBrokerChecks<'_>) -> Result<(), (i16, String)>;

/// One resource's broker-side preprocessing, as
/// [`broker_preprocess_refusal`] runs it.
pub(super) struct BrokerPreprocess<'a, Resource> {
    pub(super) resource: &'a Resource,
    /// The request-shape result, which the caller computes because each API
    /// has its own rule for a missing value.
    pub(super) shape: Result<(), (i16, String)>,
    /// Whether this resource has broker-side checks to run at all.
    pub(super) runs_preprocess: bool,
    /// The broker-side checks against this node's own settings.
    pub(super) preprocess: PreprocessBroker<Resource>,
    /// Whether this API accepts a `BROKER_LOGGER` resource, which it then
    /// authorizes against the cluster.
    pub(super) broker_logger_supported: bool,
}

/// The refusal a resource earns from the broker-side preprocessing that both
/// alter-config APIs run before they forward to the controller, or `None`
/// when the resource goes on to the controller.
///
/// Kafka's `ConfigAdminManager.preprocess` precedence: a shape error answers
/// before authorization runs, and an authorization error answers before a
/// broker-side check failure.
pub(super) fn broker_preprocess_refusal<Resource>(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    image: &MetadataImage,
    request: BrokerPreprocess<'_, Resource>,
) -> Option<(i16, String)>
where
    Resource: ResourceErrorRequestResource,
{
    let BrokerPreprocess {
        resource,
        shape,
        runs_preprocess,
        preprocess,
        broker_logger_supported,
    } = request;
    if let Err(error) = shape {
        return Some(error);
    }
    if !runs_preprocess {
        return None;
    }
    let (code, message) = preprocess(
        resource,
        BrokerConfigContext {
            image,
            serving: broker.config.node_id,
            log_dirs: &broker.config.broker_log_dirs(),
            unstable: broker.config.features.unstable_api_versions,
        },
        broker.own_config_checks(),
    )
    .err()?;
    Some(
        config_resource_refusal(
            broker.config.authorizer.as_ref(),
            image,
            ctx,
            (resource.resource_type(), resource.resource_name()),
            broker_logger_supported,
        )
        .unwrap_or((code, message)),
    )
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct ConfigResourceIdentity<'a> {
    resource_type: i8,
    resource_name: &'a str,
}

/// Kafka's `ConfigAdminManager.preprocess` rejects a request that names the
/// same `(resource_type, resource_name)` pair more than once, on every row
/// that names it. This computes that flag for each resource in request order
/// before any of them is authorized or processed, since the duplicate check
/// has to see the whole request at once.
///
/// `resources` yields each typed resource in request
/// order. `AlterConfigs` and `IncrementalAlterConfigs` both call this.
pub(super) fn duplicate_resource_flags<'a, Resource>(
    resources: impl IntoIterator<Item = &'a Resource>,
) -> Vec<bool>
where
    Resource: ResourceErrorRequestResource + 'a,
{
    let keys: Vec<_> = resources
        .into_iter()
        .map(|resource| ConfigResourceIdentity {
            resource_type: resource.resource_type(),
            resource_name: resource.resource_name(),
        })
        .collect();
    let mut counts: HashMap<ConfigResourceIdentity<'_>, usize> = HashMap::new();
    for key in &keys {
        *counts.entry(*key).or_insert(0) += 1;
    }
    keys.iter()
        .map(|key| counts.get(key).is_some_and(|&count| count > 1))
        .collect()
}

/// Kafka's `ConfigAdminManager.preprocess` shape checks, which run before
/// any authorization: a resource named more than once in the request, a
/// config key named more than once within a resource, and a null value where
/// the API does not allow one.
///
/// `configs` yields each config's `(name, null_not_allowed)`, where the flag
/// is set when the config carries no value and the operation needs one.
/// Legacy `AlterConfigs` never deletes by omitting a value, so every null is
/// refused there. `IncrementalAlterConfigs` allows a null on DELETE only.
pub(super) fn validate_resource_shape<'a>(
    is_duplicate: bool,
    configs: impl IntoIterator<Item = (&'a str, bool)>,
) -> Result<(), (i16, String)> {
    if is_duplicate {
        return Err((
            INVALID_REQUEST,
            "Each resource must appear at most once.".into(),
        ));
    }
    let configs: Vec<(&str, bool)> = configs.into_iter().collect();
    let mut seen = BTreeSet::new();
    if configs.iter().any(|(name, _)| !seen.insert(*name)) {
        return Err((INVALID_REQUEST, "Error due to duplicate config keys".into()));
    }
    let null_names: Vec<&str> = configs
        .iter()
        .filter(|(_, null_not_allowed)| *null_not_allowed)
        .map(|(name, _)| *name)
        .collect();
    if !null_names.is_empty() {
        return Err((
            INVALID_REQUEST,
            format!("Null value not supported for : {}", null_names.join(", ")),
        ));
    }
    Ok(())
}

/// The longest config value Kafka's controller writes: `Short.MAX_VALUE`
/// UTF-16 code units, which is what `String.length()` counts.
const MAX_CONFIG_VALUE_LENGTH: usize = 32_767;

/// A changed minimum ISR clears ELR; incremental topic requests opt in only
/// when they mention the key, while legacy replacement always considers it.
pub(super) fn changes_min_isr(
    image: &krabka_metadata::MetadataImage,
    record: &krabka_metadata::MetadataRecord,
    include_topic: bool,
) -> bool {
    match record {
        krabka_metadata::MetadataRecord::V1TopicConfig(config) => {
            include_topic
                && image
                    .topic_config(&config.topic)
                    .and_then(|current| current.get(MIN_INSYNC_REPLICAS))
                    != config.overrides.get(MIN_INSYNC_REPLICAS)
        }
        krabka_metadata::MetadataRecord::V1BrokerConfig(config) => {
            config.node_id == krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
                && config.config_name == MIN_INSYNC_REPLICAS
                && image
                    .broker_config(config.node_id)
                    .and_then(|current| current.get(&config.config_name))
                    != config.config_value.as_ref()
        }
        _ => false,
    }
}

/// Kafka's `ConfigurationControlManager.validateAlterConfig` refuses, for
/// every resource type, a written value longer than [`MAX_CONFIG_VALUE_LENGTH`]
/// with `INVALID_CONFIG` (`DISALLOWED_CONFIG_VALUE_SIZE_ERROR`), because a
/// `ConfigRecord` cannot carry more.
///
/// `records` are the ones a resource's alter ended up building, which hold
/// every value the alter writes.
pub(super) fn config_value_size_error(
    records: &[krabka_metadata::MetadataRecord],
) -> Option<(i16, String)> {
    use krabka_metadata::MetadataRecord;

    let too_long = |value: &String| value.encode_utf16().count() > MAX_CONFIG_VALUE_LENGTH;
    let oversized = records.iter().any(|record| match record {
        MetadataRecord::V1TopicConfig(config) => config.overrides.values().any(too_long),
        MetadataRecord::V1BrokerConfig(config) => config.config_value.iter().any(too_long),
        MetadataRecord::V1GroupConfig(config) => config.configs.values().any(too_long),
        MetadataRecord::V1ClientMetricsConfig(config) => config.configs.values().any(too_long),
        _ => false,
    });
    oversized.then(|| {
        (
            INVALID_CONFIG,
            format!(
                "The configuration value cannot be added because it exceeds the maximum value \
                 size of {MAX_CONFIG_VALUE_LENGTH} bytes."
            ),
        )
    })
}

/// The audit `resource_type` for a KIP-133 config resource-type discriminant.
pub(super) fn config_resource_type(resource_type: i8) -> &'static str {
    match resource_type {
        RESOURCE_TYPE_TOPIC => "Topic",
        RESOURCE_TYPE_BROKER => "Broker",
        RESOURCE_TYPE_BROKER_LOGGER => "BrokerLogger",
        RESOURCE_TYPE_CLIENT_METRICS => "ClientMetrics",
        RESOURCE_TYPE_GROUP => "Group",
        _ => "Unknown",
    }
}
