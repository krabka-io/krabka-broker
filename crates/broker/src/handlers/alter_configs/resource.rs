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

use {
    super::{
        RESOURCE_TYPE_TOPIC, RecordBuilders, broker_configs::broker_config_records,
        client_metrics_configs::client_metrics_config_record, empty_resource_response,
        group_configs::group_config_record, guarded_resource_response, resource_preamble_refusal,
        resource_records, topic_configs::topic_config_record, validate_resource_shape,
    },
    crate::{
        broker::Broker,
        config_keys::clear_elr_records,
        handlers::{RequestContext, audit_admin_success, response_encoding::ErrorRow as _},
    },
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        alter_configs_request::AlterConfigsResource,
        alter_configs_response::AlterConfigsResourceResponse,
    },
    krabka_raft::RaftError,
    std::sync::Arc,
};

/// `AlterConfigs`' builder for each resource type. GROUP and `CLIENT_METRICS`
/// replace the whole map, so their builders do not read the image.
const RECORD_BUILDERS: RecordBuilders<AlterConfigsResource> = RecordBuilders {
    topic: topic_config_record,
    broker: broker_config_records,
    group: |resource, _image, bounds, unstable| group_config_record(resource, bounds, unstable),
    client_metrics: |resource, _image| {
        client_metrics_config_record(resource).map(|record| vec![record])
    },
};

pub(super) async fn process_resource(
    broker: &Broker,
    _image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource: AlterConfigsResource,
    validate_only: bool,
    is_duplicate: bool,
) -> AlterConfigsResourceResponse {
    guarded_resource_response(broker, "AlterConfigs", |image| {
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
    .unwrap_or_else(|refusal| resource_refusal(&resource, refusal))
}

/// The row of `resource` whose guarded alter gave up with `refusal`, its
/// error code and message.
pub(super) fn resource_refusal(
    resource: &AlterConfigsResource,
    (error_code, error_message): (i16, Option<String>),
) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
        error_code,
        error_message,
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
    let out: AlterConfigsResourceResponse = empty_resource_response(&resource);
    let shape = validate_resource_shape(
        is_duplicate,
        resource
            .configs
            .iter()
            .map(|cfg| (cfg.name.as_str(), cfg.value.is_none())),
    );
    if let Some((code, message)) =
        resource_preamble_refusal(broker, &image, ctx, &resource, shape, false)
    {
        return Ok(out.with_error(code, message));
    }

    let records = resource_records(broker, &image, ctx, &resource, &RECORD_BUILDERS);
    let mut records = match records {
        Ok(records) => records,
        Err((code, message)) => {
            return Ok(out.with_error(code, message));
        }
    };
    if let Some((code, message)) = super::config_value_size_error(&records) {
        return Ok(out.with_error(code, message));
    }
    let min_isr_changed = records
        .iter()
        .any(|record| super::changes_min_isr(&image, record, true));
    if min_isr_changed {
        let topic = (resource.resource_type == RESOURCE_TYPE_TOPIC)
            .then_some(resource.resource_name.as_str());
        records.extend(clear_elr_records(&image, topic));
    }
    if validate_only {
        // Validation pass already happened above (per-config loop). Nothing
        // to submit; the response already carries the per-resource result
        // (NONE if all configs validated, INVALID_CONFIG with reason on any
        // rejection). This matches Apache Kafka's --dry-run behavior.
        return Ok(out);
    }
    if records.is_empty() {
        return Ok(out);
    }
    let audited = super::audit_resources_for(&resource, &image);
    broker
        .controller
        .submit_change_if_image(records, image)
        .await?;
    audit_admin_success(broker.audit_log.as_ref(), ctx, "AlterConfigs", audited);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            api_catalog::UnstableApiVersions,
            codes,
            config_keys::{REMOTE_STORAGE_ENABLE, broker_dynamic::BrokerConfigContext},
            handlers::{
                alter_configs::test_support::{
                    broker_resource, image_with_broker, image_with_topic_config, topic_resource,
                },
                forward_to_controller::guarded_operation_on_source,
            },
            metadata_source::MetadataSource,
            test_support::FakeMetadataSource,
            throttle::{FOLLOWER_THROTTLED_RATE_KEY, LEADER_THROTTLED_RATE_KEY},
            topic_policy::TopicPolicy,
        },
        assert2::assert,
        krabka_metadata::{BrokerConfigRecord, MetadataRecord},
        krabka_raft::NodeId,
        std::time::Duration,
        tokio::time::Instant,
    };

    async fn submit_replacement(
        source: &FakeMetadataSource,
        resource: &AlterConfigsResource,
        build: impl Fn(
            &AlterConfigsResource,
            &MetadataImage,
        ) -> Result<Vec<MetadataRecord>, (i16, String)>
        + Sync,
    ) -> Result<(), (i16, String)> {
        guarded_operation_on_source(source, Instant::now() + Duration::from_secs(1), |image| {
            let build = &build;
            async move {
                let records = match build(resource, &image) {
                    Ok(records) => records,
                    Err(error) => return Ok(Err(error)),
                };
                source.submit_change_if_image(records, image).await?;
                Ok(Ok(()))
            }
        })
        .await
        .expect("bounded retry")
        .expect("guarded submission")
    }

    #[bon::builder]
    fn source_with_intervening_image(
        initial: MetadataImage,
        newer: MetadataImage,
    ) -> FakeMetadataSource {
        FakeMetadataSource::builder()
            .image(initial)
            .leader(Some(NodeId(1)))
            .before_guarded_submit(move |source| source.set_image(newer))
            .build()
    }

    /// A full replacement must delete overrides committed after its first
    /// planning attempt as well as those the first image contained.
    #[tokio::test]
    async fn replacement_replans_tombstones_after_an_intervening_override() {
        let record = |key: &str, value: Option<&str>| {
            MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: NodeId(1),
                config_name: key.into(),
                config_value: value.map(str::to_owned),
            })
        };
        let mut initial = image_with_broker(1);
        initial.apply(&record(LEADER_THROTTLED_RATE_KEY, Some("1")));
        let mut newer = initial.clone();
        newer.apply(&record(FOLLOWER_THROTTLED_RATE_KEY, Some("2")));
        let source = source_with_intervening_image()
            .initial(initial)
            .newer(newer)
            .call();
        let resource = broker_resource("1", &[]);
        let result = submit_replacement(&source, &resource, |resource, image| {
            broker_config_records(resource, BrokerConfigContext {
                image,
                serving: NodeId(1),
                log_dirs: &[],
                unstable: UnstableApiVersions::Disabled,
            })
        })
        .await;
        assert!(result == Ok(()));
        assert!(
            source.submitted_records()
                == vec![
                    record(FOLLOWER_THROTTLED_RATE_KEY, None),
                    record(LEADER_THROTTLED_RATE_KEY, None)
                ]
        );
    }

    /// A storage enable committed between validation and append changes the
    /// safety precondition for a disable. The retry must reject it.
    #[tokio::test]
    async fn replacement_rechecks_remote_storage_disable_after_a_conflict() {
        let initial = image_with_topic_config("t", &[(REMOTE_STORAGE_ENABLE, "false")]);
        let newer = image_with_topic_config("t", &[(REMOTE_STORAGE_ENABLE, "true")]);
        let source = source_with_intervening_image()
            .initial(initial)
            .newer(newer)
            .call();
        let resource = topic_resource("t", &[(REMOTE_STORAGE_ENABLE, "false")]);
        let result = submit_replacement(&source, &resource, |resource, image| {
            topic_config_record(
                resource,
                image,
                &TopicPolicy::default(),
                true,
                UnstableApiVersions::Disabled,
            )
            .map(|record| vec![record])
        })
        .await;
        assert!(matches!(result, Err((codes::INVALID_CONFIG, _))));
        assert!(source.submitted_records().is_empty());
    }
}
