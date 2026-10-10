//! Configuration and feature update request fixtures.

use krabka_protocol::owned::{
    describe_configs_request::DescribeConfigsResource,
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
    update_features_request::FeatureUpdateKey,
};

pub fn incremental_config(
    name: impl Into<String>,
    value: Option<String>,
    config_operation: i8,
) -> AlterableConfig {
    AlterableConfig {
        name: name.into(),
        value,
        config_operation,
        ..Default::default()
    }
}

pub fn incremental_resource(
    resource_type: i8,
    resource_name: impl Into<String>,
    configs: Vec<AlterableConfig>,
) -> AlterConfigsResource {
    AlterConfigsResource {
        resource_type,
        resource_name: resource_name.into(),
        configs,
        ..Default::default()
    }
}

pub fn incremental_request(
    resources: Vec<AlterConfigsResource>,
    validate_only: bool,
) -> IncrementalAlterConfigsRequest {
    IncrementalAlterConfigsRequest {
        resources,
        validate_only,
        ..Default::default()
    }
}

pub fn describe_resource(
    resource_type: i8,
    resource_name: impl Into<String>,
    configuration_keys: Option<Vec<String>>,
) -> DescribeConfigsResource {
    DescribeConfigsResource {
        resource_type,
        resource_name: resource_name.into(),
        configuration_keys,
        ..Default::default()
    }
}

pub fn feature_update(
    feature: impl Into<String>,
    max_version_level: i16,
    upgrade_type: i8,
) -> FeatureUpdateKey {
    FeatureUpdateKey {
        feature: feature.into(),
        max_version_level,
        upgrade_type,
        ..Default::default()
    }
}

/// Read a named topic config through the wire API, including its error checks.
///
/// # Panics
/// Panics if the request fails or the requested topic config is absent.
pub async fn topic_config(
    client: &krabka_client_core::Client,
    topic: &str,
    name: &str,
) -> krabka_protocol::owned::describe_configs_response::DescribeConfigsResourceResult {
    let response = client
        .send(
            krabka_protocol::owned::describe_configs_request::DescribeConfigsRequest {
                resources: vec![describe_resource(
                    2,
                    topic.to_owned(),
                    Some(vec![name.to_owned()]),
                )],
                include_synonyms: false,
                include_documentation: false,
                ..Default::default()
            },
        )
        .await
        .expect("DescribeConfigs");
    let result = &response.results[0];
    assert2::assert!(
        result.error_code == krabka_broker::codes::NONE,
        "DescribeConfigs({topic}): {result:?}"
    );
    result
        .configs
        .iter()
        .find(|entry| entry.name == name)
        .cloned()
        .unwrap_or_else(|| panic!("no {name} entry for {topic}"))
}

/// Return one incremental topic-config mutation's code and message.
///
/// # Panics
/// Panics if the request fails or the broker omits the topic response.
pub async fn alter_topic_config(
    client: &krabka_client_core::Client,
    topic: &str,
    name: &str,
    operation: i8,
    value: Option<&str>,
) -> (i16, Option<String>) {
    let response = client
        .send(incremental_request(
            vec![incremental_resource(
                2,
                topic.to_owned(),
                vec![incremental_config(
                    name.to_owned(),
                    value.map(ToOwned::to_owned),
                    operation,
                )],
            )],
            false,
        ))
        .await
        .expect("IncrementalAlterConfigs");
    let row = &response.responses[0];
    (row.error_code, row.error_message.clone())
}

/// One legacy `AlterConfigs` key, including deletions expressed as a nullable value.
pub fn legacy_config(
    name: String,
    value: Option<String>,
) -> krabka_protocol::owned::alter_configs_request::AlterableConfig {
    krabka_protocol::owned::alter_configs_request::AlterableConfig {
        name,
        value,
        ..Default::default()
    }
}

/// A legacy configuration resource with caller-owned type, name and ordered keys.
pub fn legacy_resource(
    resource_type: i8,
    resource_name: String,
    configs: Vec<krabka_protocol::owned::alter_configs_request::AlterableConfig>,
) -> krabka_protocol::owned::alter_configs_request::AlterConfigsResource {
    krabka_protocol::owned::alter_configs_request::AlterConfigsResource {
        resource_type,
        resource_name,
        configs,
        ..Default::default()
    }
}

/// Preserve legacy replacement semantics and the explicit validation mode.
pub fn legacy_request(
    resources: Vec<krabka_protocol::owned::alter_configs_request::AlterConfigsResource>,
    validate_only: bool,
) -> krabka_protocol::owned::alter_configs_request::AlterConfigsRequest {
    krabka_protocol::owned::alter_configs_request::AlterConfigsRequest {
        resources,
        validate_only,
        ..Default::default()
    }
}

/// A controller supporting every stable metadata version, without endpoints.
pub fn controller_registration(
    node_id: krabka_metadata::NodeId,
) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1ControllerRegistration(
        krabka_metadata::ControllerRegistrationRecord {
            node_id,
            incarnation_id: uuid::Uuid::from_u128(u128::from(node_id.0)),
            zk_migration_ready: false,
            endpoints: Vec::new(),
            features: std::collections::BTreeMap::from([(
                "metadata.version".to_owned(),
                (7, krabka_metadata::metadata_version::METADATA_VERSION_MAX),
            )]),
        },
    )
}
