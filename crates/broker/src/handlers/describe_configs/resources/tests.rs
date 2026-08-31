//! Unit tests for `describe_one`: the entries and the config sources it
//! reports for each resource type, and the broker-config lookups the BROKER
//! branch is built on.

use std::collections::BTreeMap;

use assert2::assert;
use krabka_metadata::{BrokerConfigRecord, MetadataImage, MetadataRecord};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::describe_configs_response::{DescribeConfigsResourceResult, DescribeConfigsResult},
};
use uuid::Uuid;

use crate::config_keys;

/// The static broker values the handler synthesises. The two retention knobs
/// are distinctive so a test can tell them apart from a dynamic override.
fn static_broker() -> super::StaticBrokerConfigs {
    super::StaticBrokerConfigs {
        offsets_retention_minutes: 10_080,
        offsets_retention_check_interval_ms: 600_000,
    }
}

/// The two read-only entries [`static_broker`] produces, in the order a
/// described broker resource sorts them. Both hold Kafka's default, so both
/// report `DEFAULT_CONFIG`.
fn static_broker_entries() -> Vec<DescribeConfigsResourceResult> {
    vec![
        DescribeConfigsResourceResult {
            name: config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS.into(),
            value: Some("600000".into()),
            read_only: true,
            config_source: super::CONFIG_SOURCE_DEFAULT,
            ..Default::default()
        },
        DescribeConfigsResourceResult {
            name: config_keys::OFFSETS_RETENTION_MINUTES.into(),
            value: Some("10080".into()),
            read_only: true,
            config_source: super::CONFIG_SOURCE_DEFAULT,
            ..Default::default()
        },
    ]
}

/// Builds a minimal `MetadataImage` with one broker config entry.
fn image_with_broker_config(node_id: u64, key: &str, value: &str) -> MetadataImage {
    let mut img = MetadataImage::new(Uuid::nil());
    img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::NodeId(node_id),
        config_name: key.to_string(),
        config_value: Some(value.to_string()),
    }));
    img
}

#[test]
fn broker_resource_name_invalid_fails_parse() {
    // Non-numeric resource_name must fail to parse as NodeId.
    assert!("not-a-number".parse::<u64>().is_err());
}

#[test]
fn broker_resource_all_keys_returned_when_no_filter() {
    let img = image_with_broker_config(1, "leader.replication.throttled.rate", "1024");
    let map = img
        .broker_config(krabka_metadata::NodeId(1))
        .cloned()
        .unwrap_or_default();
    assert!(
        map.get("leader.replication.throttled.rate")
            .map(String::as_str)
            == Some("1024")
    );
}

#[test]
fn topic_describe_one_preserves_result_and_filtered_config_fields() {
    use krabka_metadata::TopicConfigRecord;

    let mut img = MetadataImage::new(Uuid::nil());
    let mut overrides = BTreeMap::new();
    overrides.insert("cleanup.policy".to_string(), "compact".to_string());
    overrides.insert("retention.ms".to_string(), "60000".to_string());
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "orders".into(),
        overrides,
    }));
    let result = super::describe_one(
        &img,
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_TOPIC,
            resource_name: "orders".into(),
            configuration_keys: Some(vec!["cleanup.policy".into()]),
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        static_broker(),
    );

    let expected = DescribeConfigsResult {
        error_code: crate::codes::NONE,
        error_message: None,
        resource_type: super::RESOURCE_TYPE_TOPIC,
        resource_name: "orders".to_string(),
        configs: vec![DescribeConfigsResourceResult {
            name: "cleanup.policy".to_string(),
            value: Some("compact".to_string()),
            read_only: false,
            config_source: super::CONFIG_SOURCE_DYNAMIC_TOPIC,
            is_sensitive: false,
            synonyms: Vec::new(),
            config_type: 0,
            documentation: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(result == expected);
}

#[test]
fn topic_describe_reports_the_fixed_data_path_key_as_read_only() {
    use krabka_metadata::TopicConfigRecord;

    let mut img = MetadataImage::new(Uuid::nil());
    let mut overrides = BTreeMap::new();
    overrides.insert(config_keys::DISKLESS.to_string(), "true".to_string());
    overrides.insert("retention.ms".to_string(), "60000".to_string());
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: "events".into(),
        overrides,
    }));

    let result = super::describe_one(
        &img,
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_TOPIC,
            resource_name: "events".into(),
            configuration_keys: Some(vec![
                config_keys::DISKLESS.to_string(),
                "retention.ms".to_string(),
            ]),
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        static_broker(),
    );

    let expected = DescribeConfigsResult {
        error_code: crate::codes::NONE,
        error_message: None,
        resource_type: super::RESOURCE_TYPE_TOPIC,
        resource_name: "events".to_string(),
        configs: vec![
            DescribeConfigsResourceResult {
                name: config_keys::DISKLESS.to_string(),
                value: Some("true".to_string()),
                read_only: true,
                config_source: super::CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
                synonyms: Vec::new(),
                config_type: 0,
                documentation: None,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            DescribeConfigsResourceResult {
                name: "retention.ms".to_string(),
                value: Some("60000".to_string()),
                read_only: false,
                config_source: super::CONFIG_SOURCE_DYNAMIC_TOPIC,
                is_sensitive: false,
                synonyms: Vec::new(),
                config_type: 0,
                documentation: None,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(result == expected);
}

#[test]
fn broker_describe_one_rejects_non_numeric_resource_name_with_fields() {
    let img = MetadataImage::new(Uuid::nil());
    let result = super::describe_one(
        &img,
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_BROKER,
            resource_name: "not-a-number".into(),
            configuration_keys: None,
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        static_broker(),
    );

    let expected = DescribeConfigsResult {
        error_code: crate::codes::INVALID_REQUEST,
        error_message: Some("resource_name `not-a-number` is not a valid broker id".to_string()),
        resource_type: super::RESOURCE_TYPE_BROKER,
        resource_name: "not-a-number".to_string(),
        configs: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(result == expected);
}

#[test]
fn broker_resource_key_filter_applied() {
    let mut img = MetadataImage::new(Uuid::nil());
    img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::NodeId(2),
        config_name: "leader.replication.throttled.rate".to_string(),
        config_value: Some("512".to_string()),
    }));
    img.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::NodeId(2),
        config_name: "follower.replication.throttled.rate".to_string(),
        config_value: Some("256".to_string()),
    }));

    let map = img
        .broker_config(krabka_metadata::NodeId(2))
        .cloned()
        .unwrap_or_default();
    let key_filter = ["leader.replication.throttled.rate".to_string()];
    let filtered: BTreeMap<_, _> = map
        .into_iter()
        .filter(|(k, _)| key_filter.iter().any(|f| f == k))
        .collect();

    let expected: BTreeMap<String, String> = [(
        "leader.replication.throttled.rate".to_string(),
        "512".to_string(),
    )]
    .into_iter()
    .collect();
    assert!(filtered == expected);
}

#[test]
fn broker_resource_missing_node_returns_empty_configs() {
    let img = MetadataImage::new(Uuid::nil());
    // Node 99 has no broker configs in the image.
    let map = img
        .broker_config(krabka_metadata::NodeId(99))
        .cloned()
        .unwrap_or_default();
    assert!(map.is_empty());
}

#[test]
fn broker_describe_inherits_default_and_prefers_per_broker_override() {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: "leader.replication.throttled.rate".into(),
        config_value: Some("1024".into()),
    }));
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: "follower.replication.throttled.rate".into(),
        config_value: Some("512".into()),
    }));
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::NodeId(1),
        config_name: "leader.replication.throttled.rate".into(),
        config_value: Some("2048".into()),
    }));

    let result = super::describe_one(
        &image,
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_BROKER,
            resource_name: "1".into(),
            configuration_keys: None,
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        static_broker(),
    );

    assert!(
        result.configs
            == vec![
                super::make_entry(
                    "follower.replication.throttled.rate",
                    "512",
                    super::CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                ),
                super::make_entry(
                    "leader.replication.throttled.rate",
                    "2048",
                    super::CONFIG_SOURCE_DYNAMIC_BROKER,
                ),
                DescribeConfigsResourceResult {
                    name: "node.id".into(),
                    value: Some("1".into()),
                    read_only: true,
                    config_source: super::CONFIG_SOURCE_STATIC_BROKER,
                    ..Default::default()
                },
            ]
            .into_iter()
            .chain(static_broker_entries())
            .collect::<Vec<_>>()
    );
}

#[test]
fn broker_describe_reports_controller_managed_configs_as_read_only() {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: config_keys::STRETCH_PREFERRED_LEADER_SITE.into(),
        config_value: Some("dc-a".into()),
    }));
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::NodeId(1),
        config_name: config_keys::BROKER_WITNESS.into(),
        config_value: Some(config_keys::WITNESS_TRUE.into()),
    }));

    let result = super::describe_one(
        &image,
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_BROKER,
            resource_name: "1".into(),
            configuration_keys: None,
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        static_broker(),
    );

    assert!(
        result.configs
            == vec![
                DescribeConfigsResourceResult {
                    name: config_keys::BROKER_WITNESS.into(),
                    value: Some(config_keys::WITNESS_TRUE.into()),
                    read_only: true,
                    config_source: super::CONFIG_SOURCE_DYNAMIC_BROKER,
                    ..Default::default()
                },
                DescribeConfigsResourceResult {
                    name: "node.id".into(),
                    value: Some("1".into()),
                    read_only: true,
                    config_source: super::CONFIG_SOURCE_STATIC_BROKER,
                    ..Default::default()
                },
            ]
            .into_iter()
            .chain(static_broker_entries())
            .chain([DescribeConfigsResourceResult {
                name: config_keys::STRETCH_PREFERRED_LEADER_SITE.into(),
                value: Some("dc-a".into()),
                read_only: true,
                config_source: super::CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
                ..Default::default()
            }])
            .collect::<Vec<_>>()
    );
}

#[test]
fn broker_describe_without_overrides_includes_static_node_id() {
    let result = super::describe_one(
        &MetadataImage::new(Uuid::nil()),
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_BROKER,
            resource_name: "7".into(),
            configuration_keys: None,
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        static_broker(),
    );

    assert!(
        result.configs
            == vec![DescribeConfigsResourceResult {
                name: "node.id".into(),
                value: Some("7".into()),
                read_only: true,
                config_source: super::CONFIG_SOURCE_STATIC_BROKER,
                ..Default::default()
            }]
            .into_iter()
            .chain(static_broker_entries())
            .collect::<Vec<_>>()
    );
}

/// The two KIP-211 retention keys obey the `configuration_keys` filter like
/// every other key, and each reports the process's own value read-only.
#[test]
fn broker_describe_filters_the_static_retention_keys() {
    let result = super::describe_one(
        &MetadataImage::new(Uuid::nil()),
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_BROKER,
            resource_name: "3".into(),
            configuration_keys: Some(vec![config_keys::OFFSETS_RETENTION_MINUTES.into()]),
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        static_broker(),
    );

    assert!(
        result.configs
            == vec![DescribeConfigsResourceResult {
                name: config_keys::OFFSETS_RETENTION_MINUTES.into(),
                value: Some("10080".into()),
                read_only: true,
                config_source: super::CONFIG_SOURCE_DEFAULT,
                ..Default::default()
            }]
    );
}

/// An operator who retunes a retention knob sees `STATIC_BROKER_CONFIG`
/// instead of `DEFAULT_CONFIG`, which is the source Kafka reports for a key
/// named in `server.properties`. It stays read-only either way: Kafka refuses
/// to reconfigure it dynamically.
#[test]
fn broker_describe_marks_a_retuned_retention_key_as_static() {
    let result = super::describe_one(
        &MetadataImage::new(Uuid::nil()),
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_BROKER,
            resource_name: "3".into(),
            configuration_keys: Some(vec![config_keys::OFFSETS_RETENTION_MINUTES.into()]),
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        super::StaticBrokerConfigs {
            offsets_retention_minutes: 60,
            ..static_broker()
        },
    );

    assert!(
        result.configs
            == vec![DescribeConfigsResourceResult {
                name: config_keys::OFFSETS_RETENTION_MINUTES.into(),
                value: Some("60".into()),
                read_only: true,
                config_source: super::CONFIG_SOURCE_STATIC_BROKER,
                ..Default::default()
            }]
    );
}

#[test]
fn empty_broker_name_describes_cluster_defaults() {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
        config_name: "leader.replication.throttled.rate".into(),
        config_value: Some("1024".into()),
    }));

    let result = super::describe_one(
        &image,
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_BROKER,
            resource_name: String::new(),
            configuration_keys: None,
            ..Default::default()
        },
        300_000,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        static_broker(),
    );

    assert!(
        result.configs
            == vec![super::make_entry(
                "leader.replication.throttled.rate",
                "1024",
                super::CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER,
            )]
    );
}

#[test]
fn client_metrics_describe_emits_defaults() {
    use krabka_metadata::{ClientMetricsConfigRecord, MetadataRecord};
    let mut img = MetadataImage::new(Uuid::nil());
    let mut cfgs = std::collections::BTreeMap::new();
    cfgs.insert("metrics".to_string(), "a.".to_string());
    img.apply(&MetadataRecord::V1ClientMetricsConfig(
        ClientMetricsConfigRecord {
            name: "sub-a".into(),
            configs: cfgs,
        },
    ));
    let r = krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
        resource_type: super::RESOURCE_TYPE_CLIENT_METRICS,
        resource_name: "sub-a".into(),
        configuration_keys: None,
        ..Default::default()
    };
    let res = super::describe_one(
        &img,
        r,
        12_345,
        &crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
        static_broker(),
    );
    assert2::assert!((res.error_code) == (crate::codes::NONE));
    let by_name: std::collections::HashMap<_, _> =
        res.configs.iter().map(|c| (c.name.as_str(), c)).collect();
    let cases = [
        ("metrics", Some("a."), super::CONFIG_SOURCE_CLIENT_METRICS),
        ("interval.ms", Some("12345"), super::CONFIG_SOURCE_DEFAULT),
    ];
    for (key, want_value, want_source) in cases {
        assert!(
            (by_name[key].value.as_deref(), by_name[key].config_source)
                == (want_value, want_source),
            "key {key}"
        );
    }
}

#[test]
fn group_describe_merges_dynamic_overrides_with_defaults() {
    use krabka_metadata::GroupConfigRecord;

    use crate::coordinator::unified::streams::config::{
        KEY_NUM_STANDBY_REPLICAS, KEY_SESSION_TIMEOUT_MS, StreamsGroupConfig,
    };

    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1GroupConfig(GroupConfigRecord {
        group_id: "streams-app".into(),
        configs: maplit::btreemap! {KEY_NUM_STANDBY_REPLICAS.into() => "1".into()},
    }));
    let result = super::describe_one(
        &image,
        krabka_protocol::owned::describe_configs_request::DescribeConfigsResource {
            resource_type: super::RESOURCE_TYPE_GROUP,
            resource_name: "streams-app".into(),
            configuration_keys: Some(vec![
                KEY_NUM_STANDBY_REPLICAS.into(),
                KEY_SESSION_TIMEOUT_MS.into(),
            ]),
            ..Default::default()
        },
        300_000,
        &StreamsGroupConfig::default(),
        static_broker(),
    );
    let by_name: std::collections::HashMap<_, _> = result
        .configs
        .iter()
        .map(|entry| (entry.name.as_str(), entry))
        .collect();
    assert!(
        by_name[KEY_NUM_STANDBY_REPLICAS].value.as_deref() == Some("1")
            && by_name[KEY_NUM_STANDBY_REPLICAS].config_source
                == super::CONFIG_SOURCE_DYNAMIC_GROUP
    );
    assert!(
        by_name[KEY_SESSION_TIMEOUT_MS].value.as_deref() == Some("45000")
            && by_name[KEY_SESSION_TIMEOUT_MS].config_source == super::CONFIG_SOURCE_DEFAULT
    );
}
