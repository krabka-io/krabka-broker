//! Fixture builders shared by the unit tests of the `IncrementalAlterConfigs`
//! submodules. They seed a `MetadataImage` with a registered broker or with a
//! topic and its overrides, and they build the request-side resource and
//! per-key config structures that each scope handler consumes.

use krabka_metadata::{
    BrokerRegistrationRecord, MetadataImage, MetadataRecord, NodeId, TopicConfigRecord,
};
use krabka_protocol::owned::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig,
};

use super::{OP_DELETE, OP_SET, RESOURCE_TYPE_BROKER, RESOURCE_TYPE_TOPIC};

pub(super) fn make_image_with_broker(node_id: NodeId) -> MetadataImage {
    let mut img = MetadataImage::new(uuid::Uuid::nil());
    img.apply(&MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            node_id,
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::nil(),
            host: "127.0.0.1".into(),
            port: 9092,
            rack: None,
            log_dirs: vec![],
            endpoints: vec![],
            features: std::collections::BTreeMap::new(),
        },
    ));
    img
}

pub(super) fn make_resource(name: &str, configs: Vec<AlterableConfig>) -> AlterConfigsResource {
    AlterConfigsResource {
        resource_type: RESOURCE_TYPE_BROKER,
        resource_name: name.into(),
        configs,
        ..Default::default()
    }
}

pub(super) fn make_set_cfg(key: &str, value: &str) -> AlterableConfig {
    AlterableConfig {
        name: key.into(),
        config_operation: OP_SET,
        value: Some(value.into()),
        ..Default::default()
    }
}

pub(super) fn make_del_cfg(key: &str) -> AlterableConfig {
    AlterableConfig {
        name: key.into(),
        config_operation: OP_DELETE,
        value: None,
        ..Default::default()
    }
}

pub(super) fn make_topic_resource(
    name: &str,
    configs: Vec<AlterableConfig>,
) -> AlterConfigsResource {
    AlterConfigsResource {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: name.into(),
        configs,
        ..Default::default()
    }
}

pub(super) fn image_with_topic_config(name: &str, overrides: &[(&str, &str)]) -> MetadataImage {
    let mut img = MetadataImage::new(uuid::Uuid::nil());
    img.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: name.into(),
        topic_id: uuid::Uuid::nil(),
        partitions: 1,
        replication_factor: 1,
    }));
    img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: name.into(),
        overrides: overrides
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    }));
    img
}
