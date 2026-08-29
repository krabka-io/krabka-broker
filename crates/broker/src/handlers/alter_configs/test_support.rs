//! Request builders, metadata-image fixtures, and the live-broker harness that
//! the `AlterConfigs` tests share.
//!
//! The topic-record, broker-record, and end-to-end tests build the same
//! resource shapes and the same seeded images, so the fixtures live in one
//! module rather than being duplicated per test file.

use std::{net::SocketAddr, sync::Arc};

use krabka_metadata::MetadataRecord;
use krabka_protocol::owned::{
    alter_configs_request::{AlterConfigsRequest, AlterConfigsResource, AlterableConfig},
    alter_configs_response::AlterConfigsResponse,
};
use krabka_security::{AuthMethod, Principal};

use super::{RESOURCE_TYPE_BROKER, RESOURCE_TYPE_TOPIC, handle};
use crate::{authorizer::Authorizer, test_support::start_broker_with_authorizer as start_broker};

crate::test_support::wire_helpers!(
    AlterConfigsRequest,
    AlterConfigsResponse,
    client_id = "admin-client"
);

pub(super) fn resource(resource_type: i8, resource_name: &str) -> AlterConfigsResource {
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
pub(super) fn image_with_broker(node_id: u64) -> krabka_metadata::MetadataImage {
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

pub(super) fn topic_resource(
    resource_name: &str,
    configs: &[(&str, &str)],
) -> AlterConfigsResource {
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

pub(super) fn image_with_topic(name: &str) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: name.into(),
        topic_id: uuid::Uuid::nil(),
        partitions: 1,
        replication_factor: 1,
    }));
    image
}

pub(super) fn broker_resource(
    resource_name: &str,
    configs: &[(&str, &str)],
) -> AlterConfigsResource {
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

pub(super) async fn drive_one(
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
