//! The single-broker fixture the epoch tests share: booting one broker,
//! creating a one-partition topic on it, resolving that topic's id, and
//! building the one-record batch they produce.
//!
//! Four of the five tests in this suite drive one broker and differ only in
//! what they do to its epoch afterwards, so the setup lives here instead of
//! being repeated beside each of them.

use bytes::Bytes;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};
use tempfile::TempDir;

pub(crate) async fn boot_single() -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

pub(crate) async fn topic_id_for(client: &Client, name: &str) -> WireUuid {
    let resp = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(name.into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("metadata");
    resp.topics
        .iter()
        .find(|t| t.name.as_deref() == Some(name))
        .map(|t| t.topic_id)
        .unwrap_or_default()
}

pub(crate) async fn create_topic(broker: &BrokerHandle, bootstrap: &str, name: &str) {
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .unwrap();
    let _ = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    broker.wait_until_partition_present(name, 0).await;
}

pub(crate) fn record(value: &str) -> RecordBatch {
    let mut b = RecordBatch::default();
    b.records.push(Record {
        offset_delta: 0,
        value: Some(Bytes::from(value.to_string())),
        ..Default::default()
    });
    b.last_offset_delta = 0;
    b
}
