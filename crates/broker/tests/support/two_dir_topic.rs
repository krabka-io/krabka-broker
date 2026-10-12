//! Ready topic fixtures for the three two-directory integration binaries.
use std::net::SocketAddr;

use krabka_broker::BrokerHandle;
use tempfile::TempDir;

pub(crate) use crate::support::storage::PartitionReadiness;
use crate::{
    kafka_wire::{AutomaticTopicSetup, create_configured_topic_plaintext},
    support::{
        storage::start_two_dir_broker,
        topics::{ConfiguredTopicSetup, TopicPartitionCount},
    },
};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(crate) struct Setup<'a> {
    #[default("krabka-jbod-test")]
    pub client_id: &'a str,
    #[default("t")]
    pub topic: &'a str,
    #[default(crate::support::topics::TopicPartitionCount(1))]
    pub partitions: TopicPartitionCount,
    pub readiness: PartitionReadiness,
}

#[bon::builder]
pub(crate) async fn create_and_wait(
    handle: &BrokerHandle,
    address: SocketAddr,
    #[builder(default = "krabka-jbod-test")] client_id: &'_ str,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(into, default = crate :: support :: topics :: TopicPartitionCount (1))]
    partitions: TopicPartitionCount,
    #[builder(default)] readiness: PartitionReadiness,
) {
    let setup = Setup {
        client_id,
        topic,
        partitions,
        readiness,
    };
    create_configured_topic_plaintext()
        .addr(address)
        .client_id(setup.client_id)
        .topic(ConfiguredTopicSetup {
            name: setup.topic.into(),
            partitions: setup.partitions,
            ..Default::default()
        })
        .call()
        .await;
    for partition in 0..setup.partitions.0 {
        match setup.readiness {
            PartitionReadiness::MetadataPublished => {
                handle
                    .wait_until_partition_present(setup.topic, partition)
                    .await;
            }
            PartitionReadiness::LocalWriterPresent => {
                handle
                    .wait_until_local_log_end_offset(setup.topic, partition, 0)
                    .await;
            }
        }
    }
}

#[bon::builder]
pub(crate) async fn start(
    #[builder(default = "krabka-jbod-test")] client_id: &'_ str,
    #[builder(default = "t")] topic: &'_ str,
    #[builder(into, default = crate :: support :: topics :: TopicPartitionCount (1))]
    partitions: TopicPartitionCount,
    #[builder(default)] readiness: PartitionReadiness,
) -> (BrokerHandle, TempDir, TempDir, SocketAddr) {
    let setup = Setup {
        client_id,
        topic,
        partitions,
        readiness,
    };
    let (handle, primary, extra, address) = start_two_dir_broker().await;
    create_and_wait()
        .handle(&handle)
        .address(address)
        .client_id(setup.client_id)
        .topic(setup.topic)
        .partitions(setup.partitions)
        .readiness(setup.readiness)
        .call()
        .await;
    (handle, primary, extra, address)
}
