//! The fixtures that the `AssignReplicasToDirs` unit tests share.
//!
//! The protocol version, the request builder, the response decoder, and the
//! single-broker harness are each used by more than one of the test modules
//! under this module, so they live in one file instead of once per module.

use assert2::assert;
use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Encode,
    owned::{
        assign_replicas_to_dirs_request::{
            AssignReplicasToDirsRequest, DirectoryData as ReqDirData, PartitionData as ReqPartData,
            TopicData as ReqTopicData,
        },
        assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
    },
    primitives::uuid::Uuid as ProtocolUuid,
};

use crate::broker::Broker;

pub(super) const VERSION: i16 = 0;

pub(super) fn request(dir_uuid: uuid::Uuid, topic_uuid: uuid::Uuid, partition_index: i32) -> Bytes {
    let req = AssignReplicasToDirsRequest {
        broker_id: 1,
        broker_epoch: -1,
        directories: vec![ReqDirData {
            id: ProtocolUuid(dir_uuid.into_bytes()),
            topics: vec![ReqTopicData {
                topic_id: ProtocolUuid(topic_uuid.into_bytes()),
                partitions: vec![ReqPartData {
                    partition_index,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
    req.encode(&mut buf, VERSION)
        .expect("encode AssignReplicasToDirsRequest");
    buf.freeze()
}

pub(super) fn decode_response(bytes: &Bytes) -> AssignReplicasToDirsResponse {
    crate::test_support::decode_response(bytes, VERSION)
}

pub(super) async fn start_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|_cfg| {}).await
}

pub(super) async fn wait_for_leader(broker: &Broker) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if broker
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == broker.config.node_id)
        {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}
