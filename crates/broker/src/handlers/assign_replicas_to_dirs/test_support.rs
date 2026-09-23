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

use crate::{broker::Broker, error::BrokerError, handlers::assign_replicas_to_dirs::handle};

pub(super) const VERSION: i16 = 0;

/// Builds a request reported by broker 1 at `broker_epoch`. Most tests pass
/// the broker's actual registered epoch (see
/// [`crate::test_support::broker_epoch`]-style lookups on the started
/// broker's image); the authorization and epoch tests pass a wrong one on
/// purpose.
pub(super) fn request(
    broker_epoch: i64,
    dir_uuid: uuid::Uuid,
    topic_uuid: uuid::Uuid,
    partition_index: i32,
) -> Bytes {
    let req = AssignReplicasToDirsRequest {
        broker_id: 1,
        broker_epoch,
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

/// The epoch broker 1 (the started broker itself) registered with. A request
/// naming this epoch is the current, non-stale one.
pub(super) fn own_broker_epoch(broker: &Broker) -> i64 {
    broker
        .controller
        .current_image()
        .broker_epoch(broker.config.node_id)
        .expect("broker 1 is self-registered")
}

/// Dispatches `body` as an `ANONYMOUS` principal that the default
/// `AllowAllAuthorizer` admits.
pub(super) async fn handle_allowed(
    broker: &Broker,
    version: i16,
    correlation_id: i32,
    body: &[u8],
) -> Result<Bytes, BrokerError> {
    let user = crate::test_support::principal("ANONYMOUS");
    let address = crate::test_support::peer();
    let ctx = crate::test_support::request_context(&user, &address, "assign-replicas-test");
    handle(broker, version, correlation_id, body, &ctx).await
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
