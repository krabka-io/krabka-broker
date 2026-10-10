//! Fetch wire fixtures shared by version and replica-role tests.

use assert2::assert;
use krabka_protocol::{
    Decode,
    owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
};

/// A sessionless fetch with the follower identity in the field served by this version.
pub(super) fn sessionless_request(version: i16, replica_id: i32) -> FetchRequest {
    use krabka_protocol::owned::fetch_request::ReplicaState;
    let (replica_id, replica_state) = if version >= 15 {
        (
            -1,
            ReplicaState {
                replica_id,
                ..Default::default()
            },
        )
    } else {
        (replica_id, ReplicaState::default())
    };
    FetchRequest {
        replica_id,
        replica_state,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 1_048_576,
        session_id: crate::fetch_session::INVALID_SESSION_ID,
        session_epoch: crate::fetch_session::FINAL_EPOCH,
        ..Default::default()
    }
}

pub(super) async fn fetch_wire(
    broker: &crate::broker::BrokerHandle,
    version: i16,
    user: &str,
    client_id: &str,
    request: &FetchRequest,
) -> FetchResponse {
    let shared = broker.broker_arc_for_test();
    request_identity!(
        (user, peer, context),
        crate::test_support::principal(user),
        client_id = client_id,
        address = crate::test_support::peer()
    );
    let bytes = crate::test_support::encode_request(request, version);
    let (response, response_version) = super::handle(&shared, version, 7, &bytes, &context)
        .await
        .expect("handle fetch");
    let wire = super::encode_fetch_response(response, response_version).expect("encode response");
    let mut cursor: &[u8] = &wire;
    let decoded = FetchResponse::decode(&mut cursor, version).expect("decode response");
    assert!(cursor.is_empty(), "the decoder consumed every byte");
    decoded
}

/// Independent single-topic response model, including the version's name/id representation.
pub(super) fn expected_single_topic(
    id_only: bool,
    topic: (&str, krabka_protocol::primitives::uuid::Uuid),
    partition: krabka_protocol::owned::fetch_response::PartitionData,
) -> FetchResponse {
    use krabka_protocol::{owned::fetch_response::FetchableTopicResponse, primitives::uuid::Uuid};
    FetchResponse {
        error_code: crate::codes::NONE,
        session_id: crate::fetch_session::INVALID_SESSION_ID,
        responses: vec![FetchableTopicResponse {
            topic: if id_only {
                String::new()
            } else {
                topic.0.to_owned()
            },
            topic_id: if id_only { topic.1 } else { Uuid::ZERO },
            partitions: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(super) fn expected_refused_partition(
    partition_index: i32,
    error_code: i16,
) -> krabka_protocol::owned::fetch_response::PartitionData {
    krabka_protocol::owned::fetch_response::PartitionData {
        partition_index,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Some(Vec::new()),
        preferred_read_replica: -1,
        records: Some(krabka_protocol::records::RecordsPayload::Legacy(
            bytes::Bytes::new(),
        )),
        ..Default::default()
    }
}

/// Default partition-zero row shared by sessionless fetch fixtures.
pub(super) fn request_partition(
    fetch_offset: i64,
) -> krabka_protocol::owned::fetch_request::FetchPartition {
    krabka_protocol::owned::fetch_request::FetchPartition {
        partition: 0,
        fetch_offset,
        partition_max_bytes: 1_048_576,
        ..Default::default()
    }
}
