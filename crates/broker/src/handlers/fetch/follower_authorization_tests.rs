//! Handler tests for the `ClusterAction` gate on a follower `Fetch`.
//!
//! Kafka's `KafkaApis.handleFetchRequest` treats a fetch whose replica id is 0
//! or more as a follower fetch. The replica id is `ReplicaId` before v15 and
//! `ReplicaState.ReplicaId` from v15 on. A follower fetch reads up to the log
//! end offset and moves the follower progress, and so the high watermark.
//! Kafka lets it through only when the principal holds `ClusterAction` on the
//! cluster resource, and then it checks no topic ACL. Without `ClusterAction`,
//! every partition row answers `TOPIC_AUTHORIZATION_FAILED`, and the fetch
//! reads nothing.
//!
//! A consumer fetch keeps its replica id at -1, also when it names a rack to
//! read from a follower replica (KIP-392). It needs topic `Read`.
//!
//! Each case replicates its own topic from this broker (node 1) to a node 2
//! that never fetches, so the high watermark stays at 0 until a fetch as node
//! 2 moves it.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use krabka_log::Offset;
use krabka_metadata::{AclOperation, MetadataRecord, PartitionRecord, ResourceType, TopicRecord};
use krabka_protocol::{
    Decode,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic, ReplicaState},
        fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};

use super::{FIRST_TOPIC_ID_VERSION, encode_fetch_response, handle};
use crate::{
    authorizer::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer},
    broker::BrokerHandle,
    codes,
    fetch_session::{FINAL_EPOCH, INVALID_SESSION_ID},
    handlers::acl_wire::CLUSTER_RESOURCE_NAME,
    partition::Partition,
    test_support::{encode_request, peer, principal, request_context, start_broker_with},
};

/// The node id of the follower that the fetches claim to be. The broker under
/// test is node 1.
const FOLLOWER: i32 = 2;

/// The replica id of a consumer fetch.
const CONSUMER: i32 = -1;

/// The records that each case appends to its leader log.
const RECORDS: [&[u8]; 2] = [b"committed-later", b"not-committed-yet"];

/// The log end offset after [`RECORDS`].
const LOG_END: i64 = 2;

/// The principal that sends the fetch, by the grant that it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Caller {
    /// `Read` on every topic, and nothing on the cluster.
    TopicReader,
    /// `ClusterAction` on the cluster, and nothing on any topic.
    Replicator,
    /// No grant.
    Stranger,
}

impl Caller {
    const fn name(self) -> &'static str {
        match self {
            Self::TopicReader => "reader",
            Self::Replicator => "replicator",
            Self::Stranger => "stranger",
        }
    }
}

/// Allows exactly the grants that [`Caller`] documents.
#[derive(Debug)]
struct Grants;

impl Authorizer for Grants {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        request: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        let allowed = match request.principal.name.as_str() {
            "reader" => {
                request.resource_type == ResourceType::Topic
                    && request.operation == AclOperation::Read
            }
            "replicator" => {
                request.resource_type == ResourceType::Cluster
                    && request.resource_name == CLUSTER_RESOURCE_NAME
                    && request.operation == AclOperation::ClusterAction
            }
            _ => false,
        };
        if allowed {
            AuthorizationResult::Allow
        } else {
            AuthorizationResult::Deny
        }
    }
}

/// How the fetch that a case sends names itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sender {
    /// A follower fetch as node [`FOLLOWER`].
    Follower,
    /// A KIP-392 consumer fetch that names a rack.
    RackAwareConsumer,
}

/// What a case expects the fetches to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// Kafka reads as a follower: records up to the log end offset, and a high
    /// watermark that follows the follower.
    FollowerRead,
    /// Kafka refuses every partition row with this error code.
    Refused(i16),
    /// Kafka reads as a consumer: nothing past the high watermark of 0.
    ConsumerRead,
}

#[derive(Debug, Clone, Copy)]
struct Case {
    version: i16,
    caller: Caller,
    sender: Sender,
    expect: Expect,
}

/// The two fetches of one case and the high watermark after them.
///
/// The first fetch starts at offset 0. The second starts at the log end
/// offset, which is what a follower sends once it holds every record.
#[derive(Debug, PartialEq)]
struct Outcome {
    case: String,
    from_start: FetchResponse,
    from_log_end: FetchResponse,
    high_watermark: Offset,
}

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(Grants);
        // Node 2 never fetches. Keep it in the ISR for the whole test, so only
        // a fetch as node 2 can move the high watermark.
        cfg.replica_lag_time_max = krabka_units::secs(600);
    })
    .await
}

/// Create `topic` with one partition that this broker leads and that node 2
/// follows, and append [`RECORDS`] to the leader log. Return the partition and
/// the batch as the log stored it.
async fn replicated_partition(
    broker: &BrokerHandle,
    topic: &str,
    topic_id: u128,
) -> (Arc<Partition>, RecordBatch) {
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1Topic(TopicRecord {
            name: topic.to_owned(),
            topic_id: uuid::Uuid::from_u128(topic_id),
            partitions: 1,
            replication_factor: 2,
        }))
        .await
        .expect("submit topic record");
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_owned(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: vec![uuid::Uuid::nil(); 2],
            partition_epoch: 0,
        }))
        .await
        .expect("submit partition record");

    let shared = broker.broker_arc_for_test();
    let follower = krabka_raft::NodeId(2);
    let partition = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(partition) = shared.partitions.get(topic, krabka_ids::PartitionIndex(0))
                && partition.replica_state.lock().await.isr.contains(&follower)
            {
                return partition;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the broker leads the partition with node 2 in the ISR");

    let mut batch = RecordBatch {
        last_offset_delta: 1,
        records: RECORDS
            .iter()
            .zip(0..)
            .map(|(value, offset_delta)| Record {
                offset_delta,
                value: Some(Bytes::from_static(value)),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    };
    partition
        .log
        .lock()
        .expect("partition log lock")
        .append(&mut batch)
        .expect("append the records");
    (partition, batch)
}

/// A sessionless one-row fetch of partition 0 of `topic`, from `fetch_offset`.
fn request(
    version: i16,
    sender: Sender,
    topic: (&str, WireUuid),
    fetch_offset: i64,
) -> FetchRequest {
    let (name, topic_id) = topic;
    let replica_id = match sender {
        Sender::Follower => FOLLOWER,
        Sender::RackAwareConsumer => CONSUMER,
    };
    // Before v15 the wire carries `ReplicaId`, from v15 on `ReplicaState`.
    let (replica_id, replica_state) = if version >= 15 {
        (
            CONSUMER,
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
        session_id: INVALID_SESSION_ID,
        session_epoch: FINAL_EPOCH,
        rack_id: if sender == Sender::RackAwareConsumer {
            "rack-a".to_owned()
        } else {
            String::new()
        },
        topics: vec![FetchTopic {
            topic: if version >= FIRST_TOPIC_ID_VERSION {
                String::new()
            } else {
                name.to_owned()
            },
            topic_id: if version >= FIRST_TOPIC_ID_VERSION {
                topic_id
            } else {
                WireUuid::ZERO
            },
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Send one fetch as `caller` and return the response as a client decodes it.
async fn fetch(
    broker: &BrokerHandle,
    version: i16,
    caller: Caller,
    request: &FetchRequest,
) -> FetchResponse {
    let shared = broker.broker_arc_for_test();
    let user = principal(caller.name());
    let address = peer();
    let ctx = request_context(&user, &address, "fetch-client");
    let request_bytes = encode_request(request, version);
    let (response, response_version) = handle(&shared, version, 7, &request_bytes, &ctx)
        .await
        .expect("handle fetch");
    let wire = encode_fetch_response(response, response_version).expect("encode response");
    let mut cursor: &[u8] = wire.as_ref();
    let decoded = FetchResponse::decode(&mut cursor, version).expect("decode response");
    assert!(cursor.is_empty(), "the decoder consumed every byte");
    decoded
}

/// The one-row response of `case` for partition 0 of `topic`.
fn response(version: i16, topic: (&str, WireUuid), partition: PartitionData) -> FetchResponse {
    let (name, topic_id) = topic;
    let id_only = version >= FIRST_TOPIC_ID_VERSION;
    FetchResponse {
        error_code: codes::NONE,
        session_id: INVALID_SESSION_ID,
        responses: vec![FetchableTopicResponse {
            topic: if id_only {
                String::new()
            } else {
                name.to_owned()
            },
            topic_id: if id_only { topic_id } else { WireUuid::ZERO },
            partitions: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The partition row of Kafka's `FetchResponse.partitionResponse`.
fn refused(error_code: i16) -> PartitionData {
    PartitionData {
        partition_index: 0,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Some(Vec::new()),
        preferred_read_replica: -1,
        records: Some(no_records()),
        ..Default::default()
    }
}

/// A partition row that the fetch read, with `watermark` as its high watermark
/// and last stable offset.
fn read(watermark: i64, records: RecordsPayload) -> PartitionData {
    PartitionData {
        partition_index: 0,
        error_code: codes::NONE,
        high_watermark: watermark,
        last_stable_offset: watermark,
        log_start_offset: 0,
        aborted_transactions: None,
        preferred_read_replica: -1,
        records: Some(records),
        ..Default::default()
    }
}

/// The record set of a row that carries nothing, as a client decodes it.
fn no_records() -> RecordsPayload {
    RecordsPayload::Legacy(Bytes::new())
}

fn expected(case: Case, label: String, topic: (&str, WireUuid), batch: RecordBatch) -> Outcome {
    let (from_start, from_log_end, high_watermark) = match case.expect {
        Expect::FollowerRead => (
            read(LOG_END, RecordsPayload::V2(vec![batch])),
            read(LOG_END, no_records()),
            Offset(LOG_END),
        ),
        Expect::Refused(error_code) => (refused(error_code), refused(error_code), Offset(0)),
        Expect::ConsumerRead => (read(0, no_records()), read(0, no_records()), Offset(0)),
    };
    Outcome {
        case: label,
        from_start: response(case.version, topic, from_start),
        from_log_end: response(case.version, topic, from_log_end),
        high_watermark,
    }
}

#[tokio::test]
async fn follower_fetch_needs_cluster_action() {
    let max_version = krabka_protocol::owned::fetch_request::MAX_VERSION;
    let mut cases = Vec::new();
    for version in [12, max_version] {
        cases.extend([
            Case {
                version,
                caller: Caller::TopicReader,
                sender: Sender::Follower,
                expect: Expect::Refused(codes::TOPIC_AUTHORIZATION_FAILED),
            },
            Case {
                version,
                caller: Caller::Stranger,
                sender: Sender::Follower,
                expect: Expect::Refused(codes::TOPIC_AUTHORIZATION_FAILED),
            },
            Case {
                version,
                caller: Caller::Replicator,
                sender: Sender::Follower,
                expect: Expect::FollowerRead,
            },
            Case {
                version,
                caller: Caller::TopicReader,
                sender: Sender::RackAwareConsumer,
                expect: Expect::ConsumerRead,
            },
            Case {
                version,
                caller: Caller::Replicator,
                sender: Sender::RackAwareConsumer,
                expect: Expect::Refused(codes::TOPIC_AUTHORIZATION_FAILED),
            },
        ]);
    }

    let (broker, _dir) = start().await;
    let mut actual = Vec::new();
    let mut want = Vec::new();
    for (index, case) in (0_u128..).zip(cases) {
        let label = format!("{case:?}");
        let name = format!("replicated-{index}");
        let topic_id = index + 1;
        let wire_id = WireUuid(uuid::Uuid::from_u128(topic_id).into_bytes());
        let (partition, batch) = replicated_partition(&broker, &name, topic_id).await;
        let topic = (name.as_str(), wire_id);

        let from_start = fetch(
            &broker,
            case.version,
            case.caller,
            &request(case.version, case.sender, topic, 0),
        )
        .await;
        let from_log_end = fetch(
            &broker,
            case.version,
            case.caller,
            &request(case.version, case.sender, topic, LOG_END),
        )
        .await;
        actual.push(Outcome {
            case: label.clone(),
            from_start,
            from_log_end,
            high_watermark: partition.high_watermark().await,
        });
        want.push(expected(case, label, topic, batch));
    }
    broker.shutdown().await;

    assert!(actual == want);
}
