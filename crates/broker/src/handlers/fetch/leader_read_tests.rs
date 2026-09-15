//! Handler tests for the leader and replica checks on a `Fetch` read.
//!
//! Kafka's `ReplicaManager.readFromLog` reads a partition through
//! `Partition.localLogWithEpochOrThrow(currentLeaderEpoch,
//! fetchParams.fetchOnlyLeader)`. `FetchParams.fetchOnlyLeader` is true for a
//! follower fetch, and for a consumer fetch without client metadata, which
//! `KafkaApis.handleFetchRequest` builds only from v11. A broker that does not
//! lead the partition then answers `NOT_LEADER_OR_FOLLOWER`.
//!
//! For a follower fetch, `Partition.followerReplicaOrThrow` also refuses a
//! replica id that is not a follower in the assignment. It answers
//! `UNKNOWN_LEADER_EPOCH` when the request carries a leader epoch and
//! `NOT_LEADER_OR_FOLLOWER` when it does not, and it moves no follower state.
//!
//! The broker under test is node 1. Each case creates its own topic, with
//! replicas 1 and 2, led by node 1 or by node 2.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use bytes::Bytes;
use krabka_log::Offset;
use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use krabka_protocol::{
    Decode,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic, ReplicaState},
        fetch_response::{FetchResponse, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData},
    },
    records::{Record, RecordBatch, RecordsPayload},
};

use super::{encode_fetch_response, handle};
use crate::{
    broker::BrokerHandle,
    codes,
    fetch_session::{FINAL_EPOCH, INVALID_SESSION_ID},
    partition::Partition,
    test_support::{encode_request, peer, principal, request_context, start_broker_with},
};

/// The node id of the broker under test.
const THIS_NODE: u64 = 1;

/// The other replica of every test partition.
const OTHER_NODE: u64 = 2;

/// The log end offset after the two records each case appends.
const LOG_END: i64 = 2;

/// Who sends the fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sender {
    /// A follower fetch with this replica id, and this current leader epoch.
    Follower { replica_id: i32, leader_epoch: i32 },
    /// A consumer fetch, with replica id -1 and no rack.
    Consumer,
}

/// What a case expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// The follower reads at the log end, and the high watermark follows it.
    FollowerRead,
    /// The consumer reads up to the high watermark, which is still 0.
    ConsumerRead,
    /// The row is refused with this code, and names this current leader.
    Refused(i16, Option<u64>),
}

#[derive(Debug, Clone, Copy)]
struct Case {
    version: i16,
    sender: Sender,
    leader: u64,
    expect: Expect,
}

/// The observable result of one case.
#[derive(Debug, PartialEq)]
struct Outcome {
    case: String,
    response: FetchResponse,
    high_watermark: Offset,
    tracked_followers: Vec<u64>,
}

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        // Node 2 never fetches. Keep it in the ISR for the whole test.
        cfg.replica_lag_time_max = krabka_units::secs(600);
    })
    .await
}

/// Create `topic` with replicas 1 and 2, led by `leader`, wait until this
/// broker holds the partition in that role, and append two records.
async fn partition(
    broker: &BrokerHandle,
    topic: &str,
    topic_id: u128,
    leader: u64,
) -> Arc<Partition> {
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
            leader: krabka_audit::NodeId(leader),
            replicas: vec![
                krabka_audit::NodeId(THIS_NODE),
                krabka_audit::NodeId(OTHER_NODE),
            ],
            isr: vec![
                krabka_audit::NodeId(THIS_NODE),
                krabka_audit::NodeId(OTHER_NODE),
            ],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: vec![uuid::Uuid::nil(); 2],
            partition_epoch: 0,
        }))
        .await
        .expect("submit partition record");

    let shared = broker.broker_arc_for_test();
    let partition = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(partition) = shared.partitions.get(topic, krabka_ids::PartitionIndex(0))
                && partition
                    .current_leader
                    .load(std::sync::atomic::Ordering::Acquire)
                    == leader
                && (leader != THIS_NODE
                    || partition
                        .replica_state
                        .lock()
                        .await
                        .isr
                        .contains(&krabka_raft::NodeId(OTHER_NODE)))
            {
                return partition;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the broker holds the partition in its role");

    let mut batch = RecordBatch {
        last_offset_delta: 1,
        records: [&b"first"[..], &b"second"[..]]
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
    partition
}

/// A sessionless one-row fetch of partition 0 of `topic`. A follower fetches
/// from the log end offset, which moves the high watermark when the leader
/// records its progress. A consumer fetches from offset 0.
fn request(version: i16, sender: Sender, topic: &str) -> FetchRequest {
    let (replica_id, current_leader_epoch, fetch_offset) = match sender {
        Sender::Follower {
            replica_id,
            leader_epoch,
        } => (replica_id, leader_epoch, LOG_END),
        Sender::Consumer => (-1, -1, 0),
    };
    // Before v15 the wire carries `ReplicaId`, from v15 on `ReplicaState`.
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
        session_id: INVALID_SESSION_ID,
        session_epoch: FINAL_EPOCH,
        topics: vec![FetchTopic {
            topic: topic.to_owned(),
            partitions: vec![FetchPartition {
                partition: 0,
                current_leader_epoch,
                fetch_offset,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn fetch(broker: &BrokerHandle, version: i16, request: &FetchRequest) -> FetchResponse {
    let shared = broker.broker_arc_for_test();
    let user = principal("client");
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

fn expected(case: Case, label: String, topic: &str) -> Outcome {
    let read = |watermark: i64, records: RecordsPayload| PartitionData {
        partition_index: 0,
        error_code: codes::NONE,
        high_watermark: watermark,
        last_stable_offset: watermark,
        log_start_offset: 0,
        aborted_transactions: None,
        preferred_read_replica: -1,
        records: Some(records),
        ..Default::default()
    };
    let (row, high_watermark, tracked_followers) = match case.expect {
        Expect::FollowerRead => (
            read(LOG_END, RecordsPayload::Legacy(Bytes::new())),
            Offset(LOG_END),
            vec![OTHER_NODE],
        ),
        Expect::ConsumerRead => (
            read(0, RecordsPayload::Legacy(Bytes::new())),
            Offset(0),
            if case.leader == THIS_NODE {
                vec![OTHER_NODE]
            } else {
                Vec::new()
            },
        ),
        Expect::Refused(error_code, current_leader) => (
            PartitionData {
                partition_index: 0,
                error_code,
                high_watermark: -1,
                last_stable_offset: -1,
                log_start_offset: -1,
                aborted_transactions: None,
                preferred_read_replica: -1,
                records: Some(RecordsPayload::Legacy(Bytes::new())),
                // `CurrentLeader` is a tagged field from v12.
                current_leader: current_leader.filter(|_| case.version >= 12).map_or_else(
                    LeaderIdAndEpoch::default,
                    |leader| LeaderIdAndEpoch {
                        leader_id: i32::try_from(leader).expect("small node id"),
                        leader_epoch: 0,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
            Offset(0),
            if case.leader == THIS_NODE {
                vec![OTHER_NODE]
            } else {
                Vec::new()
            },
        ),
    };
    Outcome {
        case: label,
        response: FetchResponse {
            error_code: codes::NONE,
            session_id: INVALID_SESSION_ID,
            responses: vec![FetchableTopicResponse {
                topic: topic.to_owned(),
                partitions: vec![row],
                ..Default::default()
            }],
            ..Default::default()
        },
        high_watermark,
        tracked_followers,
    }
}

#[tokio::test]
async fn a_read_needs_the_leader_and_an_assigned_follower() {
    let assigned = Sender::Follower {
        replica_id: 2,
        leader_epoch: -1,
    };
    let unassigned_without_epoch = Sender::Follower {
        replica_id: 3,
        leader_epoch: -1,
    };
    let unassigned_with_epoch = Sender::Follower {
        replica_id: 3,
        leader_epoch: 0,
    };
    let cases = [
        Case {
            version: 12,
            sender: assigned,
            leader: THIS_NODE,
            expect: Expect::FollowerRead,
        },
        Case {
            version: 12,
            sender: assigned,
            leader: OTHER_NODE,
            expect: Expect::Refused(codes::NOT_LEADER_OR_FOLLOWER, Some(OTHER_NODE)),
        },
        Case {
            version: 12,
            sender: unassigned_without_epoch,
            leader: THIS_NODE,
            expect: Expect::Refused(codes::NOT_LEADER_OR_FOLLOWER, Some(THIS_NODE)),
        },
        Case {
            version: 12,
            sender: unassigned_with_epoch,
            leader: THIS_NODE,
            expect: Expect::Refused(codes::UNKNOWN_LEADER_EPOCH, None),
        },
        Case {
            version: 10,
            sender: Sender::Consumer,
            leader: THIS_NODE,
            expect: Expect::ConsumerRead,
        },
        Case {
            version: 10,
            sender: Sender::Consumer,
            leader: OTHER_NODE,
            expect: Expect::Refused(codes::NOT_LEADER_OR_FOLLOWER, Some(OTHER_NODE)),
        },
        Case {
            version: 11,
            sender: Sender::Consumer,
            leader: OTHER_NODE,
            expect: Expect::ConsumerRead,
        },
    ];

    let (broker, _dir) = start().await;
    let mut actual = Vec::new();
    let mut want = Vec::new();
    for (index, case) in (0_u128..).zip(cases) {
        let label = format!("{case:?}");
        let name = format!("leader-read-{index}");
        let partition = partition(&broker, &name, index + 1, case.leader).await;

        let response = fetch(
            &broker,
            case.version,
            &request(case.version, case.sender, &name),
        )
        .await;
        let mut tracked_followers: Vec<u64> = partition
            .replica_state
            .lock()
            .await
            .per_follower
            .keys()
            .map(|node| node.0)
            .collect();
        tracked_followers.sort_unstable();
        actual.push(Outcome {
            case: label.clone(),
            response,
            high_watermark: partition.high_watermark().await,
            tracked_followers,
        });
        want.push(expected(case, label, &name));
    }
    broker.shutdown().await;

    assert!(actual == want);
}
