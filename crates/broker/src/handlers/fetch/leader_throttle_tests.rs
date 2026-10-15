//! Handler tests for the KIP-73 leader-side replication throttle on a
//! follower `Fetch` (#1340).
//!
//! Kafka's `ReplicaManager.readFromLog` asks `shouldLeaderThrottle` before it
//! hands a follower its records: an out-of-sync replica of a throttled
//! partition gets `MemoryRecords.EMPTY` while `ReplicationQuotaManager`
//! reports the quota exceeded. The empty row adds nothing toward the fetch's
//! `min_bytes`, so `DelayedFetch` parks the fetch for its `max_wait_ms`, and
//! the follower does not ask again at once. While the quota is not exceeded
//! the row carries what the read found, a batch larger than the whole quota
//! included, and `KafkaApis` records its size after the fact.
//!
//! The broker under test is node 1 and leads the partition. Node 2 is a
//! replica outside the ISR and never fetches on its own.

use {
    super::{
        follower_watermark_tests::{fetch, stored_batch},
        test_support::start_with_idle_follower,
    },
    crate::{
        broker::BrokerHandle,
        fetch_session::{FINAL_EPOCH, INVALID_SESSION_ID},
        handlers::test_support::{StartedBroker, replicated_partition, wait_for_bound_partition},
        partition::Partition,
        test_support::single_value_batch,
        throttle::LEADER_THROTTLED_REPLICAS_KEY,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_metadata::{
        BrokerConfigRecord, MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord, TopicRecord,
    },
    krabka_protocol::{
        owned::{
            fetch_request::{FetchPartition, FetchRequest, FetchTopic},
            fetch_response::FetchResponse,
        },
        records::RecordsPayload,
    },
    krabka_units::bytes_per_sec,
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

const TOPIC: &str = "throttled";

/// The node id of the follower.
const FOLLOWER: u64 = 2;

/// The leader's throttle, in bytes per second. The bucket bursts one second
/// of it.
const LEADER_RATE: u32 = 1_000;

/// How long a fetch may park. A fetch that comes back sooner than
/// [`MIN_PARK`] did not wait for its `min_bytes`.
const MAX_WAIT_MS: i32 = 400;
const MIN_PARK: Duration = Duration::from_millis(350);

/// A record value three times the leader's one-second burst, so each batch
/// is larger than anything the bucket can ever hold.
static OVERSIZED_VALUE: [u8; 3_000] = [7; 3_000];

/// Create the topic with replicas 1 and 2 and only the leader in the ISR,
/// throttle every replica of it on the leader at [`LEADER_RATE`], and wait
/// until the broker leads the partition and runs at that rate.
async fn throttled_partition(broker: &BrokerHandle) -> Arc<Partition> {
    let replicas = [NodeId(1), NodeId(FOLLOWER)];
    for record in [
        MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(1),
            config_name: "leader.replication.throttled.rate".into(),
            config_value: Some(LEADER_RATE.to_string()),
        }),
        MetadataRecord::V1Topic(TopicRecord {
            name: TOPIC.to_owned(),
            topic_id: uuid::Uuid::from_u128(1_340),
            partitions: 1,
            replication_factor: 2,
        }),
        MetadataRecord::V1Partition(PartitionRecord {
            isr: vec![NodeId(1)],
            directories: vec![uuid::Uuid::nil(); replicas.len()],
            ..replicated_partition()
                .topic(TOPIC)
                .leader(1)
                .replicas(&replicas)
                .call()
        }),
        MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: TOPIC.to_owned(),
            overrides: [(LEADER_THROTTLED_REPLICAS_KEY.to_owned(), "*".to_owned())]
                .into_iter()
                .collect(),
        }),
    ] {
        broker
            .submit_metadata_record_for_test(record)
            .await
            .expect("submit metadata record");
    }
    let shared = broker.broker_arc_for_test();
    wait_for_bound_partition(
        &shared,
        TOPIC,
        async |_| shared.throttle_state.leader_out.byte_rate() == bytes_per_sec(LEADER_RATE),
        "the broker leads the partition and throttles it",
    )
    .await
}

/// A sessionless follower fetch of partition 0 from `fetch_offset` that
/// waits for one byte.
fn request(fetch_offset: i64) -> FetchRequest {
    FetchRequest {
        replica_id: i32::try_from(FOLLOWER).expect("small node id"),
        max_wait_ms: MAX_WAIT_MS,
        min_bytes: 1,
        max_bytes: 1_048_576,
        session_id: INVALID_SESSION_ID,
        session_epoch: FINAL_EPOCH,
        topics: vec![FetchTopic {
            topic: TOPIC.to_owned(),
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

/// What a follower fetch brought back, and whether it parked.
#[derive(Debug, PartialEq)]
struct Answered {
    records: Option<RecordsPayload>,
    parked: bool,
}

async fn answered(broker: &BrokerHandle, fetch_offset: i64) -> Answered {
    let started = Instant::now();
    let response: FetchResponse = tokio::time::timeout(
        Duration::from_secs(5),
        fetch(broker, &request(fetch_offset)),
    )
    .await
    .expect("the fetch is answered");
    Answered {
        records: response.responses[0].partitions[0].records.clone(),
        parked: started.elapsed() >= MIN_PARK,
    }
}

/// A batch larger than the throttle's whole burst still replicates: the
/// quota is not exceeded when the first fetch reads, so the batch goes out
/// whole and its size is recorded past the bound. The overshoot is paid
/// back by throttling: the next fetch finds the quota exceeded, gets no
/// records, and parks for its `max_wait_ms` instead of coming straight back
/// for an immediate retry.
#[tokio::test]
async fn an_oversized_batch_replicates_and_the_overshoot_parks_the_next_fetch() {
    let StartedBroker { broker, dir: _dir } = start_with_idle_follower().await;
    let partition = throttled_partition(&broker).await;
    let append = async || {
        partition
            .produce_batch(single_value_batch(&OVERSIZED_VALUE))
            .await
            .expect("append an oversized batch");
    };

    append().await;
    let first = answered(&broker, 0).await;
    append().await;
    let second = answered(&broker, 1).await;
    broker.shutdown().await;

    assert!(
        (first, second)
            == (
                Answered {
                    records: Some(RecordsPayload::V2(vec![stored_batch(&partition, 0)])),
                    parked: false,
                },
                Answered {
                    records: Some(RecordsPayload::Legacy(Bytes::new())),
                    parked: true,
                },
            )
    );
}
