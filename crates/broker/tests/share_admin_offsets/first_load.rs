//! Tests for where a share partition with no persisted start offset starts,
//! and when that start becomes durable.
//!
//! The group coordinator initializes a share partition at start offset `-1`.
//! Kafka 4.3.1's `SharePartition.maybeInitialize` resolves
//! `share.auto.offset.reset` in memory when it first loads such a partition
//! and writes nothing. The persisted start stays `-1` until the first
//! `WriteShareGroupState`, which carries the in-memory start. A leader that
//! loads the partition before that write resolves the strategy again.

use {
    crate::{
        describe::describe_offsets,
        harness::{NONE, RestartDirectory, broker_config, join, produce_n, restart_directory},
        support::share::{
            FetchSessionMode, ShareAck, ShareSessionSetup, acknowledge_success, fetch_row,
            fetch_until_acquired, restart_broker, set_auto_offset_reset, share_fetch_req,
            start_topic, wire,
        },
    },
    assert2::assert,
    krabka_client_core::Client,
    krabka_metadata::LeaderEpoch,
    krabka_protocol::owned::{
        describe_share_group_offsets_response::{
            DescribeShareGroupOffsetsResponseGroup, DescribeShareGroupOffsetsResponsePartition,
            DescribeShareGroupOffsetsResponseTopic,
        },
        share_fetch_response::AcquiredRecords,
    },
    krabka_raft::NodeId,
    std::time::Duration,
    tokio::time,
};

const GROUP: &str = "g1";

/// Kafka's `PartitionFactory.UNINITIALIZED_START_OFFSET`.
const UNINITIALIZED_START_OFFSET: i64 = -1;

/// The `DescribeShareGroupOffsets` row of `g1` for partition 0 of `t`.
fn described(
    tid: uuid::Uuid,
    (start_offset, leader_epoch, lag): (i64, i32, i64),
) -> DescribeShareGroupOffsetsResponseGroup {
    DescribeShareGroupOffsetsResponseGroup {
        group_id: GROUP.into(),
        topics: vec![DescribeShareGroupOffsetsResponseTopic {
            topic_name: "t".into(),
            topic_id: wire(tid),
            partitions: vec![DescribeShareGroupOffsetsResponsePartition {
                partition_index: 0,
                start_offset,
                leader_epoch,
                lag,
                error_code: NONE,
                ..Default::default()
            }],
            ..Default::default()
        }],
        error_code: NONE,
        ..Default::default()
    }
}

/// The persisted `(start_offset, delivery_complete_count)` of partition 0.
async fn persisted_start(broker: &krabka_broker::BrokerHandle, tid: uuid::Uuid) -> (i64, i32) {
    let (_, _, start, delivery_complete_count) = broker
        .share_state_summary_for_test(GROUP, tid, 0)
        .await
        .expect("initialized share state");
    (start, delivery_complete_count)
}

/// The leader epoch the share state of partition 0 holds. Initialization
/// stores 0, and each load by a partition leader raises it to that leader's
/// epoch, which `DescribeShareGroupOffsets` then reports.
async fn stored_leader_epoch(broker: &krabka_broker::BrokerHandle, tid: uuid::Uuid) -> i32 {
    let (_, leader_epoch, _, _) = broker
        .share_state_summary_for_test(GROUP, tid, 0)
        .await
        .expect("initialized share state");
    leader_epoch
}

/// Waits until the image elects this broker leader of `t-0` at an epoch above
/// `above`, and the local partition runs at that epoch, then returns it.
///
/// The epoch is read rather than assumed. A broker that misses heartbeats on
/// a loaded host can be fenced and re-elected, and each election raises the
/// epoch, so it is not always 0 after the first boot or 1 after a restart.
async fn settled_leader_epoch(
    broker: &krabka_broker::BrokerHandle,
    above: Option<LeaderEpoch>,
) -> LeaderEpoch {
    let node = NodeId(broker.node_id());
    broker
        .wait_for_image(move |image| {
            image.partition("t", 0).is_some_and(|p| {
                p.leader == node && above.is_none_or(|epoch| p.leader_epoch > epoch)
            })
        })
        .await;
    let epoch = broker
        .controller_image_for_test()
        .partition("t", 0)
        .expect("t-0 is in the image")
        .leader_epoch;
    broker
        .wait_until_local_partition_target("t", 0, node, epoch)
        .await;
    epoch
}

/// Kafka's `COORDINATOR_NOT_AVAILABLE`, a retriable error.
const COORDINATOR_NOT_AVAILABLE: i16 = 15;

/// Joins `g1`, waits for its share state, and opens a share session with a
/// fetch. With `latest` and no record above the high watermark, the fetch
/// loads the share partition and acquires nothing.
///
/// The load reads the share state from the share coordinator. While the
/// coordinator (re)loads its `__share_group_state` partition, after a start
/// or a re-election, the read fails and the row answers
/// `COORDINATOR_NOT_AVAILABLE` without caching the partition. A consumer
/// retries that error, so the fetch is sent again until the load succeeds.
async fn join_and_load(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    tid: uuid::Uuid,
) -> String {
    let member = join(client, GROUP, "t").await.member;
    broker.wait_for_share_state_summary(GROUP, tid, 0).await;
    let request = share_fetch_req()
        .session(ShareSessionSetup::opening(&member, tid))
        .call();
    let mut row = fetch_row(client, request.clone(), FetchSessionMode::Initial).await;
    for _ in 0..300 {
        if row.error_code != COORDINATOR_NOT_AVAILABLE {
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
        row = fetch_row(client, request.clone(), FetchSessionMode::Initial).await;
    }
    assert!(
        (row.error_code, row.acquired_records.is_empty()) == (NONE, true),
        "{row:?}"
    );
    member
}

use {
    krabka_broker::BrokerHandle,
    krabka_protocol::owned::share_fetch_response::PartitionData,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::sync::Arc,
};

#[derive(Default)]
struct LatestShareStart {
    directory: Option<RestartDirectory>,
    broker: Option<BrokerHandle>,
    client: Option<Arc<Client>>,
    topic: Option<uuid::Uuid>,
    leading: Option<LeaderEpoch>,
    first_epoch: Option<i32>,
    second_epoch: Option<i32>,
    first_snapshot: Option<(i64, DescribeShareGroupOffsetsResponseGroup)>,
    loaded_start: Option<i64>,
    durable: Option<((i64, i32), DescribeShareGroupOffsetsResponseGroup)>,
    member: Option<String>,
    acquired: Option<PartitionData>,
}

#[fixture]
fn latest_share_start() -> LatestShareStart {
    // Retain each leader's actual epoch and persisted observations across the restart.
    LatestShareStart::default()
}

impl LatestShareStart {
    fn broker(&self) -> &BrokerHandle { self.broker.as_ref().expect("share broker") }

    fn client(&self) -> &Client { self.client.as_ref().expect("share client") }

    fn topic(&self) -> uuid::Uuid { self.topic.expect("topic identity") }
}

#[given("a latest-reset share group g1 whose topic t contains three records")]
async fn latest_share_group(latest_share_start: &mut LatestShareStart) {
    latest_share_start.directory = Some(restart_directory().await);
    let (broker, client, topic) = start_topic()
        .cfg(broker_config(
            latest_share_start
                .directory
                .as_ref()
                .expect("durable directory")
                .path
                .clone(),
        ))
        .call()
        .await;
    broker.wait_until_group_coordinator_ready().await;
    broker.wait_until_share_coordinator_ready().await;
    set_auto_offset_reset(&client, GROUP, "latest").await;
    produce_n()
        .client(&client)
        .topic_id(topic)
        .count(3)
        .call()
        .await;
    latest_share_start.leading = Some(settled_leader_epoch(&broker, None).await);
    latest_share_start.broker = Some(broker);
    latest_share_start.client = Some(client);
    latest_share_start.topic = Some(topic);
}

#[when("the first leader loads the latest share partition")]
async fn load_first_latest_start(latest_share_start: &mut LatestShareStart) {
    join_and_load(
        latest_share_start.broker(),
        latest_share_start.client(),
        latest_share_start.topic(),
    )
    .await;
    latest_share_start.first_epoch =
        Some(stored_leader_epoch(latest_share_start.broker(), latest_share_start.topic()).await);
    latest_share_start.first_snapshot = Some((
        persisted_start(latest_share_start.broker(), latest_share_start.topic())
            .await
            .0,
        describe_offsets(latest_share_start.client(), GROUP, "t", vec![0]).await,
    ));
}

#[then(
    "the first load stores its leader epoch while start offset and described lag remain uninitialized"
)]
fn first_latest_start(latest_share_start: &LatestShareStart) {
    let epoch = latest_share_start.first_epoch.expect("first stored epoch");
    assert!(epoch >= latest_share_start.leading.expect("first leader epoch").0);
    assert!(
        latest_share_start
            .first_snapshot
            .as_ref()
            .expect("first load observed")
            == &(
                UNINITIALIZED_START_OFFSET,
                described(
                    latest_share_start.topic(),
                    (UNINITIALIZED_START_OFFSET, epoch, -1)
                )
            )
    );
}

#[when("two more records arrive and the broker restarts under a newer partition leader epoch")]
async fn restart_unwritten_latest_start(latest_share_start: &mut LatestShareStart) {
    produce_n()
        .client(latest_share_start.client())
        .topic_id(latest_share_start.topic())
        .count(2)
        .call()
        .await;
    let prior = latest_share_start
        .broker()
        .controller_image_for_test()
        .partition("t", 0)
        .expect("partition in image")
        .leader_epoch;
    latest_share_start
        .broker
        .take()
        .expect("first broker")
        .shutdown()
        .await;
    let (broker, client) = restart_broker(
        latest_share_start
            .directory
            .as_ref()
            .expect("durable directory")
            .path
            .clone(),
    )
    .await;
    latest_share_start.leading = Some(settled_leader_epoch(&broker, Some(prior)).await);
    latest_share_start.broker = Some(broker);
    latest_share_start.client = Some(client);
}

#[when("the new leader loads latest again without acknowledging records")]
async fn load_second_latest_start(latest_share_start: &mut LatestShareStart) {
    latest_share_start.member = Some(
        join_and_load(
            latest_share_start.broker(),
            latest_share_start.client(),
            latest_share_start.topic(),
        )
        .await,
    );
    latest_share_start.loaded_start = Some(
        persisted_start(latest_share_start.broker(), latest_share_start.topic())
            .await
            .0,
    );
}

#[then("the new load still leaves the persisted start offset uninitialized")]
fn second_latest_start(latest_share_start: &LatestShareStart) {
    assert!(latest_share_start.loaded_start == Some(UNINITIALIZED_START_OFFSET));
}

#[when("two further records arrive and the reloaded member fetches")]
async fn fetch_second_latest_start(latest_share_start: &mut LatestShareStart) {
    produce_n()
        .client(latest_share_start.client())
        .topic_id(latest_share_start.topic())
        .count(2)
        .call()
        .await;
    latest_share_start.acquired = Some(
        fetch_until_acquired(
            latest_share_start.client(),
            share_fetch_req()
                .session(ShareSessionSetup::opening(
                    latest_share_start.member.as_ref().expect("reloaded member"),
                    latest_share_start.topic(),
                ))
                .call(),
            FetchSessionMode::Initial,
        )
        .await,
    );
}

#[then("only offsets 5 and 6 are acquired at delivery count one")]
fn second_latest_acquisition(latest_share_start: &LatestShareStart) {
    assert!(
        latest_share_start
            .acquired
            .as_ref()
            .expect("latest records acquired")
            .acquired_records
            == vec![AcquiredRecords {
                first_offset: 5,
                last_offset: 6,
                delivery_count: 1,
                ..Default::default()
            }]
    );
}

#[when("the reloaded member accepts offset 6 and waits for its delivery-complete record")]
async fn persist_second_latest_start(latest_share_start: &mut LatestShareStart) {
    acknowledge_success(
        latest_share_start.client(),
        ShareAck::single_for(
            latest_share_start.member.as_ref().expect("reloaded member"),
            latest_share_start.topic(),
            krabka_ids::Offset(6),
        ),
    )
    .await;
    latest_share_start
        .broker()
        .wait_until_share_delivery_complete(GROUP, latest_share_start.topic(), 0, 1)
        .await;
    latest_share_start.second_epoch =
        Some(stored_leader_epoch(latest_share_start.broker(), latest_share_start.topic()).await);
    latest_share_start.durable = Some((
        persisted_start(latest_share_start.broker(), latest_share_start.topic()).await,
        describe_offsets(latest_share_start.client(), GROUP, "t", vec![0]).await,
    ));
}

#[then("the newer epoch durably reports start 5, one completed record, and lag one")]
fn durable_second_latest_start(latest_share_start: &LatestShareStart) {
    let epoch = latest_share_start
        .second_epoch
        .expect("recovered stored epoch");
    assert!(
        epoch
            >= latest_share_start
                .leading
                .expect("recovered leader epoch")
                .0
    );
    assert!(epoch > latest_share_start.first_epoch.expect("first stored epoch"));
    assert!(
        latest_share_start
            .durable
            .as_ref()
            .expect("durable state observed")
            == &((5, 1), described(latest_share_start.topic(), (5, epoch, 1)))
    );
}

#[scenario(
    path = "tests/features/migrated/tests/share_admin_offsets/first_load.feature",
    name = "an unwritten start is resolved again and the first ack persists it"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwritten_start_is_resolved_again_and_the_first_ack_persists_it(
    latest_share_start: LatestShareStart,
) {
    latest_share_start
        .broker
        .take()
        .expect("recovered broker")
        .shutdown()
        .await;
    drop(latest_share_start);
}
