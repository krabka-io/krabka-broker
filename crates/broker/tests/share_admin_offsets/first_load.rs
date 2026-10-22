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
        harness::{NONE, broker_config, join, produce_n, restart_directory},
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
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        row = fetch_row(client, request.clone(), FetchSessionMode::Initial).await;
    }
    assert!(
        (row.error_code, row.acquired_records.is_empty()) == (NONE, true),
        "{row:?}"
    );
    member
}

/// `latest`, loaded on one leader and never acknowledged there, is resolved
/// again by the next leader against the high watermark it finds, and the
/// first acknowledgement persists that second resolution.
///
/// 1. Three records, then a first load: the start resolves to 3 in memory,
///    and the persisted start stays `-1`. Describe reports start and lag
///    `-1`, as Kafka's `shouldComputeSharePartitionLag` is false.
/// 2. Two more records, then a restart: the next load resolves `latest` to 5,
///    not the 3 that the first leader resolved, and the next fetch acquires
///    the records produced after it, 5 and 6, only.
/// 3. Accepting 6 alone leaves the SPSO at 5, so the persisted state is start
///    5 with one delivered record, and the lag is `7 - 5 - 1`. The restart
///    re-elected the broker at a higher leader epoch, and the second load
///    stored it.
///
/// Each load stores the epoch of the leader that ran it, and Describe reports
/// the stored epoch. The test reads the epochs instead of assuming 0 and 1,
/// because a fence and re-election on a loaded host raises them too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwritten_start_is_resolved_again_and_the_first_ack_persists_it() {
    let dir = restart_directory().await;
    let log_dir = dir.path.clone();
    let tid;
    let first_epoch;
    let before_restart;
    {
        let (broker, client, topic) = start_topic()
            .cfg(broker_config(log_dir.clone()))
            .call()
            .await;
        tid = topic;
        broker.wait_until_group_coordinator_ready().await;
        broker.wait_until_share_coordinator_ready().await;
        set_auto_offset_reset(&client, GROUP, "latest").await;
        produce_n()
            .client(&client)
            .topic_id(tid)
            .count(3)
            .call()
            .await;

        let leading = settled_leader_epoch(&broker, None).await;
        join_and_load(&broker, &client, tid).await;

        first_epoch = stored_leader_epoch(&broker, tid).await;
        assert!(first_epoch >= leading.0);
        let first_leader = (
            persisted_start(&broker, tid).await.0,
            describe_offsets(&client, GROUP, "t", vec![0]).await,
        );
        assert!(
            first_leader
                == (
                    UNINITIALIZED_START_OFFSET,
                    described(tid, (UNINITIALIZED_START_OFFSET, first_epoch, -1))
                )
        );

        produce_n()
            .client(&client)
            .topic_id(tid)
            .count(2)
            .call()
            .await;
        before_restart = broker
            .controller_image_for_test()
            .partition("t", 0)
            .expect("t-0 is in the image")
            .leader_epoch;
        broker.shutdown().await;
    }

    // The restart keeps the group's `share.auto.offset.reset=latest`.
    let (broker, client) = restart_broker(log_dir).await;
    // The rejoined broker leads the partition at its old epoch until the
    // controller re-elects it above that. Loading or acknowledging before then
    // reads or writes the share state at the old epoch.
    let leading = settled_leader_epoch(&broker, Some(before_restart)).await;
    let member = join_and_load(&broker, &client, tid).await;
    assert!(persisted_start(&broker, tid).await.0 == UNINITIALIZED_START_OFFSET);
    produce_n()
        .client(&client)
        .topic_id(tid)
        .count(2)
        .call()
        .await;
    let row = fetch_until_acquired(
        &client,
        share_fetch_req()
            .session(ShareSessionSetup::opening(&member, tid))
            .call(),
        FetchSessionMode::Initial,
    )
    .await;
    assert!(
        row.acquired_records
            == vec![AcquiredRecords {
                first_offset: 5,
                last_offset: 6,
                delivery_count: 1,
                ..Default::default()
            }]
    );

    acknowledge_success(
        &client,
        ShareAck::single_for(&member, tid, krabka_ids::Offset(6)),
    )
    .await;
    broker
        .wait_until_share_delivery_complete(GROUP, tid, 0, 1)
        .await;

    let second_epoch = stored_leader_epoch(&broker, tid).await;
    assert!(second_epoch >= leading.0);
    assert!(second_epoch > first_epoch);
    let durable = (
        persisted_start(&broker, tid).await,
        describe_offsets(&client, GROUP, "t", vec![0]).await,
    );
    assert!(durable == ((5, 1), described(tid, (5, second_epoch, 1))));
    broker.shutdown().await;
}
