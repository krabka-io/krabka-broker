//! Share-state lifecycle scenarios: the group coordinator initializing the
//! per-partition share state in the `__share_group_state` persister, and that
//! state surviving a broker restart without being re-initialized.
//!
//! These tests assert on the persister rather than on what
//! `ShareGroupDescribe` reports about members, so they are kept apart from the
//! membership scenarios.

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker};

use crate::share_group_harness::{
    boot, broker_config, connect, create_topic, describe, heartbeat, topic_id,
};

/// Kafka's `PartitionFactory.UNINITIALIZED_START_OFFSET`: the share state
/// exists but where the partition starts has not been decided yet.
const UNINITIALIZED_START_OFFSET: i64 = -1;

/// KIP-932 group-coordinator lifecycle. When a share group joins a topic with
/// `P` partitions, the coordinator initializes the per-partition share state in
/// the `__share_group_state` persister. Each entry has a present state, not the
/// missing-key sentinel, and `start_offset`
/// [`UNINITIALIZED_START_OFFSET`]: the coordinator records that the
/// partition exists and leaves where it starts to the share partition, which
/// resolves the group's `share.auto.offset.reset` on its first load, as
/// Kafka's `SharePartition.maybeInitialize` does. The heartbeat hook runs
/// after the reconcile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_initializes_share_state() {
    let (broker, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t5", 3).await;
    let tid = topic_id(&broker, "t5");

    let mut join = heartbeat("g5", "", 0);
    join.subscribed_topic_names = Some(vec!["t5".into()]);
    let r = client.send(join).await.unwrap();
    assert!(r.error_code == 0, "join failed: {:?}", r.error_code);
    let mid = r.member_id.clone().unwrap();

    // The lifecycle hook initializes assigned partitions best-effort on each
    // heartbeat (first heartbeat may fail if __share_group_state isn't ready
    // yet; the hook retries on the next). We interleave heartbeats with a
    // condition check — no fixed count, no fixed sleep — exiting as soon as
    // all three partitions have summaries.
    let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let mut hb = heartbeat("g5", &mid, r.member_epoch);
            hb.subscribed_topic_names = Some(vec!["t5".into()]);
            let _ = client.send(hb).await.unwrap();
            let mut all_done = true;
            for p in 0..3 {
                if broker
                    .share_state_summary_for_test("g5", tid, p)
                    .await
                    .is_none()
                {
                    all_done = false;
                    break;
                }
            }
            if all_done {
                break;
            }
        }
    })
    .await;
    assert!(
        res.is_ok(),
        "lifecycle did not initialize all 3 partitions within 30s"
    );

    for p in 0..3 {
        let (_se, _le, start_offset, _dcc) = broker
            .share_state_summary_for_test("g5", tid, p)
            .await
            .unwrap();
        assert!(
            start_offset == UNINITIALIZED_START_OFFSET,
            "partition {p} initialized at start_offset {UNINITIALIZED_START_OFFSET}, \
             got {start_offset}"
        );
    }
}

/// After a restart, the broker recovers the group's
/// `ShareGroupStatePartitionMetadata`, so a rejoin does not initialize the
/// state again. A stale, non-zero `start_offset` written straight to the
/// persister survives, because the coordinator skips already-initialized
/// partitions on the post-restart heartbeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_metadata_survives_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid;
    {
        let broker = Broker::start(broker_config(log_dir.clone())).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = connect(&bootstrap).await;
        create_topic(&client, "t6", 2).await;
        tid = topic_id(&broker, "t6");

        let mut join = heartbeat("g6", "", 0);
        join.subscribed_topic_names = Some(vec!["t6".into()]);
        let r = client.send(join).await.unwrap();
        assert!(r.error_code == 0, "join failed: {:?}", r.error_code);
        let mid = r.member_id.clone().unwrap();

        // Interleave heartbeats with condition check — no fixed count, no
        // fixed sleep — exiting as soon as both partitions have summaries.
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let mut hb = heartbeat("g6", &mid, r.member_epoch);
                hb.subscribed_topic_names = Some(vec!["t6".into()]);
                let _ = client.send(hb).await.unwrap();
                let mut all_done = true;
                for p in 0..2 {
                    if broker
                        .share_state_summary_for_test("g6", tid, p)
                        .await
                        .is_none()
                    {
                        all_done = false;
                        break;
                    }
                }
                if all_done {
                    break;
                }
            }
        })
        .await;
        assert!(
            res.is_ok(),
            "lifecycle did not initialize both partitions within 30s"
        );
        // Both partitions are initialized before restart.
        for p in 0..2 {
            assert!(
                broker
                    .share_state_summary_for_test("g6", tid, p)
                    .await
                    .is_some(),
                "partition {p} initialized pre-restart"
            );
        }
        broker.shutdown().await;
    }

    {
        let mut cfg = broker_config(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = connect(&bootstrap).await;

        // The recovered ShareCoordinator replays __share_group_state, so the
        // summary is present immediately after restart.
        for p in 0..2 {
            assert!(
                broker
                    .share_state_summary_for_test("g6", tid, p)
                    .await
                    .is_some(),
                "partition {p} share-state recovered after restart"
            );
        }

        // Re-join the recovered group; the coordinator's recovered
        // ShareGroupStatePartitionMetadata means the heartbeat hook treats the
        // partitions as already initialized and does NOT re-Initialize them
        // (a re-init would FENCE on the same state_epoch). The group stays
        // healthy and the state remains present.
        let desc = describe(&client, "g6").await;
        let mid = desc.groups[0].members[0].member_id.clone();
        let mut hb = heartbeat("g6", &mid, desc.groups[0].group_epoch);
        hb.subscribed_topic_names = Some(vec!["t6".into()]);
        let _ = client.send(hb).await.unwrap();

        // Await rather than sleep: confirm the recovered summaries are still
        // present (the coordinator must NOT re-initialize already-initialized
        // partitions after restart).
        for p in 0..2 {
            broker.wait_for_share_state_summary("g6", tid, p).await;
            assert!(
                broker
                    .share_state_summary_for_test("g6", tid, p)
                    .await
                    .is_some(),
                "partition {p} share-state still present after restart re-join"
            );
        }
    }
}

/// `DeleteGroups` on a share group, as Kafka's
/// `GroupCoordinatorService.deleteGroups` answers it. A group with a member
/// gets `NON_EMPTY_GROUP` (68) and keeps its share state. After the member
/// leaves, the delete succeeds, removes the share state of every initialized
/// partition, and drops the group. A second delete gets `GROUP_ID_NOT_FOUND`
/// (69).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_groups_deletes_an_empty_share_group_and_its_state() {
    use krabka_protocol::owned::delete_groups_request::DeleteGroupsRequest;

    let (broker, bootstrap, _d) = boot().await;
    let client = connect(&bootstrap).await;
    create_topic(&client, "t-delete", 2).await;
    let tid = topic_id(&broker, "t-delete");

    let mut join = heartbeat("g-delete", "", 0);
    join.subscribed_topic_names = Some(vec!["t-delete".into()]);
    let joined = client.send(join).await.unwrap();
    assert!(
        joined.error_code == 0,
        "join failed: {:?}",
        joined.error_code
    );
    let member_id = joined.member_id.clone().unwrap();
    let initialized = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let mut hb = heartbeat("g-delete", &member_id, joined.member_epoch);
            hb.subscribed_topic_names = Some(vec!["t-delete".into()]);
            let _ = client.send(hb).await.unwrap();
            let mut all = true;
            for p in 0..2 {
                all &= broker
                    .share_state_summary_for_test("g-delete", tid, p)
                    .await
                    .is_some();
            }
            if all {
                break;
            }
        }
    })
    .await;
    assert!(initialized.is_ok(), "share state never initialized");

    // (step, leave the group before the delete, expected error code, share
    //  state present after the step)
    let steps = [
        ("delete with a member", false, 68_i16, true),
        ("delete after the leave", true, 0, false),
        ("delete again", false, 69, false),
    ];
    for (step, leave, expected, state_present) in steps {
        if leave {
            let left = client
                .send(heartbeat("g-delete", &member_id, -1))
                .await
                .unwrap();
            assert!(left.error_code == 0, "{step}: leave failed");
        }
        let resp = client
            .send(DeleteGroupsRequest {
                groups_names: vec!["g-delete".into()],
                ..Default::default()
            })
            .await
            .expect("DeleteGroups");
        assert!(resp.results[0].error_code == expected, "{step}: {resp:?}");
        for p in 0..2 {
            let present = broker
                .share_state_summary_for_test("g-delete", tid, p)
                .await
                .is_some();
            assert!(present == state_present, "{step}: partition {p}");
        }
    }
}

/// Heartbeats until the share state of every partition in `partitions` of
/// `topic` exists. Each member in `members` sends one heartbeat per pass,
/// with its own subscription.
async fn heartbeat_until_initialized(
    broker: &krabka_broker::BrokerHandle,
    client: &krabka_client_core::Client,
    group: &str,
    members: &[(&str, i32, &str)],
    topic: uuid::Uuid,
    partitions: std::ops::Range<i32>,
) {
    let initialized = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            for (member_id, member_epoch, subscription) in members {
                let mut hb = heartbeat(group, member_id, *member_epoch);
                hb.subscribed_topic_names = Some(vec![(*subscription).to_owned()]);
                let _ = client.send(hb).await.unwrap();
            }
            let mut all = true;
            for p in partitions.clone() {
                all &= broker
                    .share_state_summary_for_test(group, topic, p)
                    .await
                    .is_some();
            }
            if all {
                break;
            }
        }
    })
    .await;
    assert!(
        initialized.is_ok(),
        "share state of {group} never initialized"
    );
}

/// A partition that no member is assigned keeps its share state, as in Kafka:
/// the heartbeat only initializes. Rows: the only consumer of one of two
/// topics leaves while a consumer of the other topic stays, and a member
/// moves its subscription from one topic to the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unassigned_partitions_keep_their_share_state() {
    // (group, the second member subscribes to `b` from the start, the first
    //  member moves to `b` instead of leaving)
    let rows = [("g-keep-leave", true, false), ("g-keep-move", false, true)];
    for (group, second_member, move_subscription) in rows {
        let (broker, bootstrap, _d) = boot().await;
        let client = connect(&bootstrap).await;
        create_topic(&client, "a", 2).await;
        create_topic(&client, "b", 2).await;
        let topic_a = topic_id(&broker, "a");
        let topic_b = topic_id(&broker, "b");

        let join = |member_id: &str, topic: &str| {
            let mut hb = heartbeat(group, member_id, 0);
            hb.subscribed_topic_names = Some(vec![topic.to_owned()]);
            hb
        };
        let first = client.send(join("member-a", "a")).await.unwrap();
        assert!(first.error_code == 0, "{group}: first join failed");
        heartbeat_until_initialized(
            &broker,
            &client,
            group,
            &[("member-a", first.member_epoch, "a")],
            topic_a,
            0..2,
        )
        .await;

        if second_member {
            let second = client.send(join("member-b", "b")).await.unwrap();
            assert!(second.error_code == 0, "{group}: second join failed");
            let left = client.send(heartbeat(group, "member-a", -1)).await.unwrap();
            assert!(left.error_code == 0, "{group}: leave failed");
            heartbeat_until_initialized(
                &broker,
                &client,
                group,
                &[("member-b", second.member_epoch, "b")],
                topic_b,
                0..2,
            )
            .await;
        }
        if move_subscription {
            heartbeat_until_initialized(
                &broker,
                &client,
                group,
                &[("member-a", first.member_epoch, "b")],
                topic_b,
                0..2,
            )
            .await;
        }

        for p in 0..2 {
            assert!(
                broker
                    .share_state_summary_for_test(group, topic_a, p)
                    .await
                    .is_some(),
                "{group}: share state of a-{p} was deleted"
            );
        }
        broker.shutdown().await;
    }
}
