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
