//! Tests for `DeleteShareGroupOffsets`, `api_key` 92.
//!
//! Delete removes the durable share-state of a topic in an empty group, so a
//! later Describe reads the partition as missing. It also rewrites the v14
//! state-partition-metadata record, so the topic stays out of the initialized
//! set of the group across a restart.

use std::time::Duration;

use assert2::{assert, check};
use krabka_broker::{BootstrapMode, Broker};
use krabka_protocol::owned::delete_share_group_offsets_request::{
    DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsRequestTopic,
};

use crate::{
    describe::{describe_offsets, describe_until},
    harness::{
        NONE, bootstrap_share_state, broker_config, broker_test_permit, connect, create_topic,
        fetch_until_acquired, join, leave, produce_n, topic_id, wait_for_share_init,
    },
};

/// Delete removes the durable share-state for a topic of an empty group.
///
/// A later Describe reads the partition as missing and reports
/// `start_offset` -1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_removes_topic() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, "g1", tid, 0).await;
    produce_n(&client, "t", tid, 0, 3).await;

    // Initialize the topic's share state via the join lifecycle + a consume, then
    // leave so the group is empty.
    let (member, _epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, "g1", tid, 0).await;
    let _ = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    leave(&client, "g1", &member).await;

    let resp = client
        .send(DeleteShareGroupOffsetsRequest {
            group_id: "g1".into(),
            topics: vec![DeleteShareGroupOffsetsRequestTopic {
                topic_name: "t".into(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DeleteShareGroupOffsets");
    check!(
        resp.error_code == NONE,
        "delete top-level error: {}",
        resp.error_code
    );
    check!(
        resp.responses[0].topic_name == "t",
        "delete response topic name mismatch: {}",
        resp.responses[0].topic_name
    );
    check!(
        resp.responses[0].error_code == NONE,
        "delete per-topic error: {}",
        resp.responses[0].error_code
    );

    // Describe now reads the removed partition as missing → start_offset -1.
    let group = describe_until(&client, "g1", "t", vec![0], -1).await;
    let part = &group.topics[0].partitions[0];
    assert!(
        part.start_offset == -1,
        "deleted state must read as missing (start_offset -1), got {}",
        part.start_offset
    );
    assert!(
        part.error_code == NONE,
        "describe of missing partition is not an error, got {}",
        part.error_code
    );
}

/// F6, delete-metadata rewrite: `DeleteShareGroupOffsets` rewrites the v14
/// state-partition-metadata record.
///
/// The deleted topic is then gone from the initialized set of the group, and it
/// STAYS gone after a restart, because the seed no longer lists it again.
///
/// A describe with an explicit topic name but an EMPTY partitions list
/// enumerates the *initialized* partitions of the group for that topic, read
/// from the v14 metadata cache. Before the delete, that returns partition [0].
/// After the delete rewrite, the topic has no initialized partitions, so the
/// `partitions` list of the row is empty. This holds before AND after a
/// restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_rewrites_metadata_topic_absent_after_restart() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let tid;
    {
        let broker = Broker::start(broker_config(log_dir.clone())).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        create_topic(&broker, &client, "t", 1).await;
        tid = topic_id(&broker, "t");
        bootstrap_share_state(&broker, &client, "g1", tid, 0).await;
        produce_n(&client, "t", tid, 0, 3).await;

        // Initialize the topic's share state via the join lifecycle + a consume,
        // then leave so the group is empty (Delete requires an empty group).
        let (member, _epoch) = join(&client, "g1", "t").await;
        wait_for_share_init(&broker, "g1", tid, 0).await;
        let _ = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;

        // Sanity: a describe with empty partitions enumerates the initialized
        // partitions for "t" — partition [0] is present before the delete.
        let before = describe_offsets(&client, "g1", "t", vec![]).await;
        let before_parts: Vec<i32> = before
            .topics
            .iter()
            .find(|t| t.topic_name == "t")
            .map(|t| t.partitions.iter().map(|p| p.partition_index).collect())
            .unwrap_or_default();
        assert!(
            before_parts == vec![0],
            "describe must enumerate initialized partition [0] before delete, got {before_parts:?}"
        );

        leave(&client, "g1", &member).await;

        let resp = client
            .send(DeleteShareGroupOffsetsRequest {
                group_id: "g1".into(),
                topics: vec![DeleteShareGroupOffsetsRequestTopic {
                    topic_name: "t".into(),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("DeleteShareGroupOffsets");
        assert!(
            resp.error_code == NONE && resp.responses[0].error_code == NONE,
            "delete failed: top={} per-topic={}",
            resp.error_code,
            resp.responses[0].error_code
        );

        // The describe-by-name with empty partitions no longer enumerates any
        // initialized partition for "t" (the v14 metadata record was rewritten).
        // This is a NEGATIVE condition (absence); no broker awaiter exists for
        // "metadata rewrite complete", so we poll until the absence is observed.
        // Bounded to 4s — the delete RPC already succeeded so the rewrite is
        // in-flight, not guessing arbitrary settle time.
        let mut absent = false;
        for _ in 0..40 {
            let g = describe_offsets(&client, "g1", "t", vec![]).await;
            let parts: Vec<i32> = g
                .topics
                .iter()
                .find(|t| t.topic_name == "t")
                .map(|t| t.partitions.iter().map(|p| p.partition_index).collect())
                .unwrap_or_default();
            if parts.is_empty() {
                absent = true;
                break;
            }
            // real-time wait (not a progress poll): settle between re-checks asserting the deleted topic stays absent (absence, not a positive poll)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            absent,
            "describe must not enumerate any initialized partition for the deleted topic"
        );

        // `absent` is confirmed above (positive absence observed via describe).
        // A brief flush sleep lets the v14 metadata-rewrite persist to disk
        // before shutdown, so the restart below sees the rewritten seed.
        // This is a persist-flush, not a state-guessing settle: we already know
        // the in-memory state is correct.
        tokio::time::sleep(Duration::from_millis(300)).await;
        broker.shutdown().await;
    }

    {
        let mut cfg = broker_config(log_dir);
        cfg.bootstrap_mode = BootstrapMode::Rejoin;
        let broker = Broker::start(cfg).await.unwrap();
        let client = connect(&broker.listen_addr().to_string()).await;
        bootstrap_share_state(&broker, &client, "g1", tid, 0).await;

        // After restart, the v14 seed no longer lists "t" (the rewrite removed
        // it), so the describe-by-name with empty partitions must STILL
        // enumerate zero initialized partitions. Poll a window to let the
        // coordinator finish replaying state, asserting absence throughout.
        let deadline = std::time::Instant::now() + Duration::from_secs(8);
        loop {
            let g = describe_offsets(&client, "g1", "t", vec![]).await;
            let parts: Vec<i32> = g
                .topics
                .iter()
                .find(|t| t.topic_name == "t")
                .map(|t| t.partitions.iter().map(|p| p.partition_index).collect())
                .unwrap_or_default();
            assert!(
                parts.is_empty(),
                "deleted topic must remain un-initialized after restart (v14 rewrite), got {parts:?}"
            );
            if std::time::Instant::now() >= deadline {
                break;
            }
            // real-time wait (not a progress poll): settle between re-checks asserting the deleted topic stays absent throughout the window (liveness, not a positive poll)
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}
