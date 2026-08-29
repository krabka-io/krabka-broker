//! The fail-closed test: while the topic-backed manager has not activated, the
//! copy task must tier nothing.
//!
//! The broker boots with a bootstrap address on a dead port, so the retry loop
//! never swaps the manager in and every `add_remote_log_segment_metadata` call
//! returns `NotReady`. That boot is deliberately not shared with
//! `rlmm_cluster`: the dead-port bootstrap is the whole point of the scenario.

use std::time::Duration;

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, KafkaRlmmConfig, RemoteStorageBackend, RlmmKind};
use krabka_protocol::owned::create_topics_request::{
    CreatableTopic, CreatableTopicConfig, CreateTopicsRequest,
};
use tempfile::TempDir;

use crate::{
    rlmm_cluster::{await_tiered_config, build_client},
    rlmm_round_trip::count_remote_log_files,
    run_broker_test, support,
};

/// While the topic-backed RLMM has not yet activated, the RLM copy task must
/// not tier any segment. Bootstrap points at a dead port, so the retry loop
/// never succeeds. The copy task calls `add_remote_log_segment_metadata`
/// first, and a `NotReady` error makes the copy task skip the segment
/// entirely. This proves the fail-closed guarantee: no orphaned objects
/// accumulate in the remote store while the RLMM is unavailable.
///
/// The topic config and produce volume mirror
/// [`topic_rlmm_copy_then_fetch_round_trip`] exactly, so "0 tiered objects"
/// is genuinely discriminating. The analogous loopback test tiers ≥ 1.
#[test]
fn copy_task_skips_tiering_while_rlmm_not_ready() {
    run_broker_test(copy_task_skips_tiering_while_rlmm_not_ready_case());
}

async fn copy_task_skips_tiering_while_rlmm_not_ready_case() {
    const TOPIC: &str = "tiered-not-ready-itest";

    support::init_tracing();

    // Hold both ports to eliminate bind-and-drop races under parallel nextest.
    let (client_addrs, controller_addrs, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let listen = client_addrs[0];

    let log_dir = TempDir::new().expect("log tempdir");
    let remote_dir = TempDir::new().expect("remote tempdir");

    let mut cfg = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    cfg.listen_addr = listen;
    cfg.advertised_listener = listen.to_string();
    cfg.controller_listen_addr = controller_addrs[0];
    cfg.controller_quorum_voters =
        vec![(krabka_broker::NodeId(1), controller_addrs[0].to_string())];
    cfg.remote_storage_backend = Some(RemoteStorageBackend::Local {
        dir: remote_dir.path().to_path_buf(),
    });
    cfg.remote_log_manager_interval = krabka_units::millis(200);
    // Dead port: the retry loop can never dial the bootstrap; the SwappableRlmm
    // stays on the NotReadyRlmm stub for the entire test.
    cfg.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
        bootstrap: "127.0.0.1:1".into(),
        num_partitions: 1,
        replication: 1,
        snapshot_interval: krabka_units::hours(1),
        snapshot_dir: log_dir.path().join("rlmm-snap"),
        security: None,
        ..KafkaRlmmConfig::default()
    });

    let data_listener = client_listeners.into_iter().next().unwrap();
    let controller_listener = controller_listeners.into_iter().next().unwrap();
    let broker = Broker::start_with_listeners(cfg, Some(controller_listener), Some(data_listener))
        .await
        .expect("broker starts");
    let client = build_client(&broker).await;

    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs: vec![
                    CreatableTopicConfig {
                        name: "remote.storage.enable".into(),
                        value: Some("true".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "segment.bytes".into(),
                        value: Some("1024".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "local.retention.bytes".into(),
                        value: Some("1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.bytes".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                    CreatableTopicConfig {
                        name: "retention.ms".into(),
                        value: Some("-1".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics failed: {:?}",
        resp.topics[0].error_message
    );

    // Wait for the tiered config to propagate into the partition's LogConfig
    // (same gate as the loopback round-trip test).
    // intentional: local `LogConfig` override applied by the reconcile loop —
    // no awaiter/metric exists for it, so poll directly.
    await_tiered_config(&broker, TOPIC).await;

    // Same 80 records as the loopback round-trip — enough to seal several
    // 1 KiB segments and give the copy task ample segments to try to tier.
    broker
        .produce_records_for_test(TOPIC, 0, 80)
        .await
        .expect("produce records");

    // intentional: this is the behaviour under test — a deliberate "observe
    // nothing tiered within a window" wait. We let several copy-task ticks
    // elapse (200 ms interval × ~10 ticks) and then assert 0 tiered objects;
    // there is no "did-not-happen" event to await on.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The RLMM is still NotReady, so add_remote_log_segment_metadata returns
    // NotReady and the copy task must have skipped every segment.
    let tiered = count_remote_log_files(remote_dir.path());
    assert!(
        tiered == 0,
        "expected no tiered objects while RLMM not ready, found {tiered}"
    );

    // Close the test client before broker shutdown for the same reason as
    // `topic_rlmm_copy_then_fetch_round_trip`.
    drop(client);
    broker.shutdown().await;
}
