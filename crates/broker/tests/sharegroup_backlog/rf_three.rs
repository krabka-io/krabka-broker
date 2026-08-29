//! The RF=3 case: a share coordinator that is not the data partition's leader
//! must sample the committed high watermark, never the leader's log end offset.
//!
//! The setup searches the cluster image for an offsets partition whose
//! coordinator sits on a different broker than one of the data partitions, and
//! picks the group name that hashes onto that offsets partition — Kafka's own
//! `String.hashCode` placement, which `java_hash` reproduces. The third broker
//! is stopped so that a produce to the leader stays uncommitted, which is what
//! makes the two candidate samples differ.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use krabka_broker::metrics::ShareGroupLabel;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        find_coordinator_request::FindCoordinatorRequest,
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    harness::{TOPIC, create_topic, scrape, test_lock},
    support,
};

const OFFSETS_TOPIC: &str = "__consumer_offsets";
const SHARE_STATE_TOPIC: &str = "__share_group_state";

fn java_hash(value: &str) -> i32 {
    value.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

fn group_for_partition(partition: i32, partition_count: i32) -> String {
    (0..100)
        .map(|i| format!("backlog-rf3-{i}"))
        .find(|group| {
            let hash = java_hash(group);
            let positive = if hash == i32::MIN { 0 } else { hash.abs() };
            positive % partition_count == partition
        })
        .expect("each offsets partition receives a candidate group")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)]
async fn rf_three_remote_leader_uses_committed_high_watermark() {
    let _guard = test_lock().lock().await;
    let mut attempt = 0;
    let mut cluster = loop {
        attempt += 1;
        match support::start_n_node_with(3, |_, config| {
            config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
            config.offsets_topic_num_partitions = 3;
            config.share_coordinator.state_topic_num_partitions = 1;
            config.share_group.backlog_poll_interval = Duration::from_millis(50);
            config.replica_lag_time_max = krabka_units::secs(30);
        })
        .await
        {
            Ok(cluster) => break cluster,
            Err(error) if attempt < 3 => {
                eprintln!("backlog RF=3 cluster start attempt {attempt} failed: {error}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => panic!("backlog RF=3 cluster failed to start: {error}"),
        }
    };
    support::wait_for_all_brokers_registered(&cluster, 3).await;

    let admin = Arc::new(
        Client::builder()
            .bootstrap(cluster[0].0.listen_addr().to_string())
            .client_id("backlog-rf3-admin")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&admin, 3, 3).await;
    for (broker, _, _) in &cluster {
        broker.wait_until_partition_present(TOPIC, 2).await;
    }
    let topic_id = cluster[0]
        .0
        .controller_image_for_test()
        .topic(TOPIC)
        .expect("topic metadata")
        .topic_id;
    let mut share_ready = false;
    for _ in 0..40 {
        let response = admin
            .send(FindCoordinatorRequest {
                key_type: 2,
                coordinator_keys: vec![format!("backlog-rf3-bootstrap:{topic_id}:0")],
                ..Default::default()
            })
            .await
            .unwrap();
        if response.coordinators[0].error_code == 0 {
            share_ready = true;
            break;
        }
        assert!(response.coordinators[0].error_code == 15, "{response:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(share_ready, "share coordinator did not become ready");
    for (broker, _, _) in &cluster {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, 0)
            .await;
    }

    let image = cluster[0].0.controller_image_for_test();
    let offsets_partitions = image.topic_partition_count(OFFSETS_TOPIC);
    let state_leader = image
        .partition(SHARE_STATE_TOPIC, 0)
        .expect("share-state partition 0")
        .leader;
    let controller_leader = cluster[0].0.controller_leader_id();
    let nodes = [
        krabka_broker::NodeId(1),
        krabka_broker::NodeId(2),
        krabka_broker::NodeId(3),
    ];
    let mut candidates = Vec::new();
    for offsets_partition in 0..offsets_partitions {
        let coordinator_id = image
            .partition(OFFSETS_TOPIC, offsets_partition)
            .expect("offsets partition")
            .leader;
        for data_partition in 0..3 {
            let data_leader_id = image
                .partition(TOPIC, data_partition)
                .expect("data partition")
                .leader;
            if coordinator_id == data_leader_id {
                continue;
            }
            let stopped_id = nodes
                .into_iter()
                .find(|id| *id != coordinator_id && *id != data_leader_id)
                .expect("third broker");
            if stopped_id != state_leader {
                candidates.push((
                    offsets_partition,
                    coordinator_id,
                    data_partition,
                    data_leader_id,
                    stopped_id,
                ));
            }
        }
    }
    let candidate = candidates
        .iter()
        .find(|candidate| Some(candidate.4) != controller_leader)
        .or_else(|| candidates.first())
        .copied()
        .expect("remote data leader with a live share-state leader");
    let (offsets_partition, coordinator_id, data_partition, data_leader_id, stopped_id) = candidate;
    let leader_epoch = image
        .partition(TOPIC, data_partition)
        .expect("data partition metadata")
        .leader_epoch;
    drop(image);

    let coordinator_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == coordinator_id)
        .unwrap();
    let group_id = group_for_partition(offsets_partition, offsets_partitions);
    let coordinator_client = Arc::new(
        Client::builder()
            .bootstrap(cluster[coordinator_index].0.listen_addr().to_string())
            .client_id("backlog-rf3-coordinator")
            .build()
            .await
            .unwrap(),
    );
    let share_coordinator = coordinator_client
        .send(FindCoordinatorRequest {
            key_type: 2,
            coordinator_keys: vec![format!("{group_id}:{topic_id}:{data_partition}")],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(
        share_coordinator.coordinators[0].error_code == 0,
        "{share_coordinator:?}"
    );
    let joined = coordinator_client
        .send(ShareGroupHeartbeatRequest {
            group_id: group_id.clone(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![TOPIC.into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(joined.error_code == 0, "{joined:?}");
    let member_id = joined.member_id.expect("broker mints a share member id");
    let mut member_epoch = joined.member_epoch;
    let initialized = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let heartbeat = coordinator_client
                .send(ShareGroupHeartbeatRequest {
                    group_id: group_id.clone(),
                    member_id: member_id.clone(),
                    member_epoch,
                    subscribed_topic_names: Some(vec![TOPIC.into()]),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(heartbeat.error_code == 0, "{heartbeat:?}");
            member_epoch = heartbeat.member_epoch;
            let mut present = false;
            for (broker, _, _) in &cluster {
                present |= broker
                    .share_state_summary_for_test(&group_id, topic_id, data_partition)
                    .await
                    .is_some();
            }
            if present {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    assert!(initialized.is_ok(), "RF=3 share state did not initialize");

    let label = ShareGroupLabel {
        group_id: group_id.clone(),
        topic: TOPIC.into(),
        partition: data_partition,
    };
    cluster[coordinator_index]
        .0
        .wait_for_metrics("initial RF=3 backlog sample", |metrics| {
            metrics.share_group_backlog.get_or_create(&label).get() == 0
        })
        .await;

    let stopped_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == stopped_id)
        .unwrap();
    let (stopped, _, _) = cluster.remove(stopped_index);
    stopped.shutdown().await;

    let data_leader_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == data_leader_id)
        .unwrap();
    let coordinator_index = cluster
        .iter()
        .position(|(_, config, _)| config.node_id == coordinator_id)
        .unwrap();
    assert!(
        cluster[data_leader_index]
            .0
            .partition_isr_for_test(TOPIC, data_partition)
            .is_some_and(|isr| isr.len() == 3),
        "stopped follower must remain in the ISR during the probe"
    );
    let last = cluster[data_leader_index]
        .0
        .produce_records_for_test(TOPIC, data_partition, 5)
        .await
        .unwrap();
    assert!(last == 4);
    assert!(
        cluster[data_leader_index]
            .0
            .local_log_end_offset(TOPIC, data_partition)
            == Some(5)
    );

    let data_client = Client::builder()
        .bootstrap(cluster[data_leader_index].0.listen_addr().to_string())
        .client_id("backlog-rf3-data")
        .build()
        .await
        .unwrap();
    let fetched: FetchResponse = data_client
        .send(FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 0,
            topics: vec![FetchTopic {
                topic: TOPIC.into(),
                topic_id: WireUuid(*topic_id.as_bytes()),
                partitions: vec![FetchPartition {
                    partition: data_partition,
                    current_leader_epoch: leader_epoch.0,
                    fetch_offset: i64::MAX,
                    partition_max_bytes: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    let partition = &fetched.responses[0].partitions[0];
    assert!(partition.error_code == 0, "{fetched:?}");
    assert!(partition.high_watermark == 0, "{fetched:?}");

    // A sentinel makes the poll completion observable: the next real sample
    // must replace it with HWM(0) - SPSO/log-start(0), not the leader LEO(5).
    cluster[coordinator_index]
        .0
        .metrics()
        .share_group_backlog
        .get_or_create(&label)
        .set(99);
    cluster[coordinator_index]
        .0
        .wait_for_metrics("remote RF=3 committed-HWM sample", |metrics| {
            metrics.share_group_backlog.get_or_create(&label).get() == 0
        })
        .await;
    assert!(
        cluster[data_leader_index]
            .0
            .partition_isr_for_test(TOPIC, data_partition)
            .is_some_and(|isr| isr.len() == 3),
        "the committed-HWM sample must land before ISR shrink can expose LEO"
    );

    let expected = format!(
        "krabka_broker_share_group_backlog{{group_id=\"{group_id}\",topic=\"{TOPIC}\",partition=\"{data_partition}\"}} 0"
    );
    assert!(
        scrape(cluster[coordinator_index].0.metrics_addr().unwrap())
            .await
            .contains(&expected)
    );
    for (index, (broker, _, _)) in cluster.iter().enumerate() {
        if index != coordinator_index {
            assert!(
                !scrape(broker.metrics_addr().unwrap())
                    .await
                    .contains(&format!("group_id=\"{group_id}\"")),
                "non-coordinator emitted a backlog series"
            );
        }
    }

    for (broker, _, _) in cluster {
        broker.shutdown().await;
    }
}
