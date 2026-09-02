use std::sync::Arc;

use assert2::{assert, check};
use tempfile::tempdir;

use super::*;
use crate::{
    broker::{
        Broker,
        test_support::{local_partition_with_records, submit_metadata_topic_partition},
    },
    config::BrokerConfig,
    partition::WriterMessage,
};

async fn assert_listener_stops_accepting(addr: SocketAddr) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(stream) => {
                drop(stream);
                assert!(
                    std::time::Instant::now() < deadline,
                    "listener at {addr} still accepts connections after shutdown"
                );
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(_) => return,
        }
    }
}

async fn wait_for_connection_count(broker: &Broker, expected: usize, message: &'static str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        if broker.connections.total() == expected {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "{message}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn single_broker_handle_helpers_observe_real_state_and_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    let handle = Broker::start(config).await.expect("broker start");
    let broker = handle.broker_arc_for_test();

    check!(broker.handlers().get(18).is_some());
    check!(handle.metrics_addr().is_some_and(|addr| addr.port() != 0));
    check!(handle.offset_for_leader_epoch_count_for_test() == 0);
    broker
        .offset_for_leader_epoch_requests
        .store(2, std::sync::atomic::Ordering::Release);
    assert!(handle.offset_for_leader_epoch_count_for_test() == 2);
    assert!(!handle.rlmm_topic_backed_active_for_test());
    broker.metrics.tiered_storage_rlmm_topic_backed.set(1);
    assert!(handle.rlmm_topic_backed_active_for_test());
    broker.metrics.tiered_storage_rlmm_topic_backed.set(2);
    check!(!handle.rlmm_topic_backed_active_for_test());
    check!(handle.reload_tls().is_err());
    check!(!handle.has_partition("missing-mutant-topic", 0));
    check!(handle.local_log_end_offset("missing-mutant-topic", 0) == None);
    check!(
        handle
            .test_advance_log_start("missing-mutant-topic", 0, 10)
            .await
            .is_err()
    );
    check!(
        handle
            .change_membership([krabka_raft::NodeId(1)].into_iter().collect())
            .await
            .is_ok()
    );

    let leader = handle.wait_until_controller_leader().await;
    assert!(leader == krabka_raft::NodeId(handle.node_id()));
    assert!(handle.controller_leader_id() == Some(krabka_raft::NodeId(handle.node_id())));

    let mut endpoints = handle.self_registration_endpoints();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while endpoints.is_empty() && std::time::Instant::now() < deadline {
        tokio::task::yield_now().await;
        endpoints = handle.self_registration_endpoints();
    }
    assert!(!endpoints.is_empty());

    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                node_id: krabka_raft::NodeId(handle.node_id() + 1),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::from_u128(0xBEEF),
                host: "127.0.0.1".to_string(),
                port: 19_092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![krabka_metadata::BrokerEndpoint {
                    name: "PLAINTEXT".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 19_092,
                    protocol: krabka_security::ListenerProtocol::Plaintext,
                }],
                features: std::collections::BTreeMap::new(),
            },
        ))
        .await
        .expect("submit peer broker registration");
    assert!(
        handle
            .controller_image_for_test()
            .broker(krabka_raft::NodeId(handle.node_id() + 1))
            .is_some()
    );
    handle.wait_until_brokers_registered(2).await;
    assert!(handle.broker_count() == 2);

    let topic = "handle-mutant-topic";
    let partition_leader = handle.node_id() + 1;
    let partition_isr = [partition_leader, handle.node_id()];
    submit_metadata_topic_partition(
        &handle,
        (topic, 0xCAFE),
        0,
        partition_leader,
        &partition_isr,
        &partition_isr,
        3,
    )
    .await;
    handle.wait_until_partition_present(topic, 0).await;
    check!(handle.has_partition(topic, 0));
    check!(handle.partition_leader_for_test(topic, 0) == Some(partition_leader));
    check!(handle.partition_isr_for_test(topic, 0) == Some(partition_isr.to_vec()));
    let observed_partition = handle
        .partition_record_for_test(topic, 0)
        .expect("partition record");
    let expected_partition = krabka_metadata::PartitionRecord {
        topic: topic.to_string(),
        partition: 0,
        leader: krabka_audit::NodeId(partition_leader),
        replicas: partition_isr
            .iter()
            .copied()
            .map(krabka_audit::NodeId)
            .collect(),
        isr: partition_isr
            .iter()
            .copied()
            .map(krabka_audit::NodeId)
            .collect(),
        leader_epoch: krabka_metadata::LeaderEpoch(3),
        adding_replicas: Vec::new(),
        removing_replicas: Vec::new(),
        directories: vec![uuid::Uuid::nil(); partition_isr.len()],
        partition_epoch: 0,
    };
    assert!(observed_partition == expected_partition);
    check!(handle.partition_leader_for_test("missing-mutant-topic", 0) == None);
    check!(handle.partition_isr_for_test("missing-mutant-topic", 0) == None);
    check!(handle.partition_record_for_test("missing-mutant-topic", 0) == None);
    check!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            handle.wait_until_partition_leader_changed(
                topic,
                0,
                krabka_raft::NodeId(handle.node_id())
            ),
        )
        .await
        .is_ok()
    );

    assert!(
        matches!(
            broker.controller.read_snapshot_range(0, 1),
            krabka_raft::SnapshotRange::NoSnapshot
        ),
        "test should start without a metadata snapshot"
    );
    handle
        .trigger_snapshot_for_test()
        .await
        .expect("trigger metadata snapshot");
    let krabka_raft::SnapshotRange::Slice(snapshot) = broker.controller.read_snapshot_range(0, 1)
    else {
        panic!("trigger_snapshot_for_test should write a readable snapshot");
    };
    assert!(snapshot.total_size > 0);
    assert!(!snapshot.bytes.is_empty());

    let local_topic = "handle-local-log-mutant-topic";
    let local_part = local_partition_with_records(dir.path(), local_topic, 0, &[b"a", b"b"]);
    assert!(!handle.partition_exists_for_test(local_topic, 0));
    broker.partitions.insert(
        local_topic.into(),
        PartitionIndex(0),
        Arc::clone(&local_part),
    );
    assert!(handle.partition_exists_for_test(local_topic, 0));
    assert!(handle.local_log_end_offset(local_topic, 0) == Some(2));
    handle.test_set_leader_epoch(local_topic, 0, 7);
    assert!(
        local_part
            .current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            == 7
    );
    handle
        .test_truncate_local_log(local_topic, 0, 1)
        .await
        .expect("truncate local partition");
    assert!(handle.local_log_end_offset(local_topic, 0) == Some(0));

    handle.shutdown().await;
}

#[tokio::test]
async fn start_and_shutdown_clean() {
    let dir = tempdir().unwrap();
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(config).await.unwrap();
    let broker = handle.broker_arc_for_test();
    let addr = handle.listen_addr();
    let partition = local_partition_with_records(dir.path(), "shutdown", 0, &[]);
    broker
        .partitions
        .insert("shutdown".into(), PartitionIndex(0), partition.clone());
    assert!(addr.port() != 0);
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("listener accepts before shutdown");
    wait_for_connection_count(&broker, 1, "accept_loop did not register live connection").await;
    tokio::time::timeout(std::time::Duration::from_secs(5), handle.shutdown())
        .await
        .expect("broker shutdown completes");
    assert!(broker.connections.total() == 0);
    assert!(partition.take_writer_handle().is_none());
    let (ack, _ack_rx) = tokio::sync::oneshot::channel();
    assert!(
        partition
            .writer_tx
            .send(WriterMessage::Compact { ack })
            .await
            .is_err()
    );
    drop(stream);
    assert_listener_stops_accepting(addr).await;
}

#[tokio::test]
async fn dropping_handle_stops_idle_connections_and_partition_writers() {
    let dir = tempdir().unwrap();
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(config).await.unwrap();
    let broker = handle.broker_arc_for_test();
    let addr = handle.listen_addr();
    let partition = local_partition_with_records(dir.path(), "drop-shutdown", 0, &[]);
    broker
        .partitions
        .insert("drop-shutdown".into(), PartitionIndex(0), partition.clone());
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("listener accepts before handle drop");
    wait_for_connection_count(&broker, 1, "accept_loop did not register live connection").await;

    drop(handle);

    wait_for_connection_count(
        &broker,
        0,
        "dropping BrokerHandle did not stop the idle connection",
    )
    .await;
    assert!(partition.take_writer_handle().is_none());
    assert_listener_stops_accepting(addr).await;
    drop(stream);
    broker.controller.cancel().await;
}

#[tokio::test]
async fn controlled_shutdown_timeout_stops_listener_and_reports_error() {
    let dir = tempdir().unwrap();
    let config = BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(config).await.unwrap();
    let addr = handle.listen_addr();
    let err = handle
        .controlled_shutdown(std::time::Duration::ZERO)
        .await
        .expect_err("zero-timeout controlled shutdown should report drain timeout");
    assert!(matches!(err, BrokerError::ShutdownTimeout(timeout) if timeout.is_zero()));
    assert_listener_stops_accepting(addr).await;
}
