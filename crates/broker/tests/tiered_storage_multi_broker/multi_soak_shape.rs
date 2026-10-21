//! The sustained-load contract's tiered cluster, in process: every broker's
//! topic-backed RLMM must catch up on every metadata partition it consumes.
//!
//! #1448 found the RLMM on one broker of that cluster stuck `NotReady` on the
//! metadata partitions that held events, for the whole run. The contract
//! configures `__remote_log_metadata` with three partitions and replication 3,
//! and leaves each broker's RLMM client bootstrapping against the broker's own
//! listener, so all three start racing the topic's creation.

use {
    crate::{
        multi_cluster::{TieredCluster, ready_admin, start_tiered_cluster_with},
        support::topics::{creatable_topic, create_topic_request},
        topic_fixture::{await_tiered_replicas, tiered_configs},
    },
    assert2::assert,
    krabka_broker::{KafkaRlmmConfig, RlmmKind},
    krabka_client_producer::{Compression, Producer},
    std::time::{Duration, Instant},
};

const TOPIC: &str = "tiered-soak-shape";
const PARTITIONS: i32 = 3;
const RECORDS_PER_PARTITION: usize = 60;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_rlmm_catches_up_on_the_soak_contracts_cluster() {
    let TieredCluster {
        brokers,
        log_dirs: _log_dirs,
        remote_dir: _remote_dir,
        ..
    } = start_tiered_cluster_with([krabka_units::kibibytes(1); 3], |_, config| {
        config.remote_log_metadata = RlmmKind::TopicBacked(KafkaRlmmConfig {
            bootstrap: config.advertised_listener.clone(),
            num_partitions: 3,
            replication: 3,
            min_isr: 2,
            snapshot_interval: krabka_units::hours(1),
            snapshot_dir: std::path::PathBuf::new(),
            security: None,
            ..KafkaRlmmConfig::default()
        });
    })
    .await;
    let [b1, b2, b3] = &brokers;
    let admin = ready_admin([b1, b2, b3], "soak-shape-admin").await;
    let response = admin
        .send(create_topic_request(
            krabka_protocol::owned::create_topics_request::CreatableTopic {
                configs: tiered_configs(Some("1024")),
                ..creatable_topic()
                    .name(TOPIC.into())
                    .partitions(PARTITIONS)
                    .replicas(3)
                    .call()
            },
        ))
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == 0, "{response:?}");
    await_tiered_replicas(
        &[b1, b2, b3],
        TOPIC,
        Some(krabka_units::kibibytes(1)),
        "tiered config did not propagate",
    )
    .await;

    let bootstrap = format!("127.0.0.1:{}", b1.listen_addr().port());
    let producer = Producer::builder()
        .bootstrap(bootstrap)
        .compression(Compression::Lz4)
        .build()
        .await
        .expect("producer");
    for index in 0..RECORDS_PER_PARTITION {
        for partition in 0..PARTITIONS {
            let mut value = format!("record-{partition}-{index}-").into_bytes();
            value.resize(200, b'x');
            producer
                .send(krabka_client_producer::ProducerRecord {
                    topic: TOPIC.to_owned(),
                    partition: Some(partition),
                    value: Some(bytes::Bytes::from(value)),
                    ..Default::default()
                })
                .await
                .expect("acknowledged");
        }
    }
    producer.close().await.expect("producer close");

    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let mut states = Vec::new();
        for broker in &brokers {
            for partition in 0..PARTITIONS {
                let listed = broker
                    .remote_log_segments_for_test(TOPIC, partition)
                    .await
                    .expect("a tiered broker");
                states.push((broker.node_id(), partition, listed.map(|s| s.len())));
            }
        }
        let ready = states
            .iter()
            .all(|(_, _, listed)| listed.as_ref().is_ok_and(|count| *count > 0));
        if ready {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "an RLMM never caught up: {:?}",
            states
                .iter()
                .map(|(node, partition, listed)| format!(
                    "broker {node} p{partition}: {}",
                    match listed {
                        Ok(count) => format!("{count} segments"),
                        Err(error) => error.to_string(),
                    }
                ))
                .collect::<Vec<_>>()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    drop(admin);
    for broker in brokers {
        broker.shutdown().await;
    }
}
