//! The single-broker case: the gauge appears once a share group subscribes,
//! survives the last consumer leaving, drains when the group's offsets are
//! moved to the end of the log, and disappears with the topic.
//!
//! The broker runs with one PLAINTEXT listener and a metrics listener, so the
//! same assertion can be made twice over: against the in-process registry and
//! against the scraped exposition text.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, config::ListenerSpec, metrics::ShareGroupLabel};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    alter_share_group_offsets_request::{
        AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
        AlterShareGroupOffsetsRequestTopic,
    },
    delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
    share_group_heartbeat_request::ShareGroupHeartbeatRequest,
};
use krabka_security::ListenerProtocol;

use crate::harness::{TOPIC, create_topic, produce_five, scrape, test_lock};

const GROUP: &str = "backlog-workers";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_is_scraped_and_survives_scale_to_zero() {
    let _guard = test_lock().lock().await;
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
    config.listeners = vec![ListenerSpec {
        name: "PLAINTEXT".into(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".into(),
        protocol: ListenerProtocol::Plaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    config.inter_broker_listener_name = "PLAINTEXT".into();
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    config.share_coordinator.state_topic_num_partitions = 1;
    config.share_group.backlog_poll_interval = Duration::from_millis(50);

    let broker = Broker::start(config).await.unwrap();
    let client = Arc::new(
        Client::builder()
            .bootstrap(broker.listen_addr().to_string())
            .client_id("backlog-itest")
            .build()
            .await
            .unwrap(),
    );
    create_topic(&client, 1, 1).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    let topic_id = broker
        .controller_image_for_test()
        .topic(TOPIC)
        .map(|topic| topic.topic_id)
        .expect("topic metadata");
    produce_five(&client, topic_id).await;

    let joined = client
        .send(ShareGroupHeartbeatRequest {
            group_id: GROUP.into(),
            member_id: "member-1".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![TOPIC.into()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(joined.error_code == 0, "{joined:?}");
    broker
        .wait_for_share_state_summary(GROUP, topic_id, 0)
        .await;

    let label = ShareGroupLabel {
        group_id: GROUP.into(),
        topic: TOPIC.into(),
        partition: 0,
    };
    broker
        .wait_for_metrics("share-group backlog = 5", |metrics| {
            metrics.share_group_backlog.get_or_create(&label).get() == 5
        })
        .await;

    let metrics_addr = broker.metrics_addr().unwrap();
    let expected = format!(
        "krabka_broker_share_group_backlog{{group_id=\"{GROUP}\",topic=\"{TOPIC}\",partition=\"0\"}} 5"
    );
    assert!(scrape(metrics_addr).await.contains(&expected));

    let left = client
        .send(ShareGroupHeartbeatRequest {
            group_id: GROUP.into(),
            member_id: joined.member_id.unwrap(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(left.error_code == 0, "{left:?}");
    assert!(
        broker
            .share_state_summary_for_test(GROUP, topic_id, 0)
            .await
            .is_some(),
        "the durable cursor must survive when the final consumer leaves"
    );
    assert!(scrape(metrics_addr).await.contains(&expected));

    let altered = client
        .send(AlterShareGroupOffsetsRequest {
            group_id: GROUP.into(),
            topics: vec![AlterShareGroupOffsetsRequestTopic {
                topic_name: TOPIC.into(),
                partitions: vec![AlterShareGroupOffsetsRequestPartition {
                    partition_index: 0,
                    start_offset: 5,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(altered.error_code == 0, "{altered:?}");
    assert!(
        altered.responses[0].partitions[0].error_code == 0,
        "{altered:?}"
    );
    broker
        .wait_for_metrics("share-group backlog drains to zero", |metrics| {
            metrics.share_group_backlog.get_or_create(&label).get() == 0
        })
        .await;
    let drained = format!(
        "krabka_broker_share_group_backlog{{group_id=\"{GROUP}\",topic=\"{TOPIC}\",partition=\"0\"}} 0"
    );
    assert!(scrape(metrics_addr).await.contains(&drained));

    let deleted = client
        .send(DeleteTopicsRequest {
            topics: vec![DeleteTopicState {
                name: Some(TOPIC.into()),
                ..Default::default()
            }],
            topic_names: vec![TOPIC.into()],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(deleted.responses[0].error_code == 0, "{deleted:?}");
    broker
        .wait_for_image(|image| image.topic(TOPIC).is_none())
        .await;
    broker
        .wait_for_metrics("deleted topic backlog series is removed", |metrics| {
            metrics.share_group_backlog.get(&label).is_none()
        })
        .await;
    assert!(!scrape(metrics_addr).await.contains(&format!(
        "krabka_broker_share_group_backlog{{group_id=\"{GROUP}\",topic=\"{TOPIC}\""
    )));

    broker.shutdown().await;
}
