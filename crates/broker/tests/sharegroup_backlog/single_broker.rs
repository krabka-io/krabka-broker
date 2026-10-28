//! The single-broker case: the gauge appears once a share group subscribes,
//! survives the last consumer leaving, drains when the group's offsets are
//! moved to the end of the log, and disappears with the topic.
//!
//! The broker runs with one PLAINTEXT listener and a metrics listener, so the
//! same assertion can be made twice over: against the in-process registry and
//! against the scraped exposition text.

use {
    crate::{
        harness::{TOPIC, create_topic, produce_five, scrape, test_lock},
        support::{client::connect_client, listeners},
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, metrics::ShareGroupLabel},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        alter_share_group_offsets_request::{
            AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
            AlterShareGroupOffsetsRequestTopic,
        },
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    krabka_security::ListenerProtocol,
    std::{mem, sync::Arc, time::Duration},
    tokio::sync::MutexGuard,
};

const GROUP: &str = "backlog-workers";

krabka_macros::delete_topic_request!(delete_request);

#[derive(Default)]
struct Backlog {
    guard: Option<MutexGuard<'static, ()>>,
    directory: Option<tempfile::TempDir>,
    broker: Option<BrokerHandle>,
    client: Option<Arc<Client>>,
    topic_id: Option<uuid::Uuid>,
    member_id: Option<String>,
}

#[rstest::fixture]
fn backlog() -> Backlog {
    // Metric registration and the durable cursor belong to this group alone.
    Backlog::default()
}

impl Backlog {
    fn broker(&self) -> &BrokerHandle { self.broker.as_ref().expect("backlog broker") }

    fn client(&self) -> &Arc<Client> { self.client.as_ref().expect("backlog client") }

    fn topic_id(&self) -> uuid::Uuid { self.topic_id.expect("backlog topic") }

    fn label() -> ShareGroupLabel {
        ShareGroupLabel {
            group_id: GROUP.into(),
            topic: TOPIC.into(),
            partition: 0,
        }
    }

    fn metric(value: i64) -> String {
        format!(
            "krabka_broker_share_group_backlog{{group_id=\"{GROUP}\",topic=\"{TOPIC}\",partition=\"0\"}} {value}"
        )
    }

    async fn shutdown(&mut self) {
        let Self {
            guard,
            directory,
            broker,
            client,
            ..
        } = mem::take(self);
        drop(client);
        broker.expect("backlog broker").shutdown().await;
        drop(directory);
        drop(guard);
    }
}

#[rstest_bdd_macros::given(
    "a one-partition RF1 topic contains five records and share backlog is sampled every 50 milliseconds"
)]
async fn backlog_topic(backlog: &mut Backlog) {
    backlog.guard = Some(test_lock().lock().await);
    backlog.directory = Some(tempfile::tempdir().unwrap());
    let mut config = BrokerConfig::for_tests(
        backlog
            .directory
            .as_ref()
            .expect("log directory")
            .path()
            .to_path_buf(),
    );
    config.listeners = vec![listeners::loopback_listener(
        "PLAINTEXT",
        ListenerProtocol::Plaintext,
    )];
    config.inter_broker_listener_name = "PLAINTEXT".into();
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    config.share_coordinator.state_topic_num_partitions = 1;
    config.share_group.backlog_poll_interval = Duration::from_millis(50);

    backlog.broker = Some(Broker::start(config).await.unwrap());
    // No broker creates `__consumer_offsets` or `__share_group_state` when it
    // starts. The share group needs both coordinators.
    backlog.broker().wait_until_group_coordinator_ready().await;
    backlog.broker().wait_until_share_coordinator_ready().await;
    backlog.client = Some(Arc::new(
        connect_client(
            backlog.broker().listen_addr().to_string(),
            Some("backlog-itest"),
        )
        .await,
    ));
    create_topic(backlog.client(), 1, 1).await;
    backlog
        .broker()
        .wait_until_partition_present(TOPIC, 0)
        .await;
    backlog.topic_id = Some(
        backlog
            .broker()
            .controller_image_for_test()
            .topic(TOPIC)
            .map(|topic| topic.topic_id)
            .expect("topic metadata"),
    );
    produce_five(backlog.client(), backlog.topic_id()).await;
}

#[rstest_bdd_macros::when("member-1 joins backlog-workers and initializes its share-state cursor")]
async fn join_backlog(backlog: &mut Backlog) {
    let join_req = ShareGroupHeartbeatRequest {
        group_id: GROUP.into(),
        member_id: "member-1".into(),
        member_epoch: 0,
        subscribed_topic_names: Some(vec![TOPIC.into()]),
        ..Default::default()
    };
    let joined = backlog.client().send(join_req).await.unwrap();
    assert!(joined.error_code == 0, "{joined:?}");
    backlog
        .broker()
        .wait_for_share_state_summary(GROUP, backlog.topic_id(), 0)
        .await;

    backlog.member_id = joined.member_id;
}

#[rstest_bdd_macros::then("the registry and Prometheus scrape report partition 0 backlog 5")]
async fn initial_backlog(backlog: &Backlog) {
    backlog
        .broker()
        .wait_for_metrics("share-group backlog = 5", |metrics| {
            metrics
                .share_group_backlog
                .get_or_create(&Backlog::label())
                .get()
                == 5
        })
        .await;

    assert!(
        scrape(backlog.broker().metrics_addr().expect("metrics listener"))
            .await
            .contains(&Backlog::metric(5))
    );
}

#[rstest_bdd_macros::when("the final group member leaves")]
async fn leave_backlog(backlog: &Backlog) {
    let left = backlog
        .client()
        .send(ShareGroupHeartbeatRequest {
            group_id: GROUP.into(),
            member_id: backlog.member_id.clone().expect("joined member"),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(left.error_code == 0, "{left:?}");
}

#[rstest_bdd_macros::then(
    "the durable share-state cursor and scraped backlog 5 survive scale to zero"
)]
async fn durable_empty_group_backlog(backlog: &Backlog) {
    assert!(
        backlog
            .broker()
            .share_state_summary_for_test(GROUP, backlog.topic_id(), 0)
            .await
            .is_some(),
        "the durable cursor must survive when the final consumer leaves"
    );
    assert!(
        scrape(backlog.broker().metrics_addr().expect("metrics listener"))
            .await
            .contains(&Backlog::metric(5))
    );
}

#[rstest_bdd_macros::when("AlterShareGroupOffsets advances partition 0 start offset to 5")]
async fn advance_backlog(backlog: &Backlog) {
    let alter_part = AlterShareGroupOffsetsRequestPartition {
        partition_index: 0,
        start_offset: 5,
        ..Default::default()
    };
    let alter_topic = AlterShareGroupOffsetsRequestTopic {
        topic_name: TOPIC.into(),
        partitions: vec![alter_part],
        ..Default::default()
    };
    let alter_req = AlterShareGroupOffsetsRequest {
        group_id: GROUP.into(),
        topics: vec![alter_topic],
        ..Default::default()
    };
    let altered = backlog.client().send(alter_req).await.unwrap();
    assert!(altered.error_code == 0, "{altered:?}");
    assert!(
        altered.responses[0].partitions[0].error_code == 0,
        "{altered:?}"
    );
}

#[rstest_bdd_macros::then("the registry and Prometheus scrape report partition 0 backlog 0")]
async fn drained_backlog(backlog: &Backlog) {
    backlog
        .broker()
        .wait_for_metrics("share-group backlog drains to zero", |metrics| {
            metrics
                .share_group_backlog
                .get_or_create(&Backlog::label())
                .get()
                == 0
        })
        .await;
    let drained = format!(
        "krabka_broker_share_group_backlog{{group_id=\"{GROUP}\",topic=\"{TOPIC}\",partition=\"0\"}} 0"
    );
    assert!(
        scrape(backlog.broker().metrics_addr().expect("metrics listener"))
            .await
            .contains(&drained)
    );
}

#[rstest_bdd_macros::when("the topic is deleted")]
async fn delete_backlog(backlog: &Backlog) {
    let deleted = backlog.client().send(delete_request(TOPIC)).await.unwrap();
    assert!(deleted.responses[0].error_code == 0, "{deleted:?}");
}

#[rstest_bdd_macros::then("the deleted topic and its backlog metric series disappear")]
async fn deleted_backlog(backlog: &Backlog) {
    backlog
        .broker()
        .wait_for_image(|image| image.topic(TOPIC).is_none())
        .await;
    backlog
        .broker()
        .wait_for_metrics("deleted topic backlog series is removed", |metrics| {
            metrics.share_group_backlog.get(&Backlog::label()).is_none()
        })
        .await;
    assert!(
        !scrape(backlog.broker().metrics_addr().expect("metrics listener"))
            .await
            .contains(&format!(
                "krabka_broker_share_group_backlog{{group_id=\"{GROUP}\",topic=\"{TOPIC}\""
            ))
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/sharegroup_backlog/single_broker.feature",
    name = "backlog is scraped and survives scale to zero"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backlog_is_scraped_and_survives_scale_to_zero(backlog: Backlog) {
    backlog.shutdown().await;
    drop(backlog);
}
