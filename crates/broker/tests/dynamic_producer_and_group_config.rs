//! Dynamic `producer.id.expiration.ms` and `group.consumer.assignment.interval.ms`
//! that a running broker applies without a restart.
//!
//! Kafka 4.3.1's `DynamicProducerStateManagerConfig` reconfigures the
//! `ProducerStateManagerConfig` every log shares, so the next
//! `removeExpiredProducers` sweep uses the new window. Its
//! `GroupCoordinatorConfig.RECONFIGURABLE_CONFIGS` makes the three
//! `group.*.assignment.interval.ms` keys dynamic, and `GroupMetadataManager`
//! reads the current value for the next target assignment. Each case alters
//! the `BROKER` resource with `IncrementalAlterConfigs` and observes the
//! effect on the wire.
#![cfg(test)]

mod support;

use {
    crate::support::{
        admin::describe_producers_request,
        client::{connect_client, create_topic_named},
        configs::{BROKER_RESOURCE, ConfigMutation, ConfigOperation, broker_alter_request},
        consumer_groups::consumer_heartbeat,
        produce::{SinglePartitionProduceSetup, send_batch},
        records::{now_ms, producer_values_batch},
        topic_id_for,
        topics::CreateTopicSetup,
        transactions::claim_idempotent_producer,
    },
    assert2::{assert, check},
    krabka_broker::{BrokerConfig, BrokerHandle, codes},
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            common::consumer_group_heartbeat_response::topic_partitions::TopicPartitions,
            consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
            consumer_group_heartbeat_response::{Assignment, ConsumerGroupHeartbeatResponse},
            incremental_alter_configs_response::AlterConfigsResourceResponse,
        },
        primitives::uuid::Uuid,
        records::RecordBatch,
    },
    std::time::{Duration, Instant},
    tokio::time,
};

/// How long a change may take to show on the wire.
const PROPAGATION: Duration = Duration::from_secs(10);

/// Which `BROKER` resource an alter names.
#[derive(Clone, Copy, Debug)]
enum Scope {
    /// The cluster-wide default, the empty resource name.
    ClusterDefault,
    /// The serving broker, by its node id.
    ThisBroker,
}

impl Scope {
    fn resource_name(self, broker: &BrokerHandle) -> String {
        match self {
            Self::ClusterDefault => String::new(),
            Self::ThisBroker => broker.node_id().to_string(),
        }
    }

    /// The value `broker`'s metadata image holds for `key` at this scope.
    fn image_value(
        self,
        broker: &BrokerHandle,
        image: &krabka_metadata::MetadataImage,
        key: &str,
    ) -> Option<String> {
        let configs = match self {
            Self::ClusterDefault => image.default_broker_config(),
            Self::ThisBroker => image.broker_config(krabka_metadata::NodeId(broker.node_id())),
        };
        configs.and_then(|configs| configs.get(key)).cloned()
    }
}

/// Alter `key` of the `scope` resource through `client`, and wait until
/// `broker`'s metadata image holds the new value.
#[bon::builder]
async fn alter_broker(
    client: &Client,
    broker: &BrokerHandle,
    scope: Scope,
    key: &str,
    value: Option<&str>,
) -> AlterConfigsResourceResponse {
    let operation = if value.is_some() {
        ConfigOperation::Set
    } else {
        ConfigOperation::Delete
    };
    let response = client
        .send(broker_alter_request(
            scope.resource_name(broker),
            vec![(key, operation, value)],
            ConfigMutation::Apply,
        ))
        .await
        .expect("IncrementalAlterConfigs");
    let row = response
        .responses
        .into_iter()
        .next()
        .expect("one resource row");
    if row.error_code == codes::NONE {
        let want = value.map(ToOwned::to_owned);
        broker
            .wait_for_image(|image| scope.image_value(broker, image, key) == want)
            .await;
    }
    row
}

/// The response row of an alter that succeeded on `scope`.
fn accepted(broker: &BrokerHandle, scope: Scope) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        error_code: codes::NONE,
        error_message: None,
        resource_type: BROKER_RESOURCE,
        resource_name: scope.resource_name(broker),
        ..Default::default()
    }
}

/// One idempotent batch of `producer_id` stamped with the current time, so a
/// window of a day does not expire it.
fn current_batch(producer_id: i64, producer_epoch: i16) -> RecordBatch {
    let now = now_ms();
    RecordBatch {
        base_timestamp: now,
        max_timestamp: now,
        ..producer_values_batch()
            .pid(producer_id)
            .epoch(producer_epoch)
            .call()
    }
}

/// The ids of the producers `DescribeProducers` reports for partition 0 of
/// `topic`.
async fn active_producer_ids(client: &Client, topic: &str) -> Vec<i64> {
    let response = client
        .send(describe_producers_request(topic.to_owned(), vec![0]))
        .await
        .expect("DescribeProducers");
    let partition = &response.topics[0].partitions[0];
    assert!(partition.error_code == 0, "{partition:?}");
    partition
        .active_producers
        .iter()
        .map(|state| state.producer_id)
        .collect()
}

/// A consumer heartbeat at `epoch` of the member `member_id`, subscribed to
/// `topics`.
fn subscribed(member_id: &str, epoch: i32, topics: &[&str]) -> ConsumerGroupHeartbeatRequest {
    ConsumerGroupHeartbeatRequest {
        rebalance_timeout_ms: 60_000,
        topic_partitions: (epoch == 0).then(Vec::new),
        subscribed_topic_names: Some(topics.iter().map(|&topic| topic.to_owned()).collect()),
        ..consumer_heartbeat("g-interval", member_id, epoch)
    }
}

/// Create `topic` with one partition, and return its id.
async fn one_partition_topic(client: &Client, topic: &str) -> Uuid {
    create_topic_named(client, CreateTopicSetup {
        topic,
        ..Default::default()
    })
    .await;
    topic_id_for(client, topic).await
}

/// The heartbeat answer that gives `member_id` `assignment` at `epoch`.
fn assigned(
    member_id: &str,
    epoch: i32,
    assignment: Option<Vec<TopicPartitions>>,
) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        member_id: Some(member_id.to_owned()),
        member_epoch: epoch,
        heartbeat_interval_ms: 5_000,
        assignment: assignment.map(|topic_partitions| Assignment {
            topic_partitions,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// `response` with its assigned topics in topic-id order, which the broker
/// does not promise.
fn in_topic_order(mut response: ConsumerGroupHeartbeatResponse) -> ConsumerGroupHeartbeatResponse {
    if let Some(assignment) = response.assignment.as_mut() {
        assignment
            .topic_partitions
            .sort_by_key(|topic| topic.topic_id.0);
    }
    response
}

/// Partition 0 of `topic_id`, as an assignment carries it.
fn partition_zero(topic_id: Uuid) -> TopicPartitions {
    TopicPartitions {
        topic_id,
        partitions: vec![0],
        ..Default::default()
    }
}

struct DynamicRuntime {
    broker: Option<support::InProcess>,
    client: Option<Client>,
    scope: Option<Scope>,
    altered: Option<AlterConfigsResourceResponse>,
    topic: String,
    producer: Option<i64>,
    producer_ids: Option<Vec<i64>>,
    first_topic: Option<Uuid>,
    second_topic: Option<Uuid>,
    member: String,
    heartbeat: Option<ConsumerGroupHeartbeatResponse>,
    joined_at: Option<Instant>,
    elapsed: Option<Duration>,
}
impl DynamicRuntime {
    async fn alter_key(&mut self, key: &str, scope: &str, value: Option<&str>) {
        let scope = match scope {
            "default" => Scope::ClusterDefault,
            "this-broker" => Scope::ThisBroker,
            other => panic!("unknown broker resource scope {other}"),
        };
        let broker = &self.broker.as_ref().expect("runtime broker started").broker;
        self.altered = Some(
            alter_broker()
                .client(self.client())
                .broker(broker)
                .scope(scope)
                .key(key)
                .maybe_value(value)
                .call()
                .await,
        );
        self.scope = Some(scope);
    }

    fn client(&self) -> &Client {
        self.client
            .as_ref()
            .unwrap_or_else(|| &self.broker.as_ref().expect("runtime broker started").client)
    }

    async fn shutdown(self) {
        drop(self.client);
        self.broker
            .expect("runtime broker started")
            .broker
            .shutdown()
            .await;
    }
}
#[rstest::fixture]
fn runtime_config() -> DynamicRuntime {
    // Start this scenario with fresh inputs and observations.

    DynamicRuntime {
        broker: None,
        client: None,
        scope: None,
        altered: None,
        topic: String::new(),
        producer: None,
        producer_ids: None,
        first_topic: None,
        second_topic: None,
        member: String::new(),
        heartbeat: None,
        joined_at: None,
        elapsed: None,
    }
}
#[rstest_bdd_macros::given("a broker sweeps expired producers every 50 milliseconds")]
async fn runtime_expiry_sweep(runtime_config: &mut DynamicRuntime) {
    runtime_config.broker = Some(
        support::start_configured(|config: &mut BrokerConfig| {
            config.producer_id_expiration_scan_interval = krabka_units::millis(50);
        })
        .await,
    );
}
#[rstest_bdd_macros::given("topic {topic} contains a current timestamped idempotent batch")]
async fn runtime_idle_producer(runtime_config: &mut DynamicRuntime, topic: String) {
    let p = runtime_config
        .broker
        .as_ref()
        .expect("runtime broker started");
    create_topic_named(&p.client, CreateTopicSetup {
        topic: &topic,
        ..Default::default()
    })
    .await;
    let topic_id = topic_id_for(&p.client, &topic).await;
    let producer = claim_idempotent_producer(&p.client).await;
    let appended = send_batch(
        &p.client,
        current_batch(producer.producer_id, producer.producer_epoch),
        SinglePartitionProduceSetup {
            topic: topic.clone(),
            topic_id,
            ..SinglePartitionProduceSetup::replicated()
        },
    )
    .await;
    assert!(appended.responses[0].partition_responses[0].error_code == 0);
    runtime_config.topic = topic;
    runtime_config.producer = Some(producer.producer_id);
}
#[rstest_bdd_macros::when(
    "DescribeProducers is read after 300 milliseconds of static-window sweeps"
)]
async fn runtime_retained_producer(runtime_config: &mut DynamicRuntime) {
    time::sleep(Duration::from_millis(300)).await;
    runtime_config.producer_ids =
        Some(active_producer_ids(runtime_config.client(), &runtime_config.topic).await);
}
#[rstest_bdd_macros::then("the only active producer is the one that appended the batch")]
fn runtime_producer_retained(runtime_config: &DynamicRuntime) {
    check!(
        runtime_config
            .producer_ids
            .as_ref()
            .expect("producer snapshot read")
            .as_slice()
            == [runtime_config.producer.expect("producer initialized")]
    );
}
#[rstest_bdd_macros::when("runtime config {key} on {scope} is set to {value}")]
async fn runtime_config_changed(
    runtime_config: &mut DynamicRuntime,
    key: String,
    scope: String,
    value: String,
) {
    runtime_config.alter_key(&key, &scope, Some(&value)).await;
}
#[rstest_bdd_macros::when("runtime config {key} on {scope} is deleted")]
async fn runtime_config_deleted(runtime_config: &mut DynamicRuntime, key: String, scope: String) {
    runtime_config.alter_key(&key, &scope, None).await;
}
#[rstest_bdd_macros::then("the full runtime config alteration response is successful")]
fn runtime_config_accepted(runtime_config: &DynamicRuntime) {
    let broker = &runtime_config
        .broker
        .as_ref()
        .expect("runtime broker started")
        .broker;
    assert!(
        runtime_config
            .altered
            .as_ref()
            .expect("runtime config altered")
            == &accepted(broker, runtime_config.scope.expect("resource selected"))
    );
}
#[rstest_bdd_macros::when("DescribeProducers is polled until the shortened expiry window applies")]
async fn runtime_expiry_applied(runtime_config: &mut DynamicRuntime) {
    let deadline = Instant::now() + PROPAGATION;
    loop {
        let remaining = active_producer_ids(runtime_config.client(), &runtime_config.topic).await;
        let finished = remaining.is_empty() || Instant::now() >= deadline;
        runtime_config.producer_ids = Some(remaining);
        if finished {
            break;
        }
        time::sleep(Duration::from_millis(50)).await;
    }
}
#[rstest_bdd_macros::then("the active producer list is empty")]
fn runtime_producer_expired(runtime_config: &DynamicRuntime) {
    check!(
        runtime_config
            .producer_ids
            .as_ref()
            .expect("producer snapshot read")
            .is_empty()
    );
}
#[rstest_bdd_macros::given(
    "a ready consumer group broker has interval-first and interval-second single partition topics"
)]
async fn runtime_assignment_topics(runtime_config: &mut DynamicRuntime) {
    let p = support::start_ready_group().await;
    let client = connect_client(p.broker.listen_addr().to_string(), Some("c-interval")).await;
    runtime_config.first_topic = Some(one_partition_topic(&client, "interval-first").await);
    runtime_config.second_topic = Some(one_partition_topic(&client, "interval-second").await);
    runtime_config.member = uuid::Uuid::new_v4().to_string();
    runtime_config.client = Some(client);
    runtime_config.broker = Some(p);
}
#[rstest_bdd_macros::when("the member joins g-interval at epoch 0 subscribed to interval-first")]
async fn runtime_assignment_join(runtime_config: &mut DynamicRuntime) {
    runtime_config.joined_at = Some(Instant::now());
    runtime_config.heartbeat = Some(
        runtime_config
            .client()
            .send(subscribed(&runtime_config.member, 0, &["interval-first"]))
            .await
            .expect("join"),
    );
}
#[rstest_bdd_macros::then(
    "the complete first heartbeat assigns interval-first partition 0 at epoch 2 and interval 5000"
)]
fn runtime_first_assignment(runtime_config: &DynamicRuntime) {
    check!(
        runtime_config.heartbeat.as_ref().expect("member joined")
            == &assigned(
                &runtime_config.member,
                2,
                Some(vec![partition_zero(
                    runtime_config.first_topic.expect("first topic created")
                )])
            )
    );
}
#[rstest_bdd_macros::when("the member at epoch 2 subscribes to both interval topics")]
async fn runtime_assignment_resubscribe(runtime_config: &mut DynamicRuntime) {
    runtime_config.heartbeat = Some(
        runtime_config
            .client()
            .send(subscribed(&runtime_config.member, 2, &[
                "interval-first",
                "interval-second",
            ]))
            .await
            .expect("subscription change"),
    );
}
#[rstest_bdd_macros::then(
    "the complete heartbeat remains at epoch 2 with no assignment and interval 5000"
)]
fn runtime_assignment_deferred(runtime_config: &DynamicRuntime) {
    check!(
        runtime_config
            .heartbeat
            .as_ref()
            .expect("subscription changed")
            == &assigned(&runtime_config.member, 2, None)
    );
}
#[rstest_bdd_macros::when("heartbeats continue until the deferred target assignment is released")]
async fn runtime_assignment_release(runtime_config: &mut DynamicRuntime) {
    let deadline = Instant::now() + PROPAGATION;
    loop {
        let answer = runtime_config
            .client()
            .send(subscribed(&runtime_config.member, 2, &[
                "interval-first",
                "interval-second",
            ]))
            .await
            .expect("heartbeat");
        let finished = answer.assignment.is_some() || Instant::now() >= deadline;
        runtime_config.heartbeat = Some(answer);
        if finished {
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    runtime_config.elapsed = Some(
        runtime_config
            .joined_at
            .expect("first join timed")
            .elapsed(),
    );
}
#[rstest_bdd_macros::then(
    "at least 2000 milliseconds elapsed and the complete epoch 3 heartbeat assigns both partition 0 topics"
)]
fn runtime_assignment_released(runtime_config: &DynamicRuntime) {
    check!(runtime_config.elapsed.expect("assignment release measured") >= Duration::from_secs(2));
    let actual = runtime_config
        .heartbeat
        .as_ref()
        .expect("assignment polled")
        .clone();
    let expected = assigned(
        &runtime_config.member,
        3,
        Some(vec![
            partition_zero(runtime_config.first_topic.expect("first topic created")),
            partition_zero(runtime_config.second_topic.expect("second topic created")),
        ]),
    );
    check!(in_topic_order(actual) == in_topic_order(expected));
}
#[rstest_bdd_macros::given("a default broker is ready for assignment interval validation")]
async fn runtime_assignment_validation_broker(runtime_config: &mut DynamicRuntime) {
    runtime_config.broker = Some(support::start().await);
}
#[rstest_bdd_macros::then("the full alteration response is error 42 with message {message}")]
fn runtime_assignment_out_of_range(runtime_config: &DynamicRuntime, message: String) {
    let broker = &runtime_config
        .broker
        .as_ref()
        .expect("runtime broker started")
        .broker;
    check!(
        runtime_config
            .altered
            .as_ref()
            .expect("runtime config altered")
            == &AlterConfigsResourceResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some(message),
                ..accepted(broker, runtime_config.scope.expect("resource selected"))
            }
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_producer_and_group_config.feature",
    name = "a dynamic producer id expiration reaches the running expiry sweep"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dynamic_producer_id_expiration_reaches_the_running_expiry_sweep(
    runtime_config: DynamicRuntime,
    scope: String,
    topic: String,
) {
    let _ = (scope, topic);
    runtime_config.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_producer_and_group_config.feature",
    name = "a dynamic consumer assignment interval holds back the next assignment"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dynamic_consumer_assignment_interval_holds_back_the_next_assignment(
    runtime_config: DynamicRuntime,
) {
    runtime_config.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/dynamic_producer_and_group_config.feature",
    name = "an assignment interval above the broker maximum is refused"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_assignment_interval_above_the_broker_maximum_is_refused(
    runtime_config: DynamicRuntime,
    scope: String,
    protocol: String,
    message: String,
) {
    let _ = (scope, protocol, message);
    runtime_config.shutdown().await;
}
