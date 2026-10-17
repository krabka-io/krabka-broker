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
    krabka_broker::{BrokerConfig, BrokerHandle},
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
};

/// Kafka's `TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG`.
const PRODUCER_ID_EXPIRATION_MS: &str = "producer.id.expiration.ms";

/// Kafka's `GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG`.
const CONSUMER_ASSIGNMENT_INTERVAL_MS: &str = "group.consumer.assignment.interval.ms";

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
    if row.error_code == krabka_broker::codes::NONE {
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
        error_code: krabka_broker::codes::NONE,
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

/// A dynamic `producer.id.expiration.ms`, on either resource, shortens the
/// window of the producer-expiry sweep that is already running: the sweep
/// removes a producer that went idle under the static day-long window, and
/// `DescribeProducers` no longer reports it. Kafka's
/// `DynamicProducerStateManagerConfig` reconfigures the window of every log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dynamic_producer_id_expiration_reaches_the_running_expiry_sweep() {
    let p = support::start_configured(|config: &mut BrokerConfig| {
        // The sweep runs every 50 ms instead of every ten minutes, so the
        // test sees several sweeps under each window.
        config.producer_id_expiration_scan_interval = krabka_units::millis(50);
    })
    .await;
    for scope in [Scope::ClusterDefault, Scope::ThisBroker] {
        let topic = format!("expiring-{scope:?}").to_lowercase();
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

        // Several sweeps under the static day-long window keep the producer.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let kept = active_producer_ids(&p.client, &topic).await;
        check!(kept == [producer.producer_id], "{scope:?}");

        let altered = alter_broker()
            .client(&p.client)
            .broker(&p.broker)
            .scope(scope)
            .key(PRODUCER_ID_EXPIRATION_MS)
            .value("1")
            .call()
            .await;
        assert!(altered == accepted(&p.broker, scope));
        let deadline = Instant::now() + PROPAGATION;
        let remaining = loop {
            let remaining = active_producer_ids(&p.client, &topic).await;
            if remaining.is_empty() || Instant::now() >= deadline {
                break remaining;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        check!(
            remaining == Vec::<i64>::new(),
            "{scope:?}: producer.id.expiration.ms=1 must expire the idle producer"
        );

        let deleted = alter_broker()
            .client(&p.client)
            .broker(&p.broker)
            .scope(scope)
            .key(PRODUCER_ID_EXPIRATION_MS)
            .call()
            .await;
        assert!(deleted == accepted(&p.broker, scope));
    }
    p.broker.shutdown().await;
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

/// A broker-level `group.consumer.assignment.interval.ms` reaches a running
/// group: with the static interval at zero, a cluster-wide 2 s holds back the
/// target assignment of a subscription change until the interval since the
/// group's last assignment has passed, as Kafka's
/// `GroupMetadataManager.maybeUpdateTargetAssignment` holds it back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dynamic_consumer_assignment_interval_holds_back_the_next_assignment() {
    const INTERVAL: Duration = Duration::from_secs(2);
    let p = support::start_ready_group().await;
    let client = connect_client(p.broker.listen_addr().to_string(), Some("c-interval")).await;
    let first = one_partition_topic(&client, "interval-first").await;
    let second = one_partition_topic(&client, "interval-second").await;

    let altered = alter_broker()
        .client(&client)
        .broker(&p.broker)
        .scope(Scope::ClusterDefault)
        .key(CONSUMER_ASSIGNMENT_INTERVAL_MS)
        .value("2000")
        .call()
        .await;
    assert!(altered == accepted(&p.broker, Scope::ClusterDefault));

    let member_id = uuid::Uuid::new_v4().to_string();
    // The group's first target assignment is not held back: there is no
    // earlier one to wait from. It is computed after this instant.
    let joined_at = Instant::now();
    let joined = client
        .send(subscribed(&member_id, 0, &["interval-first"]))
        .await
        .expect("join");
    check!(joined == assigned(&member_id, 2, Some(vec![partition_zero(first)])));

    let resubscribed = client
        .send(subscribed(&member_id, 2, &[
            "interval-first",
            "interval-second",
        ]))
        .await
        .expect("subscription change");
    check!(
        resubscribed == assigned(&member_id, 2, None),
        "the subscription change must wait for the 2 s interval"
    );

    let deadline = Instant::now() + PROPAGATION;
    let released = loop {
        let answer = client
            .send(subscribed(&member_id, 2, &[
                "interval-first",
                "interval-second",
            ]))
            .await
            .expect("heartbeat");
        if answer.assignment.is_some() || Instant::now() >= deadline {
            break answer;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    check!(joined_at.elapsed() >= INTERVAL);
    check!(
        in_topic_order(released)
            == in_topic_order(assigned(
                &member_id,
                3,
                Some(vec![partition_zero(first), partition_zero(second)])
            ))
    );
    p.broker.shutdown().await;
}

/// Kafka's `GroupCoordinatorConfig` refuses a broker-level assignment interval
/// outside the broker's bounds when the alter builds the new `KafkaConfig`,
/// and the refusal reaches the client as `INVALID_REQUEST` with the
/// `require` message (KAFKA-13609).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_assignment_interval_above_the_broker_maximum_is_refused() {
    let p = support::start().await;
    let mut refusals = Vec::new();
    for (scope, protocol) in [
        (Scope::ClusterDefault, "consumer"),
        (Scope::ThisBroker, "share"),
        (Scope::ClusterDefault, "streams"),
    ] {
        let key = format!("group.{protocol}.assignment.interval.ms");
        let refused = alter_broker()
            .client(&p.client)
            .broker(&p.broker)
            .scope(scope)
            .key(&key)
            .value("15001")
            .call()
            .await;
        refusals.push(refused);
    }

    let refusal = |scope: Scope, protocol: &str| AlterConfigsResourceResponse {
        error_code: krabka_broker::codes::INVALID_REQUEST,
        error_message: Some(format!(
            "group.{protocol}.assignment.interval.ms must be less than or equal to \
             group.{protocol}.max.assignment.interval.ms"
        )),
        resource_type: BROKER_RESOURCE,
        resource_name: scope.resource_name(&p.broker),
        ..Default::default()
    };
    check!(
        refusals
            == [
                refusal(Scope::ClusterDefault, "consumer"),
                refusal(Scope::ThisBroker, "share"),
                refusal(Scope::ClusterDefault, "streams"),
            ]
    );
    p.broker.shutdown().await;
}
