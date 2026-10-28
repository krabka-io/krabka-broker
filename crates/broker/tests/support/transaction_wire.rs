//! Protocol and client fixtures shared by transaction coordinator scenarios.

use {
    crate::support::{
        produce::{SinglePartitionProduceSetup, batch_request},
        records::{batch_from_records, value_record},
        topics::{
            AssignedTopicSetup, TopicPartitionCount, assigned_topic, creatable_topic,
            create_topic_request,
        },
        transactions::{
            PartitionRegistration, ProducerIdentity,
            init_producer_request as producer_initialization,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel},
    krabka_client_producer::ProducerError,
    krabka_protocol::{
        owned::{
            add_partitions_to_txn_request::{
                AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction,
            },
            add_partitions_to_txn_response::AddPartitionsToTxnResponse,
            common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
            create_topics_request::CreatableTopic,
            init_producer_id_request::InitProducerIdRequest,
            init_producer_id_response::InitProducerIdResponse,
            produce_request::ProduceRequest,
            produce_response::ProduceResponse,
        },
        primitives::uuid::Uuid,
        records::{Attributes, RecordBatch},
    },
    std::{
        fmt,
        future::Future,
        time::{Duration, Instant},
    },
    tokio::time,
};

pub fn init_producer_request(transactional_id: &str) -> InitProducerIdRequest {
    producer_initialization()
        .maybe_transactional_id(Some(transactional_id.into()))
        .call()
}

/// Supply both layouts so the client can negotiate either protocol version.
pub fn add_partition_request(
    transactional_id: &str,
    topic: &str,
    (producer_id, epoch): (i64, i16),
) -> AddPartitionsToTxnRequest {
    partitions_request()
        .transactional_id(transactional_id)
        .producer(ProducerIdentity::from_wire((producer_id, epoch)))
        .topics(vec![transaction_topic(topic, vec![0])])
        .call()
}

pub fn transaction_topic(name: &str, partitions: Vec<i32>) -> AddPartitionsToTxnTopic {
    AddPartitionsToTxnTopic {
        name: name.into(),
        partitions,
        ..Default::default()
    }
}

/// Both request layouts carry the same explicit transaction and partition set.
#[derive(krabka_macros::FieldDefaults)]
pub struct TransactionPartitionsSetup<'a> {
    #[default("transaction")]
    pub transactional_id: &'a str,
    pub producer: ProducerIdentity,
    pub registration: PartitionRegistration,
    #[default(vec![transaction_topic("orders", vec![0])])]
    pub topics: Vec<AddPartitionsToTxnTopic>,
}

#[bon::builder]
pub fn partitions_request(
    #[builder(default = "transaction")] transactional_id: &'_ str,
    #[builder(default)] producer: ProducerIdentity,
    #[builder(default)] registration: PartitionRegistration,
    #[builder(default = vec ! [transaction_topic ("orders" , vec ! [0])])] topics: Vec<
        AddPartitionsToTxnTopic,
    >,
) -> AddPartitionsToTxnRequest {
    let setup = TransactionPartitionsSetup {
        transactional_id,
        producer,
        registration,
        topics,
    };
    let TransactionPartitionsSetup {
        transactional_id,
        producer,
        registration,
        topics,
    } = setup;
    AddPartitionsToTxnRequest {
        transactions: vec![AddPartitionsToTxnTransaction {
            transactional_id: transactional_id.into(),
            producer_id: producer.id.0,
            producer_epoch: producer.epoch.0,
            verify_only: registration == PartitionRegistration::VerifyOnly,
            topics: topics.clone(),
            ..Default::default()
        }],
        v3_and_below_transactional_id: transactional_id.into(),
        v3_and_below_producer_id: producer.id.0,
        v3_and_below_producer_epoch: producer.epoch.0,
        v3_and_below_topics: topics,
        ..Default::default()
    }
}

pub fn assert_partition_added(response: &AddPartitionsToTxnResponse) {
    let code = partition_error(response, false);
    assert!(code == 0, "AddPartitionsToTxn: {response:?}");
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub struct TransactionProduceSetup<'a> {
    #[default("transaction")]
    pub transactional_id: &'a str,
    #[default("orders")]
    pub topic: &'a str,
    pub topic_id: Uuid,
    pub producer: Option<ProducerIdentity>,
    #[default(&["v"])]
    pub values: &'a [&'static str],
}

fn build_produce_request(setup: TransactionProduceSetup<'_>) -> ProduceRequest {
    let TransactionProduceSetup {
        transactional_id,
        topic,
        topic_id,
        producer,
        values,
    } = setup;
    let batch = records_batch(
        producer.map(|identity| (identity.id.0, identity.epoch.0)),
        values,
    );
    ProduceRequest {
        // KIP-890 verification needs the transactional id for transactional batches.
        transactional_id: producer.map(|_| transactional_id.to_owned()),
        ..batch_request(batch, SinglePartitionProduceSetup {
            topic: (topic).into(),
            topic_id,
            ..SinglePartitionProduceSetup::replicated()
        })
    }
}

pub fn records_batch(producer: Option<(i64, i16)>, values: &[&'static str]) -> RecordBatch {
    let records = i32::try_from(values.len()).expect("record count");
    RecordBatch {
        attributes: Attributes::default().with_transactional(producer.is_some()),
        producer_id: producer.map_or(-1, |(producer_id, _)| producer_id),
        producer_epoch: producer.map_or(-1, |(_, epoch)| epoch),
        base_sequence: if producer.is_some() { 0 } else { -1 },
        last_offset_delta: records - 1,
        max_timestamp: 1,
        ..batch_from_records(
            values
                .iter()
                .zip(0..)
                .map(|(value, offset_delta)| {
                    value_record(offset_delta, Some(Bytes::from_static(value.as_bytes())))
                })
                .collect(),
        )
    }
}

/// Read the first partition error, with the legacy fallback only in fixtures that originally used it.
pub fn partition_error(response: &AddPartitionsToTxnResponse, legacy: bool) -> i16 {
    let topic = response
        .results_by_transaction
        .first()
        .and_then(|transaction| transaction.topic_results.first());
    let topic = if legacy {
        topic.or_else(|| response.results_by_topic_v3_and_below.first())
    } else {
        topic
    };
    topic
        .and_then(|topic| topic.results_by_partition.first())
        .map_or(response.error_code, |row| row.partition_error_code)
}

use krabka_protocol::owned::create_topics_request::CreatableTopicConfig;

#[derive(krabka_macros::FieldDefaults)]
pub struct TransactionTopicSetup<'a> {
    #[default("orders")]
    pub name: &'a str,
    #[default(TopicPartitionCount(1))]
    pub partitions: TopicPartitionCount,
    pub configs: Vec<CreatableTopicConfig>,
    #[default("CreateTopics")]
    pub context: &'a str,
}

#[bon::builder]
pub async fn create_topic(
    client: &krabka_client_core::Client,
    #[builder(default = "orders")] name: &'_ str,
    #[builder(default = TopicPartitionCount(1))] partitions: TopicPartitionCount,
    #[builder(default)] configs: Vec<CreatableTopicConfig>,
    #[builder(default = "CreateTopics")] context: &'_ str,
) {
    let setup = TransactionTopicSetup {
        name,
        partitions,
        configs,
        context,
    };
    let TransactionTopicSetup {
        name,
        partitions,
        configs,
        context,
    } = setup;
    let response = client
        .send(create_topic_request(CreatableTopic {
            configs,
            ..creatable_topic()
                .name((name).into())
                .partitions(partitions.0)
                .call()
        }))
        .await
        .unwrap();
    assert!(
        response.topics[0].error_code == 0 || response.topics[0].error_code == 36,
        "{context} {name}: error_code={}",
        response.topics[0].error_code
    );
}

/// Loading answers are shared by the coordinator readiness probes, not their response oracles.
pub fn coordinator_loading(code: i16) -> bool { matches!(code, 14..=16) }

/// Poll the RPC answer: coordinator loading has no materialization awaiter.
pub async fn retry_coordinator<R, F: Future<Output = R>>(
    mut send: impl FnMut() -> F,
    code: impl Fn(&R) -> i16,
    retriable: impl Fn(i16) -> bool,
    timeout: Duration,
) -> R {
    let deadline = Instant::now() + timeout;
    loop {
        let response = send().await;
        if retriable(code(&response)) && Instant::now() < deadline {
            // Intentional: only the RPC answer signals coordinator readiness.
            time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return response;
    }
}

pub async fn initialize_producer<F>(
    send: impl FnMut() -> F,
    retriable: impl Fn(i16) -> bool,
    timeout: Duration,
) -> InitProducerIdResponse
where
    F: Future<Output = InitProducerIdResponse>,
{
    let response =
        retry_coordinator(send, |response| response.error_code, retriable, timeout).await;
    assert!(response.error_code == 0, "InitProducerId: {response:?}");
    response
}

/// The image watch has no awaiter for any leader, so preserve the bounded 100ms probe.
pub async fn partition_leader(
    handle: &krabka_broker::BrokerHandle,
    topic: &str,
    timeout: Duration,
) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(leader) = handle.partition_leader_for_test(topic, 0) {
            return leader;
        }
        assert!(Instant::now() < deadline, "{topic}-0 has no leader");
        // Intentional: the image watch has no awaiter for any leader.
        time::sleep(Duration::from_millis(100)).await;
    }
}

/// The same Produce success oracle for client-routed and direct-connection requests.
pub async fn produce_succeeds<E: fmt::Debug>(
    response: impl Future<Output = Result<ProduceResponse, E>>,
) {
    let response = response.await.expect("Produce");
    let code = response.responses[0].partition_responses[0].error_code;
    assert!(code == 0, "Produce: {response:?}");
}

/// Send the fixture through either a client or a direct connection, keeping the success oracle.
#[bon::builder]
pub async fn produce_fixture<E: fmt::Debug, F>(
    #[builder(default = "transaction")] transactional_id: &'_ str,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default)] topic_id: Uuid,
    producer: Option<ProducerIdentity>,
    #[builder(default = & ["v"])] values: &'_ [&'static str],
    send: impl FnOnce(ProduceRequest) -> F,
) where
    F: Future<Output = Result<ProduceResponse, E>>,
{
    let setup = TransactionProduceSetup {
        transactional_id,
        topic,
        topic_id,
        producer,
        values,
    };
    produce_succeeds(send(build_produce_request(setup))).await;
}

pub async fn partition_added<E: fmt::Debug>(
    response: impl Future<Output = Result<AddPartitionsToTxnResponse, E>>,
) {
    assert_partition_added(&response.await.expect("AddPartitionsToTxn"));
}

pub async fn initialized_identity<F>(
    send: impl FnMut() -> F,
    retriable: impl Fn(i16) -> bool,
    timeout: Duration,
) -> (i64, i16)
where
    F: Future<Output = InitProducerIdResponse>,
{
    let response = initialize_producer(send, retriable, timeout).await;
    (response.producer_id, response.producer_epoch)
}

/// Only the three original metadata/leadership readiness errors trigger another Produce.
pub async fn settled_produce(
    client: &krabka_client_core::Client,
    request: ProduceRequest,
    timeout: Duration,
) -> ProduceResponse {
    retry_coordinator(
        || async { client.send(request.clone()).await.expect("Produce") },
        |response| response.responses[0].partition_responses[0].error_code,
        |code| matches!(code, 6 | 3 | 100),
        timeout,
    )
    .await
}

#[derive(krabka_macros::FieldDefaults)]
pub struct AssignedTransactionTopicSetup<'a> {
    pub topic: AssignedTopicSetup,
    pub context: Option<&'a str>,
}

#[bon::builder]
pub async fn create_assigned_topic(
    client: &krabka_client_core::Client,
    #[builder(default)] topic: AssignedTopicSetup,
    context: Option<&'_ str>,
) -> Uuid {
    let setup = AssignedTransactionTopicSetup { topic, context };
    let created = client
        .send(create_topic_request(
            assigned_topic()
                .name(setup.topic.name)
                .replicas(setup.topic.replicas)
                .call(),
        ))
        .await
        .expect("CreateTopics");
    match setup.context {
        Some(context) => assert!(created.topics[0].error_code == 0, "{context}: {created:?}"),
        None => assert!(created.topics[0].error_code == 0, "{created:?}"),
    }
    created.topics[0].topic_id
}

/// Retain the 200ms polls and check completion before each poll, including the first.
pub async fn poll_values_until(
    consumer: &mut Consumer,
    timeout: Duration,
    done: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    poll_values(consumer, timeout, done, Some("poll")).await
}

async fn poll_values(
    consumer: &mut Consumer,
    timeout: Duration,
    done: impl Fn(&[String]) -> bool,
    context: Option<&str>,
) -> Vec<String> {
    let mut seen = Vec::new();
    let deadline = Instant::now() + timeout;
    while !done(&seen) && Instant::now() < deadline {
        let records = consumer.poll(krabka_units::millis(200)).await;
        let records = match context {
            Some(context) => records.expect(context),
            None => records.unwrap(),
        };
        for record in records {
            seen.push(String::from_utf8_lossy(record.value.as_deref().unwrap_or(b"")).into_owned());
        }
    }
    seen
}

pub async fn read_committed_through(
    bootstrap: &str,
    topic: &str,
    last: &str,
    timeout: Duration,
) -> Vec<String> {
    let mut consumer = Consumer::builder()
        .bootstrap(bootstrap.to_owned())
        .group_id(format!("{topic}-reader"))
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe([topic.to_owned()])
        .build()
        .await
        .expect("consumer");
    let seen = poll_values_until(&mut consumer, timeout, |seen| {
        seen.last().map(String::as_str) == Some(last)
    })
    .await;
    consumer.close().await.expect("close consumer");
    seen
}

use krabka_client_core::security::ClientSecurity;

#[derive(krabka_macros::FieldDefaults)]
pub struct TransactionConsumerSetup<'a> {
    #[default("transaction-reader".into())]
    pub group: String,
    #[default("orders")]
    pub topic: &'a str,
    #[default(IsolationLevel::ReadCommitted)]
    pub isolation: IsolationLevel,
    pub security: Option<ClientSecurity>,
}

#[bon::builder]
pub async fn consumer(
    bootstrap: impl Into<String>,
    #[builder(default = "transaction-reader" . into ())] group: String,
    #[builder(default = "orders")] topic: &'_ str,
    #[builder(default = IsolationLevel :: ReadCommitted)] isolation: IsolationLevel,
    security: Option<ClientSecurity>,
) -> Result<Consumer, krabka_client_consumer::ConsumerError> {
    let setup = TransactionConsumerSetup {
        group,
        topic,
        isolation,
        security,
    };
    let TransactionConsumerSetup {
        group,
        topic,
        isolation,
        security,
    } = setup;
    let builder = Consumer::builder()
        .bootstrap(bootstrap)
        .group_id(group)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(isolation)
        .subscribe([topic.to_owned()]);
    match security {
        Some(security) => builder.security(security).build().await,
        None => builder.build().await,
    }
}

/// Common committed-record probe; callers retain the exact value oracle and teardown order.
pub async fn committed_values(
    bootstrap: impl Into<String>,
    group: impl Into<String>,
    topic: &str,
    security: Option<ClientSecurity>,
) -> (Consumer, Vec<String>) {
    observed_values(
        bootstrap,
        group,
        topic,
        (IsolationLevel::ReadCommitted, security),
        Duration::from_secs(10),
        |seen| seen.len() >= 3,
        Some("poll"),
    )
    .await
}

/// Poll the same consumer while leaving value expectations and close order with the scenario.
pub async fn observed_values(
    bootstrap: impl Into<String>,
    group: impl Into<String>,
    topic: &str,
    (isolation, security): (IsolationLevel, Option<ClientSecurity>),
    timeout: Duration,
    done: impl Fn(&[String]) -> bool,
    context: Option<&str>,
) -> (Consumer, Vec<String>) {
    let mut consumer = consumer()
        .bootstrap(bootstrap)
        .group(group.into())
        .topic(topic)
        .isolation(isolation)
        .maybe_security(security)
        .call()
        .await
        .unwrap();
    let seen = poll_values(&mut consumer, timeout, done, context).await;
    (consumer, seen)
}

/// Initialize a transactional identity through the ordinary client with the caller's exact retry policy.
pub async fn initialize_transactional(
    client: &krabka_client_core::Client,
    transactional_id: &str,
    retriable: impl Fn(i16) -> bool,
    timeout: Duration,
) -> InitProducerIdResponse {
    initialize_producer(
        || async {
            client
                .send(init_producer_request(transactional_id))
                .await
                .expect("InitProducerId")
        },
        retriable,
        timeout,
    )
    .await
}

/// The plain read-committed probes keep their original ten-second bound and unwrap diagnostic.
pub async fn read_committed_at_least(
    bootstrap: impl Into<String>,
    group: impl Into<String>,
    topic: &str,
    records: usize,
) -> (Consumer, Vec<String>) {
    observed_values(
        bootstrap,
        group,
        topic,
        (IsolationLevel::ReadCommitted, None),
        Duration::from_secs(10),
        |seen| seen.len() >= records,
        None,
    )
    .await
}

/// Close the producer before its read-back consumer, surfacing either error.
pub async fn close_transaction_clients(
    producer_close: impl Future<Output = Result<(), ProducerError>>,
    consumer: Consumer,
) {
    producer_close.await.unwrap();
    consumer.close().await.unwrap();
}
