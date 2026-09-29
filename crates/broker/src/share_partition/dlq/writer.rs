//! The dead-letter write: Kafka's `ShareGroupDLQStateManager`.
//!
//! [`DlqWriter`] validates the group's topic, creates it when the cluster
//! allows and it is missing, and produces the dead-letter records to the
//! leader of the partition that the source partition maps to. Kafka sends
//! both requests through an inter-broker `NetworkClient`, to a random broker
//! for the topic creation and to the partition leader for the produce, and
//! the leader authorizes and appends like any other produce. This does the
//! same over the inter-broker listener, so the append takes the ordinary
//! produce path with its checks, its throttles and its replication.
//!
//! Each request retries as Kafka's does: up to 5 attempts, backing off from
//! 1 s to 30 s. A write that runs out of attempts, or that a broker refuses
//! for good, fails, and the leader manager archives the records regardless.
//!
//! The produce requests to one leader share one connection, opened when the
//! first needs it and kept for the ones after it, as Kafka's send thread keeps
//! one `NetworkClient` for every dead-letter produce. A write that pays for a
//! TCP, TLS and SASL setup each time would turn a consumer that rejects at a
//! high rate into a storm of connections.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use async_trait::async_trait;
use exponential_backoff::Backoff;
use krabka_compression::RecordDecompressionPolicy;
use krabka_ids::PartitionIndex;
use krabka_log::LogConfig;
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::{
    owned::{
        create_topics_response::CreateTopicsResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordBatch,
};
use krabka_security::ListenerProtocol;
use krabka_units::convert::ByteSizeExt as _;

use super::{
    DlqError, DlqRequest, DlqSink,
    record::{RangeContext, RoundBounds, build_round, destination_partition},
    source::{Fetched, fetch},
    validate::{ClusterSettings, GroupSettings, TopicState, validate},
};
use crate::{
    auto_topic_creation::AutoTopicCreation,
    codes,
    config::BrokerConfig,
    config_keys::{BrokerLogDefaults, resolve_max_message_bytes},
    metadata_source::MetadataSource,
    network::client::InterBrokerClient,
    partition_registry::PartitionRegistry,
    topic_creator::TopicCreatorError,
};

/// The attempts of one request, and the pauses between them: Kafka's
/// `MAX_REQUEST_ATTEMPTS`, `REQUEST_BACKOFF_MS` and `REQUEST_BACKOFF_MAX_MS`.
const MAX_ATTEMPTS: u32 = 5;
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// The `timeout.ms` of the produce request: Kafka's
/// `ServerConfigs.REQUEST_TIMEOUT_MS_DEFAULT`.
const PRODUCE_TIMEOUT_MS: i32 = 30_000;

/// The result of one attempt of a request.
enum Attempt<T> {
    Done(T),
    /// Try again after a pause: a leader that is not there yet, a metadata
    /// image that lags a topic creation, a connection that dropped.
    Retry(String),
    /// A broker refused for good.
    Fatal(DlqError),
}

/// Where the dead-letter records go: the leader of one partition of the
/// dead-letter topic.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    topic: String,
    topic_id: uuid::Uuid,
    partition: i32,
    leader: NodeId,
    /// The topic's `max.message.bytes`.
    max_message_bytes: i32,
}

/// The dead-letter topic of `topic` as `image` knows it, for the records of
/// `source_partition`: Kafka's `populateDLQTopicData`.
///
/// # Errors
///
/// Why the topic is not usable yet. The topic may be a moment old, so the
/// caller tries again.
fn resolve_target(
    image: &MetadataImage,
    topic: &str,
    source_partition: i32,
    max_message_bytes: i32,
) -> Result<Target, String> {
    let record = image
        .topic(topic)
        .ok_or_else(|| format!("DLQ topic {topic} is not in the metadata image"))?;
    // The count of the partition records is the authoritative one: a
    // `TopicRecord` carries none.
    let partitions = image.topic_partition_count(topic);
    if partitions <= 0 {
        return Err(format!("DLQ topic {topic} has no partitions yet"));
    }
    let partition = destination_partition(source_partition, partitions);
    let leader = image
        .partition(topic, partition)
        .map(|partition| partition.leader)
        .filter(|leader| image.broker(*leader).is_some())
        .ok_or_else(|| format!("DLQ topic {topic}-{partition} has no leader that is up yet"))?;
    Ok(Target {
        topic: topic.to_owned(),
        topic_id: record.topic_id,
        partition,
        leader,
        max_message_bytes,
    })
}

/// What one produce answer means: Kafka's `handleProduceResponse`.
fn classify_produce(response: &ProduceResponse, target: &Target) -> Attempt<()> {
    let row = response
        .responses
        .iter()
        .flat_map(|topic| &topic.partition_responses)
        .find(|row| row.index == target.partition);
    let Some(row) = row else {
        return Attempt::Fatal(DlqError::Write(format!(
            "Received empty partition produce response from the DLQ topic {}-{}.",
            target.topic, target.partition
        )));
    };
    match row.error_code {
        codes::NONE => Attempt::Done(()),
        codes::NOT_LEADER_OR_FOLLOWER => Attempt::Retry(format!(
            "{}-{} is not led by node {}",
            target.topic, target.partition, target.leader
        )),
        code => Attempt::Fatal(DlqError::Write(format!(
            "Unable to produce to the DLQ topic {}-{}: error {code}{}.",
            target.topic,
            target.partition,
            row.error_message
                .as_deref()
                .map(|message| format!(", {message}"))
                .unwrap_or_default()
        ))),
    }
}

/// What one topic creation answer means: Kafka's `handleCreateTopicsResponse`.
fn classify_create(
    result: Result<CreateTopicsResponse, TopicCreatorError>,
    topic: &str,
) -> Attempt<()> {
    match result {
        Ok(response) => match response.topics.first().map(|row| row.error_code) {
            Some(codes::NONE) => Attempt::Done(()),
            // The topic was created by a request that was in flight, or the
            // controller is throttling: the next attempt finds the topic, or
            // asks again.
            Some(code @ (codes::TOPIC_ALREADY_EXISTS | codes::THROTTLING_QUOTA_EXCEEDED)) => {
                Attempt::Retry(format!("creating {topic}: error {code}"))
            }
            Some(code) => Attempt::Fatal(DlqError::Write(format!(
                "Unable to create the DLQ topic {topic}: error {code}."
            ))),
            None => Attempt::Fatal(DlqError::Write(format!(
                "The DLQ topic {topic} is not in the create topics response."
            ))),
        },
        Err(TopicCreatorError::Timeout) => Attempt::Retry(format!("creating {topic} timed out")),
        Err(error) => Attempt::Fatal(DlqError::Write(format!(
            "Unable to create the DLQ topic {topic}: {error}."
        ))),
    }
}

/// The inter-broker listener that the requests go over.
struct Transport {
    protocol: ListenerProtocol,
    listener_name: String,
    server_name: String,
}

/// A connection that a pool can tell has closed.
trait Pooled: Clone {
    fn is_open(&self) -> bool;
}

impl Pooled for krabka_client_core::Connection {
    fn is_open(&self) -> bool {
        !self.is_closed()
    }
}

/// One connection for each leader, dialled when a write first needs it and
/// shared by every write that follows, until it closes.
///
/// A [`krabka_client_core::Connection`] pipelines the requests of its clones,
/// so writers that reach one leader at once use the one connection. Writers
/// that need a leader with no open connection wait for a single dial.
struct LeaderConnections<C> {
    slots: std::sync::Mutex<HashMap<NodeId, Arc<tokio::sync::Mutex<Option<C>>>>>,
}

impl<C> Default for LeaderConnections<C> {
    fn default() -> Self {
        Self {
            slots: std::sync::Mutex::default(),
        }
    }
}

impl<C: Pooled> LeaderConnections<C> {
    /// The open connection to `leader`, dialled with `dial` when there is
    /// none.
    async fn get<F, Fut>(&self, leader: NodeId, dial: F) -> Result<C, String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<C, String>>,
    {
        let slot = {
            let mut slots = self.slots.lock().expect("connection pool lock");
            Arc::clone(slots.entry(leader).or_default())
        };
        let mut held = slot.lock().await;
        if let Some(open) = held.as_ref().filter(|connection| connection.is_open()) {
            return Ok(open.clone());
        }
        let dialled = dial().await?;
        *held = Some(dialled.clone());
        Ok(dialled)
    }
}

/// Writes dead-letter records: the [`DlqSink`] of a running broker.
pub struct DlqWriter {
    node_id: NodeId,
    controller: Arc<dyn MetadataSource>,
    partitions: Arc<PartitionRegistry>,
    topics: Arc<AutoTopicCreation>,
    client: Arc<InterBrokerClient>,
    transport: Transport,
    /// The connection to each leader that a produce goes to.
    connections: LeaderConnections<krabka_client_core::Connection>,
    /// The static log settings, under the dynamic broker defaults: the
    /// `message.max.bytes` of a topic that sets no `max.message.bytes`.
    base_log: LogConfig,
    decompression: RecordDecompressionPolicy,
}

impl DlqWriter {
    pub fn new(
        config: &BrokerConfig,
        controller: Arc<dyn MetadataSource>,
        partitions: Arc<PartitionRegistry>,
        topics: Arc<AutoTopicCreation>,
        client: Arc<InterBrokerClient>,
        listener_protocol: ListenerProtocol,
    ) -> Self {
        Self {
            node_id: NodeId(config.node_id.0),
            controller,
            partitions,
            topics,
            client,
            transport: Transport {
                protocol: listener_protocol,
                listener_name: config.inter_broker_listener_name.clone(),
                server_name: config.inter_broker_server_name.clone(),
            },
            connections: LeaderConnections::default(),
            base_log: config.log_config.clone(),
            decompression: config.record_decompression_policy().unwrap_or_default(),
        }
    }

    /// The `max.message.bytes` of `topic`, as this node runs it.
    fn max_message_bytes(&self, image: &MetadataImage, topic: &str) -> i32 {
        let broker_default =
            BrokerLogDefaults::resolve(image, self.node_id, &self.base_log, (None, None))
                .max_message_size;
        i32::try_from(resolve_max_message_bytes(image, topic, broker_default).bytes_usize())
            .unwrap_or(i32::MAX)
    }

    /// Runs `attempt` under Kafka's retry rule: up to [`MAX_ATTEMPTS`] tries
    /// with a growing pause between them.
    async fn with_backoff<T, F, Fut>(&self, what: &str, mut attempt: F) -> Result<T, DlqError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Attempt<T>>,
    {
        let backoff = Backoff::new(MAX_ATTEMPTS, BACKOFF_MIN, Some(BACKOFF_MAX));
        for delay in backoff {
            match attempt().await {
                Attempt::Done(value) => return Ok(value),
                Attempt::Fatal(error) => return Err(error),
                Attempt::Retry(reason) => match delay {
                    Some(delay) => {
                        tracing::debug!(%reason, what, "dead-letter request will retry");
                        tokio::time::sleep(delay).await;
                    }
                    None => {
                        return Err(DlqError::Write(format!(
                            "Exhausted max retries to {what}: {reason}."
                        )));
                    }
                },
            }
        }
        Err(DlqError::Write(format!("No attempt was made to {what}.")))
    }

    /// Creates the dead-letter topic and waits for it to show in the metadata
    /// image.
    async fn create_topic(&self, topic: &str) -> Result<(), DlqError> {
        self.with_backoff(&format!("create the DLQ topic {topic}"), || async {
            let created = self.topics.create_dead_letter_topic(topic).await;
            classify_create(created, topic)
        })
        .await
    }

    /// The dead-letter partition for `source_partition`, once the image shows
    /// it. It takes a moment for a new topic.
    async fn target(&self, topic: &str, source_partition: i32) -> Result<Target, DlqError> {
        self.with_backoff(&format!("find the DLQ topic {topic}"), || async {
            let image = self.controller.current_image();
            let max_message_bytes = self.max_message_bytes(&image, topic);
            match resolve_target(&image, topic, source_partition, max_message_bytes) {
                Ok(target) => Attempt::Done(target),
                Err(reason) => Attempt::Retry(reason),
            }
        })
        .await
    }

    /// Produces `batch` to the leader of `target`, with all replicas
    /// acknowledging.
    async fn produce(&self, target: &Target, batch: &RecordBatch) -> Result<(), DlqError> {
        self.with_backoff(
            &format!(
                "produce to the DLQ topic {}-{}",
                target.topic, target.partition
            ),
            || async {
                let response = match self.send_produce(target, batch).await {
                    Ok(response) => response,
                    Err(reason) => return Attempt::Retry(reason),
                };
                classify_produce(&response, target)
            },
        )
        .await
    }

    /// One produce request, over the connection to the leader that the writes
    /// share.
    async fn send_produce(
        &self,
        target: &Target,
        batch: &RecordBatch,
    ) -> Result<ProduceResponse, String> {
        // The leader can move between attempts, so it is read afresh.
        let image = self.controller.current_image();
        let leader = image
            .partition(&target.topic, target.partition)
            .map_or(target.leader, |partition| partition.leader);
        let broker = image
            .broker(leader)
            .ok_or_else(|| format!("node {leader} is not in the metadata image"))?;
        let (host, port) = broker
            .endpoints
            .iter()
            .find(|endpoint| endpoint.name == self.transport.listener_name)
            .map_or_else(
                || (broker.host.clone(), broker.port),
                |endpoint| (endpoint.host.clone(), endpoint.port),
            );
        let connection = self
            .connections
            .get(leader, || async {
                let options = krabka_client_core::ConnectionOptions {
                    client_id: format!("krabka-broker-dlq-{}", self.node_id),
                    ..krabka_client_core::ConnectionOptions::default()
                };
                self.client
                    .connect_as_connection(
                        &host,
                        port,
                        self.transport.protocol,
                        &self.transport.server_name,
                        options,
                    )
                    .await
                    .map_err(|error| format!("connect to {host}:{port}: {error}"))
            })
            .await?;
        let request = ProduceRequest {
            transactional_id: None,
            acks: -1,
            timeout_ms: PRODUCE_TIMEOUT_MS,
            topic_data: vec![TopicProduceData {
                name: target.topic.clone(),
                topic_id: WireUuid(*target.topic_id.as_bytes()),
                partition_data: vec![PartitionProduceData {
                    index: target.partition,
                    records: Some(batch.clone().into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        connection.send(request).await.map_err(|error| {
            // A connection that failed a request may be half dead: closing it
            // makes the next attempt dial afresh.
            connection.clone().close();
            format!("produce to {host}:{port}: {error}")
        })
    }
}

#[async_trait]
impl DlqSink for DlqWriter {
    async fn write(&self, request: DlqRequest) -> Result<(), DlqError> {
        let image = self.controller.current_image();
        let cluster = ClusterSettings::resolve(&image, self.node_id);
        let (settings, state) = validate(
            &image,
            &request.group,
            GroupSettings::resolve(&image, &request.group),
            &cluster,
        )?;
        if state == TopicState::Missing {
            self.create_topic(&settings.topic).await?;
        }
        // Kafka puts the id of a topic whose name it cannot find in the header.
        let source_topic = image
            .topic_name_by_id(&request.topic_id)
            .map_or_else(|| request.topic_id.to_string(), str::to_owned);
        let source = self
            .partitions
            .get(&source_topic, PartitionIndex(request.source_partition));
        let context = RangeContext {
            group: &request.group,
            source_topic: &source_topic,
            source_partition: request.source_partition,
            delivery_count: request.delivery_count,
            cause: request.cause,
        };
        let (first, last) = (request.first.0, request.last.0);
        let mut next = first;
        while next <= last {
            let target = self
                .target(&settings.topic, request.source_partition)
                .await?;
            let fetched = if settings.copy_record {
                let budget = usize::try_from(target.max_message_bytes).unwrap_or(usize::MAX);
                fetch(source.as_ref(), (next, last), budget, self.decompression).await
            } else {
                Fetched {
                    records: std::collections::BTreeMap::new(),
                    last_resolved: last,
                }
            };
            let round = build_round(
                &context,
                &fetched.records,
                RoundBounds {
                    next,
                    last,
                    last_resolved: fetched.last_resolved,
                    max_message_bytes: target.max_message_bytes,
                },
                crate::time_util::now_ms(),
            );
            self.produce(&target, &round.batch).await?;
            next = round.last_offset + 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord, PartitionRecord, TopicRecord};
    use krabka_protocol::owned::{
        create_topics_response::CreatableTopicResult,
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
    };

    use super::*;

    /// An image with the topic `dlq.g`, one partition for each of `leaders`,
    /// as `(leader, whether that broker is registered)`.
    fn image_with_topic(leaders: &[(u64, bool)]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "dlq.g".into(),
            topic_id: uuid::Uuid::from_bytes([7; 16]),
            partitions: i32::try_from(leaders.len()).unwrap(),
            replication_factor: 1,
        }));
        for (index, (leader, registered)) in leaders.iter().enumerate() {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                topic: "dlq.g".into(),
                partition: i32::try_from(index).unwrap(),
                leader: NodeId(*leader),
                replicas: vec![NodeId(*leader)],
                isr: vec![NodeId(*leader)],
                ..Default::default()
            }));
            if *registered {
                image.apply(&MetadataRecord::V1BrokerRegistration(
                    BrokerRegistrationRecord {
                        fenced: false,
                        in_controlled_shutdown: false,
                        cordoned_log_dirs: None,
                        node_id: NodeId(*leader),
                        broker_epoch: 0,
                        incarnation_id: uuid::Uuid::nil(),
                        host: "h".into(),
                        port: 9092,
                        rack: None,
                        endpoints: Vec::new(),
                        log_dirs: Vec::new(),
                        features: std::collections::BTreeMap::new(),
                    },
                ));
            }
        }
        image
    }

    fn target(partition: i32, leader: u64) -> Target {
        Target {
            topic: "dlq.g".into(),
            topic_id: uuid::Uuid::from_bytes([7; 16]),
            partition,
            leader: NodeId(leader),
            max_message_bytes: 1_048_588,
        }
    }

    /// Kafka's `populateDLQTopicData`: the partition is the source partition
    /// modulo the count, and the topic is not usable until the image holds it
    /// with a leader that is registered.
    #[test]
    fn the_target_follows_the_source_partition_and_waits_for_a_leader() {
        let ready = image_with_topic(&[(1, true), (2, true), (3, true)]);
        // Partition 1 is led by node 9, which has not registered.
        let no_leader = image_with_topic(&[(1, true), (9, false), (3, true)]);

        let actual = [
            resolve_target(&ready, "dlq.g", 4, 1_048_588),
            resolve_target(&ready, "dlq.g", 3, 1_048_588),
            resolve_target(&no_leader, "dlq.g", 4, 1_048_588),
            resolve_target(&ready, "missing", 0, 1_048_588),
        ];

        assert!(
            actual
                == [
                    Ok(target(1, 2)),
                    Ok(target(0, 1)),
                    Err("DLQ topic dlq.g-1 has no leader that is up yet".to_owned()),
                    Err("DLQ topic missing is not in the metadata image".to_owned()),
                ]
        );
    }

    fn produce_response(index: i32, error_code: i16) -> ProduceResponse {
        ProduceResponse {
            responses: vec![TopicProduceResponse {
                partition_responses: vec![PartitionProduceResponse {
                    index,
                    error_code,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn outcome<T>(attempt: &Attempt<T>) -> &'static str {
        match attempt {
            Attempt::Done(_) => "done",
            Attempt::Retry(_) => "retry",
            Attempt::Fatal(_) => "fatal",
        }
    }

    /// Kafka's `handleProduceResponse`: success is done, a leader that has
    /// moved is tried again, any other error is final, and so is an answer with
    /// no row for the partition.
    #[test]
    fn a_produce_answer_is_done_retried_or_final() {
        let target = target(1, 2);
        let cases = [
            (produce_response(1, codes::NONE), "done"),
            (produce_response(1, codes::NOT_LEADER_OR_FOLLOWER), "retry"),
            (produce_response(1, codes::MESSAGE_TOO_LARGE), "fatal"),
            (
                produce_response(1, codes::TOPIC_AUTHORIZATION_FAILED),
                "fatal",
            ),
            (produce_response(0, codes::NONE), "fatal"),
            (ProduceResponse::default(), "fatal"),
        ];

        assert!(
            cases
                .iter()
                .map(|(response, _)| outcome(&classify_produce(response, &target)))
                .collect::<Vec<_>>()
                == cases.iter().map(|(_, want)| *want).collect::<Vec<_>>()
        );
    }

    /// A connection that records whether it is open, and which dial made it.
    #[derive(Clone)]
    struct FakeConnection {
        dial: usize,
        open: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Pooled for FakeConnection {
        fn is_open(&self) -> bool {
            self.open.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// A dialler that counts its dials, and lets other tasks run mid-dial.
    #[derive(Default)]
    struct Dialler {
        dials: std::sync::atomic::AtomicUsize,
        fail_first: bool,
    }

    impl Dialler {
        async fn dial(&self) -> Result<FakeConnection, String> {
            let dial = self.dials.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::task::yield_now().await;
            if self.fail_first && dial == 0 {
                return Err("connection refused".to_owned());
            }
            Ok(FakeConnection {
                dial,
                open: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            })
        }

        fn dials(&self) -> usize {
            self.dials.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// The produce requests to one leader share a connection, another leader
    /// has its own, and a connection that has closed is dialled again.
    #[tokio::test]
    async fn writes_to_one_leader_share_a_connection_until_it_closes() {
        let pool = LeaderConnections::<FakeConnection>::default();
        let dialler = Dialler::default();

        let first = pool.get(NodeId(1), || dialler.dial()).await.unwrap();
        let again = pool.get(NodeId(1), || dialler.dial()).await.unwrap();
        let other = pool.get(NodeId(2), || dialler.dial()).await.unwrap();
        first.open.store(false, std::sync::atomic::Ordering::SeqCst);
        let redialled = pool.get(NodeId(1), || dialler.dial()).await.unwrap();

        assert!(
            (
                first.dial,
                again.dial,
                other.dial,
                redialled.dial,
                dialler.dials()
            ) == (0, 0, 1, 2, 3)
        );
    }

    /// Writers that need a leader at the same moment wait for one dial rather
    /// than each opening a connection, which is the storm a consumer that
    /// rejects at a high rate would otherwise cause.
    #[tokio::test]
    async fn concurrent_writes_to_one_leader_dial_once() {
        let pool = LeaderConnections::<FakeConnection>::default();
        let dialler = Dialler::default();

        let connections =
            futures_util::future::join_all((0..16).map(|_| pool.get(NodeId(1), || dialler.dial())))
                .await;

        assert!(
            (
                connections
                    .iter()
                    .map(|connection| connection.as_ref().map(|c| c.dial))
                    .collect::<Vec<_>>(),
                dialler.dials(),
            ) == (vec![Ok(0); 16], 1)
        );
    }

    /// A dial that fails is not remembered: the next write dials again.
    #[tokio::test]
    async fn a_failed_dial_is_tried_again_by_the_next_write() {
        let pool = LeaderConnections::<FakeConnection>::default();
        let dialler = Dialler {
            fail_first: true,
            ..Dialler::default()
        };

        let failed = pool.get(NodeId(1), || dialler.dial()).await.map(|c| c.dial);
        let retried = pool.get(NodeId(1), || dialler.dial()).await.map(|c| c.dial);

        assert!((failed, retried) == (Err("connection refused".to_owned()), Ok(1)));
    }

    fn created(error_code: i16) -> CreateTopicsResponse {
        CreateTopicsResponse {
            topics: vec![CreatableTopicResult {
                name: "dlq.g".into(),
                error_code,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// Kafka's `handleCreateTopicsResponse`: a topic that was created is done,
    /// an existing one or a throttle is tried again, and any other refusal is
    /// final.
    #[test]
    fn a_topic_creation_answer_is_done_retried_or_final() {
        let cases = [
            (Ok(created(codes::NONE)), "done"),
            (Ok(created(codes::TOPIC_ALREADY_EXISTS)), "retry"),
            (Ok(created(codes::THROTTLING_QUOTA_EXCEEDED)), "retry"),
            (Ok(created(codes::TOPIC_AUTHORIZATION_FAILED)), "fatal"),
            (Ok(created(codes::INVALID_CONFIG)), "fatal"),
            (Ok(CreateTopicsResponse::default()), "fatal"),
            (Err(TopicCreatorError::Timeout), "retry"),
            (
                Err(TopicCreatorError::Envelope(
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                )),
                "fatal",
            ),
        ];
        let expected: Vec<&str> = cases.iter().map(|(_, want)| *want).collect();

        let actual: Vec<&str> = cases
            .into_iter()
            .map(|(result, _)| outcome(&classify_create(result, "dlq.g")))
            .collect();

        assert!(actual == expected);
    }
}
