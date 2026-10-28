//! Live consumer state for one [`MetadataEventLog::subscribe`] call.
//!
//! A subscription owns one cancellable manual-`Fetch` task per assigned
//! partition. Each task drives its own dedicated
//! [`krabka_client_core::Connection`], because the broker is serial per
//! connection and a long-`max_wait_ms` fetch on a shared socket would
//! head-of-line-block every other RPC. All tasks emit
//! [`MetadataEventRecord`]s into one shared queue, and the
//! [`AssignmentHandle`] this module implements adds and removes partitions
//! while the subscription is live.
//!
//! [`MetadataEventLog::subscribe`]: crate::log::MetadataEventLog::subscribe

use {
    crate::{
        kafka_log::{METADATA_TOPIC_SCOPE, config::MetadataEventQueueCapacity, partition_leader},
        log::{AssignmentHandle, MetadataEventRecord, PartitionStart},
    },
    krabka_client_core::{
        Client, ClientFrameMax, ConnectionDispatchQueueCapacity, FetchMinBytes, IsolatedFetch,
        security::ClientSecurity,
    },
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
    krabka_units::prelude::{ByteSize, Time, TimeExt as _},
    std::{
        collections::HashMap,
        fmt,
        future::Future,
        sync::{Arc, Mutex as StdMutex, PoisonError},
        time::Duration,
    },
    tokio::{sync::mpsc, time},
    tokio_util::sync::CancellationToken,
    tracing::{instrument, warn},
};

/// Per-subscription live consumer: one cancellable fetch task per assigned
/// partition, and every task emits into the shared `tx`.
pub(super) struct ConsumerState {
    pub(super) bootstrap: String,
    pub(super) client_id: String,
    pub(super) security: Option<ClientSecurity>,
    pub(super) topic: String,
    pub(super) topic_id: WireUuid,
    pub(super) tx: mpsc::Sender<MetadataEventRecord>,
    pub(super) fetch_max_wait: Time,
    pub(super) fetch_max_bytes: ByteSize,
    pub(super) fetch_retry_backoff: Time,
    pub(super) dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    pub(super) frame_max: ClientFrameMax,
    /// partition -> cancel token for its fetch task.
    pub(super) tasks: StdMutex<HashMap<i32, CancellationToken>>,
}

impl ConsumerState {
    pub(super) fn spawn_partition(self: &Arc<Self>, start: PartitionStart) {
        let cancel = CancellationToken::new();
        {
            let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
            if tasks.contains_key(&start.partition) {
                return; // already assigned
            }
            tasks.insert(start.partition, cancel.clone());
        }
        tokio::spawn(partition_fetch_loop(
            Arc::clone(self),
            start.partition,
            start.start_offset,
            cancel,
        ));
    }

    fn cancel_partition(&self, partition: i32) {
        let task = self
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&partition);
        if let Some(tok) = task {
            tok.cancel();
        }
    }

    pub(super) fn cancel_all(&self) {
        let mut tasks = self.tasks.lock().unwrap_or_else(PoisonError::into_inner);
        for (_, tok) in tasks.drain() {
            tok.cancel();
        }
    }
}

pub(super) struct KafkaAssignmentHandle {
    pub(super) state: Arc<ConsumerState>,
}

impl AssignmentHandle for KafkaAssignmentHandle {
    fn add(&self, start: PartitionStart) { self.state.spawn_partition(start); }

    fn remove(&self, partition: i32) { self.state.cancel_partition(partition); }

    fn assigned(&self) -> Vec<i32> {
        let mut v: Vec<i32> = self
            .state
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect();
        v.sort_unstable();
        v
    }
}

/// Both ends of a metadata event queue.
pub(super) struct MetadataEventChannel {
    pub sender: mpsc::Sender<MetadataEventRecord>,
    pub receiver: mpsc::Receiver<MetadataEventRecord>,
}

pub(super) fn metadata_event_channel(capacity: MetadataEventQueueCapacity) -> MetadataEventChannel {
    let (sender, receiver) = mpsc::channel(capacity.capacity());
    MetadataEventChannel { sender, receiver }
}

async fn connect_with_retry<T, E, F, Fut>(
    cancel: &CancellationToken,
    backoff: Duration,
    mut connect: F,
) -> Option<T>
where
    E: fmt::Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    loop {
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            result = connect() => result,
        };
        match result {
            Ok(client) => return Some(client),
            Err(error) => warn!(%error, "metadata consumer: connect failed; retrying"),
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return None,
            () = time::sleep(backoff) => {}
        }
    }
}

/// Manual single-partition fetch loop over a dedicated connection.
///
/// A dedicated connection per partition keeps the metadata consumer off any
/// parkable or shared stream. The broker is serial per-connection, so a
/// long-`max_wait_ms` fetch must not head-of-line-block other RPCs.
// cargo-mutants: live-broker fetch loop over a real connection, not unit-testable
#[cfg_attr(test, mutants::skip)]
#[instrument(level = "debug", skip_all, fields(partition, start_offset))]
async fn partition_fetch_loop(
    state: Arc<ConsumerState>,
    partition: i32,
    start_offset: i64,
    cancel: CancellationToken,
) {
    let Some(client) = connect_with_retry(&cancel, state.fetch_retry_backoff.to_std(), || {
        Client::builder()
            .bootstrap(state.bootstrap.clone())
            .client_id(state.client_id.clone())
            .dispatch_queue_capacity(state.dispatch_queue_capacity.get())
            .frame_max(state.frame_max.size())
            .maybe_security(state.security.clone())
            .metadata_scope(METADATA_TOPIC_SCOPE)
            .build()
    })
    .await
    else {
        return;
    };
    client.metadata_topics().set([state.topic.as_str()]);

    let mut next_offset = start_offset.max(0);
    let mut leader = None;
    loop {
        let current_leader = if let Some(current_leader) = leader {
            current_leader
        } else {
            match client.refresh_metadata().await {
                Ok(metadata) => {
                    leader = partition_leader(&metadata, &state.topic, partition);
                    let Some(current_leader) = leader else {
                        warn!(
                            partition,
                            "metadata consumer: partition leader is unavailable"
                        );
                        time::sleep(state.fetch_retry_backoff.to_std()).await;
                        continue;
                    };
                    current_leader
                }
                Err(error) => {
                    warn!(%error, partition, "metadata consumer: metadata refresh failed");
                    time::sleep(state.fetch_retry_backoff.to_std()).await;
                    continue;
                }
            }
        };
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return;
            }
            res = client.fetch_partition_with_isolation_on(current_leader, IsolatedFetch {
                topic: &state.topic,
                topic_id: state.topic_id,
                partition,
                fetch_offset: next_offset,
                max_wait: state.fetch_max_wait,
                max: krabka_client_core::DEFAULT_FETCH_RESPONSE_MAX,
                partition_max: state.fetch_max_bytes,
                fetch_min: FetchMinBytes::default(),
                isolation_level: 0,
            }) => {
                match res {
                    Ok(records) => {
                        for r in records {
                            // Re-check cancellation before every send: a
                            // remove() (for reassignment) that fires
                            // after fetch_partition resolved must not flush
                            // the rest of an already-fetched batch, or a
                            // task spawned on re-add from a new start_offset
                            // would double-deliver these same records.
                            if cancel.is_cancelled() {
                                return;
                            }
                            if r.offset < next_offset {
                                continue; // defensive: never go backwards
                            }
                            let tombstone = r.value.is_none();
                            let record = MetadataEventRecord {
                                partition,
                                offset: r.offset,
                                key: r.key,
                                payload: r.value.unwrap_or_default(),
                                tombstone,
                            };
                            next_offset = r.offset + 1;
                            let sent = tokio::select! {
                                biased;
                                () = cancel.cancelled() => return,
                                result = state.tx.send(record) => result,
                            };
                            if sent.is_err() {
                                return; // stream dropped
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, partition, "metadata consumer: fetch failed; retrying");
                        leader = None;
                        time::sleep(state.fetch_retry_backoff.to_std()).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::kafka_log::config::METADATA_TOPIC,
        assert2::{assert, check},
        krabka_protocol::owned::metadata_response::{
            MetadataResponse, MetadataResponsePartition, MetadataResponseTopic,
        },
        krabka_units::prelude::{mebibytes, millis},
        std::{future, time::Duration},
        tokio::{sync::Notify, task},
    };

    enum ConnectionOutcome {
        Connected(i32),
        Cancelled,
    }
    struct ConsumerConnectionScenario {
        cancel: CancellationToken,
        retry_backoff: Duration,
        failures_before_success: usize,
        attempts: usize,
        outcome: Option<ConnectionOutcome>,
    }
    #[rstest::fixture]
    fn consumer_connection() -> ConsumerConnectionScenario {
        let cancel = CancellationToken::new();
        ConsumerConnectionScenario {
            cancel,
            retry_backoff: Duration::ZERO,
            failures_before_success: 0,
            attempts: 0,
            outcome: None,
        }
    }
    #[rstest_bdd_macros::given(
        "the initial metadata connection fails once before returning client 42"
    )]
    fn transient_initial_failure(consumer_connection: &mut ConsumerConnectionScenario) {
        consumer_connection.failures_before_success = 1;
        consumer_connection.retry_backoff = Duration::ZERO;
    }
    #[rstest_bdd_macros::given("metadata connection retries wait one hour")]
    fn long_retry_delay(consumer_connection: &mut ConsumerConnectionScenario) {
        consumer_connection.retry_backoff = Duration::from_secs(3600);
    }
    #[rstest_bdd_macros::when("the metadata consumer connects without being reassigned")]
    async fn connect_consumer(consumer_connection: &mut ConsumerConnectionScenario) {
        let attempts = &mut consumer_connection.attempts;
        let failures = consumer_connection.failures_before_success;
        let client = connect_with_retry(
            &consumer_connection.cancel,
            consumer_connection.retry_backoff,
            || {
                *attempts += 1;
                future::ready(if *attempts <= failures {
                    Err("temporary connection failure")
                } else {
                    Ok(42)
                })
            },
        )
        .await;
        consumer_connection.outcome =
            Some(client.map_or(ConnectionOutcome::Cancelled, ConnectionOutcome::Connected));
    }
    #[rstest_bdd_macros::when("the consumer is cancelled while {stage}")]
    async fn cancel_connection(
        consumer_connection: &mut ConsumerConnectionScenario,
        stage: String,
    ) {
        let result = match stage.as_str() {
            "its initial connection is pending" => {
                let pending = connect_with_retry::<(), &str, _, _>(
                    &consumer_connection.cancel,
                    consumer_connection.retry_backoff,
                    future::pending,
                );
                tokio::pin!(pending);
                tokio::select! {biased;result=&mut pending=>panic!("pending connection completed: {result:?}"),()=task::yield_now()=>{}}
                consumer_connection.cancel.cancel();
                pending.await
            }
            "waiting to retry a failed connection" => {
                let started = Notify::new();
                let retrying = connect_with_retry::<(), _, _, _>(
                    &consumer_connection.cancel,
                    consumer_connection.retry_backoff,
                    || {
                        started.notify_one();
                        future::ready(Err("temporary connection failure"))
                    },
                );
                tokio::pin!(retrying);
                tokio::select! {biased;result=&mut retrying=>panic!("retry ended: {result:?}"),()=started.notified()=>{}}
                consumer_connection.cancel.cancel();
                retrying.await
            }
            _ => panic!("unknown connection stage: {stage}"),
        };
        consumer_connection.outcome = Some(match result {
            None => ConnectionOutcome::Cancelled,
            Some(()) => panic!("cancelled connection returned a client"),
        });
    }
    #[rstest_bdd_macros::then("client 42 is returned after exactly two connection attempts")]
    fn check_connection_retry(consumer_connection: &ConsumerConnectionScenario) {
        assert!(matches!(
            consumer_connection.outcome,
            Some(ConnectionOutcome::Connected(42))
        ));
        assert!(consumer_connection.attempts == 2);
    }
    #[rstest_bdd_macros::then("connection completion reports cancellation immediately")]
    fn check_connection_cancelled(consumer_connection: &ConsumerConnectionScenario) {
        assert!(matches!(
            consumer_connection.outcome,
            Some(ConnectionOutcome::Cancelled)
        ));
    }

    #[derive(Default)]
    struct ConsumerRoutingScenario {
        capacity: Option<MetadataEventQueueCapacity>,
        channel: Option<MetadataEventChannel>,
        metadata: Option<MetadataResponse>,
        leaders: Option<(Option<i32>, Option<i32>)>,
        state: Option<Arc<ConsumerState>>,
        assigned: Option<Vec<i32>>,
    }
    #[rstest::fixture]
    fn consumer_routing() -> ConsumerRoutingScenario {
        let assigned = None;
        ConsumerRoutingScenario {
            assigned,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given("the metadata event queue capacity is {capacity}")]
    fn event_capacity(consumer_routing: &mut ConsumerRoutingScenario, capacity: usize) {
        consumer_routing.capacity = Some(MetadataEventQueueCapacity::new(capacity).unwrap());
    }
    #[rstest_bdd_macros::when("the metadata event channel is allocated")]
    fn allocate_channel(consumer_routing: &mut ConsumerRoutingScenario) {
        consumer_routing.channel = Some(metadata_event_channel(
            consumer_routing.capacity.take().unwrap(),
        ));
    }
    #[rstest_bdd_macros::then("the channel can buffer 2048 metadata events")]
    fn check_channel_capacity(consumer_routing: &ConsumerRoutingScenario) {
        check!(
            consumer_routing
                .channel
                .as_ref()
                .unwrap()
                .sender
                .max_capacity()
                == 2048
        );
    }
    #[rstest_bdd_macros::given("remote metadata partition 2 is led by broker 9")]
    fn metadata_partition_leader(consumer_routing: &mut ConsumerRoutingScenario) {
        consumer_routing.metadata = Some(MetadataResponse {
            topics: vec![MetadataResponseTopic {
                name: Some(METADATA_TOPIC.into()),
                partitions: vec![MetadataResponsePartition {
                    partition_index: 2,
                    leader_id: 9,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    #[rstest_bdd_macros::when("leaders are requested for metadata partitions 2 and 1")]
    fn lookup_metadata_leaders(consumer_routing: &mut ConsumerRoutingScenario) {
        let metadata = consumer_routing.metadata.as_ref().unwrap();
        consumer_routing.leaders = Some((
            partition_leader(metadata, METADATA_TOPIC, 2),
            partition_leader(metadata, METADATA_TOPIC, 1),
        ));
    }
    #[rstest_bdd_macros::then("partition 2 routes to broker 9 and partition 1 has no leader")]
    fn check_metadata_leaders(consumer_routing: &ConsumerRoutingScenario) {
        check!(consumer_routing.leaders.unwrap() == (Some(9), None));
    }
    #[rstest_bdd_macros::given(
        "a metadata consumer has fetch wait 750ms size 2MiB and retry delay 300ms"
    )]
    fn assignment_consumer(consumer_routing: &mut ConsumerRoutingScenario) {
        let (tx, _rx) = mpsc::channel::<MetadataEventRecord>(1);
        consumer_routing.state = Some(Arc::new(ConsumerState {
            bootstrap: "invalid-bootstrap".into(),
            client_id: "test-consumer".into(),
            security: None,
            topic: METADATA_TOPIC.into(),
            topic_id: WireUuid::ZERO,
            tx,
            fetch_max_wait: millis(750),
            fetch_max_bytes: mebibytes(2),
            fetch_retry_backoff: millis(300),
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
            frame_max: ClientFrameMax::default(),
            tasks: StdMutex::new(HashMap::new()),
        }));
    }
    #[rstest_bdd_macros::when(
        "partition 2 is spawned at 7 then 9 and partition 0 is added before removing partition 2"
    )]
    async fn change_assignments(consumer_routing: &mut ConsumerRoutingScenario) {
        let state = consumer_routing.state.as_ref().unwrap();
        let handle = KafkaAssignmentHandle {
            state: Arc::clone(state),
        };
        state.spawn_partition(PartitionStart {
            partition: 2,
            start_offset: 7,
        });
        state.spawn_partition(PartitionStart {
            partition: 2,
            start_offset: 9,
        });
        handle.add(PartitionStart {
            partition: 0,
            start_offset: 0,
        });
        handle.remove(2);
        consumer_routing.assigned = Some(handle.assigned());
        state.cancel_all();
    }
    #[rstest_bdd_macros::then(
        "only partition 0 remains assigned and the configured fetch policy is retained"
    )]
    fn check_assignment_state(consumer_routing: &ConsumerRoutingScenario) {
        let state = consumer_routing.state.as_ref().unwrap();
        check!(state.fetch_max_wait == millis(750));
        check!(state.fetch_max_bytes == mebibytes(2));
        check!(state.fetch_retry_backoff == millis(300));
        assert!(consumer_routing.assigned.as_ref().unwrap() == &vec![0]);
    }
    rstest_bdd_macros::scenarios!("tests/features/consumer_routing",fixtures=[consumer_routing:ConsumerRoutingScenario]);
    #[rstest_bdd_macros::scenario(
        path = "tests/features/consumer_lifecycle/lifecycle.feature",
        name = "Initial connection retries without reassignment"
    )]
    #[tokio::test]
    async fn initial_connection_retries_without_reassignment(
        consumer_connection: ConsumerConnectionScenario,
    ) {
        let _ = consumer_connection;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/consumer_lifecycle/lifecycle.feature",
        name = "Cancellation interrupts connection and retry waits"
    )]
    #[tokio::test]
    async fn initial_connection_and_retry_delay_are_cancellable(
        consumer_connection: ConsumerConnectionScenario,
        stage: String,
    ) {
        let _ = (consumer_connection, stage);
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/consumer_lifecycle/lifecycle.feature",
        name = "Assignments track replacement and removal of partition tasks"
    )]
    #[tokio::test]
    async fn assignment_handle_tracks_spawned_partitions(
        consumer_routing: ConsumerRoutingScenario,
    ) {
        let _ = consumer_routing;
    }
}
