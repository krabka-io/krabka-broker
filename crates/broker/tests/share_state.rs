//! End-to-end integration tests for the KIP-932 share coordinator (persister).
//! The tests run against an in-process Krabka broker with `krabka-client-core`.
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 83-87.
//! Each `*ShareGroupState*Request` impls `ProtocolRequest`, so
//! `client.send(req)` exercises the real wire path. That path includes version
//! negotiation through `ApiVersions`.
//!
//! Timing note: the raw persister RPC handlers do NOT create
//! `__share_group_state`. `FindCoordinator(SHARE)` does. The first lookup asks
//! for the topic and answers `COORDINATOR_NOT_AVAILABLE`, and a client retries
//! it. After the broker creates the topic, it materializes and leads the
//! topic's partitions asynchronously in the replicator supervisor. So the first
//! `Initialize` may briefly return a coordinator-not-ready code. The `*_ready`
//! helpers retry, exactly as a real client would.
#![cfg(test)]

mod support;

use {
    crate::support::{
        client::{connect_client, create_topic_with},
        discovery::coordinator_lookup_request,
        topics::CreateTopicSetup,
    },
    assert2::{assert, check},
    krabka_broker::{BootstrapMode, Broker, BrokerConfig},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        delete_share_group_state_request::{
            DeleteShareGroupStateRequest, DeleteStateData, PartitionData as DeletePart,
        },
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::Coordinator,
        initialize_share_group_state_request::{
            InitializeShareGroupStateRequest, InitializeStateData, PartitionData as InitPart,
        },
        read_share_group_state_request::{
            PartitionData as ReadPart, ReadShareGroupStateRequest, ReadStateData,
        },
        read_share_group_state_response,
        read_share_group_state_summary_request::{
            PartitionData as SummaryPart, ReadShareGroupStateSummaryRequest, ReadStateSummaryData,
        },
        read_share_group_state_summary_response,
        write_share_group_state_request::{
            PartitionData as WritePart, StateBatch, WriteShareGroupStateRequest, WriteStateData,
        },
    },
    std::{sync::Arc, time::Duration},
    tokio::time,
};

const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;
const FENCED_STATE_EPOCH: i16 = 124;
const INVALID_REQUEST: i16 = 42;

fn not_ready(code: i16) -> bool {
    code == COORDINATOR_LOAD_IN_PROGRESS
        || code == COORDINATOR_NOT_AVAILABLE
        || code == NOT_COORDINATOR
}

use crate::support::boot_single as boot;

async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(connect_client(bootstrap, Some("c1")).await)
}

/// Create a one-partition data topic and return its topic id. The share
/// coordinator refuses a read or a write of a topic partition that the
/// metadata image does not hold.
async fn create_topic(client: &Client, name: &str) -> uuid::Uuid {
    let id = create_topic_with(client, CreateTopicSetup {
        topic: name,
        ..Default::default()
    })
    .await;
    uuid::Uuid::from_bytes(id.0)
}

use crate::support::share::{coordinator_key as share_coordinator_key, wire};

/// Sends one `FindCoordinator(SHARE)` for `key` and returns its row. A lookup
/// before `__share_group_state` exists asks for the topic.
async fn find_share_once(client: &Client, key: &str) -> Coordinator {
    let resp = client
        .send(coordinator_lookup_request(
            FindCoordinatorRequest::default().key,
            support::KEY_TYPE_SHARE,
            vec![key.to_owned()],
        ))
        .await
        .expect("FindCoordinator(SHARE)");
    let [row] = resp.coordinators.as_slice() else {
        panic!("one coordinator row: {resp:?}");
    };
    row.clone()
}

/// Initialize one (group, topic, partition). This helper retries while the
/// coordinator still materializes the state partition. Returns the final
/// `error_code`.
async fn initialize_ready(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
    state_epoch: i32,
    start_offset: i64,
) -> i16 {
    for _ in 0..40 {
        let resp = client
            .send(InitializeShareGroupStateRequest {
                group_id: group.into(),
                topics: vec![InitializeStateData {
                    topic_id: wire(tid),
                    partitions: vec![InitPart {
                        partition,
                        state_epoch,
                        start_offset,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("InitializeShareGroupState");
        let code = resp.results[0].partitions[0].error_code;
        if !not_ready(code) {
            return code;
        }
        // intentional: bounded retry of the Initialize RPC while the share
        // coordinator is still asynchronously loading/materializing the
        // __share_group_state partition. This helper holds only a `Client`
        // (no `BrokerHandle`), and the awaited condition is coordinator LOAD
        // state (COORDINATOR_LOAD_IN_PROGRESS) — not share-partition state, so
        // the wait_until_share_* awaiters don't apply (they observe SPSO /
        // delivery / acquired state that this very Initialize creates).
        time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share coordinator never became ready for {group}:{tid}:{partition}");
}

struct StateWrite<'a> {
    group: &'a str,
    topic_id: uuid::Uuid,
    partition: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: i64,
    delivery_complete_count: i32,
    batches: Vec<StateBatch>,
}

async fn write_state(client: &Client, write: StateWrite<'_>) -> i16 {
    let resp = client
        .send(WriteShareGroupStateRequest {
            group_id: write.group.into(),
            topics: vec![WriteStateData {
                topic_id: wire(write.topic_id),
                partitions: vec![WritePart {
                    partition: write.partition,
                    state_epoch: write.state_epoch,
                    leader_epoch: write.leader_epoch,
                    start_offset: write.start_offset,
                    delivery_complete_count: write.delivery_complete_count,
                    state_batches: write.batches,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("WriteShareGroupState");
    resp.results[0].partitions[0].error_code
}

async fn read_summary(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) -> read_share_group_state_summary_response::PartitionResult {
    let resp = client
        .send(ReadShareGroupStateSummaryRequest {
            group_id: group.into(),
            topics: vec![ReadStateSummaryData {
                topic_id: wire(tid),
                partitions: vec![SummaryPart {
                    partition,
                    leader_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("ReadShareGroupStateSummary");
    resp.results[0].partitions[0].clone()
}

/// [`read_summary`], retried while the share coordinator answers with a
/// coordinator-not-ready code, as a Kafka client retries a retriable error.
/// After a restart the request path can briefly lag the recovered state.
async fn read_summary_ready(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) -> read_share_group_state_summary_response::PartitionResult {
    for _ in 0..40 {
        let result = read_summary(client, group, tid, partition).await;
        if !not_ready(result.error_code) {
            return result;
        }
        // intentional: bounded retry while the restarted coordinator still
        // takes over `__share_group_state`; see `initialize_ready`.
        time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share coordinator never became ready for {group}:{tid}:{partition}");
}

async fn read_state(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) -> read_share_group_state_response::PartitionResult {
    let resp = client
        .send(ReadShareGroupStateRequest {
            group_id: group.into(),
            topics: vec![ReadStateData {
                topic_id: wire(tid),
                partitions: vec![ReadPart {
                    partition,
                    leader_epoch: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("ReadShareGroupState");
    resp.results[0].partitions[0].clone()
}

/// The state epoch and start offset a share partition is initialized with.
#[derive(Clone, Copy)]
struct InitialState {
    state_epoch: i32,
    start_offset: i64,
}

/// Resolve and initialize a share partition before its first write.
async fn initialize_partition(
    client: &Client,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
    InitialState {
        state_epoch,
        start_offset,
    }: InitialState,
) {
    support::find_coordinator(
        client,
        support::KEY_TYPE_SHARE,
        &share_coordinator_key(group, tid, partition),
    )
    .await;
    let init = initialize_ready(client, group, tid, partition, state_epoch, start_offset).await;
    assert!(init == 0, "initialize error: {init}");
}

use {
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct SharePersister {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Arc<Client>>,
    topic: Option<uuid::Uuid>,
    key: String,
    coordinator: Option<Coordinator>,
    write_code: Option<i16>,
    delete_code: Option<i16>,
    read: Option<read_share_group_state_response::PartitionResult>,
    summary: Option<read_share_group_state_summary_response::PartitionResult>,
}

#[fixture]
fn share_persister() -> SharePersister {
    // Each wire operation records a separate state or coordinator observation.
    SharePersister::default()
}

impl SharePersister {
    fn client(&self) -> &Client { self.client.as_ref().expect("persister client") }

    fn broker(&self) -> &BrokerHandle { self.broker.as_ref().expect("persister broker") }

    fn topic(&self) -> uuid::Uuid { self.topic.expect("data topic identity") }
}

#[given("a fresh broker with no share coordinator lookup yet")]
async fn new_share_persister(share_persister: &mut SharePersister) {
    let (broker, bootstrap, directory) = boot().await;
    share_persister.client = Some(connect(&bootstrap).await);
    share_persister.broker = Some(broker);
    share_persister.directory = Some(directory);
}

#[given("a durable broker ready to serve share-state recovery")]
async fn durable_share_persister(share_persister: &mut SharePersister) {
    let directory = TempDir::new().expect("restart tempdir");
    let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
        .await
        .unwrap();
    broker.wait_until_share_coordinator_ready().await;
    share_persister.client = Some(connect(&broker.listen_addr().to_string()).await);
    share_persister.broker = Some(broker);
    share_persister.directory = Some(directory);
}

#[given("g1's topic {topic} partition zero is initialized at state epoch {epoch} and start zero")]
async fn initialized_persister_partition(
    share_persister: &mut SharePersister,
    topic: String,
    epoch: i32,
) {
    let identity = create_topic(share_persister.client(), &topic).await;
    initialize_partition(share_persister.client(), "g1", identity, 0, InitialState {
        state_epoch: epoch,
        start_offset: 0,
    })
    .await;
    share_persister.topic = Some(identity);
}

#[when(
    "g1 first looks up a share coordinator for topic UUID 03030303030303030303030303030303 partition zero"
)]
async fn initial_share_coordinator(share_persister: &mut SharePersister) {
    share_persister.key = share_coordinator_key("g1", uuid::Uuid::from_bytes([3u8; 16]), 0);
    share_persister.coordinator =
        Some(find_share_once(share_persister.client(), &share_persister.key).await);
}

#[then("the first share coordinator answer contains no node and COORDINATOR_NOT_AVAILABLE")]
fn unavailable_share_coordinator(share_persister: &SharePersister) {
    check!(
        share_persister
            .coordinator
            .as_ref()
            .expect("lookup answered")
            == &Coordinator {
                key: share_persister.key.clone(),
                node_id: -1,
                host: String::new(),
                port: -1,
                error_code: COORDINATOR_NOT_AVAILABLE,
                error_message: None,
                ..Default::default()
            }
    );
}

#[when("g1 retries the same share coordinator lookup until it is available")]
async fn retry_share_coordinator(share_persister: &mut SharePersister) {
    share_persister.coordinator = Some(
        support::find_coordinator(
            share_persister.client(),
            support::KEY_TYPE_SHARE,
            &share_persister.key,
        )
        .await,
    );
}

#[then("the share coordinator answer names the running broker's exact listener")]
fn available_share_coordinator(share_persister: &SharePersister) {
    let broker = share_persister.broker();
    let address = broker.listen_addr();
    check!(
        share_persister
            .coordinator
            .as_ref()
            .expect("retry answered")
            == &Coordinator {
                key: share_persister.key.clone(),
                node_id: i32::try_from(broker.node_id()).unwrap(),
                host: address.ip().to_string(),
                port: i32::from(address.port()),
                error_code: 0,
                error_message: None,
                ..Default::default()
            }
    );
}

#[when(
    "the persister writes state epoch {epoch}, start {start}, complete count {complete}, and acquired batch {batch}"
)]
async fn write_persister_state(
    share_persister: &mut SharePersister,
    epoch: i32,
    start: i64,
    complete: i32,
    batch: String,
) {
    let batches = if batch == "none" {
        vec![]
    } else {
        let (first, last) = batch.split_once("..").expect("acquired offset range");
        vec![StateBatch {
            first_offset: first.parse().unwrap(),
            last_offset: last.parse().unwrap(),
            delivery_state: 2,
            delivery_count: 1,
            ..Default::default()
        }]
    };
    share_persister.write_code = Some(
        write_state(share_persister.client(), StateWrite {
            group: "g1",
            topic_id: share_persister.topic(),
            partition: 0,
            state_epoch: epoch,
            leader_epoch: 0,
            start_offset: start,
            delivery_complete_count: complete,
            batches,
        })
        .await,
    );
}

#[then("the share-state write reports {error}")]
fn persister_write_code(share_persister: &SharePersister, error: String) {
    let code = match error.as_str() {
        "NONE" => 0,
        "FENCED_STATE_EPOCH" => FENCED_STATE_EPOCH,
        _ => panic!("unknown write error {error}"),
    };
    assert!(share_persister.write_code == Some(code));
}

#[when("the full state of g1's partition is read")]
async fn read_persister_state(share_persister: &mut SharePersister) {
    share_persister.read =
        Some(read_state(share_persister.client(), "g1", share_persister.topic(), 0).await);
}

#[then("the full share state has start five and includes acquired offsets five through nine")]
fn persister_state_round_trip(share_persister: &SharePersister) {
    let state = share_persister.read.as_ref().expect("full state read");
    check!(state.error_code == 0);
    check!(state.start_offset == 5);
    check!(
        state
            .state_batches
            .iter()
            .any(|batch| batch.first_offset == 5 && batch.last_offset == 9)
    );
}

#[when("the summary of g1's partition is read")]
async fn read_persister_summary(share_persister: &mut SharePersister) {
    share_persister.summary =
        Some(read_summary_ready(share_persister.client(), "g1", share_persister.topic(), 0).await);
}

#[then("the summary reports state epoch zero, start five, and complete count two")]
fn persister_summary_round_trip(share_persister: &SharePersister) {
    let summary = share_persister.summary.as_ref().expect("summary read");
    check!(summary.error_code == 0);
    check!(summary.start_offset == 5);
    check!(summary.state_epoch == 0);
    check!(summary.delivery_complete_count == 2);
}

#[when("g1's share-state partition is deleted")]
async fn delete_persister_state(share_persister: &mut SharePersister) {
    let response = share_persister
        .client()
        .send(DeleteShareGroupStateRequest {
            group_id: "g1".into(),
            topics: vec![DeleteStateData {
                topic_id: wire(share_persister.topic()),
                partitions: vec![DeletePart {
                    partition: 0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("DeleteShareGroupState");
    share_persister.delete_code = Some(response.results[0].partitions[0].error_code);
}

#[then("the share-state deletion succeeds")]
fn persister_state_deleted(share_persister: &SharePersister) {
    assert!(share_persister.delete_code == Some(0));
}

#[then("reading the deleted share partition returns the complete uninitialized-partition refusal")]
fn deleted_persister_state(share_persister: &SharePersister) {
    assert!(
        share_persister.read.as_ref().expect("deleted state read")
            == &read_share_group_state_response::PartitionResult {
                partition: 0,
                error_code: INVALID_REQUEST,
                error_message: Some(
                    "Read operation on uninitialized share partition not allowed.".to_owned()
                ),
                ..Default::default()
            }
    );
}

#[when("start offset seven is persisted and the share-state broker restarts on the same directory")]
async fn restart_persister_state(share_persister: &mut SharePersister) {
    share_persister
        .broker()
        .wait_until_share_spso("g1", share_persister.topic(), 0, 7)
        .await;
    share_persister
        .broker
        .take()
        .expect("first broker")
        .shutdown()
        .await;
    let mut config = BrokerConfig::for_tests(
        share_persister
            .directory
            .as_ref()
            .expect("durable directory")
            .path()
            .to_path_buf(),
    );
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let broker = Broker::start(config).await.unwrap();
    share_persister.client = Some(connect(&broker.listen_addr().to_string()).await);
    broker
        .wait_until_share_spso("g1", share_persister.topic(), 0, 7)
        .await;
    share_persister.broker = Some(broker);
}

#[then("the recovered share-state summary succeeds with start offset seven")]
fn recovered_persister_state(share_persister: &SharePersister) {
    let summary = share_persister
        .summary
        .as_ref()
        .expect("recovered summary read");
    assert!(
        (summary.error_code, summary.start_offset) == (0, 7),
        "{summary:?}"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/share_state.feature",
    name = "find coordinator share creates topic and returns broker"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_coordinator_share_creates_topic_and_returns_broker(share_persister: SharePersister) {
    share_persister
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_persister);
}

#[scenario(
    path = "tests/features/migrated/tests/share_state.feature",
    name = "persister round trip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persister_round_trip(share_persister: SharePersister) {
    share_persister
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_persister);
}

#[scenario(
    path = "tests/features/migrated/tests/share_state.feature",
    name = "write fences stale state epoch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_fences_stale_state_epoch(share_persister: SharePersister) {
    share_persister
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_persister);
}

#[scenario(
    path = "tests/features/migrated/tests/share_state.feature",
    name = "state survives restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_survives_restart(share_persister: SharePersister) {
    share_persister
        .broker
        .take()
        .expect("share broker")
        .shutdown()
        .await;
    drop(share_persister);
}
