//! A share coordinator answers a share-state request only once the records of
//! its `__share_group_state` partition are committed.
//!
//! Kafka's share coordinator is a `CoordinatorRuntime` shard. The runtime
//! completes a write when the high watermark of the partition passes it, and
//! it fails the write with a timeout after `share.coordinator.write.timeout.ms`.
//! `CoordinatorOperationExceptionHelper` answers that timeout as
//! `COORDINATOR_NOT_AVAILABLE`. A coordinator that answered after the local
//! append only could acknowledge state that the next leader of the partition
//! never gets. A share-partition leader then loses or delivers again the
//! records that the state covered.
#![cfg(test)]

use {
    crate::support::{
        client::connect_owned,
        share::crash_follower,
        shutdown_cluster,
        topics::{creatable_topic, create_topic_request},
    },
    assert2::assert,
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    krabka_broker::{BrokerConfig, BrokerHandle},
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            delete_share_group_state_request::{
                DeleteShareGroupStateRequest, DeleteStateData, PartitionData as DeletePart,
            },
            delete_share_group_state_response::{
                DeleteShareGroupStateResponse, DeleteStateResult, PartitionResult as DeleteRow,
            },
            initialize_share_group_state_request::{
                InitializeShareGroupStateRequest, InitializeStateData, PartitionData as InitPart,
            },
            initialize_share_group_state_response::{
                InitializeShareGroupStateResponse, InitializeStateResult,
                PartitionResult as InitRow,
            },
            read_share_group_state_request::{
                PartitionData as ReadPart, ReadShareGroupStateRequest, ReadStateData,
            },
            read_share_group_state_response::{
                PartitionResult as ReadRow, ReadShareGroupStateResponse, ReadStateResult,
            },
            write_share_group_state_request::{
                PartitionData as WritePart, StateBatch, WriteShareGroupStateRequest, WriteStateData,
            },
            write_share_group_state_response::{
                PartitionResult as WriteRow, WriteShareGroupStateResponse, WriteStateResult,
            },
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::time::Duration,
    tempfile::TempDir,
    tokio::time,
};

mod support;

const GROUP: &str = "share-commit";
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;

/// The share coordinator's `share.coordinator.write.timeout.ms` in this
/// suite: short, so that each write that cannot commit answers quickly.
const WRITE_TIMEOUT: Duration = Duration::from_secs(1);

/// Kafka's message of a coordinator write that timed out, after the prefix
/// of the operation.
const TIMED_OUT: &str = "The request timed out.";

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

/// Three brokers with one `__share_group_state` partition, replicated on all
/// of them. A stopped broker stays in the ISR: the leader does not shrink it
/// on lag, and the controller does not fence the broker, for longer than the
/// test runs.
async fn start_three() -> Cluster {
    support::fixed_internal_isr_cluster(|config| {
        config.share_coordinator.write_timeout = WRITE_TIMEOUT;
    })
    .await
}

async fn client(handle: &BrokerHandle) -> Client {
    connect_owned(
        handle.listen_addr().to_string(),
        "share-state-commit",
        "client",
    )
    .await
}

/// Creates the one-partition data topic `orders` and returns its topic id.
/// The share coordinator refuses the state of a topic partition that the
/// metadata image does not hold.
async fn create_orders(client: &Client) -> uuid::Uuid {
    let created = client
        .send(create_topic_request(creatable_topic().call()))
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == 0, "{created:?}");
    uuid::Uuid::from_bytes(created.topics[0].topic_id.0)
}

fn wire(topic_id: uuid::Uuid) -> WireUuid { WireUuid(*topic_id.as_bytes()) }

fn position_of(cluster: &Cluster, node_id: u64) -> usize {
    cluster
        .iter()
        .position(|(handle, _, _)| handle.node_id() == node_id)
        .expect("a cluster member")
}

/// One share-state request on `(GROUP, orders, 0)`.
#[derive(Debug, Clone, Copy)]
enum Rpc {
    Initialize { state_epoch: i32 },
    Write { start_offset: i64 },
    Read,
    Delete,
}

/// The whole response of an [`Rpc`].
#[derive(Debug, PartialEq)]
enum Answer {
    Initialize(InitializeShareGroupStateResponse),
    Write(WriteShareGroupStateResponse),
    Read(ReadShareGroupStateResponse),
    Delete(DeleteShareGroupStateResponse),
}

impl Rpc {
    async fn send(self, client: &Client, topic_id: uuid::Uuid) -> Answer {
        let topic_id = wire(topic_id);
        match self {
            Self::Initialize { state_epoch } => Answer::Initialize(
                client
                    .send(InitializeShareGroupStateRequest {
                        group_id: GROUP.into(),
                        topics: vec![InitializeStateData {
                            topic_id,
                            partitions: vec![InitPart {
                                partition: 0,
                                state_epoch,
                                start_offset: 0,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .expect("InitializeShareGroupState"),
            ),
            Self::Write { start_offset } => Answer::Write(
                client
                    .send(WriteShareGroupStateRequest {
                        group_id: GROUP.into(),
                        topics: vec![WriteStateData {
                            topic_id,
                            partitions: vec![WritePart {
                                partition: 0,
                                state_epoch: 0,
                                leader_epoch: 0,
                                start_offset,
                                delivery_complete_count: 0,
                                state_batches: vec![StateBatch {
                                    first_offset: start_offset,
                                    last_offset: start_offset + 9,
                                    delivery_state: 2,
                                    delivery_count: 1,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .expect("WriteShareGroupState"),
            ),
            Self::Read => Answer::Read(
                client
                    .send(ReadShareGroupStateRequest {
                        group_id: GROUP.into(),
                        topics: vec![ReadStateData {
                            topic_id,
                            partitions: vec![ReadPart {
                                partition: 0,
                                leader_epoch: 0,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .expect("ReadShareGroupState"),
            ),
            Self::Delete => Answer::Delete(
                client
                    .send(DeleteShareGroupStateRequest {
                        group_id: GROUP.into(),
                        topics: vec![DeleteStateData {
                            topic_id,
                            partitions: vec![DeletePart {
                                partition: 0,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                        ..Default::default()
                    })
                    .await
                    .expect("DeleteShareGroupState"),
            ),
        }
    }

    /// The answer with `error_code` and, for an error, Kafka's message: the
    /// prefix of the operation and `message`.
    fn answer(self, topic_id: uuid::Uuid, error_code: i16, message: &str) -> Answer {
        let topic_id = wire(topic_id);
        let error = |operation: &str| {
            (error_code != 0).then(|| format!("Unable to {operation} share group state: {message}"))
        };
        match self {
            Self::Initialize { .. } => Answer::Initialize(InitializeShareGroupStateResponse {
                results: vec![InitializeStateResult {
                    topic_id,
                    partitions: vec![InitRow {
                        partition: 0,
                        error_code,
                        error_message: error("initialize"),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Self::Write { .. } => Answer::Write(WriteShareGroupStateResponse {
                results: vec![WriteStateResult {
                    topic_id,
                    partitions: vec![WriteRow {
                        partition: 0,
                        error_code,
                        error_message: error("write"),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Self::Read => Answer::Read(ReadShareGroupStateResponse {
                results: vec![ReadStateResult {
                    topic_id,
                    partitions: vec![ReadRow {
                        partition: 0,
                        error_code,
                        error_message: error("read"),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            Self::Delete => Answer::Delete(DeleteShareGroupStateResponse {
                results: vec![DeleteStateResult {
                    topic_id,
                    partitions: vec![DeleteRow {
                        partition: 0,
                        error_code,
                        error_message: error("delete"),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        }
    }
}

/// Initializes `(GROUP, orders, 0)` on `client`'s broker, and retries while
/// the share coordinator loads the state partition, as Kafka's persister
/// retries it.
async fn initialize_when_loaded(client: &Client, topic_id: uuid::Uuid) -> Answer {
    let rpc = Rpc::Initialize { state_epoch: 0 };
    for _ in 0..100 {
        let answer = rpc.send(client, topic_id).await;
        let Answer::Initialize(response) = &answer else {
            unreachable!("an Initialize answers an Initialize");
        };
        let code = response.results[0].partitions[0].error_code;
        if ![
            COORDINATOR_LOAD_IN_PROGRESS,
            COORDINATOR_NOT_AVAILABLE,
            NOT_COORDINATOR,
        ]
        .contains(&code)
        {
            return answer;
        }
        // intentional: the client has no awaiter for the load of the state
        // partition. It retries, as Kafka's persister does.
        time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the share coordinator did not load __share_group_state-0");
}

/// State and observations belong to one coordinator workload.
#[derive(Default)]
struct ShareStateCommit {
    cluster: Cluster,
    member: Option<Client>,
    topic_id: Option<uuid::Uuid>,
    coordinator: u64,
    stopped_dir: Option<TempDir>,
    answers: Vec<(Rpc, Answer)>,
}

#[rstest::fixture]
fn share_state_commit() -> ShareStateCommit {
    // Each scenario starts its own replicated state partition.
    ShareStateCommit::default()
}

impl ShareStateCommit {
    fn member(&self) -> &Client { self.member.as_ref().expect("coordinator connection") }

    fn topic_id(&self) -> uuid::Uuid { self.topic_id.expect("orders topic") }
}

#[rstest_bdd_macros::given(
    "orders has a three-replica share-state partition whose ISR never shrinks during the scenario"
)]
async fn replicated_share_state(share_state_commit: &mut ShareStateCommit) {
    share_state_commit.cluster = start_three().await;
    share_state_commit.member = Some(client(&share_state_commit.cluster[0].0).await);
    share_state_commit.topic_id = Some(create_orders(share_state_commit.member()).await);
    let key = format!(
        "{GROUP}:{}:0",
        URL_SAFE_NO_PAD.encode(share_state_commit.topic_id().as_bytes())
    );
    share_state_commit.coordinator = u64::try_from(
        support::find_coordinator(share_state_commit.member(), support::KEY_TYPE_SHARE, &key)
            .await
            .node_id,
    )
    .expect("a node id");
    share_state_commit
        .member
        .take()
        .expect("lookup connection")
        .close();
    share_state_commit.member = Some(
        client(
            &share_state_commit.cluster
                [position_of(&share_state_commit.cluster, share_state_commit.coordinator)]
            .0,
        )
        .await,
    );
}

#[rstest_bdd_macros::when(
    "the coordinator initializes state epoch 0 and writes start offset 0 with all replicas available"
)]
async fn commit_available_share_state(share_state_commit: &mut ShareStateCommit) {
    let initialized =
        initialize_when_loaded(share_state_commit.member(), share_state_commit.topic_id()).await;
    share_state_commit
        .answers
        .push((Rpc::Initialize { state_epoch: 0 }, initialized));
    let written = Rpc::Write { start_offset: 0 }
        .send(share_state_commit.member(), share_state_commit.topic_id())
        .await;
    share_state_commit
        .answers
        .push((Rpc::Write { start_offset: 0 }, written));
}

#[rstest_bdd_macros::then(
    "both complete share-state responses report success with no error message"
)]
fn committed_share_state(share_state_commit: &ShareStateCommit) {
    assert!(share_state_commit.answers.len() == 2);
    for (rpc, answer) in &share_state_commit.answers {
        assert!(
            *answer == rpc.answer(share_state_commit.topic_id(), 0, ""),
            "{rpc:?}"
        );
    }
}

#[rstest_bdd_macros::when(
    "a follower crashes while staying in the ISR and the coordinator writes offset 10, reads, initializes epoch 1 and deletes state"
)]
async fn uncommittable_share_state(share_state_commit: &mut ShareStateCommit) {
    share_state_commit.stopped_dir = Some(
        crash_follower(
            &mut share_state_commit.cluster,
            share_state_commit.coordinator,
        )
        .await,
    );
    share_state_commit.answers.clear();
    for rpc in [
        Rpc::Write { start_offset: 10 },
        Rpc::Read,
        Rpc::Initialize { state_epoch: 1 },
        Rpc::Delete,
    ] {
        let answer = rpc
            .send(share_state_commit.member(), share_state_commit.topic_id())
            .await;
        share_state_commit.answers.push((rpc, answer));
    }
}

#[rstest_bdd_macros::then(
    "all four complete responses report coordinator unavailable code 15 and The request timed out."
)]
fn timed_out_share_state(share_state_commit: &ShareStateCommit) {
    assert!(share_state_commit.answers.len() == 4);
    for (rpc, answer) in &share_state_commit.answers {
        assert!(
            *answer
                == rpc.answer(
                    share_state_commit.topic_id(),
                    COORDINATOR_NOT_AVAILABLE,
                    TIMED_OUT
                ),
            "{rpc:?}"
        );
    }
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/share_state_commit.feature",
    name = "a share state request is answered only when committed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_share_state_request_is_answered_only_when_committed(
    share_state_commit: ShareStateCommit,
) {
    let ShareStateCommit {
        cluster,
        member,
        stopped_dir,
        ..
    } = share_state_commit;
    member.expect("coordinator connection").close();
    shutdown_cluster(cluster).await;
    drop(stopped_dir);
}
