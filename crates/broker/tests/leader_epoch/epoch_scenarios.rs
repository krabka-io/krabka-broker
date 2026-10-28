//! Real leader-epoch append, fetch and checkpoint operations shared by boundary stories.
use {
    crate::{
        epoch_harness::{boot_single, create_topic, record, set_leader_epoch, topic_id_for},
        support::{
            client::connect_client,
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
            produce::{SinglePartitionProduceSetup, batch_request},
        },
    },
    assert2::{assert, check},
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::{
        owned::{
            fetch_request::{FetchPartition, FetchRequest, ReplicaState},
            fetch_response::FetchResponse,
        },
        primitives::uuid::Uuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::{fs, time::Duration},
    tempfile::TempDir,
    tokio::time,
};
pub struct EpochBoundary {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Client>,
    topic: String,
    id: Uuid,
    fetched: Option<FetchResponse>,
    checkpoint: Option<String>,
}
#[fixture]
pub fn epoch() -> EpochBoundary {
    // Start this scenario with fresh inputs and observations.

    EpochBoundary {
        broker: None,
        directory: None,
        client: None,
        topic: String::new(),
        id: Uuid::ZERO,
        fetched: None,
        checkpoint: None,
    }
}
#[given("a single broker hosts topic {topic} at leader epoch zero")]
async fn topic(epoch: &mut EpochBoundary, topic: String) {
    let (broker, bootstrap, directory) = boot_single().await;
    create_topic(&broker, &bootstrap, &topic).await;
    let client = connect_client(bootstrap, None).await;
    epoch.id = topic_id_for(&client, &topic).await;
    epoch.broker = Some(broker);
    epoch.directory = Some(directory);
    epoch.client = Some(client);
    epoch.topic = topic;
}
async fn append_record(epoch: &EpochBoundary, value: &str) {
    epoch
        .client
        .as_ref()
        .expect("epoch topic created")
        .send(batch_request(record(value), SinglePartitionProduceSetup {
            topic: epoch.topic.clone(),
            topic_id: epoch.id,
            ..Default::default()
        }))
        .await
        .expect("produce");
}
#[given("record {value} is already appended under leader epoch zero")]
async fn initial_record(epoch: &mut EpochBoundary, value: String) {
    append_record(epoch, &value).await;
}
#[when("a producer appends record {value} under the current leader epoch")]
async fn append(epoch: &mut EpochBoundary, value: String) { append_record(epoch, &value).await; }
#[when("the controller advances the topic leader epoch to {leader_epoch}")]
async fn advance(epoch: &mut EpochBoundary, leader_epoch: i32) {
    set_leader_epoch(
        epoch.broker.as_ref().expect("epoch broker started"),
        &epoch.topic,
        leader_epoch,
    )
    .await;
}
#[when("replica ninety nine fetches with current leader epoch {leader_epoch}")]
async fn fetch(epoch: &mut EpochBoundary, leader_epoch: i32) {
    epoch.fetched = Some(
        epoch
            .client
            .as_ref()
            .expect("epoch client connected")
            .send(FetchRequest {
                replica_id: 99,
                ..single_partition_fetch()
                    .topic(epoch.topic.clone())
                    .topic_id(epoch.id)
                    .partition(FetchPartition {
                        current_leader_epoch: leader_epoch,
                        ..fetch_partition().call()
                    })
                    .limits(FetchLimits::one_mebibyte(RequestWaitMillis(100)))
                    .call()
            })
            .await
            .expect("fetch"),
    );
}
#[then("the fetch partition returns Kafka epoch error code {code}")]
fn fenced(epoch: &EpochBoundary, code: i16) {
    assert!(
        epoch
            .fetched
            .as_ref()
            .expect("epoch fetch completed")
            .responses[0]
            .partitions[0]
            .error_code
            == code
    );
}
#[when("the leader epoch checkpoint is read from the partition directory")]
fn read_checkpoint(epoch: &mut EpochBoundary) {
    let path = epoch
        .directory
        .as_ref()
        .expect("epoch log directory")
        .path()
        .join(format!("{}-0", epoch.topic))
        .join("leader-epoch-checkpoint");
    epoch.checkpoint = Some(fs::read_to_string(path).expect("checkpoint file"));
}
#[then(
    "the checkpoint version is {version} its row count is {count} and its epoch offsets contain"
)]
fn checkpoint(epoch: &EpochBoundary, version: i32, count: usize, datatable: Vec<Vec<String>>) {
    let text = epoch.checkpoint.as_ref().expect("checkpoint read");
    check!(
        text.starts_with(&format!("{version}\n")),
        "checkpoint header: {text:?}"
    );
    check!(
        text.contains(&format!("\n{count}\n")),
        "checkpoint row count: {text:?}"
    );
    for row in datatable.into_iter().skip(1) {
        let [leader_epoch, offset]: [String; 2] = row.try_into().expect("leader epoch and offset");
        check!(
            text.contains(&format!("{leader_epoch} {offset}\n")),
            "epoch row missing: {text:?}"
        );
    }
}
pub async fn shutdown(epoch: EpochBoundary) {
    epoch.broker.expect("epoch broker started").shutdown().await;
}

#[when(
    "the controller starts leader epoch {leader_epoch} at offset {offset} with assigned follower {follower}"
)]
async fn epoch_boundary(epoch: &mut EpochBoundary, leader_epoch: i32, offset: i64, follower: u64) {
    let broker = epoch.broker.as_ref().expect("epoch broker started");
    let mut partition = broker
        .partition_record_for_test(&epoch.topic, 0)
        .expect("partition metadata");
    partition.leader_epoch = krabka_metadata::LeaderEpoch(leader_epoch);
    partition.replicas.push(krabka_broker::NodeId(follower));
    broker
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1Partition(partition))
        .await
        .expect("advance leader epoch");
    let path = epoch
        .directory
        .as_ref()
        .expect("epoch directory")
        .path()
        .join(format!("{}-0", epoch.topic))
        .join("leader-epoch-checkpoint");
    let boundary = format!("{leader_epoch} {offset}");
    time::timeout(Duration::from_secs(5), async {
        loop {
            if fs::read_to_string(&path).is_ok_and(|text| text.lines().any(|line| line == boundary))
            {
                break;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("supervisor records the new epoch boundary");
}
#[then("the leader's local log ends at offset {offset}")]
fn log_end(epoch: &EpochBoundary, offset: i64) {
    assert!(
        epoch
            .broker
            .as_ref()
            .expect("epoch broker started")
            .local_log_end_offset(&epoch.topic, 0)
            == Some(offset)
    );
}
#[when(
    "assigned follower {follower} fetches offset {offset} claiming last fetched epoch {leader_epoch}"
)]
async fn fetch_divergence(
    epoch: &mut EpochBoundary,
    follower: i32,
    offset: i64,
    leader_epoch: i32,
) {
    epoch.fetched = Some(
        epoch
            .client
            .as_ref()
            .expect("epoch client connected")
            .send(FetchRequest {
                replica_id: follower,
                replica_state: ReplicaState {
                    replica_id: follower,
                    ..Default::default()
                },
                ..single_partition_fetch()
                    .topic(epoch.topic.clone())
                    .topic_id(epoch.id)
                    .partition(FetchPartition {
                        last_fetched_epoch: leader_epoch,
                        ..fetch_partition().offset(offset).call()
                    })
                    .limits(FetchLimits::one_mebibyte(RequestWaitMillis(100)))
                    .call()
            })
            .await
            .expect("follower fetch"),
    );
}
#[then(
    "the successful fetch reports divergence epoch {leader_epoch} ending at offset {offset} without records"
)]
fn divergence(epoch: &EpochBoundary, leader_epoch: i32, offset: i64) {
    let partition = &epoch
        .fetched
        .as_ref()
        .expect("follower fetch completed")
        .responses[0]
        .partitions[0];
    check!(partition.error_code == 0, "divergence is reported in band");
    check!(partition.diverging_epoch.end_offset == offset);
    check!(partition.diverging_epoch.epoch == leader_epoch);
    check!(
        partition.records.is_none()
            || partition
                .records
                .as_ref()
                .and_then(|records| records.as_v2())
                .is_none_or(<[_]>::is_empty),
        "diverging fetch must serve no records"
    );
}
