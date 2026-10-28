#![cfg(test)]

//! KIP-112 runtime log-dir failure path.
//!
//! The test boots a single broker with two log dirs, `primary` and `extra`,
//! and creates a 6-partition topic so that JBOD placement spreads the
//! partitions across both dirs. It then flips the `extra` dir offline through
//! the test seam and asserts that:
//!
//!   1. A Produce to a partition that lives on the now-offline `extra` dir
//!      returns `KAFKA_STORAGE_ERROR` (error code 56).
//!   2. A Produce to a partition that lives on the still-online `primary` dir
//!      returns error code 0.

mod kafka_wire;

mod support;
#[path = "support/two_dir_topic.rs"]
mod two_dir_topic;

use {
    crate::{
        support::{
            produce::encode_batch_request,
            records::{batch_from_records, value_record},
            topics::TopicPartitionCount,
            wire::ApiRequestSetup,
        },
        two_dir_topic::{Setup, create_and_wait, start},
    },
    assert2::{assert, check},
    bytes::BytesMut,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle},
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
            broker_heartbeat_request::BrokerHeartbeatRequest,
            broker_heartbeat_response::BrokerHeartbeatResponse, produce_response::ProduceResponse,
        },
        primitives::uuid::Uuid as ProtocolUuid,
    },
    std::{fs, net::SocketAddr, path::Path, time::Duration},
    tokio::{net::TcpStream, time},
};

krabka_macros::assignment_dirs_fixture!(assignment_dirs_request);

const CLIENT_ID: &str = "krabka-jbod-disk-failure-test";
const PRODUCE_VERSION: i16 = 9; // flexible, acks=1

// Flexible headers and correlation ID 1 for every request in this suite.
krabka_macros::wire_test_driver!(flexible round_trip, CLIENT_ID, 1);

/// Lists the partition indices of `topic` whose data dir lives directly under
/// `dir`.
fn partitions_in_dir(dir: &Path, topic: &str) -> Vec<i32> {
    let prefix = format!("{topic}-");
    fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter_map(|e| {
            let name = e.file_name();
            let s = name.to_str()?;
            let suffix = s.strip_prefix(&prefix)?;
            suffix.parse::<i32>().ok()
        })
        .collect()
}

/// Produces one record to `(topic, partition)` and returns the per-partition
/// `error_code` from the response.
async fn produce_and_get_error(addr: SocketAddr, topic: &str, partition: i32) -> i16 {
    let batch = batch_from_records(vec![value_record(
        0,
        Some(bytes::Bytes::from_static(b"kip-112-test")),
    )]);
    let body = encode_batch_request(batch, topic, partition, PRODUCE_VERSION);
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let resp_bytes = round_trip(
        &mut stream,
        ApiRequestSetup {
            api_key: WireApiKey(0),
            version: WireApiVersion(PRODUCE_VERSION),
        },
        &body,
    )
    .await
    .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = ProduceResponse::decode(&mut cur, PRODUCE_VERSION).unwrap();
    resp.responses[0].partition_responses[0].error_code
}

/// A running broker, the directory that backs it, and its listen address.
struct SingleDirectoryBroker {
    primary: tempfile::TempDir,
    handle: BrokerHandle,
    addr: SocketAddr,
}

/// KIP-112 and KIP-858: the controller-leader broker accepts
/// `AssignReplicasToDirs` (`api_key=73`), records the assignment, and echoes
/// the request back with `error_code=0` on every partition.
///
/// This exercises the real async `handle` path: decode, the leader gate,
/// `plan_assignments`, `submit_change`, and encode.
async fn single_directory_broker() -> SingleDirectoryBroker {
    let primary = tempfile::tempdir().unwrap();
    let config = BrokerConfig::for_tests(primary.path().to_path_buf());
    let broker = Box::pin(Broker::start(config)).await.expect("broker start");
    let address = broker.listen_addr();
    SingleDirectoryBroker {
        primary,
        handle: broker,
        addr: address,
    }
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
    tokio::sync::watch,
};
struct DirectoryFailure {
    paired: Option<(BrokerHandle, TempDir, TempDir, SocketAddr)>,
    single: Option<SingleDirectoryBroker>,
    topic: String,
    primary_partition: Option<i32>,
    extra_partition: Option<i32>,
    marked: Option<bool>,
    produced: Option<i16>,
    shutdown: Option<watch::Receiver<bool>>,
}
#[fixture]
fn failure() -> DirectoryFailure {
    // Start this scenario with fresh inputs and observations.

    DirectoryFailure {
        paired: None,
        single: None,
        topic: String::new(),
        primary_partition: None,
        extra_partition: None,
        marked: None,
        produced: None,
        shutdown: None,
    }
}
#[given("topic {topic} has six partitions spread across primary and extra log directories")]
async fn spread(failure: &mut DirectoryFailure, topic: String) {
    let paired = start()
        .client_id(CLIENT_ID)
        .topic(&topic)
        .partitions(6)
        .call()
        .await;
    let mut primary = partitions_in_dir(paired.1.path(), &topic);
    let mut extra = partitions_in_dir(paired.2.path(), &topic);
    assert!(
        !primary.is_empty(),
        "test premise: primary directory hosts a partition"
    );
    assert!(
        !extra.is_empty(),
        "test premise: extra directory hosts a partition"
    );
    primary.sort_unstable();
    extra.sort_unstable();
    failure.primary_partition = Some(primary[0]);
    failure.extra_partition = Some(extra[0]);
    failure.topic = topic;
    failure.paired = Some(paired);
}
#[given("a broker has exactly one registered log directory and its shutdown signal is observed")]
async fn single(failure: &mut DirectoryFailure) {
    let single = single_directory_broker().await;
    failure.shutdown = Some(single.handle.should_shutdown_rx());
    failure.single = Some(single);
}
#[when("the extra log directory goes offline")]
fn fail_extra(failure: &mut DirectoryFailure) {
    let (broker, _, extra, _) = failure.paired.as_ref().expect("paired directory broker");
    failure.marked = Some(broker.test_mark_log_dir_offline(extra.path()));
}
#[when("the broker's only log directory goes offline")]
fn fail_only(failure: &mut DirectoryFailure) {
    let single = failure.single.as_ref().expect("single directory broker");
    failure.marked = Some(
        single
            .handle
            .test_mark_log_dir_offline(single.primary.path()),
    );
}
#[then("the registered online directory transitions offline")]
fn transitioned(failure: &DirectoryFailure) {
    assert!(
        failure.marked == Some(true),
        "mark_offline must report an online registered directory transitioned"
    );
}
#[when("a client produces to a partition in the {directory} directory")]
async fn produce(failure: &mut DirectoryFailure, directory: String) {
    let partition = match directory.as_str() {
        "extra" => failure.extra_partition.expect("extra partition selected"),
        "primary" => failure
            .primary_partition
            .expect("primary partition selected"),
        _ => panic!("unknown directory {directory}"),
    };
    let address = failure.paired.as_ref().expect("paired directory broker").3;
    failure.produced = Some(produce_and_get_error(address, &failure.topic, partition).await);
}
#[then("the partition produce returns Kafka error code {code}")]
fn produce_result(failure: &DirectoryFailure, code: i16) {
    assert!(failure.produced == Some(code));
}
#[then("within fifteen seconds the broker latches its self shutdown signal")]
async fn shutdown_signal(failure: &mut DirectoryFailure) {
    let receiver = failure
        .shutdown
        .as_mut()
        .expect("shutdown subscribed before fault");
    let woke = time::timeout(Duration::from_secs(15), async {
        loop {
            if *receiver.borrow_and_update() {
                return;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(
        woke.is_ok(),
        "broker did not signal self shutdown when all log dirs went offline"
    );
    assert!(
        *receiver.borrow(),
        "should_shutdown must be true after all dirs offline"
    );
}
async fn shutdown_failure(failure: DirectoryFailure) {
    if let Some((broker, _primary, _extra, _address)) = failure.paired {
        broker.shutdown().await;
    }
    if let Some(SingleDirectoryBroker {
        handle, primary, ..
    }) = failure.single
    {
        handle.shutdown().await;
        drop(primary);
    }
}

struct DirectoryController {
    broker: Option<SingleDirectoryBroker>,
    topic: Option<uuid::Uuid>,
    epoch: Option<i64>,
    assignment: Option<AssignReplicasToDirsResponse>,
    heartbeat: Option<BrokerHeartbeatResponse>,
}
#[fixture]
fn controller() -> DirectoryController {
    // Start this scenario with fresh inputs and observations.

    DirectoryController {
        broker: None,
        topic: None,
        epoch: None,
        assignment: None,
        heartbeat: None,
    }
}
#[given("a controller leader broker has topic {topic} with two partitions")]
async fn assignment_topic(controller: &mut DirectoryController, topic: String) {
    let single = single_directory_broker().await;
    create_and_wait(&single.handle, single.addr, Setup {
        client_id: CLIENT_ID,
        topic: &topic,
        partitions: TopicPartitionCount(2),
        ..Default::default()
    })
    .await;
    let (id, epoch) = {
        let image = single.handle.controller_image_for_test();
        let id = image
            .topics()
            .find(|entry| entry.name == topic)
            .map(|entry| entry.topic_id)
            .expect("topic published");
        let epoch = image
            .broker(krabka_raft::NodeId(1))
            .expect("broker registered")
            .broker_epoch;
        (id, epoch)
    };
    controller.topic = Some(id);
    controller.epoch = Some(epoch);
    controller.broker = Some(single);
}
#[given("a single broker has registered its live epoch and become controller leader")]
async fn heartbeat_controller(controller: &mut DirectoryController) {
    let single = single_directory_broker().await;
    single.handle.wait_until_controller_leader().await.unwrap();
    controller.epoch = Some(
        single
            .handle
            .controller_image_for_test()
            .broker(krabka_raft::NodeId(single.handle.node_id()))
            .expect("registered broker")
            .broker_epoch,
    );
    controller.broker = Some(single);
}
impl DirectoryController {
    async fn connect(&self) -> TcpStream {
        TcpStream::connect(self.broker.as_ref().expect("controller broker").addr)
            .await
            .expect("controller connection")
    }
}

#[when("broker one assigns partition zero to directory UUID hex {directory}")]
async fn assign(controller: &mut DirectoryController, directory: String) {
    let id =
        uuid::Uuid::from_u128(u128::from_str_radix(&directory, 16).expect("hex directory UUID"));
    let request = assignment_dirs_request(DirectoryAssignmentSetup {
        broker_epoch: controller.epoch.expect("registered epoch"),
        dir: id,
        topic: controller.topic.expect("created topic"),
        ..Default::default()
    });
    let mut body = BytesMut::new();
    request.encode(&mut body, 0).unwrap();
    let mut stream = controller.connect().await;
    let response = round_trip(
        &mut stream,
        ApiRequestSetup {
            api_key: WireApiKey(73),
            version: WireApiVersion(0),
        },
        &body,
    )
    .await
    .unwrap();
    controller.assignment =
        Some(AssignReplicasToDirsResponse::decode(&mut &response[..], 0).unwrap());
}
#[then(
    "the assignment echoes a directory and reports success at both response and partition levels"
)]
fn assignment_echo(controller: &DirectoryController) {
    let response = controller
        .assignment
        .as_ref()
        .expect("AssignReplicasToDirs completed");
    check!(
        response.error_code == 0,
        "top-level assignment must succeed"
    );
    assert!(
        !response.directories.is_empty(),
        "response must echo at least one directory"
    );
    check!(
        response.directories[0].topics[0].partitions[0].error_code == 0,
        "partition assignment must succeed"
    );
}
#[when("broker one heartbeats with offline directory UUID hex {directory}")]
async fn heartbeat(controller: &mut DirectoryController, directory: String) {
    use krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION as HB_MAX_VERSION;
    let id =
        uuid::Uuid::from_u128(u128::from_str_radix(&directory, 16).expect("hex directory UUID"));
    let request = BrokerHeartbeatRequest {
        broker_id: 1,
        broker_epoch: controller.epoch.expect("registered epoch"),
        current_metadata_offset: 0,
        want_fence: false,
        want_shut_down: false,
        offline_log_dirs: vec![ProtocolUuid(id.into_bytes())],
        cordoned_log_dirs: None,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    request.encode(&mut body, HB_MAX_VERSION).unwrap();
    let mut stream = controller.connect().await;
    let response = round_trip(
        &mut stream,
        ApiRequestSetup {
            api_key: WireApiKey(63),
            version: WireApiVersion(HB_MAX_VERSION),
        },
        &body,
    )
    .await
    .unwrap();
    controller.heartbeat =
        Some(BrokerHeartbeatResponse::decode(&mut &response[..], HB_MAX_VERSION).unwrap());
}
#[then("the controller accepts the offline directory heartbeat without an ISR alternative")]
fn heartbeat_accepted(controller: &DirectoryController) {
    assert!(
        controller
            .heartbeat
            .as_ref()
            .expect("heartbeat completed")
            .error_code
            == 0
    );
}
async fn shutdown_controller(controller: DirectoryController) {
    let SingleDirectoryBroker {
        handle, primary, ..
    } = controller.broker.expect("controller broker started");
    handle.shutdown().await;
    drop(primary);
}

#[scenario(
    path = "tests/features/migrated/tests/jbod_disk_failure.feature",
    name = "A directory failure rejects its partitions while the healthy directory still accepts writes"
)]
#[tokio::test]
async fn produce_to_partition_on_offline_dir_returns_storage_error(failure: DirectoryFailure) {
    shutdown_failure(failure).await;
}

#[scenario(
    path = "tests/features/migrated/tests/jbod_disk_failure.feature",
    name = "Losing the only log directory requests broker shutdown"
)]
#[tokio::test]
async fn all_log_dirs_offline_triggers_self_shutdown(failure: DirectoryFailure) {
    shutdown_failure(failure).await;
}

#[scenario(
    path = "tests/features/migrated/tests/jbod_disk_failure.feature",
    name = "A controller accepts and echoes a live broker directory assignment"
)]
#[tokio::test]
async fn assign_replicas_to_dirs_reports_and_echoes(controller: DirectoryController) {
    shutdown_controller(controller).await;
}

#[scenario(
    path = "tests/features/migrated/tests/jbod_disk_failure.feature",
    name = "A controller accepts an offline directory heartbeat with no alternative ISR"
)]
#[tokio::test]
async fn heartbeat_with_offline_log_dirs_is_accepted(controller: DirectoryController) {
    shutdown_controller(controller).await;
}
