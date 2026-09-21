//! End-to-end test for the `BrokerHeartbeat` wire handler against a live
//! broker, plus the request builder and the leader wait it needs.
//!
//! It pins the response shape a controller leader returns for a registered,
//! caught-up broker.

use std::{sync::Arc, time::Duration};

use assert2::{assert, check};
use bytes::BytesMut;
use krabka_protocol::{
    Encode, owned::broker_heartbeat_response::BrokerHeartbeatResponse,
    primitives::uuid::Uuid as ProtocolUuid,
};

use super::*;
use crate::{codes, test_support::start_broker_with_authorizer as start_broker};

fn request(
    broker_epoch: i64,
    current_metadata_offset: i64,
    offline_log_dirs: Vec<uuid::Uuid>,
) -> Bytes {
    let req = BrokerHeartbeatRequest {
        broker_id: 1,
        broker_epoch,
        current_metadata_offset,
        want_fence: false,
        want_shut_down: false,
        offline_log_dirs: offline_log_dirs
            .into_iter()
            .map(|u| ProtocolUuid(u.into_bytes()))
            .collect(),
        cordoned_log_dirs: None,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(
        req.encoded_len(krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION),
    );
    req.encode(
        &mut buf,
        krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION,
    )
    .expect("encode BrokerHeartbeatRequest");
    buf.freeze()
}

crate::test_support::response_helpers!(
    BrokerHeartbeatResponse,
    client_id = "broker-heartbeat-test"
);

async fn wait_for_leader(broker: &Broker) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if broker
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == broker.config.node_id)
        {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Every heartbeat is authorized against its own principal, on the
/// controller listener too, as Kafka's
/// `ControllerApis.handleBrokerHeartBeatRequest` does (#684). A principal
/// without `ClusterAction` gets `CLUSTER_AUTHORIZATION_FAILED`, and one with
/// it gets the heartbeat answer.
#[tokio::test]
async fn every_heartbeat_needs_cluster_action() {
    let (broker_handle, _dir) =
        start_broker(Arc::new(crate::test_support::GrantsInPrincipalName)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
    let version = krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION;
    let broker_epoch = broker
        .controller
        .current_image()
        .broker_epoch(NodeId(1))
        .expect("broker registration should be applied");
    let req = request(broker_epoch, broker_epoch, vec![]);

    let cases = [
        ("none", codes::CLUSTER_AUTHORIZATION_FAILED),
        (
            "Cluster:Alter+Cluster:Describe",
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
        ("Cluster:ClusterAction", codes::NONE),
    ];
    for (name, error_code) in cases {
        let principal = crate::test_support::principal(name);
        let ctx = test_context(&principal, &peer);
        let response = decode_response(
            &handle(&broker, version, 11, &req, &ctx)
                .await
                .expect("BrokerHeartbeat handler"),
            version,
        );
        assert!(response.error_code == error_code, "{name}: {response:?}");
    }

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_leader_success_preserves_response_shape() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    let principal = krabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: krabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };
    let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
    let ctx = test_context(&principal, &peer);
    let version = krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION;
    let image = broker.controller.current_image();
    let broker_epoch = image
        .broker_epoch(NodeId(1))
        .expect("broker registration should be applied");
    let req = request(broker_epoch, broker_epoch, vec![]);

    let bytes = handle(&broker, version, 11, &req, &ctx)
        .await
        .expect("BrokerHeartbeat handler");
    let resp = decode_response(&bytes, version);

    let expected = BrokerHeartbeatResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        is_caught_up: true,
        is_fenced: false,
        should_shut_down: false,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");

    broker_handle.shutdown().await;
}

/// What one heartbeat was answered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Answer {
    error_code: i16,
    is_caught_up: bool,
    is_fenced: bool,
    should_shut_down: bool,
}

const fn answer(is_fenced: bool, should_shut_down: bool) -> Answer {
    Answer {
        error_code: codes::NONE,
        is_caught_up: true,
        is_fenced,
        should_shut_down,
    }
}

/// A controller, node 1, with brokers 2 and 3 registered and one topic `t`:
/// partition 0 led by 2 with the ISR `[2, 3, 1]`, partition 1 led by 3 with
/// the ISR `[3, 2]`.
struct Cluster {
    handle: crate::BrokerHandle,
    _dir: tempfile::TempDir,
    broker: Arc<Broker>,
}

impl Cluster {
    async fn start() -> Self {
        use krabka_metadata::{
            BrokerRegistrationRecord, LeaderEpoch, MetadataRecord, PartitionRecord, TopicRecord,
        };

        let (handle, dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        let registration = |node: u64| {
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                node_id: NodeId(node),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::from_u128(u128::from(node)),
                host: "127.0.0.1".into(),
                port: 19_090 + u16::try_from(node).expect("a small node id"),
                rack: None,
                endpoints: vec![],
                log_dirs: vec![uuid::Uuid::from_u128(1000 + u128::from(node))],
                features: std::collections::BTreeMap::new(),
            })
        };
        let partition = |index: i32, leader: u64, isr: &[u64]| {
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "t".into(),
                partition: index,
                leader: NodeId(leader),
                replicas: isr.iter().copied().map(NodeId).collect(),
                isr: isr.iter().copied().map(NodeId).collect(),
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![uuid::Uuid::nil(); isr.len()],
                partition_epoch: 0,
            })
        };
        broker
            .controller
            .submit_change(vec![
                registration(2),
                registration(3),
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: uuid::Uuid::from_u128(0x7),
                    partitions: 2,
                    replication_factor: 2,
                }),
                partition(0, 2, &[2, 3, 1]),
                partition(1, 3, &[3, 2]),
            ])
            .await
            .expect("seed the cluster");
        Self {
            handle,
            _dir: dir,
            broker,
        }
    }

    fn epoch(&self, node: u64) -> i64 {
        self.broker
            .controller
            .current_image()
            .broker_epoch(NodeId(node))
            .expect("a registered broker")
    }

    async fn heartbeat(&self, broker_id: i32, epoch: i64, offset: i64, shut_down: bool) -> Answer {
        let principal = krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = test_context(&principal, &peer);
        let version = krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION;
        let req = BrokerHeartbeatRequest {
            broker_id,
            broker_epoch: epoch,
            current_metadata_offset: offset,
            want_shut_down: shut_down,
            ..Default::default()
        };
        let bytes = handle(
            &self.broker,
            version,
            1,
            &crate::test_support::encode_request(&req, version),
            &ctx,
        )
        .await
        .expect("BrokerHeartbeat handler");
        let response = decode_response(&bytes, version);
        Answer {
            error_code: response.error_code,
            is_caught_up: response.is_caught_up,
            is_fenced: response.is_fenced,
            should_shut_down: response.should_shut_down,
        }
    }

    /// The leader and ISR of partition `index` of `t`.
    fn leader_and_isr(&self, index: i32) -> (u64, Vec<u64>) {
        let image = self.broker.controller.current_image();
        let partition = image.partition("t", index).expect("a seeded partition");
        (
            partition.leader.0,
            partition.isr.iter().map(|node| node.0).collect(),
        )
    }

    fn applied(&self) -> i64 {
        self.broker.controller.current_metadata_offset()
    }
}

/// krabka-io/krabka-broker#824: `BrokerHeartbeat` follows Kafka's
/// `ReplicationControlManager.processBrokerHeartbeat`.
///
/// A heartbeat from a broker id with no registration, or with a stale epoch,
/// is `STALE_BROKER_EPOCH` (`ClusterControlManager.checkBrokerEpoch`). A
/// broker that asks to shut down while it leads a partition enters controlled
/// shutdown: it leaves every ISR, not only the ones it leads, and its
/// leaderships move. It may stop only once every active broker reports a
/// metadata offset past the records that did that
/// (`BrokerHeartbeatManager.calculateNextBrokerState`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_controlled_shutdown_drains_the_isrs_and_waits_for_active_brokers() {
    let cluster = Cluster::start().await;
    let epoch_2 = cluster.epoch(2);
    let epoch_3 = cluster.epoch(3);
    let mut answers = Vec::new();
    let mut expected = Vec::new();

    let stale = Answer {
        error_code: codes::STALE_BROKER_EPOCH,
        ..success_response_default()
    };
    expected.push(("a broker id with no registration", stale));
    answers.push((
        "a broker id with no registration",
        cluster
            .heartbeat(9, epoch_2, cluster.applied(), false)
            .await,
    ));
    expected.push(("a stale broker epoch", stale));
    answers.push((
        "a stale broker epoch",
        cluster
            .heartbeat(2, epoch_2 - 1, cluster.applied(), false)
            .await,
    ));

    expected.push(("node 1 unfences", answer(false, false)));
    answers.push((
        "node 1 unfences",
        cluster
            .heartbeat(1, cluster.epoch(1), cluster.applied(), false)
            .await,
    ));
    expected.push(("broker 3 unfences", answer(false, false)));
    answers.push((
        "broker 3 unfences",
        cluster
            .heartbeat(3, epoch_3, cluster.applied(), false)
            .await,
    ));
    expected.push(("broker 2 unfences", answer(false, false)));
    answers.push((
        "broker 2 unfences",
        cluster
            .heartbeat(2, epoch_2, cluster.applied(), false)
            .await,
    ));

    expected.push(("broker 2 asks to shut down", answer(false, false)));
    answers.push((
        "broker 2 asks to shut down",
        cluster.heartbeat(2, epoch_2, cluster.applied(), true).await,
    ));
    let drained = cluster.applied();
    let (leader_0, isr_0) = cluster.leader_and_isr(0);
    let (leader_1, isr_1) = cluster.leader_and_isr(1);
    check!(
        leader_0 != 2 && !isr_0.contains(&2),
        "partition 0: {leader_0} {isr_0:?}"
    );
    check!(
        (leader_1, isr_1) == (3, vec![3]),
        "broker 2 left the ISR it only follows"
    );

    // Broker 3 is still behind the drain, and node 1, the controller's own
    // broker, has caught up to it.
    expected.push(("node 1 at the drain", answer(false, false)));
    answers.push((
        "node 1 at the drain",
        cluster.heartbeat(1, cluster.epoch(1), drained, false).await,
    ));
    expected.push(("broker 3 behind the drain", answer(false, false)));
    answers.push((
        "broker 3 behind the drain",
        cluster.heartbeat(3, epoch_3, epoch_3, false).await,
    ));
    expected.push(("broker 2 waits for broker 3", answer(false, false)));
    answers.push((
        "broker 2 waits for broker 3",
        cluster.heartbeat(2, epoch_2, cluster.applied(), true).await,
    ));

    expected.push(("broker 3 at the drain", answer(false, false)));
    answers.push((
        "broker 3 at the drain",
        cluster
            .heartbeat(3, epoch_3, cluster.applied(), false)
            .await,
    ));
    expected.push(("broker 2 may shut down", answer(true, true)));
    answers.push((
        "broker 2 may shut down",
        cluster.heartbeat(2, epoch_2, cluster.applied(), true).await,
    ));

    check!(answers == expected);
    cluster.handle.shutdown().await;
}

/// The response fields an error answer leaves at their schema defaults.
fn success_response_default() -> Answer {
    let defaults = BrokerHeartbeatResponse::default();
    Answer {
        error_code: defaults.error_code,
        is_caught_up: defaults.is_caught_up,
        is_fenced: defaults.is_fenced,
        should_shut_down: defaults.should_shut_down,
    }
}
