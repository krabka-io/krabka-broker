//! `FetchSnapshot` (`api_key=59`, KIP-630). Serves a byte range of the
//! controller's `__cluster_metadata` snapshot to a replica catching up.
//!
//! Krabka runs a single raft log, the controller quorum, and snapshots its
//! `MetadataImage`. A replica fetches the snapshot one page at a time as it
//! advances `position`. Each response carries the requested byte range
//! verbatim, plus the snapshot's `(end_offset, epoch)` id and total `size`.
//!
//! The broker listener does not answer the request itself. It hands the body to
//! the controller's engine, which is what the controller listener does too, so
//! both listeners give one answer to one request: Kafka's
//! `KafkaRaftClient.handleFetchSnapshotRequest`. The engine serves exactly the
//! snapshot the request names and answers `SNAPSHOT_NOT_FOUND` for any other,
//! `UNKNOWN_TOPIC_OR_PARTITION` for another topic or partition,
//! `FENCED_LEADER_EPOCH`, `UNKNOWN_LEADER_EPOCH` or `NOT_LEADER_OR_FOLLOWER`
//! for a `CurrentLeaderEpoch` that this node cannot serve, and
//! `POSITION_OUT_OF_RANGE` for a position outside the snapshot. It accepts the
//! cluster id in Kafka's base64 `Uuid` form as well as the hyphenated one, and
//! fills `CurrentLeader` and the leader's endpoint into the response.
//!
//! The returned bytes are *unaligned*. A snapshot is many concatenated
//! record batches, and a paged byte range is by design not batch-aligned.
//!
//! A broker-only observer runs no engine and keeps no metadata log to serve.
//! It answers `SNAPSHOT_NOT_FOUND` for the metadata partition and
//! `UNKNOWN_TOPIC_OR_PARTITION` for any other.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        broker::Broker,
        codes,
        handlers::{
            ApiVersion, DispatchResult, RequestContext, cluster_action_denied, encode_response,
        },
    },
    bytes::Bytes,
    krabka_protocol::owned::{
        fetch_snapshot_request::FetchSnapshotRequest,
        fetch_snapshot_response::{FetchSnapshotResponse, PartitionSnapshot, TopicSnapshot},
    },
};

/// The single Kafka-side topic name that represents the `KRaft` metadata
/// log. It mirrors
/// `org.apache.kafka.common.Topic.CLUSTER_METADATA_TOPIC_NAME`.
const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

/// Checks `ClusterAction` on the cluster, then serves the byte range.
///
/// The snapshot is the serialized metadata image, with every ACL, config,
/// SCRAM credential and delegation token record. Kafka's
/// `ControllerApis.handleFetchSnapshot` calls
/// `authorizeClusterOperation(request, CLUSTER_ACTION)` first, and a denial
/// becomes `FetchSnapshotRequest.getErrorResponse`: a top-level
/// `CLUSTER_AUTHORIZATION_FAILED` and no topics.
#[krabka_macros::decode_dispatch(request = FetchSnapshotRequest, binding(req))]
pub async fn handle(
    broker: &Broker,
    version: ApiVersion,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> DispatchResult {
    if cluster_action_denied(
        broker.config.authorizer.as_ref(),
        &broker.controller.current_image(),
        ctx,
    ) {
        return encode_response(
            &FetchSnapshotResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            },
            version,
        );
    }
    match broker
        .controller
        .fetch_snapshot(version, Bytes::copy_from_slice(req_bytes))
        .await
    {
        Some(answer) => Ok(answer?),
        None => encode_response(&without_metadata_log(&req), version),
    }
}

/// The answer of a node that keeps no metadata log: no snapshot of the
/// metadata partition to serve, and no such partition for anything else.
fn without_metadata_log(req: &FetchSnapshotRequest) -> FetchSnapshotResponse {
    let topics = req
        .topics
        .iter()
        .map(|topic| TopicSnapshot {
            name: topic.name.clone(),
            partitions: topic
                .partitions
                .iter()
                .map(|part| PartitionSnapshot {
                    index: part.partition,
                    error_code: if topic.name == CLUSTER_METADATA_TOPIC && part.partition == 0 {
                        codes::SNAPSHOT_NOT_FOUND
                    } else {
                        codes::UNKNOWN_TOPIC_OR_PARTITION
                    },
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    FetchSnapshotResponse {
        topics,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{
            authorizer::AllowAllAuthorizer,
            test_support::{
                GrantsInPrincipalName, decode_response, dispatch_context, encode_request, peer,
                principal, request_context, start_broker_no_audit_with,
            },
        },
        assert2::{assert, check},
        base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
        krabka_protocol::owned::{
            fetch_snapshot_request::{
                self, PartitionSnapshot as ReqPartition, SnapshotId as ReqSnapshotId,
                TopicSnapshot as ReqTopic,
            },
            fetch_snapshot_response::{self, LeaderIdAndEpoch},
        },
        krabka_raft::SnapshotRange,
        std::{
            net::SocketAddr,
            sync::Arc,
            time::{Duration, Instant},
        },
        tokio::time,
    };

    /// A request for partition 0 of the metadata topic, at the given id,
    /// leader epoch and position.
    fn request(
        snapshot_id: ReqSnapshotId,
        current_leader_epoch: i32,
        position: i64,
    ) -> FetchSnapshotRequest {
        FetchSnapshotRequest {
            replica_id: -1,
            max_bytes: 16,
            topics: vec![ReqTopic {
                name: CLUSTER_METADATA_TOPIC.into(),
                partitions: vec![ReqPartition {
                    partition: 0,
                    current_leader_epoch,
                    snapshot_id,
                    position,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    async fn snapshot_wire(
        grants: &str,
        broker: &Broker,
        version: i16,
        address: &SocketAddr,
    ) -> Bytes {
        let user = principal(grants);
        let context = request_context(&user, address, "snapshot-test");
        dispatch_context(
            broker,
            fetch_snapshot_request::API_KEY,
            version,
            &encode_request(&request(ReqSnapshotId::default(), 0, 0), version),
            &context,
        )
        .await
    }

    /// A broker listener serves `FetchSnapshot` only to a principal with
    /// `ClusterAction` on the cluster (#682). Kafka's
    /// `ControllerApis.handleFetchSnapshot` authorizes first, and a denial is
    /// `FetchSnapshotRequest.getErrorResponse`: a top-level
    /// `CLUSTER_AUTHORIZATION_FAILED` and no snapshot bytes.
    #[tokio::test]
    async fn fetch_snapshot_needs_cluster_action() {
        let HandlerStartedBroker {
            broker: handle,
            dir: _dir,
        } = start_broker_no_audit_with(|config| {
            config.authorizer = Arc::new(GrantsInPrincipalName);
        })
        .await;
        let broker = handle.broker_arc_for_test();
        let refused = FetchSnapshotResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };

        let address = peer();
        for version in [
            fetch_snapshot_response::MIN_VERSION,
            fetch_snapshot_response::MAX_VERSION,
        ] {
            for grants in ["none", "Cluster:Describe+Cluster:Alter+Topic:Read"] {
                let bytes = snapshot_wire(grants, &broker, version, &address).await;
                check!(
                    decode_response::<FetchSnapshotResponse>(&bytes, version) == refused,
                    "v{version} {grants}"
                );
            }
            // With ClusterAction the request reaches the engine, which answers
            // a partition of its own.
            let bytes = snapshot_wire("Cluster:ClusterAction", &broker, version, &address).await;
            let served = decode_response::<FetchSnapshotResponse>(&bytes, version);
            check!(served.error_code == codes::NONE, "v{version}");
            check!(served.topics.len() == 1, "v{version}");
        }
        handle.shutdown().await;
    }

    #[derive(Default)]
    struct SnapshotFetchScenario {
        started: Option<HandlerStartedBroker>,
        snapshot: ReqSnapshotId,
        size: i64,
        first_page: Bytes,
        leader: LeaderIdAndEpoch,
        request: Option<FetchSnapshotRequest>,
        response: Option<FetchSnapshotResponse>,
    }
    #[rstest::fixture]
    fn snapshot_fetch() -> SnapshotFetchScenario {
        let started = None;
        SnapshotFetchScenario {
            started,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "an allow-all controller has a completed metadata snapshot and a captured first 16-byte page"
    )]
    async fn completed_metadata_snapshot(snapshot_fetch: &mut SnapshotFetchScenario) {
        let started = start_broker_no_audit_with(|config| {
            config.authorizer = Arc::new(AllowAllAuthorizer);
        })
        .await;
        let broker = started.broker.broker_arc_for_test();
        started
            .broker
            .trigger_snapshot_for_test()
            .await
            .expect("trigger snapshot");
        let deadline = Instant::now() + Duration::from_secs(30);
        let slice = loop {
            if let SnapshotRange::Slice(slice) = broker.controller.read_snapshot_range(0, 16) {
                break slice;
            }
            assert!(
                Instant::now() <= deadline,
                "no metadata snapshot within 30 seconds"
            );
            time::sleep(Duration::from_millis(25)).await;
        };
        snapshot_fetch.snapshot = ReqSnapshotId {
            end_offset: slice.end_offset,
            epoch: slice.epoch,
            ..Default::default()
        };
        snapshot_fetch.size = slice.total_size;
        snapshot_fetch.first_page = slice.bytes;
        snapshot_fetch.leader = LeaderIdAndEpoch {
            leader_id: i32::try_from(broker.config.node_id.0).expect("wire node ID"),
            leader_epoch: i32::try_from(broker.controller.quorum_state().current_term)
                .expect("wire leader epoch"),
            ..Default::default()
        };
        snapshot_fetch.started = Some(started);
    }
    #[rstest_bdd_macros::when(
        "FetchSnapshot reads the captured snapshot with these request fields"
    )]
    async fn fetch_metadata_snapshot(
        snapshot_fetch: &mut SnapshotFetchScenario,
        datatable: Vec<Vec<String>>,
    ) {
        let broker = snapshot_fetch
            .started
            .as_ref()
            .expect("controller snapshot")
            .broker
            .broker_arc_for_test();
        let mut req = request(
            snapshot_fetch.snapshot.clone(),
            snapshot_fetch.leader.leader_epoch,
            0,
        );
        for cells in datatable.into_iter().skip(1) {
            let value = cells[1].as_str();
            match cells[0].as_str() {
                "topic" => req.topics[0].name = value.to_owned(),
                "partition" => {
                    req.topics[0].partitions[0].partition = value.parse().expect("partition")
                }
                "end offset delta" => {
                    req.topics[0].partitions[0].snapshot_id.end_offset +=
                        value.parse::<i64>().expect("end offset delta")
                }
                "snapshot epoch delta" => {
                    req.topics[0].partitions[0].snapshot_id.epoch +=
                        value.parse::<i32>().expect("snapshot epoch delta")
                }
                "leader epoch delta" => {
                    req.topics[0].partitions[0].current_leader_epoch +=
                        value.parse::<i32>().expect("leader epoch delta")
                }
                "position" => {
                    req.topics[0].partitions[0].position = if value == "snapshot size" {
                        snapshot_fetch.size
                    } else {
                        value.parse().expect("position")
                    }
                }
                "cluster ID" => {
                    let id = broker.controller.current_image().cluster_id();
                    req.cluster_id = match value {
                        "omitted" => None,
                        "base64" => Some(URL_SAFE_NO_PAD.encode(id.as_bytes())),
                        "hyphenated" => Some(id.to_string()),
                        other => Some(other.to_owned()),
                    };
                }
                other => panic!("unknown snapshot request field {other}"),
            }
        }
        krabka_macros::request_test_identity! { bind(principal, address, ctx); user ["admin"]; client ["snapshot-test"]; };
        let version = fetch_snapshot_response::MAX_VERSION;
        let bytes = super::handle(&broker, version, &encode_request(&req, version), &ctx)
            .await
            .expect("handle snapshot fetch");
        snapshot_fetch.response = Some(decode_response::<FetchSnapshotResponse>(&bytes, version));
        snapshot_fetch.request = Some(req);
    }
    #[rstest_bdd_macros::then(
        "snapshot fetch returns top-level code {top}, partition code {partition} and current leader present {leader}"
    )]
    async fn snapshot_fetch_response(
        snapshot_fetch: &mut SnapshotFetchScenario,
        top: i16,
        partition: i16,
        leader: bool,
    ) {
        let response = snapshot_fetch.response.as_ref().expect("snapshot response");
        check!(response.error_code == top);
        if top != codes::NONE {
            check!(response.topics.is_empty());
            snapshot_fetch
                .started
                .take()
                .expect("snapshot controller")
                .broker
                .shutdown()
                .await;
            return;
        }
        let [topic] = response.topics.as_slice() else {
            panic!("expected one topic row: {response:?}")
        };
        let [part] = topic.partitions.as_slice() else {
            panic!("expected one partition row: {response:?}")
        };
        let requested = snapshot_fetch.request.as_ref().expect("snapshot request");
        check!(topic.name == requested.topics[0].name);
        check!(part.index == requested.topics[0].partitions[0].partition);
        check!(part.error_code == partition);
        check!(
            part.current_leader
                == if leader {
                    snapshot_fetch.leader.clone()
                } else {
                    LeaderIdAndEpoch::default()
                }
        );
        snapshot_fetch
            .started
            .take()
            .expect("snapshot controller")
            .broker
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::then(
        "the snapshot response preserves the captured identity, total size, position zero and exact first-page bytes"
    )]
    fn snapshot_fetch_page(snapshot_fetch: &SnapshotFetchScenario) {
        let part = &snapshot_fetch
            .response
            .as_ref()
            .expect("snapshot response")
            .topics[0]
            .partitions[0];
        let mut page = bytes::BytesMut::new();
        part.unaligned_records
            .encode_to(&mut page)
            .expect("page bytes");
        check!(part.snapshot_id.end_offset == snapshot_fetch.snapshot.end_offset);
        check!(part.snapshot_id.epoch == snapshot_fetch.snapshot.epoch);
        check!(part.size == snapshot_fetch.size);
        check!(part.position == 0);
        check!(page.freeze() == snapshot_fetch.first_page);
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "The named snapshot starts with the captured page"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_0(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Another end offset does not identify this snapshot"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_1(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Another snapshot epoch does not identify this snapshot"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_2(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Another topic has no metadata snapshot"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_3(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Another partition has no metadata snapshot"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_4(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Position at the end is outside the snapshot"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_5(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Negative position is outside the snapshot"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_6(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Stale leader epochs are fenced"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_7(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Future leader epochs are unknown"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_8(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Base64 cluster identity is accepted"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_9(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Hyphenated cluster identity is accepted"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_10(snapshot_fetch: SnapshotFetchScenario) {}

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_fetch_snapshot.feature",
        name = "Another cluster identity has no topic rows"
    )]
    #[tokio::test]
    async fn snapshot_fetch_case_11(snapshot_fetch: SnapshotFetchScenario) {}

    /// A broker-only observer keeps no metadata log, so it answers without
    /// asking an engine.
    #[test]
    fn a_node_without_a_metadata_log_has_no_snapshot_and_no_other_partition() {
        let mut req = request(
            ReqSnapshotId {
                end_offset: 7,
                epoch: 1,
                ..Default::default()
            },
            1,
            0,
        );
        req.topics.push(ReqTopic {
            name: "not-metadata".into(),
            partitions: vec![ReqPartition {
                partition: 0,
                ..Default::default()
            }],
            ..Default::default()
        });

        let resp = without_metadata_log(&req);

        let expected = FetchSnapshotResponse {
            topics: vec![
                TopicSnapshot {
                    name: CLUSTER_METADATA_TOPIC.into(),
                    partitions: vec![PartitionSnapshot {
                        index: 0,
                        error_code: codes::SNAPSHOT_NOT_FOUND,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                TopicSnapshot {
                    name: "not-metadata".into(),
                    partitions: vec![PartitionSnapshot {
                        index: 0,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(resp == expected);
    }
}
