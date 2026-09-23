//! `OffsetForLeaderEpoch` (`api_key=23`). For each requested (topic,
//! partition, `leader_epoch`), this handler returns the `end_offset` of that
//! epoch. That offset is the first offset of the *next* epoch, and it is the
//! truncation point a follower should use when it recovers from
//! `FENCED_LEADER_EPOCH`.
//!
//! Protocol:
//! - `requested_epoch == current_leader_epoch` → `end_offset = log_end_offset`
//! - `requested_epoch != current_leader_epoch` (above or below) →
//!   `end_offset` from the checkpoint, or `-1` (`UNDEFINED_OFFSET`) with
//!   `leader_epoch = -1` (`UNDEFINED_EPOCH`) when the checkpoint has no
//!   entry for it. No error either way: `LeaderEpochFileCache.endOffsetFor`
//!   (`storage/.../LeaderEpochFileCache.java:299-303`) answers a requested
//!   epoch above everything the same way it answers an untracked one below
//!   it, and only the hosting checks above ever set an error code here.
//!
//! Reference: KIP-101 (Alter Replication Protocol to use Leader Epoch
//! rather than High Watermark for Truncation).
//!
//! KIP-320 layers two more checks on top, in the order Kafka's
//! `ReplicaManager.lastOffsetForLeaderEpoch` -> `Partition.getLocalLog`
//! applies them, both ahead of the KIP-101 domain logic above:
//!
//! 1. The `current_leader_epoch` field (decodes from v2 up, this API's
//!    `MIN_VERSION`) is fenced against the partition's live epoch via
//!    `Partition::list_offsets_leader_epoch_fence`. `ReplicaManager` builds
//!    this field's `Optional` the same way `RequestUtils.getLeaderEpoch`
//!    does for `ListOffsets` -- only the exact `NO_PARTITION_LEADER_EPOCH`
//!    sentinel (`-1`) counts as "no epoch asserted", so every other
//!    negative value is fenced too. That is *not* `Fetch`'s rule, which
//!    treats every negative epoch as unasserted; the two request schemas
//!    just happen to share a field name.
//! 2. Only this node's locally installed leader may answer, matching
//!    `fetchOnlyFromLeader = true` in `getLocalLog`. Checked against both
//!    the metadata image's `record.leader` and the partition's installed
//!    `current_leader` atomic, mirroring
//!    `crate::handlers::fetch::plan::leader_refusal`: the image can name a
//!    new leader before the supervisor installs the new role, and the
//!    installed role can lag the other way while a promotion prepares the
//!    log.

use std::sync::atomic::Ordering;

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        offset_for_leader_epoch_request::OffsetForLeaderEpochRequest,
        offset_for_leader_epoch_response::{
            EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
        },
    },
};

use crate::{broker::Broker, codes, error::BrokerError, partition::Partition};

/// The two-state leader check Kafka's `ReplicaManager.lastOffsetForLeaderEpoch`
/// applies via `Partition.getLocalLog(currentLeaderEpoch, fetchOnlyFromLeader
/// = true)`. Mirrors `crate::handlers::fetch::plan::leader_refusal`: checked
/// against both the metadata image's `record.leader` and the partition's
/// installed `current_leader` atomic, because the image can name a new leader
/// before the supervisor installs the new role, and the installed role can
/// lag the other way while a promotion prepares the log.
fn not_leader(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
    partition: &Partition,
    node_id: krabka_metadata::NodeId,
) -> bool {
    let installed = partition.current_leader.load(Ordering::Acquire) == node_id.0;
    let committed_elsewhere = image
        .partition(topic, partition_index)
        .is_some_and(|record| record.leader != node_id);
    !installed || committed_elsewhere
}

#[tracing::instrument(
    name = "handle_offset_for_leader_epoch",
    level = "info",
    skip_all,
    fields(api = "OffsetForLeaderEpoch", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let partitions = broker.partitions.clone();
    // Test-only: count served OFLE requests so the KIP-320 proactive-validation
    // integration test can prove the consumer's validate pass issued an OFLE
    // RPC (vs. the reactive in-band fetch paths, which issue none).
    #[cfg(any(test, feature = "test-helpers"))]
    let ofle_counter = broker.offset_for_leader_epoch_requests.clone();
    {
        #[cfg(any(test, feature = "test-helpers"))]
        ofle_counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        let mut cur: &[u8] = req_bytes;
        let req = OffsetForLeaderEpochRequest::decode(&mut cur, version)?;

        // ── ACL preamble ────────────────────────────────────────────
        // Per-topic `Describe` on `Topic(name)`. A denied topic gets
        // `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row it
        // requested; authorized topics proceed unchanged.
        let acl_image = broker.controller.current_image();

        let mut topics_out: Vec<OffsetForLeaderTopicResult> = Vec::with_capacity(req.topics.len());

        for topic in req.topics {
            if crate::handlers::acl_denied(
                broker.config.authorizer.as_ref(),
                &acl_image,
                ctx,
                ResourceType::Topic,
                &topic.topic,
                AclOperation::Describe,
            ) {
                let parts_out = topic
                    .partitions
                    .iter()
                    .map(|part| EpochEndOffset {
                        partition: part.partition,
                        leader_epoch: part.leader_epoch,
                        end_offset: -1,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        ..Default::default()
                    })
                    .collect();
                topics_out.push(OffsetForLeaderTopicResult {
                    topic: topic.topic,
                    partitions: parts_out,
                    ..Default::default()
                });
                continue;
            }
            let mut parts_out: Vec<EpochEndOffset> = Vec::with_capacity(topic.partitions.len());

            for part in &topic.partitions {
                let mut out = EpochEndOffset {
                    partition: part.partition,
                    leader_epoch: part.leader_epoch,
                    end_offset: -1,
                    ..Default::default()
                };

                let Some(p) =
                    partitions.get(&topic.topic, krabka_ids::PartitionIndex(part.partition))
                else {
                    out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
                    parts_out.push(out);
                    continue;
                };

                // KIP-320 leader-epoch fence, ahead of the leader-only gate and
                // the KIP-101 domain logic below, exactly as
                // `Partition.getLocalLog` runs `checkCurrentLeaderEpoch` before
                // its leader check. `list_offsets_leader_epoch_fence` reads
                // only `-1` as "no epoch asserted", the rule
                // `ReplicaManager.lastOffsetForLeaderEpoch` applies here too.
                if let Some((error_code, _)) =
                    p.list_offsets_leader_epoch_fence(part.current_leader_epoch)
                {
                    out.error_code = error_code;
                    out.leader_epoch = -1;
                    parts_out.push(out);
                    continue;
                }

                if not_leader(
                    &acl_image,
                    &topic.topic,
                    part.partition,
                    &p,
                    broker.config.node_id,
                ) {
                    out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                    out.leader_epoch = -1;
                    parts_out.push(out);
                    continue;
                }

                // Compute end_offset via the epoch checkpoint.
                // `end_offset_for_epoch` returns log_end_offset when
                // leader_epoch == the partition's current epoch (the epoch is
                // still open), the start-offset of the next epoch (the
                // truncation point) for an older epoch the checkpoint
                // tracked, or the `UNDEFINED_OFFSET` sentinel (`-1`) when the
                // checkpoint has no entry for the requested epoch at all --
                // which includes a requested epoch above the current one,
                // exactly like `LeaderEpochFileCache.endOffsetFor` answering
                // an epoch past the latest tracked one. No error either way:
                // Kafka's hosting checks above are what set an error code,
                // not this KIP-101 lookup.
                let log = p.log.lock().expect("log mutex poisoned");
                let leo = log.log_end_offset();
                // Wrap the raw wire `requested_epoch` for the log-crate seam.
                let end_offset = log
                    .epoch_checkpoint()
                    .end_offset_for_epoch(krabka_log::LeaderEpoch(part.leader_epoch), leo);
                drop(log);
                out.error_code = codes::NONE;
                // Unwrap the log-layer `Offset` into the wire `i64` field.
                out.end_offset = end_offset.0;
                // Report the leader's view of the epoch: the requested one
                // when the checkpoint found it, or `UNDEFINED_EPOCH` (-1)
                // alongside the `UNDEFINED_OFFSET` end_offset when it did not.
                out.leader_epoch = if end_offset.0 == -1 {
                    -1
                } else {
                    part.leader_epoch
                };

                parts_out.push(out);
            }

            topics_out.push(OffsetForLeaderTopicResult {
                topic: topic.topic,
                partitions: parts_out,
                ..Default::default()
            });
        }

        let resp = OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: topics_out,
            ..Default::default()
        };
        crate::handlers::encode_response(&resp, version)
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BytesMut;
    use krabka_protocol::Encode;

    use super::*;

    #[test]
    fn topic_describe_denied_yields_topic_authorization_failed_rows() {
        use krabka_protocol::owned::offset_for_leader_epoch_response::{
            self, EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
        };

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = crate::handlers::RequestContext {
            principal: &principal,
            peer: &peer,
            client_id: "client-a",
            connection_id: "connection-a",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
            throttle: crate::quota::ThrottleSlot::default(),
        };
        assert!(crate::handlers::acl_denied(
            &authorizer,
            &image,
            &ctx,
            ResourceType::Topic,
            "t",
            AclOperation::Describe,
        ));

        let resp = OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: vec![OffsetForLeaderTopicResult {
                topic: "t".into(),
                partitions: vec![EpochEndOffset {
                    partition: 0,
                    leader_epoch: 0,
                    end_offset: -1,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let version = offset_for_leader_epoch_response::MAX_VERSION;
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version).expect("encode");
        let mut cur: &[u8] = &buf;
        let decoded = OffsetForLeaderEpochResponse::decode(&mut cur, version).unwrap();
        assert!(decoded.topics[0].partitions[0].error_code == codes::TOPIC_AUTHORIZATION_FAILED);
    }

    /// The leader check needs this node in the installed local role, and no
    /// other leader in the committed image. Mirrors
    /// `fetch::plan::a_read_needs_the_installed_role_and_the_image_to_name_this_node`.
    /// The third case is the installed-role-vs-image race: the image already
    /// commits this node as leader, but the supervisor has not installed the
    /// role locally yet, so the request must still be refused.
    #[tokio::test]
    async fn leader_check_needs_the_installed_role_and_the_image_to_name_this_node() {
        use krabka_ids::PartitionIndex;
        use krabka_log::{Log, LogConfig};
        use krabka_metadata::{MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};

        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: uuid::Uuid::nil(),
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "orders".into(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));

        let dir = tempfile::tempdir().expect("tempdir");
        let partition = crate::broker::spawn_partition(
            "orders".to_string(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            Log::open(dir.path(), LogConfig::default()).expect("open partition log"),
            crate::log_dir_status::LogDirRegistry::default(),
            std::sync::Arc::new(crate::producer_state::ProducerState::new()),
            false,
        );

        // (this node, installed leader, refused)
        let cases = [
            ("installed and committed", 1, 1, false),
            ("installed, committed to another node", 2, 2, true),
            (
                "committed, not installed yet (installed/image race)",
                1,
                2,
                true,
            ),
        ];
        for (name, node, installed, want_refused) in cases {
            partition
                .install_replication_target(None, installed, 0)
                .await;
            let got = not_leader(
                &image,
                "orders",
                0,
                &partition,
                krabka_metadata::NodeId(node),
            );
            assert!(got == want_refused, "{name}");
        }
    }

    /// Create `topic` with replicas 1 and 2, led by `leader`, wait until this
    /// broker (node 1) holds the partition in that role, and append two
    /// records under `leader_epoch` so `end_offset_for_epoch` has an entry
    /// for that epoch to answer -- the leader-epoch checkpoint only learns
    /// an epoch from a batch actually carrying it (`Log::append`), so the
    /// appended `partition_leader_epoch` has to agree with the partition's
    /// `current_leader_epoch`, which this also sets via
    /// `test_set_leader_epoch`.
    async fn seeded_topic(
        broker_handle: &crate::broker::BrokerHandle,
        topic: &str,
        topic_id: u128,
        leader: u64,
        leader_epoch: i32,
    ) -> std::sync::Arc<Partition> {
        use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};

        broker_handle
            .submit_metadata_record_for_test(MetadataRecord::V1Topic(TopicRecord {
                name: topic.to_owned(),
                topic_id: uuid::Uuid::from_u128(topic_id),
                partitions: 1,
                replication_factor: 2,
            }))
            .await
            .expect("submit topic record");
        broker_handle
            .submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_owned(),
                partition: 0,
                leader: krabka_audit::NodeId(leader),
                replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
                isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: Vec::new(),
                removing_replicas: Vec::new(),
                directories: vec![uuid::Uuid::nil(); 2],
                partition_epoch: 0,
            }))
            .await
            .expect("submit partition record");

        let shared = broker_handle.broker_arc_for_test();
        let partition = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(partition) = shared.partitions.get(topic, krabka_ids::PartitionIndex(0))
                    && partition.current_leader.load(Ordering::Acquire) == leader
                {
                    return partition;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the broker holds the partition in its role");

        partition.test_set_leader_epoch(leader_epoch);

        let mut batch = krabka_protocol::records::RecordBatch {
            last_offset_delta: 1,
            partition_leader_epoch: leader_epoch,
            records: [&b"first"[..], &b"second"[..]]
                .iter()
                .zip(0..)
                .map(|(value, offset_delta)| krabka_protocol::records::Record {
                    offset_delta,
                    value: Some(bytes::Bytes::from_static(value)),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        partition
            .log
            .lock()
            .expect("partition log lock")
            .append(&mut batch)
            .expect("append the records");
        partition
    }

    fn ofle_request(
        topic: &str,
        leader_epoch: i32,
        current_leader_epoch: i32,
    ) -> OffsetForLeaderEpochRequest {
        use krabka_protocol::owned::offset_for_leader_epoch_request::{
            OffsetForLeaderPartition, OffsetForLeaderTopic,
        };

        OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderTopic {
                topic: topic.to_owned(),
                partitions: vec![OffsetForLeaderPartition {
                    partition: 0,
                    current_leader_epoch,
                    leader_epoch,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn ofle(
        broker_handle: &crate::broker::BrokerHandle,
        version: i16,
        request: &OffsetForLeaderEpochRequest,
    ) -> EpochEndOffset {
        let shared = broker_handle.broker_arc_for_test();
        let user = crate::test_support::principal("client");
        let address = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&user, &address, "ofle-client");
        let request_bytes = crate::test_support::encode_request(request, version);
        let wire = handle(&shared, version, 9, &request_bytes, &ctx).expect("handle ofle");
        let decoded: OffsetForLeaderEpochResponse =
            crate::test_support::decode_response(&wire, version);
        decoded
            .topics
            .into_iter()
            .next()
            .expect("one topic in response")
            .partitions
            .into_iter()
            .next()
            .expect("one partition in response")
    }

    /// KIP-320's leader-epoch fence and leader-only gate, table-driven across
    /// the scenarios the two checks must cover together: a leader answers
    /// normally, a follower is refused, an unknown partition short-circuits
    /// before either check runs, a stale or future `current_leader_epoch` is
    /// fenced ahead of the leadership check (on both a leader and a follower
    /// partition, to prove the fence really does run first), and a negative
    /// `current_leader_epoch` other than the `-1` sentinel is fenced too --
    /// `ReplicaManager.lastOffsetForLeaderEpoch` shares `ListOffsets`'
    /// stricter sentinel rule, not `Fetch`'s "every negative epoch is
    /// unasserted" one.
    #[tokio::test]
    async fn kip_320_fence_and_leader_gate_answer_offset_for_leader_epoch_rows() {
        use krabka_protocol::owned::offset_for_leader_epoch_request;
        use offset_for_leader_epoch_request::MAX_VERSION as VERSION;

        let (broker, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
        })
        .await;

        let leader_topic = seeded_topic(&broker, "ofle-leader", 1, 1, 0).await;
        seeded_topic(&broker, "ofle-follower", 2, 2, 0).await;
        seeded_topic(&broker, "ofle-epoch", 3, 1, 3).await;

        let refused = |error_code| EpochEndOffset {
            partition: 0,
            error_code,
            leader_epoch: -1,
            end_offset: -1,
            ..Default::default()
        };
        let resolved = |leader_epoch, end_offset| EpochEndOffset {
            partition: 0,
            error_code: codes::NONE,
            leader_epoch,
            end_offset,
            ..Default::default()
        };

        let cases = [
            (
                "leader answers correctly",
                "ofle-leader",
                0,
                -1,
                resolved(0, 2),
            ),
            (
                "a requested leader_epoch above the checkpoint's latest is UNDEFINED_EPOCH/-1, not an error -- \
                 LeaderEpochFileCache.endOffsetFor answers it exactly like an untracked older epoch",
                "ofle-leader",
                5,
                -1,
                resolved(-1, -1),
            ),
            (
                "follower is refused",
                "ofle-follower",
                0,
                -1,
                refused(codes::NOT_LEADER_OR_FOLLOWER),
            ),
            (
                "stale current_leader_epoch is fenced ahead of the leader check",
                "ofle-epoch",
                3,
                2,
                refused(codes::FENCED_LEADER_EPOCH),
            ),
            (
                "future current_leader_epoch is fenced ahead of the leader check",
                "ofle-epoch",
                3,
                4,
                refused(codes::UNKNOWN_LEADER_EPOCH),
            ),
            (
                "a negative current_leader_epoch that is not the -1 sentinel is fenced",
                "ofle-epoch",
                3,
                -2,
                refused(codes::FENCED_LEADER_EPOCH),
            ),
            (
                "the -1 sentinel passes the fence and reaches the KIP-101 domain logic",
                "ofle-epoch",
                3,
                -1,
                resolved(3, 2),
            ),
            (
                "the fence outranks the leader-only gate on a follower partition too",
                "ofle-follower",
                0,
                -2,
                refused(codes::FENCED_LEADER_EPOCH),
            ),
        ];
        for (name, topic, leader_epoch, current_leader_epoch, want) in cases {
            let got = ofle(
                &broker,
                VERSION,
                &ofle_request(topic, leader_epoch, current_leader_epoch),
            );
            assert!(got == want, "{name}");
        }

        // Unknown partition: the fence and leader-only gate never run, since
        // the partition lookup answers first. Its wire default echoes the
        // requested `leader_epoch` back (unrelated to KIP-320), unlike every
        // fenced or refused row above.
        let unknown = ofle(&broker, VERSION, &ofle_request("ofle-missing", 5, -1));
        assert!(
            unknown
                == EpochEndOffset {
                    partition: 0,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    leader_epoch: 5,
                    end_offset: -1,
                    ..Default::default()
                }
        );

        drop(leader_topic);
        broker.shutdown().await;
    }
}
