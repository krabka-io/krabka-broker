//! `OffsetForLeaderEpoch` (`api_key=23`). For each requested (topic,
//! partition, `leader_epoch`), this handler returns the `end_offset` of that
//! epoch. That offset is the first offset of the *next* epoch, and it is the
//! truncation point a follower should use when it recovers from
//! `FENCED_LEADER_EPOCH`.
//!
//! Protocol:
//! - `requested_epoch > current_leader_epoch` → `UNKNOWN_LEADER_EPOCH`
//! - `requested_epoch == current_leader_epoch` → `end_offset = log_end_offset`
//! - `requested_epoch < current_leader_epoch` → `end_offset` from the
//!   checkpoint, or `-1` (`UNDEFINED_OFFSET`) when the checkpoint has no
//!   entry.
//!
//! Reference: KIP-101 (Alter Replication Protocol to use Leader Epoch
//! rather than High Watermark for Truncation).

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

use crate::{broker::Broker, codes, error::BrokerError};

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
        // Kafka checks `ClusterAction` on `Cluster("kafka-cluster")` once as
        // a shortcut: an Allow there authorizes every topic in the request,
        // with no further lookup. This is what lets a follower broker (whose
        // principal typically holds `ClusterAction` but no topic ACLs) fetch
        // leader-epoch info from other brokers. A Deny falls back to
        // per-topic `Describe` on `Topic(name)`; a denied topic gets
        // `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row it
        // requested, with `leader_epoch` and `end_offset` both `-1` (Kafka's
        // schema default for an unauthorized row), and authorized topics
        // proceed unchanged.
        let acl_image = broker.controller.current_image();
        let cluster_action_allowed = !crate::handlers::cluster_action_denied(
            broker.config.authorizer.as_ref(),
            &acl_image,
            ctx,
        );

        let mut authorized_out: Vec<OffsetForLeaderTopicResult> =
            Vec::with_capacity(req.topics.len());
        let mut unauthorized_out: Vec<OffsetForLeaderTopicResult> = Vec::new();

        for topic in req.topics {
            if !cluster_action_allowed
                && crate::handlers::acl_denied(
                    broker.config.authorizer.as_ref(),
                    &acl_image,
                    ctx,
                    ResourceType::Topic,
                    &topic.topic,
                    AclOperation::Describe,
                )
            {
                let parts_out = topic
                    .partitions
                    .iter()
                    .map(|part| EpochEndOffset {
                        partition: part.partition,
                        leader_epoch: -1,
                        end_offset: -1,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        ..Default::default()
                    })
                    .collect();
                unauthorized_out.push(OffsetForLeaderTopicResult {
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

                let current_epoch = p.current_leader_epoch.load(Ordering::Acquire);

                if part.leader_epoch > current_epoch {
                    // Follower is ahead of us — stale metadata on our side.
                    out.error_code = codes::UNKNOWN_LEADER_EPOCH;
                    out.end_offset = -1;
                } else {
                    // Compute end_offset via the epoch checkpoint.
                    // `end_offset_for_epoch` returns log_end_offset when
                    // leader_epoch == current_epoch (the epoch is still
                    // open), or the start-offset of the next epoch (which
                    // is the truncation point) for older epochs.
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
                    // Report the leader's view of the epoch (same as
                    // requested unless our checkpoint doesn't know the
                    // exact epoch, in which case end_offset == -1).
                    out.leader_epoch = current_epoch;
                }

                parts_out.push(out);
            }

            authorized_out.push(OffsetForLeaderTopicResult {
                topic: topic.topic,
                partitions: parts_out,
                ..Default::default()
            });
        }

        // Authorized rows first, then unauthorized rows -- matches Kafka's
        // `endOffsetsForAuthorizedPartitions ++ endOffsetsForUnauthorizedPartitions`.
        authorized_out.extend(unauthorized_out);

        let resp = OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: authorized_out,
            ..Default::default()
        };
        crate::handlers::encode_response(&resp, version)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_protocol::owned::{
        offset_for_leader_epoch_request::{
            OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
        },
        offset_for_leader_epoch_response::{
            self, EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
        },
    };

    use super::*;
    use crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        test_support::{peer, principal, request_context, start_broker_with_authorizer_no_audit},
    };

    const VERSION: i16 = offset_for_leader_epoch_response::MAX_VERSION;

    /// A topic named `"orders"` is always `Describe`-authorized (it stands
    /// in for a topic a follower's principal has an ACL on); `"payments"`'s
    /// `Describe` and the cluster's `ClusterAction` both vary per test case.
    /// Every other request is denied.
    #[derive(Debug)]
    struct TestAuthorizer {
        cluster_action: bool,
        payments_describe: bool,
    }

    impl Authorizer for TestAuthorizer {
        fn authorize(
            &self,
            _source: &dyn crate::authorizer::AclSource,
            req: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            let allow = match (req.resource_type, req.operation) {
                (ResourceType::Cluster, AclOperation::ClusterAction) => self.cluster_action,
                (ResourceType::Topic, AclOperation::Describe) if req.resource_name == "orders" => {
                    true
                }
                (ResourceType::Topic, AclOperation::Describe)
                    if req.resource_name == "payments" =>
                {
                    self.payments_describe
                }
                _ => false,
            };
            if allow {
                AuthorizationResult::Allow
            } else {
                AuthorizationResult::Deny
            }
        }
    }

    fn request() -> OffsetForLeaderEpochRequest {
        OffsetForLeaderEpochRequest {
            topics: vec![
                OffsetForLeaderTopic {
                    topic: "orders".into(),
                    partitions: vec![OffsetForLeaderPartition {
                        partition: 0,
                        leader_epoch: 7,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                OffsetForLeaderTopic {
                    topic: "payments".into(),
                    partitions: vec![OffsetForLeaderPartition {
                        partition: 0,
                        leader_epoch: 7,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    /// The row a topic that isn't hosted anywhere on this broker gets, once
    /// it clears authorization: `UNKNOWN_TOPIC_OR_PARTITION`, with the
    /// requested `leader_epoch` echoed back (Kafka does not reset it for
    /// this error) and `end_offset = -1`.
    fn unknown_topic_row(topic: &str) -> OffsetForLeaderTopicResult {
        OffsetForLeaderTopicResult {
            topic: topic.into(),
            partitions: vec![EpochEndOffset {
                partition: 0,
                leader_epoch: 7,
                end_offset: -1,
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// The row a `Describe`-denied topic gets: only the partition index and
    /// `TOPIC_AUTHORIZATION_FAILED (29)` are meaningful, and Kafka's schema
    /// default puts `-1` in both `leader_epoch` and `end_offset` rather than
    /// echoing the request.
    fn denied_row(topic: &str) -> OffsetForLeaderTopicResult {
        OffsetForLeaderTopicResult {
            topic: topic.into(),
            partitions: vec![EpochEndOffset {
                partition: 0,
                leader_epoch: -1,
                end_offset: -1,
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// `(cluster ClusterAction, topic Describe on "payments")` ->
    /// `expected topics`, in the order the response must carry them.
    ///
    /// When `ClusterAction` is allowed, Kafka's `ClusterAction` fast path
    /// authorizes every topic without a per-topic `Describe` lookup, so
    /// `"payments"` is never denied even when its own `Describe` ACL would
    /// deny it. Only the `(deny, deny)` case denies `"payments"`, and its
    /// row is appended after the authorized `"orders"` row rather than kept
    /// in request order.
    #[tokio::test]
    async fn cluster_action_fast_path_table() {
        let cases: [(bool, bool, Vec<OffsetForLeaderTopicResult>); 4] = [
            (
                true,
                false,
                vec![unknown_topic_row("orders"), unknown_topic_row("payments")],
            ),
            (
                false,
                true,
                vec![unknown_topic_row("orders"), unknown_topic_row("payments")],
            ),
            (
                false,
                false,
                vec![unknown_topic_row("orders"), denied_row("payments")],
            ),
            (
                true,
                true,
                vec![unknown_topic_row("orders"), unknown_topic_row("payments")],
            ),
        ];

        for (cluster_action, payments_describe, expected_topics) in cases {
            let authorizer = Arc::new(TestAuthorizer {
                cluster_action,
                payments_describe,
            });
            let (broker_handle, _dir) = start_broker_with_authorizer_no_audit(authorizer).await;
            let broker = broker_handle.broker_arc_for_test();

            let p = principal("follower");
            let peer = peer();
            let ctx = request_context(&p, &peer, "follower-client");
            let req_bytes = crate::test_support::encode_request(&request(), VERSION);

            let bytes = handle(&broker, VERSION, 123, &req_bytes, &ctx).expect("handle");
            let resp: OffsetForLeaderEpochResponse =
                crate::test_support::decode_response(&bytes, VERSION);

            let expected = OffsetForLeaderEpochResponse {
                throttle_time_ms: 0,
                topics: expected_topics,
                ..Default::default()
            };
            assert!(
                resp == expected,
                "cluster_action={cluster_action} payments_describe={payments_describe}: \
                 got {resp:?}, want {expected:?}"
            );

            broker_handle.shutdown().await;
        }
    }
}
