//! `TriggerBarrier`, api key 1012.
//!
//! The handler runs one injection for the named group and returns the cut that
//! the injection published. The coordinator consumes an epoch whether the cut
//! reaches every partition or not, so a partial cut still carries an epoch and
//! names the partitions it missed.
//!
//! The coordinator runs one injection per group at a time. A second request
//! that arrives while the first still runs gets
//! `BARRIER_INJECTION_IN_PROGRESS` (1000), and the caller should retry after a
//! brief back-off.
//!
//! # The `timeout_ms` field
//!
//! The coordinator applies the broker-wide `barrier.injection.timeout` to
//! every injection, and `timeout_ms` does not shorten it. The handler cannot
//! bound the wait on its own: it would have to drop the injection future, and
//! that leaves the epoch's injection-start record with no cut record after it.
//! Recovery then finalises that epoch on the next coordinator.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On a deny the response
//! carries `CLUSTER_AUTHORIZATION_FAILED` (31).

use bytes::Bytes;
use crabka_protocol::{
    Decode,
    krabka::barrier::{CUT_STATUS_PARTIAL, TriggerBarrierRequest, TriggerBarrierResponse},
};

use crate::{
    barrier::{
        coordinator::InjectionOutcome,
        error::BarrierError,
        handlers::{NO_EPOCH, cut_missing, cut_topics, error_code, error_text},
    },
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{RequestContext, cluster_alter_denied, encode_response},
};

#[tracing::instrument(
    name = "handle_trigger_barrier",
    level = "info",
    skip_all,
    fields(api = "TriggerBarrier"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur = req_bytes;
    let req = TriggerBarrierRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();
    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return encode_response(&denied_response("trigger-barrier denied"), version);
    }

    let resp = match broker
        .barrier_coordinator
        .trigger_injection(&req.group)
        .await
    {
        Ok(outcome) => cut_response(&outcome),
        Err(error) => error_response(&error),
    };
    encode_response(&resp, version)
}

/// The response that carries the cut of a finished injection.
fn cut_response(outcome: &InjectionOutcome) -> TriggerBarrierResponse {
    TriggerBarrierResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        epoch: outcome.epoch,
        status: outcome.cut.status.code(),
        topics: cut_topics(&outcome.cut),
        missing: cut_missing(&outcome.cut),
        ..TriggerBarrierResponse::default()
    }
}

/// The response of an injection that the coordinator refused.
///
/// It carries no cut, so the epoch is [`NO_EPOCH`] and the status is partial.
fn error_response(error: &BarrierError) -> TriggerBarrierResponse {
    refused(error_code(error), Some(error_text(error)))
}

/// The response of a request that the authorizer denied.
fn denied_response(reason: &str) -> TriggerBarrierResponse {
    refused(codes::CLUSTER_AUTHORIZATION_FAILED, Some(reason.to_owned()))
}

/// The shared shape of every response that carries no cut.
fn refused(error_code: i16, error_message: Option<String>) -> TriggerBarrierResponse {
    TriggerBarrierResponse {
        throttle_time_ms: 0,
        error_code,
        error_message,
        epoch: NO_EPOCH,
        status: CUT_STATUS_PARTIAL,
        topics: Vec::new(),
        missing: Vec::new(),
        ..TriggerBarrierResponse::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use crabka_ids::PartitionIndex;
    use crabka_log::Offset;
    use crabka_protocol::krabka::barrier::{
        BarrierCutPartition, BarrierCutTopic, BarrierMissingPartition, CUT_STATUS_COMPLETE,
    };

    use super::*;
    use crate::barrier::persistence::{
        CutStatus, CutValue, MissingPartition, PartitionOffset, TopicOffsets,
    };

    fn outcome(status: CutStatus, missing: Vec<MissingPartition>) -> InjectionOutcome {
        InjectionOutcome {
            epoch: 9,
            cut: CutValue {
                triggered_at: 1_724_500_000_000,
                completed_at: 1_724_500_000_030,
                status,
                topics: vec![TopicOffsets {
                    topic: "orders".to_owned(),
                    partitions: vec![PartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(41),
                    }],
                }],
                missing,
            },
        }
    }

    #[test]
    fn a_complete_cut_returns_its_epoch_and_every_offset() {
        let expected = TriggerBarrierResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            epoch: 9,
            status: CUT_STATUS_COMPLETE,
            topics: vec![BarrierCutTopic {
                topic: "orders".to_owned(),
                partitions: vec![BarrierCutPartition {
                    partition: 0,
                    offset: 41,
                    ..BarrierCutPartition::default()
                }],
                ..BarrierCutTopic::default()
            }],
            missing: Vec::new(),
            ..TriggerBarrierResponse::default()
        };
        check!(cut_response(&outcome(CutStatus::Complete, Vec::new())) == expected);
    }

    #[test]
    fn a_partial_cut_names_the_partitions_that_took_no_marker() {
        let missing = vec![MissingPartition {
            topic: "payments".to_owned(),
            partition: PartitionIndex(2),
        }];
        let resp = cut_response(&outcome(CutStatus::Partial, missing));
        check!(resp.status == CUT_STATUS_PARTIAL);
        check!(
            resp.missing
                == vec![BarrierMissingPartition {
                    topic: "payments".to_owned(),
                    partition: 2,
                    ..BarrierMissingPartition::default()
                }]
        );
        check!(resp.epoch == 9);
    }

    #[test]
    fn a_refused_injection_returns_the_code_and_no_cut() {
        let cases: Vec<(BarrierError, i16)> = vec![
            (
                BarrierError::InjectionInProgress {
                    group: "orders-cut".to_owned(),
                },
                codes::BARRIER_INJECTION_IN_PROGRESS,
            ),
            (
                BarrierError::NotCoordinator {
                    group: "orders-cut".to_owned(),
                },
                codes::NOT_COORDINATOR,
            ),
            (
                BarrierError::UnknownGroup {
                    group: "orders-cut".to_owned(),
                },
                codes::RESOURCE_NOT_FOUND,
            ),
        ];
        for (error, code) in cases {
            let expected = TriggerBarrierResponse {
                throttle_time_ms: 0,
                error_code: code,
                error_message: Some(error.to_string()),
                epoch: NO_EPOCH,
                status: CUT_STATUS_PARTIAL,
                topics: Vec::new(),
                missing: Vec::new(),
                ..TriggerBarrierResponse::default()
            };
            check!(error_response(&error) == expected, "{error}");
        }
    }

    #[test]
    fn a_denied_request_returns_cluster_authorization_failed_and_no_cut() {
        let expected = TriggerBarrierResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("trigger-barrier denied".to_owned()),
            epoch: NO_EPOCH,
            status: CUT_STATUS_PARTIAL,
            topics: Vec::new(),
            missing: Vec::new(),
            ..TriggerBarrierResponse::default()
        };
        check!(denied_response("trigger-barrier denied") == expected);
    }
}
