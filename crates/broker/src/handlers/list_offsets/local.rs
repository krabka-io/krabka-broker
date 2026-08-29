//! The offsets that come from a partition's own log rather than from the
//! remote tier: the `LATEST` sentinel's answer, and the leader epoch that
//! belongs with a resolved offset.
//!
//! KFC-1 makes `LATEST` depend on the topic's [`DeliveryPolicy`], which is why
//! the sentinel needs a function of its own rather than the log end offset the
//! rest of the handler reads.

use krabka_log::DeliveryPolicy;

/// The offset the `LATEST` sentinel reports for one partition.
///
/// A topic that delivers immediately answers the local log end offset. A topic
/// that schedules delivery (KFC-1) answers its delivery watermark instead,
/// because a consumer that seeks to end takes this value as its position. The
/// log end offset would step that consumer over every record that has not come
/// due, and a classic consumer's position is one offset per partition, so those
/// records would be unreachable for it forever. The watermark instead lands it
/// on the first record that still waits, and it receives that record when the
/// record activates.
///
/// The watermark is recomputed under the log mutex rather than read from the
/// partition's lock-free mirror. The mirror is only as fresh as the last
/// scheduler sweep left it, and that scheduler is a liveness aid that may lag
/// or die without making a fetch wrong. A `LATEST` taken from the mirror could
/// therefore sit behind what a fetch at the same instant serves, and hand a
/// seek-to-end consumer records it asked to skip. The recompute answers from
/// the rule and the clock reading that the fetch cap uses.
///
/// A topic that delivers immediately never reaches the recompute, so it pays
/// no extra lock and no walk here. `None` means the topic stopped scheduling
/// delivery between the config snapshot and the recompute, where the log end
/// offset is again the right answer.
pub(super) fn latest_offset(
    partition: &crate::partition::Partition,
    policy: DeliveryPolicy,
    local_end: i64,
) -> i64 {
    if policy != DeliveryPolicy::Scheduled {
        return local_end;
    }
    partition
        .delivery
        .publish_now(&partition.log)
        .map_or(local_end, |delivery| delivery.watermark.0)
}

pub(super) fn leader_epoch_for_offset(partition: &crate::partition::Partition, offset: i64) -> i32 {
    let log = partition.log.lock().expect("log mutex poisoned");
    log.epoch_checkpoint()
        .epoch_for_offset(krabka_log::Offset(offset))
        .map_or(-1, |epoch| epoch.0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};
    use krabka_protocol::owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        list_offsets_response::ListOffsetsPartitionResponse,
    };

    use super::*;
    use crate::{
        broker::Broker,
        codes,
        delivery::test_support::{BOUND_MS, NOW_MS},
        handlers::list_offsets::{
            handle,
            sentinels::{
                EARLIEST_LOCAL_TIMESTAMP, EARLIEST_PENDING_UPLOAD_TIMESTAMP, EARLIEST_TIMESTAMP,
                LATEST_TIERED_TIMESTAMP, LATEST_TIMESTAMP, MAX_TIMESTAMP, UNKNOWN_TIMESTAMP,
            },
            test_support::{decode_response, encode_request, test_context},
        },
        test_support::{peer, principal, start_broker_with_authorizer_no_audit as start_broker},
    };

    // Activation time of a batch that has long since come due.
    const DELIVERED_MS: i64 = 1_700_000_000_000;
    // Activation time of a batch that still waits. It sits far enough ahead
    // that every clock this test can read calls it pending, so the schedule
    // holds without a mock timeline.
    const PENDING_MS: i64 = 4_100_000_000_000;
    // One batch that has come due and one that has not, two records each. The
    // log end offset is therefore 4, and a scheduled topic's delivery
    // watermark is 2.
    const ACTIVATIONS: [i64; 2] = [DELIVERED_MS, PENDING_MS];

    // Put a partition holding both activations under `topic` on this broker.
    fn register_delivery_partition(
        broker: &Broker,
        logs: &tempfile::TempDir,
        topic: &str,
        policy: DeliveryPolicy,
    ) {
        let clock: Arc<dyn qubit_clock::Clock> = Arc::new(qubit_clock::SystemClock::new());
        let partition = crate::delivery::test_support::scheduled_partition(
            logs,
            topic,
            policy,
            &ACTIVATIONS,
            broker.config.node_id.get(),
            &clock,
        );
        crate::delivery::test_support::register(&broker.partitions, &partition);
    }

    async fn list_partition(
        broker: &Broker,
        topic: &str,
        timestamp: i64,
        ctx: &crate::handlers::RequestContext<'_>,
    ) -> ListOffsetsPartitionResponse {
        let version = krabka_protocol::owned::list_offsets_response::MAX_VERSION;
        let req = encode_request(
            &ListOffsetsRequest {
                replica_id: -1,
                topics: vec![ListOffsetsTopic {
                    name: topic.to_string(),
                    partitions: vec![ListOffsetsPartition {
                        partition_index: 0,
                        current_leader_epoch: -1,
                        timestamp,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            },
            version,
        );
        let bytes = handle(broker, version, 123, &req, ctx)
            .await
            .expect("handle");
        let mut response = decode_response(&bytes, version);
        response.topics.remove(0).partitions.remove(0)
    }

    // The whole LATEST row for a healthy partition 0.
    fn latest_row(offset: i64) -> ListOffsetsPartitionResponse {
        ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: codes::NONE,
            timestamp: UNKNOWN_TIMESTAMP,
            offset,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn scheduled_latest_answers_the_delivery_watermark_and_leaves_the_rest_alone() {
        const IMMEDIATE: &str = "list-offsets-delivery-immediate";
        const SCHEDULED: &str = "list-offsets-delivery-scheduled";

        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let logs = tempfile::tempdir().expect("log root");
        register_delivery_partition(&broker, &logs, IMMEDIATE, DeliveryPolicy::Immediate);
        register_delivery_partition(&broker, &logs, SCHEDULED, DeliveryPolicy::Scheduled);
        let admin = principal("admin");
        let peer = peer();
        let ctx = test_context(&admin, &peer);

        // Both topics hold the same records, so every sentinel but LATEST
        // answers the same on both. KFC-1 moves where a seek to end lands and
        // nothing else.
        for timestamp in [
            EARLIEST_TIMESTAMP,
            MAX_TIMESTAMP,
            EARLIEST_LOCAL_TIMESTAMP,
            LATEST_TIERED_TIMESTAMP,
            EARLIEST_PENDING_UPLOAD_TIMESTAMP,
            DELIVERED_MS,
            PENDING_MS,
        ] {
            let immediate = list_partition(&broker, IMMEDIATE, timestamp, &ctx).await;
            let scheduled = list_partition(&broker, SCHEDULED, timestamp, &ctx).await;
            check!(scheduled == immediate, "timestamp {timestamp}");
        }

        // LATEST is the log end offset on a topic that delivers immediately,
        // and the first pending offset on one that schedules delivery. A
        // consumer that seeks to end therefore lands on the waiting batch
        // instead of stepping over it.
        check!(list_partition(&broker, IMMEDIATE, LATEST_TIMESTAMP, &ctx).await == latest_row(4));
        check!(list_partition(&broker, SCHEDULED, LATEST_TIMESTAMP, &ctx).await == latest_row(2));

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn scheduled_latest_recomputes_the_watermark_instead_of_reading_the_mirror() {
        const TOPIC: &str = "list-offsets-delivery-recompute";
        // Another broker leads this partition, so the broker-wide delivery
        // scheduler passes over it. The mirror then moves only when the
        // request path itself publishes a recompute, which is what this test
        // is about.
        const OTHER_BROKER: u64 = 7;

        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        assert!(broker.config.node_id.get() != OTHER_BROKER);
        let logs = tempfile::tempdir().expect("log root");
        let time = qubit_clock::MockTime::at(
            qubit_clock::DateTime::from_timestamp_millis(NOW_MS).expect("a representable instant"),
        );
        let clock: Arc<dyn qubit_clock::Clock> = Arc::new(time.clock());
        let partition = crate::delivery::test_support::scheduled_partition(
            &logs,
            TOPIC,
            DeliveryPolicy::Scheduled,
            &[NOW_MS - 60_000, NOW_MS + 10_000],
            OTHER_BROKER,
            &clock,
        );
        crate::delivery::test_support::register(&broker.partitions, &partition);
        let admin = principal("admin");
        let peer = peer();
        let ctx = test_context(&admin, &peer);

        check!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(2));

        // Cross the activation boundary. The batch comes due on the clock
        // alone: no append and no scheduler sweep publishes the new watermark,
        // so the mirror still reports the old one.
        time.advance(std::time::Duration::from_millis(
            u64::try_from(10_000 + BOUND_MS).expect("a positive delay"),
        ));
        check!(partition.delivery_watermark() == krabka_log::Offset(2));

        check!(list_partition(&broker, TOPIC, LATEST_TIMESTAMP, &ctx).await == latest_row(4));

        broker_handle.shutdown().await;
    }
}
